package anilist

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var Endpoint = "https://graphql.anilist.co"

const (
	defaultRequestsPerMinute = 30
	rateLimitWindow          = time.Minute
	rateLimitPadding         = 100 * time.Millisecond

	defaultClientTimeout = 15 * time.Second
	listEntriesTimeout   = 120 * time.Second
)

var sharedRateLimiter = newRateLimiter(defaultRequestsPerMinute)

type Client struct {
	HTTPClient  *http.Client
	AccessToken string
	Endpoint    string
	limiter     *rateLimiter
}

type Error struct {
	Status     int
	Message    string
	RetryAfter time.Duration
	// InvalidToken is set when AniList rejected the access token. AniList
	// answers a revoked or expired token with HTTP 400 "Invalid token", not
	// 401, so the status alone does not identify a credential failure.
	InvalidToken bool
}

// MismatchError reports a mapping that contradicts AniList's own data, such
// as progress past the episode count. Retrying cannot fix it.
type MismatchError struct {
	Message string
}

func (e *MismatchError) Error() string { return e.Message }

func (e *Error) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return fmt.Sprintf("AniList HTTP %d", e.Status)
}

type graphQLError struct {
	Message string `json:"message"`
}

type mediaListEntry struct {
	ID       int     `json:"id"`
	Progress int     `json:"progress"`
	Status   string  `json:"status"`
	Repeat   int     `json:"repeat"`
	Score    float64 `json:"score"`
}

type mediaResponse struct {
	Data struct {
		Media struct {
			Episodes       *int            `json:"episodes"`
			MediaListEntry *mediaListEntry `json:"mediaListEntry"`
		} `json:"Media"`
	} `json:"data"`
	Errors []graphQLError `json:"errors"`
}

const mediaQuery = `query ($id: Int!) { Media(id: $id, type: ANIME) { episodes mediaListEntry { id progress status repeat score(format: POINT_100) } } }`

type ListEntry struct {
	ID        int
	MediaID   int
	Status    string
	Progress  int
	UpdatedAt int
	// Score is the entry's score on AniList's 100-point scale; 0 is unscored.
	Score float64
	Media struct {
		ID       int
		Format   string
		Episodes *int
		Title    struct {
			Romaji  string `json:"romaji"`
			English string `json:"english"`
			Native  string `json:"native"`
		} `json:"title"`
		StartDate struct {
			Year int `json:"year"`
		} `json:"startDate"`
	} `json:"media"`
}

func (e ListEntry) PreferredTitle() string {
	for _, title := range []string{e.Media.Title.English, e.Media.Title.Romaji, e.Media.Title.Native} {
		if title != "" {
			return title
		}
	}
	return ""
}

func (e ListEntry) CompletedProgress() int {
	progress := e.Progress
	if e.Status == "COMPLETED" && e.Media.Episodes != nil && *e.Media.Episodes > progress {
		progress = *e.Media.Episodes
	}
	if e.Status == "COMPLETED" && e.Media.Format == "MOVIE" && progress < 1 {
		progress = 1
	}
	return progress
}

func NewClient(token string, httpClient *http.Client) *Client {
	limiter := &rateLimiter{}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultClientTimeout}
		limiter = sharedRateLimiter
	}
	return &Client{HTTPClient: httpClient, AccessToken: token, Endpoint: Endpoint, limiter: limiter}
}

type rateLimiter struct {
	mu       sync.Mutex
	next     time.Time
	interval time.Duration

	limit         int
	throttledAt   time.Time
	throttleCount int
}

// RateStatus describes the shared AniList request pacing.
type RateStatus struct {
	// Limit is AniList's advertised requests per minute, 0 until seen.
	Limit int
	// Interval is the current spacing between requests.
	Interval time.Duration
	// ThrottledAt is the last time AniList answered 429.
	ThrottledAt time.Time
	// Throttled counts 429 answers since the plugin started.
	Throttled int
}

// SharedRateStatus reports the pacing shared by every default client.
func SharedRateStatus() RateStatus {
	l := sharedRateLimiter
	l.mu.Lock()
	defer l.mu.Unlock()
	return RateStatus{Limit: l.limit, Interval: l.interval, ThrottledAt: l.throttledAt, Throttled: l.throttleCount}
}

// EstimatedDelay is how long the given number of requests would take to
// start on the shared limiter from now.
func EstimatedDelay(requests int) time.Duration {
	if requests < 1 {
		return 0
	}
	l := sharedRateLimiter
	l.mu.Lock()
	defer l.mu.Unlock()
	wait := max(0, time.Until(l.next))
	return wait + time.Duration(requests-1)*l.interval
}

func newRateLimiter(requestsPerMinute int) *rateLimiter {
	return &rateLimiter{interval: rateLimitInterval(requestsPerMinute)}
}

func rateLimitInterval(requestsPerMinute int) time.Duration {
	if requestsPerMinute < 1 {
		return 0
	}
	interval := rateLimitWindow / time.Duration(requestsPerMinute)
	return interval + max(rateLimitPadding, interval/20)
}

func (l *rateLimiter) wait(ctx context.Context) error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	now := time.Now()
	ready := l.next
	if ready.Before(now) {
		ready = now
	}
	if l.interval > 0 {
		l.next = ready.Add(l.interval)
	}
	l.mu.Unlock()

	delay := time.Until(ready)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (l *rateLimiter) observe(header http.Header, status int) time.Duration {
	if l == nil {
		return 0
	}
	now := time.Now()
	interval := time.Duration(0)
	if limit, ok := nonNegativeHeaderInt(header, "X-RateLimit-Limit"); ok && limit > 0 {
		interval = rateLimitInterval(limit)
	}
	remaining, hasRemaining := nonNegativeHeaderInt(header, "X-RateLimit-Remaining")
	resetAfter, hasReset := rateLimitResetAfter(header, now)
	retryAfter := retryAfterDelay(header, now)
	if status == http.StatusTooManyRequests {
		if hasReset && resetAfter > retryAfter {
			retryAfter = resetAfter
		}
		if retryAfter <= 0 {
			retryAfter = rateLimitWindow
		}
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if limit, ok := nonNegativeHeaderInt(header, "X-RateLimit-Limit"); ok && limit > 0 {
		l.limit = limit
	}
	if status == http.StatusTooManyRequests {
		l.throttledAt = now
		l.throttleCount++
	}
	if interval > 0 {
		l.interval = interval
	} else if status == http.StatusTooManyRequests && l.interval <= 0 {
		l.interval = rateLimitInterval(defaultRequestsPerMinute)
	}
	if l.interval > 0 {
		l.next = laterTime(l.next, now.Add(l.interval))
	}
	switch {
	case status == http.StatusTooManyRequests:
		l.next = laterTime(l.next, now.Add(retryAfter+rateLimitPadding))
	case hasRemaining && remaining <= 1 && hasReset:
		l.next = laterTime(l.next, now.Add(resetAfter+rateLimitPadding))
	case hasRemaining && remaining == 0:
		l.next = laterTime(l.next, now.Add(rateLimitWindow))
	case hasRemaining && hasReset && resetAfter > 0 && remaining > 1:
		required := resetAfter / time.Duration(remaining)
		if required > l.interval {
			l.interval = required
			l.next = laterTime(l.next, now.Add(required))
		}
	}
	return retryAfter
}

func nonNegativeHeaderInt(header http.Header, name string) (int, bool) {
	value, err := strconv.Atoi(header.Get(name))
	return value, err == nil && value >= 0
}

func retryAfterDelay(header http.Header, now time.Time) time.Duration {
	value := header.Get("Retry-After")
	if seconds, err := strconv.Atoi(value); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	if retryAt, err := http.ParseTime(value); err == nil && retryAt.After(now) {
		return retryAt.Sub(now)
	}
	return 0
}

func rateLimitResetAfter(header http.Header, now time.Time) (time.Duration, bool) {
	resetAt, err := strconv.ParseInt(header.Get("X-RateLimit-Reset"), 10, 64)
	if err != nil {
		return 0, false
	}
	delay := time.Unix(resetAt, 0).Sub(now)
	if delay < 0 {
		delay = 0
	}
	return delay, true
}

func laterTime(left, right time.Time) time.Time {
	if right.After(left) {
		return right
	}
	return left
}

func (c *Client) ListEntries(ctx context.Context, userID int) ([]ListEntry, error) {
	if userID < 1 {
		return nil, fmt.Errorf("AniList user ID is required")
	}
	var response struct {
		Data struct {
			MediaListCollection struct {
				Lists []struct {
					Entries []ListEntry `json:"entries"`
				} `json:"lists"`
			} `json:"MediaListCollection"`
		} `json:"data"`
	}
	query := `query ($userId: Int!) { MediaListCollection(userId: $userId, type: ANIME) { lists { entries { id mediaId status progress updatedAt score(format: POINT_100) media { id format episodes title { romaji english native } startDate { year } } } } } }`
	httpClient := *c.HTTPClient
	// Very large lists can legitimately exceed the default 15s request
	// budget; give the full-list import its own longer deadline while
	// honoring any explicit caller-configured timeout.
	if httpClient.Timeout == defaultClientTimeout {
		httpClient.Timeout = listEntriesTimeout
	}
	if err := c.doWithLimit(ctx, &httpClient, query, map[string]any{"userId": userID}, &response, 64<<20); err != nil {
		return nil, err
	}
	entries := make([]ListEntry, 0)
	seen := make(map[int]bool)
	for _, list := range response.Data.MediaListCollection.Lists {
		for _, entry := range list.Entries {
			if seen[entry.ID] {
				continue
			}
			seen[entry.ID] = true
			entries = append(entries, entry)
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].MediaID != entries[j].MediaID {
			return entries[i].MediaID < entries[j].MediaID
		}
		return entries[i].ID < entries[j].ID
	})
	return entries, nil
}

func (c *Client) AdvanceProgress(ctx context.Context, mediaID, progress int) error {
	_, err := c.advanceProgress(ctx, mediaID, progress, false)
	return err
}

// AdvanceProgressIfExisting advances progress only when the authenticated
// AniList user already has a list entry for the anime. It returns whether a
// SaveMediaListEntry mutation was applied.
func (c *Client) AdvanceProgressIfExisting(ctx context.Context, mediaID, progress int) (bool, error) {
	return c.advanceProgress(ctx, mediaID, progress, true)
}

func (c *Client) advanceProgress(ctx context.Context, mediaID, progress int, onlyExisting bool) (bool, error) {
	if c.AccessToken == "" {
		return false, errNoToken()
	}
	if progress < 1 {
		return false, nil
	}
	var current mediaResponse
	if err := c.do(ctx, mediaQuery, map[string]any{"id": mediaID}, &current); err != nil {
		return false, err
	}
	entry := current.Data.Media.MediaListEntry
	if entry == nil && onlyExisting {
		return false, nil
	}
	episodes := current.Data.Media.Episodes
	if episodes != nil && *episodes > 0 && progress > *episodes {
		return false, &MismatchError{Message: fmt.Sprintf("mapped progress %d exceeds AniList episode count %d", progress, *episodes)}
	}
	final := episodes != nil && progress == *episodes
	if entry == nil {
		status := "CURRENT"
		if final {
			status = "COMPLETED"
		}
		mutation := `mutation ($mediaId: Int!, $progress: Int!, $status: MediaListStatus!) { SaveMediaListEntry(mediaId: $mediaId, progress: $progress, status: $status) { id } }`
		if err := c.do(ctx, mutation, map[string]any{"mediaId": mediaID, "progress": progress, "status": status}, &struct{}{}); err != nil {
			return false, err
		}
		return true, nil
	}
	if entry.Status == "COMPLETED" || entry.Progress >= progress {
		return false, nil
	}
	variables := map[string]any{"id": entry.ID, "progress": progress}
	status := nextStatus(entry.Status, final)
	if status != entry.Status {
		variables["status"] = status
	}
	// Finishing a rewatch completes the entry and counts the rewatch, the
	// way AniList's own list editor does.
	if entry.Status == "REPEATING" && final {
		variables["repeat"] = entry.Repeat + 1
	}
	if err := c.saveEntry(ctx, variables); err != nil {
		return false, err
	}
	return true, nil
}

// nextStatus is the list status after watching more of an entry. Watching a
// planned, paused or dropped anime resumes it, and the final episode
// completes it.
func nextStatus(status string, final bool) string {
	switch status {
	case "PLANNING", "PAUSED", "DROPPED", "CURRENT":
		if final {
			return "COMPLETED"
		}
		return "CURRENT"
	case "REPEATING":
		if final {
			return "COMPLETED"
		}
	}
	return status
}

// saveEntry updates an existing list entry with the given fields. Only
// fields present in variables change.
func (c *Client) saveEntry(ctx context.Context, variables map[string]any) error {
	params := []string{"$id: Int!"}
	args := []string{"id: $id"}
	for _, field := range []struct{ name, kind string }{
		{"progress", "Int"}, {"status", "MediaListStatus"}, {"repeat", "Int"}, {"scoreRaw", "Int"},
	} {
		if _, ok := variables[field.name]; ok {
			params = append(params, "$"+field.name+": "+field.kind+"!")
			args = append(args, field.name+": $"+field.name)
		}
	}
	mutation := "mutation (" + strings.Join(params, ", ") + ") { SaveMediaListEntry(" + strings.Join(args, ", ") + ") { id } }"
	return c.do(ctx, mutation, variables, &struct{}{})
}

// SetScore sets an anime's score from a 1 to 10 rating. Without a list
// entry it adds the anime as completed, because Silo sends a new rating only
// after the title was watched. It reports whether AniList changed.
func (c *Client) SetScore(ctx context.Context, mediaID, rating int, onlyExisting bool) (bool, error) {
	if c.AccessToken == "" {
		return false, errNoToken()
	}
	if rating < 1 || rating > 10 {
		return false, &MismatchError{Message: fmt.Sprintf("rating %d is outside 1 to 10", rating)}
	}
	var current mediaResponse
	if err := c.do(ctx, mediaQuery, map[string]any{"id": mediaID}, &current); err != nil {
		return false, err
	}
	scoreRaw := rating * 10
	entry := current.Data.Media.MediaListEntry
	if entry == nil {
		if onlyExisting {
			return false, nil
		}
		variables := map[string]any{"mediaId": mediaID, "status": "COMPLETED", "scoreRaw": scoreRaw}
		params, args := "$mediaId: Int!, $status: MediaListStatus!, $scoreRaw: Int!", "mediaId: $mediaId, status: $status, scoreRaw: $scoreRaw"
		if episodes := current.Data.Media.Episodes; episodes != nil && *episodes > 0 {
			variables["progress"] = *episodes
			params += ", $progress: Int!"
			args += ", progress: $progress"
		}
		mutation := "mutation (" + params + ") { SaveMediaListEntry(" + args + ") { id } }"
		if err := c.do(ctx, mutation, variables, &struct{}{}); err != nil {
			return false, err
		}
		return true, nil
	}
	if RatingFromScore(entry.Score) == rating {
		return false, nil
	}
	if err := c.saveEntry(ctx, map[string]any{"id": entry.ID, "scoreRaw": scoreRaw}); err != nil {
		return false, err
	}
	return true, nil
}

// RemoveScore clears an anime's score. It reports whether AniList changed.
func (c *Client) RemoveScore(ctx context.Context, mediaID int) (bool, error) {
	if c.AccessToken == "" {
		return false, errNoToken()
	}
	var current mediaResponse
	if err := c.do(ctx, mediaQuery, map[string]any{"id": mediaID}, &current); err != nil {
		return false, err
	}
	entry := current.Data.Media.MediaListEntry
	if entry == nil || entry.Score <= 0 {
		return false, nil
	}
	if err := c.saveEntry(ctx, map[string]any{"id": entry.ID, "scoreRaw": 0}); err != nil {
		return false, err
	}
	return true, nil
}

// RatingFromScore converts a 100-point AniList score to Silo's 1 to 10
// rating, rounding half up. An unscored entry is 0.
func RatingFromScore(score float64) int {
	if score <= 0 {
		return 0
	}
	rating := int(math.Floor(score/10 + 0.5))
	return min(10, max(1, rating))
}

func errNoToken() error {
	return &Error{Status: http.StatusUnauthorized, Message: "AniList access token is not configured", InvalidToken: true}
}

func isInvalidToken(message string) bool {
	return strings.EqualFold(strings.TrimSpace(message), "invalid token")
}

func (c *Client) do(ctx context.Context, query string, variables map[string]any, out any) error {
	return c.doWithLimit(ctx, c.HTTPClient, query, variables, out, 4<<20)
}

func (c *Client) doWithLimit(ctx context.Context, httpClient *http.Client, query string, variables map[string]any, out any, maxBytes int64) error {
	body, err := json.Marshal(map[string]any{"query": query, "variables": variables})
	if err != nil {
		return fmt.Errorf("encode AniList request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.Endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create AniList request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.AccessToken)
	req.Header.Set("Content-Type", "application/json")
	if err := c.limiter.wait(ctx); err != nil {
		return fmt.Errorf("wait for AniList request allowance: %w", err)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("call AniList: %w", err)
	}
	defer resp.Body.Close()
	retryAfter := c.limiter.observe(resp.Header, resp.StatusCode)
	if resp.StatusCode != http.StatusOK {
		// AniList explains most failures in a GraphQL error body, whatever
		// the status. Keep its message so faults can say what went wrong.
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		apiErr := &Error{Status: resp.StatusCode, RetryAfter: retryAfter}
		var envelope struct {
			Errors []graphQLError `json:"errors"`
		}
		if json.Unmarshal(raw, &envelope) == nil && len(envelope.Errors) > 0 {
			apiErr.Message = "AniList GraphQL: " + envelope.Errors[0].Message
			apiErr.InvalidToken = isInvalidToken(envelope.Errors[0].Message)
		}
		apiErr.InvalidToken = apiErr.InvalidToken || resp.StatusCode == http.StatusUnauthorized
		return apiErr
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes))
	if err != nil {
		return fmt.Errorf("read AniList response: %w", err)
	}
	var envelope struct {
		Errors []graphQLError `json:"errors"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return fmt.Errorf("decode AniList response: %w", err)
	}
	if len(envelope.Errors) > 0 {
		return &Error{Status: resp.StatusCode, Message: "AniList GraphQL: " + envelope.Errors[0].Message, InvalidToken: isInvalidToken(envelope.Errors[0].Message)}
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decode AniList response data: %w", err)
	}
	return nil
}
