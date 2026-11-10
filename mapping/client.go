package mapping

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

const DefaultURL = "https://github.com/anibridge/anibridge-mappings/releases/download/v3/mappings.min.json"

type Catalog struct {
	AniBridge    Dataset
	AnimeLists   AnimeListsDataset
	arm          *ARMClient
	reverse      ReverseIndex
	aniBridgeErr error
	animeListErr error
}

// Reverse resolves completed AniList progress to catalog identities, using
// the dataset's reverse index when the client built one.
func (c Catalog) Reverse(anilistID, progress int) []MediaSource {
	if c.reverse != nil {
		return c.AniBridge.ReverseIndexed(c.reverse, anilistID, progress)
	}
	return c.AniBridge.Reverse(anilistID, progress)
}

// ReverseUnavailable returns a temporary error when the AniBridge dataset,
// the only source Reverse reads, has not loaded. A catalog built from
// Anime-Lists alone can resolve exports but maps no AniList entry back.
func (c Catalog) ReverseUnavailable() error {
	if c.AniBridge == nil {
		return c.aniBridgeErr
	}
	return nil
}

func (c Catalog) Resolve(ctx context.Context, provider, id string, season, episode int) ([]Target, error) {
	direct, directErr := c.AniBridge.Resolve(provider, id, season, episode)
	if directErr == nil && len(direct) > 0 {
		return direct, nil
	}
	if c.AnimeLists == nil {
		if directErr != nil {
			return nil, directErr
		}
		if c.animeListErr != nil {
			return nil, c.animeListErr
		}
		return nil, nil
	}

	var resolved map[int]int
	var fallbackErr error
	for _, aniDB := range c.AnimeLists.Resolve(provider, id, season, episode) {
		targets, err := c.resolveAniDB(ctx, aniDB)
		if err != nil {
			fallbackErr = err
			continue
		}
		candidate := targetProgress(targets)
		if len(candidate) == 0 {
			continue
		}
		if resolved != nil && !sameProgress(resolved, candidate) {
			return nil, fmt.Errorf("Anime-Lists fallback is ambiguous for %s ID %q", provider, id)
		}
		resolved = candidate
	}
	if fallbackErr != nil {
		return nil, fallbackErr
	}
	if resolved != nil {
		targets := make([]Target, 0, len(resolved))
		for id, progress := range resolved {
			targets = append(targets, Target{AniListID: id, Episode: progress})
		}
		return targets, nil
	}
	if directErr != nil {
		return nil, directErr
	}
	if c.aniBridgeErr != nil {
		return nil, c.aniBridgeErr
	}
	return nil, nil
}

func (c Catalog) resolveAniDB(ctx context.Context, aniDB aniDBTarget) ([]Target, error) {
	targets, bridgeErr := c.AniBridge.resolveAniDB(aniDB.ID, aniDB.Scope, aniDB.Episode)
	if bridgeErr == nil && len(targets) > 0 {
		return targets, nil
	}
	if aniDB.Scope != "R" {
		return nil, bridgeErr
	}
	if c.arm != nil {
		aniListID, found, err := c.arm.ResolveAniDB(ctx, aniDB.ID)
		if err != nil {
			return nil, err
		}
		if found {
			return []Target{{AniListID: aniListID, Episode: aniDB.Episode}}, nil
		}
	}
	return nil, bridgeErr
}

func targetProgress(targets []Target) map[int]int {
	out := make(map[int]int, len(targets))
	for _, target := range targets {
		if target.Episode > out[target.AniListID] {
			out[target.AniListID] = target.Episode
		}
	}
	return out
}

func sameProgress(a, b map[int]int) bool {
	if len(a) != len(b) {
		return false
	}
	for id, progress := range a {
		if b[id] != progress {
			return false
		}
	}
	return true
}

type Client struct {
	url           string
	animeListsURL string
	httpClient    *http.Client
	maxAge        time.Duration
	now           func() time.Time

	mu         sync.RWMutex
	dataset    Dataset
	reverse    ReverseIndex
	aniBridge  sourceState
	animeLists AnimeListsDataset
	animeList  sourceState
	arm        *ARMClient
}

// sourceState tracks one downloaded dataset: when it loaded and, after a
// failed download, when the next attempt is allowed.
type sourceState struct {
	loadedAt  time.Time
	entries   int
	failures  int
	lastErr   error
	lastErrAt time.Time
	retryAt   time.Time
}

const (
	retryBackoffBase = 2 * time.Minute
	retryBackoffMax  = time.Hour
)

func (s *sourceState) failed(err error, now time.Time) {
	s.failures++
	s.lastErr, s.lastErrAt = err, now
	backoff := retryBackoffBase << min(s.failures-1, 5)
	s.retryAt = now.Add(min(backoff, retryBackoffMax))
}

func (s *sourceState) loaded(entries int, now time.Time) {
	*s = sourceState{loadedAt: now, entries: entries}
}

// SourceStatus describes one mapping dataset for the status page.
type SourceStatus struct {
	Name      string
	LoadedAt  time.Time
	Entries   int
	LastError string
	ErrorAt   time.Time
	RetryAt   time.Time
}

// Status reports both mapping datasets. LastError is set only while the
// latest download attempt failed.
func (c *Client) Status() []SourceStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()
	status := func(name string, s sourceState) SourceStatus {
		out := SourceStatus{Name: name, LoadedAt: s.loadedAt, Entries: s.entries, RetryAt: s.retryAt}
		if s.lastErr != nil {
			out.LastError, out.ErrorAt = s.lastErr.Error(), s.lastErrAt
		}
		return out
	}
	return []SourceStatus{status("AniBridge", c.aniBridge), status("Anime-Lists", c.animeList)}
}

func NewClient(httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	client := &Client{
		url: DefaultURL, animeListsURL: DefaultAnimeListsURL,
		httpClient: httpClient, maxAge: 24 * time.Hour, now: time.Now,
	}
	client.arm = newARMClient(httpClient, client.maxAge)
	return client
}

// Dataset returns the mapping catalog, downloading a dataset when it is
// older than a day. A failed download keeps the previous copy and is not
// retried until its backoff passes, so an outage of the mapping host does
// not stall every request behind a download that will fail again.
func (c *Client) Dataset(ctx context.Context) (Catalog, error) {
	c.mu.RLock()
	if !c.due(c.aniBridge) && !c.due(c.animeList) {
		catalog, err := c.catalog()
		c.mu.RUnlock()
		return catalog, err
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.due(c.aniBridge) {
		dataset, err := c.downloadAniBridge(ctx)
		if err != nil {
			c.aniBridge.failed(err, c.clock())
		} else {
			c.dataset, c.reverse = dataset, dataset.BuildReverseIndex()
			c.aniBridge.loaded(len(dataset), c.clock())
		}
	}
	if c.due(c.animeList) {
		animeLists, err := c.downloadAnimeLists(ctx)
		if err != nil {
			c.animeList.failed(err, c.clock())
		} else {
			c.animeLists = animeLists
			c.animeList.loaded(len(animeLists), c.clock())
		}
	}
	return c.catalog()
}

// due reports whether a dataset should be downloaded now.
func (c *Client) due(s sourceState) bool {
	now := c.clock()
	fresh := !s.loadedAt.IsZero() && now.Sub(s.loadedAt) < c.maxAge
	return !fresh && !now.Before(s.retryAt)
}

func (c *Client) clock() time.Time {
	if c.now == nil {
		return time.Now()
	}
	return c.now()
}

// catalog builds the catalog from what is loaded. A dataset that never
// loaded contributes a temporary error, so events wait for it rather than
// being rejected for a mapping that may exist.
func (c *Client) catalog() (Catalog, error) {
	var aniBridgeErr, animeListErr error
	if c.dataset == nil {
		aniBridgeErr = &TemporaryError{Err: errOrUnavailable(c.aniBridge.lastErr, "AniBridge mappings are not loaded")}
	}
	if c.animeLists == nil {
		animeListErr = &TemporaryError{Err: errOrUnavailable(c.animeList.lastErr, "Anime-Lists mappings are not loaded")}
	}
	if c.dataset == nil && c.animeLists == nil {
		return Catalog{}, aniBridgeErr
	}
	return Catalog{
		AniBridge: c.dataset, AnimeLists: c.animeLists, arm: c.arm, reverse: c.reverse,
		aniBridgeErr: aniBridgeErr, animeListErr: animeListErr,
	}, nil
}

func errOrUnavailable(err error, message string) error {
	if err != nil {
		return err
	}
	return errors.New(message)
}

func (c *Client) downloadAniBridge(ctx context.Context) (Dataset, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, nil)
	if err != nil {
		return nil, fmt.Errorf("create mappings request: %w", err)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("download AniBridge mappings: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("download AniBridge mappings: HTTP %d", resp.StatusCode)
	}
	const maxMappingsSize = 64 << 20
	limited := io.LimitReader(resp.Body, maxMappingsSize+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("read AniBridge mappings: %w", err)
	}
	if len(data) > maxMappingsSize {
		return nil, fmt.Errorf("AniBridge mappings exceed %d bytes", maxMappingsSize)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("decode AniBridge mappings: %w", err)
	}
	var metadata struct {
		SchemaVersion string `json:"schema_version"`
	}
	if err := json.Unmarshal(raw["$meta"], &metadata); err != nil || !strings.HasPrefix(metadata.SchemaVersion, "3.") {
		return nil, fmt.Errorf("unsupported AniBridge schema version %q", metadata.SchemaVersion)
	}
	dataset := make(Dataset, len(raw)-1)
	for descriptor, encoded := range raw {
		if descriptor == "$meta" {
			continue
		}
		var targets map[string]map[string]string
		if err := json.Unmarshal(encoded, &targets); err != nil {
			return nil, fmt.Errorf("decode AniBridge descriptor %q: %w", descriptor, err)
		}
		dataset[descriptor] = targets
	}
	return dataset, nil
}

func (c *Client) downloadAnimeLists(ctx context.Context) (AnimeListsDataset, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.animeListsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create Anime-Lists request: %w", err)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("download Anime-Lists mappings: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("download Anime-Lists mappings: HTTP %d", resp.StatusCode)
	}
	const maxMappingsSize = 64 << 20
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxMappingsSize+1))
	if err != nil {
		return nil, fmt.Errorf("read Anime-Lists mappings: %w", err)
	}
	if len(data) > maxMappingsSize {
		return nil, fmt.Errorf("Anime-Lists mappings exceed %d bytes", maxMappingsSize)
	}
	return parseAnimeLists(data)
}
