package mapping

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const DefaultARMURL = "https://arm.haglund.dev"

type TemporaryError struct {
	Err error
}

func (e *TemporaryError) Error() string {
	return e.Err.Error()
}

func (e *TemporaryError) Unwrap() error {
	return e.Err
}

type armCacheEntry struct {
	AniListID int
	Found     bool
	LoadedAt  time.Time
}

type ARMClient struct {
	baseURL    string
	httpClient *http.Client
	maxAge     time.Duration

	mu    sync.RWMutex
	cache map[string]armCacheEntry
}

func newARMClient(httpClient *http.Client, maxAge time.Duration) *ARMClient {
	return &ARMClient{
		baseURL: DefaultARMURL, httpClient: httpClient, maxAge: maxAge,
		cache: make(map[string]armCacheEntry),
	}
}

func (c *ARMClient) ResolveAniDB(ctx context.Context, aniDBID string) (int, bool, error) {
	aniDBID = strings.TrimSpace(aniDBID)
	value, err := strconv.Atoi(aniDBID)
	if err != nil || value < 1 {
		return 0, false, fmt.Errorf("invalid AniDB ID %q", aniDBID)
	}

	c.mu.RLock()
	cached, ok := c.cache[aniDBID]
	c.mu.RUnlock()
	if ok && time.Since(cached.LoadedAt) < c.maxAge {
		return cached.AniListID, cached.Found, nil
	}

	endpoint, err := url.Parse(strings.TrimRight(c.baseURL, "/") + "/api/v2/ids")
	if err != nil {
		return 0, false, &TemporaryError{Err: fmt.Errorf("create ARM lookup URL: %w", err)}
	}
	query := endpoint.Query()
	query.Set("source", "anidb")
	query.Set("id", aniDBID)
	query.Set("include", "anidb,anilist")
	endpoint.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return 0, false, &TemporaryError{Err: fmt.Errorf("create ARM lookup request: %w", err)}
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, false, &TemporaryError{Err: fmt.Errorf("query ARM mappings: %w", err)}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, false, &TemporaryError{Err: fmt.Errorf("query ARM mappings: HTTP %d", resp.StatusCode)}
	}

	const maxResponseSize = 64 << 10
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseSize+1))
	if err != nil {
		return 0, false, &TemporaryError{Err: fmt.Errorf("read ARM mapping: %w", err)}
	}
	if len(data) > maxResponseSize {
		return 0, false, &TemporaryError{Err: fmt.Errorf("ARM mapping exceeds %d bytes", maxResponseSize)}
	}
	var result *struct {
		AniDB   *int `json:"anidb"`
		AniList *int `json:"anilist"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return 0, false, &TemporaryError{Err: fmt.Errorf("decode ARM mapping: %w", err)}
	}

	entry := armCacheEntry{LoadedAt: time.Now()}
	if result != nil && (result.AniDB == nil || *result.AniDB != value) {
		return 0, false, &TemporaryError{Err: fmt.Errorf("ARM mapping returned a mismatched AniDB ID")}
	}
	if result != nil && result.AniList != nil && *result.AniList > 0 {
		entry.AniListID = *result.AniList
		entry.Found = true
	}
	c.mu.Lock()
	c.cache[aniDBID] = entry
	c.mu.Unlock()
	return entry.AniListID, entry.Found, nil
}
