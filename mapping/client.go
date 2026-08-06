package mapping

import (
	"context"
	"encoding/json"
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
	aniBridgeErr error
	animeListErr error
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

	mu                 sync.RWMutex
	dataset            Dataset
	loadedAt           time.Time
	animeLists         AnimeListsDataset
	animeListsLoadedAt time.Time
	arm                *ARMClient
}

func NewClient(httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	client := &Client{
		url: DefaultURL, animeListsURL: DefaultAnimeListsURL,
		httpClient: httpClient, maxAge: 24 * time.Hour,
	}
	client.arm = newARMClient(httpClient, client.maxAge)
	return client
}

func (c *Client) Dataset(ctx context.Context) (Catalog, error) {
	c.mu.RLock()
	if c.fresh(c.loadedAt) && c.fresh(c.animeListsLoadedAt) {
		catalog := c.catalog(nil, nil)
		c.mu.RUnlock()
		return catalog, nil
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fresh(c.loadedAt) && c.fresh(c.animeListsLoadedAt) {
		return c.catalog(nil, nil), nil
	}

	var aniBridgeErr error
	if !c.fresh(c.loadedAt) {
		dataset, err := c.downloadAniBridge(ctx)
		if err != nil {
			if c.dataset == nil {
				aniBridgeErr = &TemporaryError{Err: err}
			}
		} else {
			c.dataset = dataset
			c.loadedAt = time.Now()
		}
	}

	var animeListErr error
	if !c.fresh(c.animeListsLoadedAt) {
		animeLists, err := c.downloadAnimeLists(ctx)
		if err != nil {
			if c.animeLists == nil {
				animeListErr = err
			}
		} else {
			c.animeLists = animeLists
			c.animeListsLoadedAt = time.Now()
		}
	}
	if c.dataset == nil && c.animeLists == nil {
		if aniBridgeErr != nil {
			return Catalog{}, aniBridgeErr
		}
		return Catalog{}, animeListErr
	}
	return c.catalog(aniBridgeErr, animeListErr), nil
}

func (c *Client) catalog(aniBridgeErr, animeListErr error) Catalog {
	return Catalog{
		AniBridge: c.dataset, AnimeLists: c.animeLists, arm: c.arm,
		aniBridgeErr: aniBridgeErr, animeListErr: animeListErr,
	}
}

func (c *Client) fresh(loadedAt time.Time) bool {
	return !loadedAt.IsZero() && time.Since(loadedAt) < c.maxAge
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
