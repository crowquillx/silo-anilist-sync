package silostate

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// catalogResponseBytes caps a catalog listing page. Listings carry overviews
// and artwork links for every item, so they are larger than the item reads
// the watched check makes.
const catalogResponseBytes = 16 << 20

// maxCatalogPages bounds a paged listing, so a cursor that never ends cannot
// keep a request running.
const maxCatalogPages = 1000

const lookupPurpose = "catalog lookup"

// Series is a series in the profile's catalog with its own external IDs.
type Series struct {
	ContentID string `json:"content_id"`
	Type      string `json:"type"`
	Title     string `json:"title"`
	TmdbID    string `json:"tmdb_id"`
	TvdbID    string `json:"tvdb_id"`
	ImdbID    string `json:"imdb_id"`
}

// Episode is an episode of a series with its own external IDs, which name the
// episode rather than its series.
type Episode struct {
	ContentID     string `json:"content_id"`
	SeasonNumber  int    `json:"season_number"`
	EpisodeNumber int    `json:"episode_number"`
	TmdbID        string `json:"tmdb_id"`
	TvdbID        string `json:"tvdb_id"`
	ImdbID        string `json:"imdb_id"`
}

type page struct {
	NextCursor string `json:"next_cursor"`
	HasMore    bool   `json:"has_more"`
}

func (c *Client) validID(id string) bool {
	id = strings.TrimSpace(id)
	return id != "" && id != "." && id != ".."
}

// FileNames returns the file names of an item's versions. found is false when
// the profile has no such item. Silo hides file paths from viewers without
// file-path visibility but always returns the names.
func (c *Client) FileNames(ctx context.Context, itemID string) ([]string, bool, error) {
	if err := c.config.Validate(); err != nil {
		return nil, false, err
	}
	if !c.validID(itemID) {
		return nil, false, &Error{InvalidConfig, "Silo catalog lookup requires an exact media item ID"}
	}
	ctx, cancel := contextWithRequestTimeout(ctx)
	defer cancel()
	var response struct {
		Items []struct {
			FileName string `json:"file_name"`
		} `json:"items"`
	}
	status, err := c.getJSON(ctx, "/api/v2/catalog/items/"+url.PathEscape(itemID)+"/versions", nil, &response, lookupPurpose, catalogResponseBytes)
	if err != nil {
		return nil, false, err
	}
	if status == http.StatusNotFound {
		return nil, false, nil
	}
	names := make([]string, 0, len(response.Items))
	for _, item := range response.Items {
		names = append(names, item.FileName)
	}
	return names, true, nil
}

// Series lists every series the profile can see.
func (c *Client) Series(ctx context.Context) ([]Series, error) {
	if err := c.config.Validate(); err != nil {
		return nil, err
	}
	var out []Series
	cursor := ""
	for range maxCatalogPages {
		query := url.Values{"source": {"query"}, "type": {"series"}, "sort": {"title"}, "limit": {"200"}, "skip_total": {"true"}}
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		var response struct {
			Items []Series `json:"items"`
			Page  *page    `json:"page"`
		}
		if err := c.getPage(ctx, "/api/v2/catalog", query, &response); err != nil {
			return nil, err
		}
		for _, item := range response.Items {
			if strings.EqualFold(strings.TrimSpace(item.Type), "series") && c.validID(item.ContentID) {
				out = append(out, item)
			}
		}
		next, done, err := nextCursor(response.Page, cursor)
		if err != nil || done {
			return out, err
		}
		cursor = next
	}
	return nil, &Error{Permanent, "Silo catalog listing did not end"}
}

// Seasons lists a series' season numbers. found is false when the profile
// has no such series.
func (c *Client) Seasons(ctx context.Context, seriesID string) ([]int, bool, error) {
	if err := c.config.Validate(); err != nil {
		return nil, false, err
	}
	if !c.validID(seriesID) {
		return nil, false, &Error{InvalidConfig, "Silo catalog lookup requires an exact series ID"}
	}
	var response struct {
		Items []struct {
			SeasonNumber int `json:"season_number"`
		} `json:"items"`
		Page *page `json:"page"`
	}
	ctx, cancel := contextWithRequestTimeout(ctx)
	defer cancel()
	status, err := c.getJSON(ctx, "/api/v2/catalog/series/"+url.PathEscape(seriesID)+"/seasons", nil, &response, lookupPurpose, catalogResponseBytes)
	if err != nil {
		return nil, false, err
	}
	if status == http.StatusNotFound {
		return nil, false, nil
	}
	if response.Page != nil && response.Page.HasMore {
		return nil, false, &Error{Permanent, "Silo season listing is unexpectedly paged"}
	}
	seasons := make([]int, 0, len(response.Items))
	for _, item := range response.Items {
		seasons = append(seasons, item.SeasonNumber)
	}
	return seasons, true, nil
}

// Episodes lists the episodes of one season of a series. found is false when
// the profile has no such season.
func (c *Client) Episodes(ctx context.Context, seriesID string, season int) ([]Episode, bool, error) {
	if err := c.config.Validate(); err != nil {
		return nil, false, err
	}
	if !c.validID(seriesID) || season < 0 {
		return nil, false, &Error{InvalidConfig, "Silo catalog lookup requires an exact series ID and season"}
	}
	path := "/api/v2/catalog/series/" + url.PathEscape(seriesID) + "/seasons/" + strconv.Itoa(season) + "/episodes"
	var out []Episode
	cursor := ""
	for range maxCatalogPages {
		var query url.Values
		if cursor != "" {
			query = url.Values{"cursor": {cursor}}
		}
		var response struct {
			Items []Episode `json:"items"`
			Page  *page     `json:"page"`
		}
		reqCtx, cancel := contextWithRequestTimeout(ctx)
		status, err := c.getJSON(reqCtx, path, query, &response, lookupPurpose, catalogResponseBytes)
		cancel()
		if err != nil {
			return nil, false, err
		}
		if status == http.StatusNotFound {
			return nil, false, nil
		}
		out = append(out, response.Items...)
		next, done, err := nextCursor(response.Page, cursor)
		if err != nil {
			return nil, false, err
		}
		if done {
			return out, true, nil
		}
		cursor = next
	}
	return nil, false, &Error{Permanent, "Silo episode listing did not end"}
}

func (c *Client) getPage(ctx context.Context, path string, query url.Values, target any) error {
	ctx, cancel := contextWithRequestTimeout(ctx)
	defer cancel()
	status, err := c.getJSON(ctx, path, query, target, lookupPurpose, catalogResponseBytes)
	if err != nil {
		return err
	}
	if status == http.StatusNotFound {
		return &Error{Permanent, "Silo catalog listing was not found; check the server URL"}
	}
	return nil
}

// nextCursor returns the cursor of the next page, or done when the listing
// has ended. A cursor that does not advance is an error, so a listing cannot
// loop.
func nextCursor(p *page, current string) (string, bool, error) {
	if p == nil || !p.HasMore {
		return "", true, nil
	}
	next := strings.TrimSpace(p.NextCursor)
	if next == "" || next == current {
		return "", false, &Error{Permanent, "Silo catalog cursor did not advance"}
	}
	return next, false, nil
}

func contextWithRequestTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, catalogRequestTimeout)
}
