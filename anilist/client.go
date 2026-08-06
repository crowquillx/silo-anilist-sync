package anilist

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

const endpoint = "https://graphql.anilist.co"

type Client struct {
	HTTPClient  *http.Client
	AccessToken string
	Endpoint    string
}

type Error struct {
	Status     int
	Message    string
	RetryAfter time.Duration
}

func (e *Error) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return fmt.Sprintf("AniList HTTP %d", e.Status)
}

type graphQLError struct {
	Message string `json:"message"`
}

type mediaResponse struct {
	Data struct {
		Media struct {
			Episodes       *int `json:"episodes"`
			MediaListEntry *struct {
				ID       int    `json:"id"`
				Progress int    `json:"progress"`
				Status   string `json:"status"`
			} `json:"mediaListEntry"`
		} `json:"Media"`
	} `json:"data"`
	Errors []graphQLError `json:"errors"`
}

type ListEntry struct {
	ID       int
	MediaID  int
	Status   string
	Progress int
	Media    struct {
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

type ListPage struct {
	Entries     []ListEntry
	HasNextPage bool
}

func NewClient(token string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	return &Client{HTTPClient: httpClient, AccessToken: token, Endpoint: endpoint}
}

func (c *Client) ListEntriesPage(ctx context.Context, userID, page, perPage int) (ListPage, error) {
	if userID < 1 {
		return ListPage{}, fmt.Errorf("AniList user ID is required")
	}
	if page < 1 {
		return ListPage{}, fmt.Errorf("AniList list page must be positive")
	}
	if perPage < 1 || perPage > 50 {
		return ListPage{}, fmt.Errorf("AniList list page size must be between 1 and 50")
	}
	var response struct {
		Data struct {
			Page struct {
				PageInfo struct {
					HasNextPage bool `json:"hasNextPage"`
				} `json:"pageInfo"`
				MediaList []ListEntry `json:"mediaList"`
			} `json:"Page"`
		} `json:"data"`
	}
	query := `query ($userId: Int!, $page: Int!, $perPage: Int!) {
		Page(page: $page, perPage: $perPage) {
			pageInfo { hasNextPage }
			mediaList(userId: $userId, type: ANIME, sort: MEDIA_ID) {
				id mediaId status progress
				media { id format episodes title { romaji english native } startDate { year } }
			}
		}
	}`
	if err := c.do(ctx, query, map[string]any{"userId": userID, "page": page, "perPage": perPage}, &response); err != nil {
		return ListPage{}, err
	}
	return ListPage{Entries: response.Data.Page.MediaList, HasNextPage: response.Data.Page.PageInfo.HasNextPage}, nil
}

func (c *Client) AdvanceProgress(ctx context.Context, mediaID, progress int) error {
	if c.AccessToken == "" {
		return fmt.Errorf("AniList access token is not configured")
	}
	if progress < 1 {
		return nil
	}
	var current mediaResponse
	query := `query ($id: Int!) { Media(id: $id, type: ANIME) { episodes mediaListEntry { id progress status } } }`
	if err := c.do(ctx, query, map[string]any{"id": mediaID}, &current); err != nil {
		return err
	}
	episodes := current.Data.Media.Episodes
	if episodes != nil && *episodes > 0 && progress > *episodes {
		return fmt.Errorf("mapped progress %d exceeds AniList episode count %d", progress, *episodes)
	}
	entry := current.Data.Media.MediaListEntry
	if entry == nil {
		status := "CURRENT"
		if episodes != nil && progress == *episodes {
			status = "COMPLETED"
		}
		mutation := `mutation ($mediaId: Int!, $progress: Int!, $status: MediaListStatus!) { SaveMediaListEntry(mediaId: $mediaId, progress: $progress, status: $status) { id } }`
		return c.do(ctx, mutation, map[string]any{"mediaId": mediaID, "progress": progress, "status": status}, &struct{}{})
	}
	if entry.Status == "COMPLETED" || entry.Progress >= progress {
		return nil
	}
	status := entry.Status
	switch entry.Status {
	case "PLANNING":
		status = "CURRENT"
	case "CURRENT":
		if episodes != nil && progress == *episodes {
			status = "COMPLETED"
		}
	}
	if status == entry.Status {
		mutation := `mutation ($id: Int!, $progress: Int!) { SaveMediaListEntry(id: $id, progress: $progress) { id } }`
		return c.do(ctx, mutation, map[string]any{"id": entry.ID, "progress": progress}, &struct{}{})
	}
	mutation := `mutation ($id: Int!, $progress: Int!, $status: MediaListStatus!) { SaveMediaListEntry(id: $id, progress: $progress, status: $status) { id } }`
	return c.do(ctx, mutation, map[string]any{"id": entry.ID, "progress": progress, "status": status}, &struct{}{})
}

func (c *Client) do(ctx context.Context, query string, variables map[string]any, out any) error {
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
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("call AniList: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		retryAfter := time.Duration(0)
		if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds > 0 {
			retryAfter = time.Duration(seconds) * time.Second
		}
		return &Error{Status: resp.StatusCode, RetryAfter: retryAfter}
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
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
		return &Error{Status: resp.StatusCode, Message: "AniList GraphQL: " + envelope.Errors[0].Message}
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decode AniList response data: %w", err)
	}
	return nil
}
