// Package silostate reads saved watched state for an explicitly selected Silo
// account and profile. It never writes watch history.
package silostate

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const requestTimeout = 5 * time.Second
const maxResponseBytes = 1 << 20

type Kind int

const (
	Temporary Kind = iota
	InvalidConfig
	InvalidCredential
	PermissionDenied
	RateLimited
	Permanent
)

// Error contains only fixed messages. Silo response bodies, URLs and transport
// errors may contain credentials and must not reach provider fault messages.
type Error struct {
	Kind    Kind
	message string
}

func (e *Error) Error() string { return e.message }

type Config struct {
	BaseURL   string
	APIKey    string
	ProfileID string
}

func (c Config) Validate() error {
	u, err := url.Parse(c.BaseURL)
	if err != nil || u.Hostname() == "" || (u.Scheme != "https" && u.Scheme != "http") ||
		u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" {
		return &Error{InvalidConfig, "Silo verification requires an HTTP or HTTPS server URL without credentials, query, or fragment"}
	}
	if strings.TrimSpace(c.APIKey) == "" || strings.ContainsAny(c.APIKey, "\r\n") {
		return &Error{InvalidConfig, "Silo verification requires a Silo API key"}
	}
	if strings.TrimSpace(c.ProfileID) == "" || strings.ContainsAny(c.ProfileID, "\r\n") {
		return &Error{InvalidConfig, "Silo verification requires a Silo profile ID"}
	}
	return nil
}

type Client struct {
	config Config
	http   *http.Client
}

func NewClient(config Config) *Client {
	return &Client{config: config, http: &http.Client{
		Timeout:       requestTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

func (c *Client) ValidateProfile(ctx context.Context) error {
	if err := c.config.Validate(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	return c.validateProfile(ctx)
}

func (c *Client) validateProfile(ctx context.Context) error {
	var response struct {
		Profiles []struct {
			ID string `json:"id"`
		} `json:"profiles"`
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	status, err := c.get(ctx, "/api/v1/profiles", &response)
	if err != nil {
		return err
	}
	if status == http.StatusNotFound {
		// Silo answers an unknown X-Profile-Id with its own not_found error;
		// any other 404 means the URL does not lead to Silo's API.
		if response.Error == "not_found" && strings.Contains(strings.ToLower(response.Message), "profile") {
			return &Error{InvalidConfig, "Silo profile ID was not found for this API key's account; check the profile ID"}
		}
		return &Error{Permanent, "Silo profile verification endpoint was not found; check the server URL"}
	}
	for _, profile := range response.Profiles {
		if profile.ID == c.config.ProfileID {
			return nil
		}
	}
	return &Error{PermissionDenied, "Silo API key cannot access the selected profile; reconnect with the correct account and profile"}
}

func (c *Client) Played(ctx context.Context, mediaItemID string) (bool, error) {
	if err := c.config.Validate(); err != nil {
		return false, err
	}
	if strings.TrimSpace(mediaItemID) == "" || mediaItemID == "." || mediaItemID == ".." {
		return false, &Error{InvalidConfig, "Silo watched verification requires an exact media item ID"}
	}
	// Share one deadline across both requests. A stale profile must never cause
	// the catalog endpoint to silently use the account's primary profile.
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	if err := c.validateProfile(ctx); err != nil {
		return false, err
	}
	var item struct {
		ContentID string `json:"content_id"`
		UserState struct {
			Played *bool `json:"played"`
		} `json:"user_state"`
	}
	status, err := c.get(ctx, "/api/v1/catalog/items/"+url.PathEscape(mediaItemID), &item)
	if err != nil {
		return false, err
	}
	if status == http.StatusNotFound {
		return false, nil
	}
	if item.ContentID != mediaItemID {
		return false, &Error{Permanent, "Silo returned a different item during watched verification"}
	}
	return item.UserState.Played != nil && *item.UserState.Played, nil
}

func (c *Client) get(ctx context.Context, path string, target any) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(c.config.BaseURL, "/")+path, nil)
	if err != nil {
		return 0, &Error{InvalidConfig, "Silo verification server URL is invalid"}
	}
	req.Header.Set("Authorization", "Bearer "+c.config.APIKey)
	req.Header.Set("X-Profile-Id", c.config.ProfileID)
	req.Header.Set("Accept", "application/json")
	response, err := c.http.Do(req)
	if err != nil {
		return 0, &Error{Temporary, "Silo watched verification request failed; try again"}
	}
	defer response.Body.Close()
	switch response.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		// A 404 body is read only to tell Silo's own errors apart. It is
		// never shown, so an unreadable one is not an error.
		body, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
		_ = json.Unmarshal(body, target)
		return response.StatusCode, nil
	case http.StatusUnauthorized:
		return response.StatusCode, &Error{InvalidCredential, "Silo verification API key was rejected; reconnect with a valid key"}
	case http.StatusForbidden:
		return response.StatusCode, &Error{PermissionDenied, "Silo denied watched verification for the selected profile"}
	case http.StatusTooManyRequests:
		return response.StatusCode, &Error{RateLimited, "Silo watched verification reached its request limit; try again"}
	default:
		if response.StatusCode >= 500 || response.StatusCode == http.StatusRequestTimeout {
			return response.StatusCode, &Error{Temporary, "Silo watched verification is temporarily unavailable"}
		}
		return response.StatusCode, &Error{Permanent, "Silo rejected watched verification; check the server URL and profile"}
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil || len(body) > maxResponseBytes || json.Unmarshal(body, target) != nil {
		return response.StatusCode, &Error{Temporary, "Silo returned an unreadable watched verification response"}
	}
	return response.StatusCode, nil
}
