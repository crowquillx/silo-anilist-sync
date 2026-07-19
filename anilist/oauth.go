package anilist

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	AuthorizeEndpoint = "https://anilist.co/api/v2/oauth/authorize"
	TokenEndpoint     = "https://anilist.co/api/v2/oauth/token"
)

type OAuthClient struct {
	HTTPClient *http.Client
	TokenURL   string
}

type Token struct {
	AccessToken string
	TokenType   string
	ExpiresAt   time.Time
}

type Account struct {
	ID         int
	Name       string
	AvatarURL  string
	ProfileURL string
}

func AuthorizationURL(clientID, redirectURI, state string) (string, error) {
	if strings.TrimSpace(clientID) == "" || strings.TrimSpace(redirectURI) == "" || strings.TrimSpace(state) == "" {
		return "", fmt.Errorf("client ID, redirect URI, and state are required")
	}
	values := url.Values{
		"client_id":     {clientID},
		"redirect_uri":  {redirectURI},
		"response_type": {"code"},
		"state":         {state},
	}
	return AuthorizeEndpoint + "?" + values.Encode(), nil
}

func (c *OAuthClient) ExchangeCode(ctx context.Context, clientID, clientSecret, redirectURI, code string) (Token, error) {
	if c.HTTPClient == nil {
		c.HTTPClient = &http.Client{Timeout: 15 * time.Second}
	}
	tokenURL := c.TokenURL
	if tokenURL == "" {
		tokenURL = TokenEndpoint
	}
	body, err := json.Marshal(map[string]string{
		"grant_type":    "authorization_code",
		"client_id":     clientID,
		"client_secret": clientSecret,
		"redirect_uri":  redirectURI,
		"code":          code,
	})
	if err != nil {
		return Token{}, fmt.Errorf("encode AniList token request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, bytes.NewReader(body))
	if err != nil {
		return Token{}, fmt.Errorf("create AniList token request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return Token{}, fmt.Errorf("exchange AniList authorization code: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return Token{}, fmt.Errorf("exchange AniList authorization code: HTTP %d", resp.StatusCode)
	}
	var result struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return Token{}, fmt.Errorf("decode AniList token response: %w", err)
	}
	if result.AccessToken == "" || result.ExpiresIn <= 0 {
		return Token{}, fmt.Errorf("AniList token response is incomplete")
	}
	return Token{
		AccessToken: result.AccessToken,
		TokenType:   result.TokenType,
		ExpiresAt:   time.Now().UTC().Add(time.Duration(result.ExpiresIn) * time.Second),
	}, nil
}

func (c *Client) Viewer(ctx context.Context) (Account, error) {
	var response struct {
		Data struct {
			Viewer struct {
				ID     int    `json:"id"`
				Name   string `json:"name"`
				Avatar struct {
					Large string `json:"large"`
				} `json:"avatar"`
				SiteURL string `json:"siteUrl"`
			} `json:"Viewer"`
		} `json:"data"`
	}
	if err := c.do(ctx, `query { Viewer { id name avatar { large } siteUrl } }`, nil, &response); err != nil {
		return Account{}, err
	}
	if response.Data.Viewer.ID == 0 || response.Data.Viewer.Name == "" {
		return Account{}, fmt.Errorf("AniList Viewer response is incomplete")
	}
	return Account{
		ID:         response.Data.Viewer.ID,
		Name:       response.Data.Viewer.Name,
		AvatarURL:  response.Data.Viewer.Avatar.Large,
		ProfileURL: response.Data.Viewer.SiteURL,
	}, nil
}
