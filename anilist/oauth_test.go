package anilist

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestExchangeCode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["client_id"] != "client" || body["client_secret"] != "secret" || body["code"] != "code" {
			t.Fatalf("token request = %#v", body)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "token",
			"token_type":   "Bearer",
			"expires_in":   3600,
		})
	}))
	defer server.Close()
	client := &OAuthClient{HTTPClient: server.Client(), TokenURL: server.URL}
	token, err := client.ExchangeCode(context.Background(), "client", "secret", "https://silo/callback", "code")
	if err != nil {
		t.Fatal(err)
	}
	if token.AccessToken != "token" || token.TokenType != "Bearer" || token.ExpiresAt.IsZero() {
		t.Fatalf("token = %#v", token)
	}
}
