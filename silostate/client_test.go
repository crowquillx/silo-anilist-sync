package silostate

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPlayedRequiresExactProfileAndItem(t *testing.T) {
	for _, tc := range []struct {
		name, profiles, item string
		status               int
		wantPlayed           bool
		wantKind             *Kind
	}{
		{"watched", `{"profiles":[{"id":"p"}]}`, `{"content_id":"episode-1","user_state":{"played":true}}`, 200, true, nil},
		{"unwatched", `{"profiles":[{"id":"p"}]}`, `{"content_id":"episode-1","user_state":{"played":false}}`, 200, false, nil},
		{"missing state", `{"profiles":[{"id":"p"}]}`, `{"content_id":"episode-1"}`, 200, false, nil},
		{"missing item", `{"profiles":[{"id":"p"}]}`, ``, 404, false, nil},
		{"wrong profile", `{"profiles":[{"id":"other"}]}`, `{"content_id":"episode-1","user_state":{"played":true}}`, 200, false, kindPtr(PermissionDenied)},
		{"wrong item", `{"profiles":[{"id":"p"}]}`, `{"content_id":"other","user_state":{"played":true}}`, 200, false, kindPtr(Permanent)},
		{"malformed response", `{"profiles":[{"id":"p"}]}`, `{"private":"api-secret",`, 200, false, kindPtr(Temporary)},
		{"oversized response", `{"profiles":[{"id":"p"}]}`, strings.Repeat("x", maxResponseBytes+1), 200, false, kindPtr(Temporary)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalogCalls := 0
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer api-secret" || r.Header.Get("X-Profile-Id") != "p" {
					t.Error("scope missing")
				}
				if r.URL.Path == "/base/api/v1/profiles" {
					_, _ = w.Write([]byte(tc.profiles))
					return
				}
				catalogCalls++
				if r.URL.Path != "/base/api/v1/catalog/items/episode-1" {
					t.Errorf("wrong item path %q", r.URL.Path)
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.item))
			}))
			defer upstream.Close()
			played, err := NewClient(Config{upstream.URL + "/base/", "api-secret", "p"}).Played(context.Background(), "episode-1")
			if played != tc.wantPlayed {
				t.Errorf("played=%v want %v", played, tc.wantPlayed)
			}
			if tc.wantKind == nil {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				var e *Error
				if !errors.As(err, &e) || e.Kind != *tc.wantKind {
					t.Fatalf("error=%v", err)
				}
			}
			if tc.name == "wrong profile" && catalogCalls != 0 {
				t.Fatal("looked up watched state for wrong profile")
			}
			if err != nil && strings.Contains(err.Error(), "api-secret") {
				t.Fatal("secret in error")
			}
		})
	}
}

func TestNoRedirectOrSecretExposure(t *testing.T) {
	targetCalls := 0
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { targetCalls++; t.Error("followed redirect") }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/api-secret", http.StatusFound)
	}))
	defer redirect.Close()
	err := NewClient(Config{redirect.URL, "api-secret", "p"}).ValidateProfile(context.Background())
	if err == nil || targetCalls != 0 {
		t.Fatal("redirect accepted")
	}
	if strings.Contains(err.Error(), "api-secret") || strings.Contains(err.Error(), redirect.URL) {
		t.Fatal("unsafe failure message")
	}
}

func TestCanceledRequestFailsClosed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	played, err := NewClient(Config{"http://127.0.0.1:1/private-key", "api-secret", "p"}).Played(ctx, "episode-1")
	var e *Error
	if played || !errors.As(err, &e) || e.Kind != Temporary {
		t.Fatalf("played=%v error=%v", played, err)
	}
	if strings.Contains(err.Error(), "private-key") || strings.Contains(err.Error(), "api-secret") {
		t.Fatal("unsafe transport error")
	}
}

func TestInvalidConfig(t *testing.T) {
	for _, base := range []string{"", "://bad", "ftp://silo", "https://user:secret@silo", "https://silo?key=secret", "https://silo/#secret"} {
		if err := (Config{base, "key", "p"}).Validate(); err == nil {
			t.Errorf("accepted invalid URL %q", base)
		}
	}
	for _, c := range []Config{{"http://silo", "", "p"}, {"http://silo", "key", ""}, {"http://silo", "key\r\nprivate", "p"}, {"http://silo", "key", "p\nother"}} {
		if c.Validate() == nil {
			t.Fatal("accepted incomplete or invalid scope")
		}
	}
}

func kindPtr(k Kind) *Kind { return &k }

func TestValidateProfileTellsUnknownProfileFromWrongURL(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		kind       Kind
		message    string
	}{
		{"unknown profile", `{"error":"not_found","message":"Profile not found"}`, InvalidConfig, "Silo profile ID was not found"},
		{"not Silo", `404 page not found`, Permanent, "check the server URL"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer upstream.Close()
			err := NewClient(Config{BaseURL: upstream.URL, APIKey: "api-secret", ProfileID: "p"}).ValidateProfile(context.Background())
			var siloErr *Error
			if !errors.As(err, &siloErr) || siloErr.Kind != tc.kind || !strings.Contains(siloErr.Error(), tc.message) {
				t.Fatalf("error = %#v", err)
			}
		})
	}
}
