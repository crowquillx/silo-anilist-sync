package mapping

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestARMClientResolvesAndCachesAniListID(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Query().Get("include") != "anidb,anilist" {
			t.Fatalf("include = %q", r.URL.Query().Get("include"))
		}
		_, _ = w.Write([]byte(`{"anidb":23,"anilist":1}`))
	}))
	defer server.Close()

	client := newARMClient(server.Client(), time.Hour)
	client.baseURL = server.URL
	for range 2 {
		id, found, err := client.ResolveAniDB(context.Background(), "23")
		if err != nil {
			t.Fatal(err)
		}
		if !found || id != 1 {
			t.Fatalf("ResolveAniDB() = %d, %v", id, found)
		}
	}
	if calls != 1 {
		t.Fatalf("calls = %d; want 1", calls)
	}
}

func TestARMClientCachesMissingMapping(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_, _ = w.Write([]byte(`null`))
	}))
	defer server.Close()

	client := newARMClient(server.Client(), time.Hour)
	client.baseURL = server.URL
	for range 2 {
		if id, found, err := client.ResolveAniDB(context.Background(), "23"); err != nil || found || id != 0 {
			t.Fatalf("ResolveAniDB() = %d, %v, %v", id, found, err)
		}
	}
	if calls != 1 {
		t.Fatalf("calls = %d; want 1", calls)
	}
}

func TestARMClientReturnsTemporaryError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "offline", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client := newARMClient(server.Client(), time.Hour)
	client.baseURL = server.URL
	_, _, err := client.ResolveAniDB(context.Background(), "23")
	var temporary *TemporaryError
	if !errors.As(err, &temporary) {
		t.Fatalf("error = %v; want TemporaryError", err)
	}
}

func TestARMClientRejectsMismatchedAniDBID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"anidb":24,"anilist":1}`))
	}))
	defer server.Close()

	client := newARMClient(server.Client(), time.Hour)
	client.baseURL = server.URL
	_, _, err := client.ResolveAniDB(context.Background(), "23")
	var temporary *TemporaryError
	if !errors.As(err, &temporary) {
		t.Fatalf("error = %v; want TemporaryError", err)
	}
}
