package silostate

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func catalogClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer key" || r.Header.Get("X-Profile-Id") != "p1" {
			t.Error("incorrect credential or profile")
		}
		handler(w, r)
	}))
	t.Cleanup(server.Close)
	return NewClient(Config{BaseURL: server.URL, APIKey: "key", ProfileID: "p1"})
}

func TestSeriesFollowsCursors(t *testing.T) {
	client := catalogClient(t, func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if r.URL.Path != "/api/v2/catalog" || q.Get("type") != "series" || q.Get("source") != "query" {
			t.Errorf("unexpected request %s", r.URL)
		}
		switch q.Get("cursor") {
		case "":
			_, _ = w.Write([]byte(`{"items":[{"content_id":"s1","type":"series","tvdb_id":"100"},{"content_id":"m1","type":"movie"}],"page":{"next_cursor":"c2","has_more":true}}`))
		case "c2":
			_, _ = w.Write([]byte(`{"items":[{"content_id":"s2","type":"series","tmdb_id":"500","imdb_id":"tt1"}],"page":{"has_more":false}}`))
		default:
			t.Errorf("unexpected cursor %q", q.Get("cursor"))
		}
	})
	series, err := client.Series(context.Background())
	if err != nil || len(series) != 2 || series[0].TvdbID != "100" || series[1].TmdbID != "500" || series[1].ImdbID != "tt1" {
		t.Fatalf("series=%v err=%v", series, err)
	}
}

func TestSeriesRejectsACursorThatDoesNotAdvance(t *testing.T) {
	client := catalogClient(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"items":[],"page":{"next_cursor":"same","has_more":true}}`))
	})
	if _, err := client.Series(context.Background()); err == nil {
		t.Fatal("a looping cursor was followed")
	}
}

func TestEpisodesAndFileNames(t *testing.T) {
	client := catalogClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v2/catalog/series/s1/seasons":
			_, _ = w.Write([]byte(`{"items":[{"season_number":0},{"season_number":1}]}`))
		case "/api/v2/catalog/series/s1/seasons/1/episodes":
			if r.URL.Query().Get("cursor") == "" {
				_, _ = w.Write([]byte(`{"items":[{"content_id":"e1","season_number":1,"episode_number":1,"tvdb_id":"7"}],"page":{"next_cursor":"n","has_more":true}}`))
				return
			}
			_, _ = w.Write([]byte(`{"items":[{"content_id":"e2","season_number":1,"episode_number":2,"tmdb_id":"8"}]}`))
		case "/api/v2/catalog/items/e1/versions":
			_, _ = w.Write([]byte(`{"items":[{"file_name":"a.mkv","file_path":""},{"file_name":"b.mkv"}]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	ctx := context.Background()
	seasons, found, err := client.Seasons(ctx, "s1")
	if err != nil || !found || len(seasons) != 2 || seasons[1] != 1 {
		t.Fatalf("seasons=%v found=%v err=%v", seasons, found, err)
	}
	episodes, found, err := client.Episodes(ctx, "s1", 1)
	if err != nil || !found || len(episodes) != 2 || episodes[0].TvdbID != "7" || episodes[1].TmdbID != "8" || episodes[1].EpisodeNumber != 2 {
		t.Fatalf("episodes=%v found=%v err=%v", episodes, found, err)
	}
	names, found, err := client.FileNames(ctx, "e1")
	if err != nil || !found || len(names) != 2 || names[1] != "b.mkv" {
		t.Fatalf("names=%v found=%v err=%v", names, found, err)
	}
	if _, found, err := client.FileNames(ctx, "gone"); err != nil || found {
		t.Fatalf("missing item: found=%v err=%v", found, err)
	}
	if _, found, err := client.Seasons(ctx, "gone"); err != nil || found {
		t.Fatalf("missing series: found=%v err=%v", found, err)
	}
}

func TestCatalogErrorsAreSafe(t *testing.T) {
	for status, kind := range map[int]Kind{401: InvalidCredential, 403: PermissionDenied, 429: RateLimited, 500: Temporary, 400: Permanent} {
		client := catalogClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(status)
			_, _ = w.Write([]byte("private key body"))
		})
		_, _, err := client.FileNames(context.Background(), "e1")
		siloErr, ok := err.(*Error)
		if !ok || siloErr.Kind != kind {
			t.Errorf("status %d: err=%v", status, err)
			continue
		}
		if msg := siloErr.Error(); msg == "" || strings.Contains(msg, "private") || strings.Contains(msg, "key body") {
			t.Errorf("status %d: unsafe message %q", status, msg)
		}
	}
}
