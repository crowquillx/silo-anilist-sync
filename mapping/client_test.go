package mapping

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestClientValidatesSchemaMajor(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"$meta":{"schema_version":"4.0.0"}}`))
	}))
	defer server.Close()
	client := NewClient(server.Client())
	client.url = server.URL
	if _, err := client.downloadAniBridge(context.Background()); err == nil {
		t.Fatal("expected unsupported schema to fail")
	}
}

func TestClientKeepsLastKnownGoodDataset(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/anime-list.xml" {
			_, _ = w.Write([]byte(`<anime-list/>`))
			return
		}
		calls++
		if calls == 1 {
			_, _ = w.Write([]byte(`{"$meta":{"schema_version":"3.0.3"},"tmdb_movie:1":{"anilist:2":{"1":"1"}}}`))
			return
		}
		http.Error(w, "offline", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client := NewClient(server.Client())
	client.url = server.URL
	client.animeListsURL = server.URL + "/anime-list.xml"
	client.maxAge = time.Nanosecond
	first, err := client.Dataset(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	second, err := client.Dataset(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first.AniBridge["tmdb_movie:1"] == nil || second.AniBridge["tmdb_movie:1"] == nil {
		t.Fatalf("last known good mapping was not retained")
	}
}

func TestClientColdStartFallsBackWhenAniBridgeIsOffline(t *testing.T) {
	var armCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/anibridge":
			http.Error(w, "offline", http.StatusServiceUnavailable)
		case "/anime-list.xml":
			_, _ = w.Write([]byte(`<anime-list><anime anidbid="23" tvdbid="76885" defaulttvdbseason="1"/></anime-list>`))
		case "/api/v2/ids":
			armCalls++
			if r.URL.Query().Get("source") != "anidb" || r.URL.Query().Get("id") != "23" {
				t.Fatalf("ARM query = %v", r.URL.Query())
			}
			_, _ = w.Write([]byte(`{"anidb":23,"anilist":1}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := NewClient(server.Client())
	client.url = server.URL + "/anibridge"
	client.animeListsURL = server.URL + "/anime-list.xml"
	client.arm.baseURL = server.URL

	catalog, err := client.Dataset(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		targets, err := catalog.Resolve(context.Background(), "tvdb", "76885", 1, 7)
		if err != nil {
			t.Fatal(err)
		}
		if len(targets) != 1 || targets[0] != (Target{AniListID: 1, Episode: 7}) {
			t.Fatalf("targets = %#v", targets)
		}
	}
	if armCalls != 1 {
		t.Fatalf("ARM calls = %d; want 1 cached lookup", armCalls)
	}
}

func TestClientBacksOffAfterFailedDownload(t *testing.T) {
	aniBridgeCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/anime-list.xml" {
			_, _ = w.Write([]byte(`<anime-list/>`))
			return
		}
		aniBridgeCalls++
		if aniBridgeCalls == 1 {
			_, _ = w.Write([]byte(`{"$meta":{"schema_version":"3.0.3"},"tmdb_movie:1":{"anilist:2":{}}}`))
			return
		}
		http.Error(w, "offline", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	client := NewClient(server.Client())
	client.url = server.URL
	client.animeListsURL = server.URL + "/anime-list.xml"
	client.now = func() time.Time { return now }
	if _, err := client.Dataset(context.Background()); err != nil {
		t.Fatal(err)
	}

	now = now.Add(25 * time.Hour)
	for range 3 {
		catalog, err := client.Dataset(context.Background())
		if err != nil || catalog.AniBridge["tmdb_movie:1"] == nil {
			t.Fatalf("stale dataset should be kept: %v", err)
		}
	}
	if aniBridgeCalls != 2 {
		t.Fatalf("AniBridge downloads = %d; want one retry during backoff", aniBridgeCalls)
	}
	status := client.Status()[0]
	if status.Name != "AniBridge" || status.LastError == "" || !status.RetryAt.Equal(now.Add(retryBackoffBase)) || status.Entries != 1 {
		t.Fatalf("status = %#v", status)
	}

	now = now.Add(retryBackoffBase)
	if _, err := client.Dataset(context.Background()); err != nil {
		t.Fatal(err)
	}
	if aniBridgeCalls != 3 {
		t.Fatalf("AniBridge downloads = %d; want a retry after the backoff", aniBridgeCalls)
	}
	if got := client.Status()[0].RetryAt; !got.Equal(now.Add(2 * retryBackoffBase)) {
		t.Fatalf("second backoff ends %v; want doubled", got)
	}
}

func TestCatalogReverseUsesIndex(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/anime-list.xml" {
			_, _ = w.Write([]byte(`<anime-list/>`))
			return
		}
		_, _ = w.Write([]byte(`{"$meta":{"schema_version":"3.0.3"},"tvdb_show:9:s1":{"anilist:2":{"1-12":"1-12"}}}`))
	}))
	defer server.Close()
	client := NewClient(server.Client())
	client.url = server.URL
	client.animeListsURL = server.URL + "/anime-list.xml"
	catalog, err := client.Dataset(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if catalog.reverse == nil {
		t.Fatal("catalog has no reverse index")
	}
	if sources := catalog.Reverse(2, 3); len(sources) != 3 || sources[2].Episode != 3 {
		t.Fatalf("sources = %#v", sources)
	}
}
