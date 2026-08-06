package mapping

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCatalogFallsBackThroughAnimeLists(t *testing.T) {
	animeLists, err := parseAnimeLists([]byte(`
<anime-list>
  <anime anidbid="23" tvdbid="76885" defaulttvdbseason="1" tmdbtv="123" tmdbseason="2"/>
</anime-list>`))
	if err != nil {
		t.Fatal(err)
	}
	catalog := Catalog{
		AniBridge: Dataset{
			"anidb:23:R": {"anilist:1": {"1-26": "1-26"}},
		},
		AnimeLists: animeLists,
	}

	targets, err := catalog.Resolve(context.Background(), "tvdb", "76885", 1, 7)
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 1 || targets[0] != (Target{AniListID: 1, Episode: 7}) {
		t.Fatalf("targets = %#v", targets)
	}
}

func TestCatalogPrefersAniBridgeDirectMapping(t *testing.T) {
	animeLists, err := parseAnimeLists([]byte(`
<anime-list>
  <anime anidbid="23" tvdbid="76885" defaulttvdbseason="1"/>
</anime-list>`))
	if err != nil {
		t.Fatal(err)
	}
	catalog := Catalog{
		AniBridge: Dataset{
			"tvdb_show:76885:s1": {"anilist:2": {"1-26": "1-26"}},
			"anidb:23:R":         {"anilist:1": {"1-26": "1-26"}},
		},
		AnimeLists: animeLists,
	}

	targets, err := catalog.Resolve(context.Background(), "tvdb", "76885", 1, 7)
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 1 || targets[0].AniListID != 2 {
		t.Fatalf("direct AniBridge mapping was not preferred: %#v", targets)
	}
}

func TestCatalogFallsBackWhenDirectAniBridgeMappingIsInvalid(t *testing.T) {
	animeLists, err := parseAnimeLists([]byte(`
<anime-list>
  <anime anidbid="23" tvdbid="76885" defaulttvdbseason="1"/>
</anime-list>`))
	if err != nil {
		t.Fatal(err)
	}
	catalog := Catalog{
		AniBridge: Dataset{
			"tvdb_show:76885:s1": {"anilist:2": {"invalid": "1"}},
			"anidb:23:R":         {"anilist:1": {"1-26": "1-26"}},
		},
		AnimeLists: animeLists,
	}

	targets, err := catalog.Resolve(context.Background(), "tvdb", "76885", 1, 7)
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 1 || targets[0] != (Target{AniListID: 1, Episode: 7}) {
		t.Fatalf("targets = %#v", targets)
	}
}

func TestCatalogReportsUnavailableFallbackOnlyForDirectMiss(t *testing.T) {
	fallbackErr := errors.New("Anime-Lists offline")
	catalog := Catalog{
		AniBridge: Dataset{
			"tvdb_show:1:s1": {"anilist:10": {"1": "1"}},
		},
		animeListErr: fallbackErr,
	}
	if _, err := catalog.Resolve(context.Background(), "tvdb", "1", 1, 1); err != nil {
		t.Fatalf("direct mapping should not depend on fallback: %v", err)
	}
	if _, err := catalog.Resolve(context.Background(), "tvdb", "2", 1, 1); !errors.Is(err, fallbackErr) {
		t.Fatalf("fallback error = %v", err)
	}
}

func TestAnimeListsExplicitEpisodeMappingOverridesDefault(t *testing.T) {
	animeLists, err := parseAnimeLists([]byte(`
<anime-list>
  <anime anidbid="23" tvdbid="76885" defaulttvdbseason="1">
    <mapping-list>
      <mapping anidbseason="1" tvdbseason="1">;2-7;</mapping>
    </mapping-list>
  </anime>
</anime-list>`))
	if err != nil {
		t.Fatal(err)
	}
	targets := animeLists.Resolve("tvdb", "76885", 1, 7)
	if len(targets) != 1 || targets[0] != (aniDBTarget{ID: "23", Scope: "R", Episode: 2}) {
		t.Fatalf("targets = %#v", targets)
	}
}

func TestAnimeListsSupportsOffsetsAndMovies(t *testing.T) {
	animeLists, err := parseAnimeLists([]byte(`
<anime-list>
  <anime anidbid="23" tmdbtv="123" tmdbseason="2" tmdboffset="4"/>
  <anime anidbid="24" tmdbid="9001, 9002" imdbid="tt1234567"/>
</anime-list>`))
	if err != nil {
		t.Fatal(err)
	}
	episode := animeLists.Resolve("tmdb", "123", 2, 5)
	if len(episode) != 1 || episode[0].Episode != 1 {
		t.Fatalf("offset targets = %#v", episode)
	}
	for _, tc := range []struct {
		provider string
		id       string
	}{
		{provider: "tmdb", id: "9002"},
		{provider: "imdb", id: "tt1234567"},
	} {
		targets := animeLists.Resolve(tc.provider, tc.id, -1, 1)
		if len(targets) != 1 || targets[0].ID != "24" {
			t.Fatalf("%s movie targets = %#v", tc.provider, targets)
		}
	}
}

func TestAnimeListsClipsNegativeExplicitOffsets(t *testing.T) {
	animeLists, err := parseAnimeLists([]byte(`
<anime-list>
  <anime anidbid="23" tvdbid="100" defaulttvdbseason="1">
    <mapping-list>
      <mapping anidbseason="1" tvdbseason="1" start="1" end="3" offset="-1"/>
    </mapping-list>
  </anime>
</anime-list>`))
	if err != nil {
		t.Fatal(err)
	}
	targets := animeLists.Resolve("tvdb", "100", 1, 1)
	if len(targets) != 1 || targets[0].Episode != 2 {
		t.Fatalf("targets = %#v", targets)
	}
}

func TestCatalogRejectsAmbiguousAnimeListsFallback(t *testing.T) {
	animeLists, err := parseAnimeLists([]byte(`
<anime-list>
  <anime anidbid="1" tvdbid="100" defaulttvdbseason="1"/>
  <anime anidbid="2" tvdbid="100" defaulttvdbseason="1"/>
</anime-list>`))
	if err != nil {
		t.Fatal(err)
	}
	catalog := Catalog{
		AniBridge: Dataset{
			"anidb:1:R": {"anilist:10": {}},
			"anidb:2:R": {"anilist:20": {}},
		},
		AnimeLists: animeLists,
	}
	if _, err := catalog.Resolve(context.Background(), "tvdb", "100", 1, 1); err == nil {
		t.Fatal("expected ambiguous fallback to fail")
	} else if !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("error = %v", err)
	}
}

func TestCatalogRetriesWhenAnOverlappingCandidateCannotBeChecked(t *testing.T) {
	animeLists, err := parseAnimeLists([]byte(`
<anime-list>
  <anime anidbid="1" tvdbid="100" defaulttvdbseason="1"/>
  <anime anidbid="2" tvdbid="100" defaulttvdbseason="1"/>
</anime-list>`))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "offline", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	arm := newARMClient(server.Client(), time.Hour)
	arm.baseURL = server.URL
	catalog := Catalog{
		AniBridge: Dataset{
			"anidb:1:R": {"anilist:10": {}},
		},
		AnimeLists: animeLists,
		arm:        arm,
	}

	_, err = catalog.Resolve(context.Background(), "tvdb", "100", 1, 1)
	var temporary *TemporaryError
	if !errors.As(err, &temporary) {
		t.Fatalf("error = %v; want TemporaryError", err)
	}
}

func TestCatalogDoesNotProjectAniDBSpecialsThroughARM(t *testing.T) {
	animeLists, err := parseAnimeLists([]byte(`
<anime-list>
  <anime anidbid="23" tvdbid="100" defaulttvdbseason="1">
    <mapping-list>
      <mapping anidbseason="0" tvdbseason="0">;3-1;</mapping>
    </mapping-list>
  </anime>
</anime-list>`))
	if err != nil {
		t.Fatal(err)
	}
	armCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		armCalls++
		_, _ = w.Write([]byte(`{"anidb":23,"anilist":1}`))
	}))
	defer server.Close()
	arm := newARMClient(server.Client(), time.Hour)
	arm.baseURL = server.URL

	targets, err := (Catalog{AnimeLists: animeLists, arm: arm}).Resolve(
		context.Background(), "tvdb", "100", 0, 1,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 0 {
		t.Fatalf("targets = %#v", targets)
	}
	if armCalls != 0 {
		t.Fatalf("ARM calls = %d; want 0", armCalls)
	}
}
