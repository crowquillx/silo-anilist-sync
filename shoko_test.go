package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"github.com/crowquillx/silo-anilist-sync/anilist"
	"github.com/crowquillx/silo-anilist-sync/mapping"
	"github.com/crowquillx/silo-anilist-sync/silostate"
)

const vfsLink = "Show - S03E05 [Shoko Group=5] [Shoko Series=7] [AniDB Anime=23] [Shoko Episode=70] [Shoko File=700].mkv"

func TestShokoAnimeNeedsEveryFileToAgree(t *testing.T) {
	for _, tc := range []struct {
		name  string
		names []string
		want  string
	}{
		{"one link", []string{vfsLink}, "23"},
		{"two links of one anime", []string{vfsLink, strings.Replace(vfsLink, "File=700", "File=701", 1)}, "23"},
		{"links of two anime", []string{vfsLink, strings.Replace(vfsLink, "Anime=23", "Anime=24", 1)}, ""},
		{"link merged with an ordinary file", []string{vfsLink, "Show - S03E05.mkv"}, ""},
		{"ordinary file", []string{"Show - S03E05.mkv"}, ""},
		{"no files", nil, ""},
		{"zero is not an ID", []string{"Show [AniDB Anime=0].mkv"}, ""},
	} {
		if got := shokoAnime(tc.names); got != tc.want {
			t.Errorf("%s: shokoAnime = %q want %q", tc.name, got, tc.want)
		}
	}
}

func TestShokoAniDBEpisodeNumbers(t *testing.T) {
	for _, tc := range []struct {
		season, episode int
		scope           string
		number          int
		ok              bool
	}{
		{1, 5, "R", 5, true},
		{3, 12, "R", 12, true},
		{0, 3, "S", 3, true},
		{0, 1003, "S", 3, true},
		{0, 2399, "S", 399, true},
		{0, 401, "", 0, false},  // credits
		{0, 1601, "", 0, false}, // a later series' trailer
		{0, 1901, "", 0, false}, // other extra
		{0, 1000, "", 0, false},
		{2, 0, "", 0, false},
		{-1, 1, "", 0, false},
	} {
		scope, number, ok := shokoAniDBEpisode(tc.season, tc.episode)
		if scope != tc.scope || number != tc.number || ok != tc.ok {
			t.Errorf("S%02dE%02d = %q %d %v, want %q %d %v", tc.season, tc.episode, scope, number, ok, tc.scope, tc.number, tc.ok)
		}
	}
}

// shokoMappingTransport serves an AniBridge dataset in which AniDB anime 23
// maps to AniList 30, and TVDB show 100 season 3 maps to AniList 11, so a
// test can tell an AniDB mapping from a TVDB fallback. ARM knows nothing.
type shokoMappingTransport struct{}

func (shokoMappingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	status, body := http.StatusNotFound, ""
	switch {
	case req.URL.String() == mapping.DefaultURL:
		status, body = http.StatusOK, `{"$meta":{"schema_version":"3.0.0"},`+
			`"anidb:23:R":{"anilist:30":{"1-12":"1-12"}},"anidb:23:S":{"anilist:31":{"1-2":"1-2"}},`+
			`"tvdb_show:100:s3":{"anilist:11":{"1-12":"1-12"}},"tvdb_show:100:s0":{"anilist:12":{"1-5":"1-5"}}}`
	case req.URL.String() == mapping.DefaultAnimeListsURL:
		status, body = http.StatusOK, `<anime-list/>`
	case strings.HasPrefix(req.URL.String(), mapping.DefaultARMURL):
		status, body = http.StatusOK, `null`
	}
	return &http.Response{StatusCode: status, Status: http.StatusText(status), Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
}

// fakeAniListWrites records the progress each AniList write saves.
func fakeAniListWrites(t *testing.T) (*[]map[string]any, func()) {
	t.Helper()
	var mu sync.Mutex
	writes := []map[string]any{}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Query     string         `json:"query"`
			Variables map[string]any `json:"variables"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if strings.Contains(req.Query, "SaveMediaListEntry") {
			mu.Lock()
			writes = append(writes, req.Variables)
			mu.Unlock()
			_, _ = w.Write([]byte(`{"data":{"SaveMediaListEntry":{"id":1}}}`))
			return
		}
		_, _ = w.Write([]byte(`{"data":{"Media":{"id":1,"episodes":12,"mediaListEntry":null}}}`))
	}))
	old := anilist.Endpoint
	anilist.Endpoint = upstream.URL
	return &writes, func() {
		anilist.Endpoint = old
		upstream.Close()
	}
}

func verifiedContext(siloURL string) *pluginv1.WatchSyncAuthenticatedContext {
	return &pluginv1.WatchSyncAuthenticatedContext{
		CapabilityId: siloVerifiedCapability,
		Credentials: &pluginv1.WatchSyncCredentials{AccessToken: "anilist-secret", SecretAttributes: map[string]string{
			"user_id": "7", "silo.base_url": siloURL, "silo.api_key": "silo-secret", "silo.profile_id": "profile-a",
		}},
	}
}

func finishedEpisode(id, itemID string, season, episode int32) *pluginv1.WatchSyncEvent {
	return &pluginv1.WatchSyncEvent{
		EventId: id, Operation: pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_WATCHED,
		Origin: pluginv1.WatchSyncOrigin_WATCH_SYNC_ORIGIN_PLAYBACK_COMPLETION,
		Media: &pluginv1.WatchSyncMedia{
			MediaItemId: itemID, MediaType: pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE,
			SeasonNumber: season, EpisodeNumber: episode, SeriesExternalIds: map[string]string{"tvdb": "100"},
		},
	}
}

func TestVerifiedExportMapsShokoEpisodesByAniDB(t *testing.T) {
	versions := map[string]string{
		"vfs-regular": `{"items":[{"file_name":"` + vfsLink + `"}]}`,
		"vfs-special": `{"items":[{"file_name":"Show - S00E1002 [Shoko Group=5] [Shoko Series=8] [AniDB Anime=23] [Shoko Episode=80] [Shoko File=800].mkv"}]}`,
		"vfs-credits": `{"items":[{"file_name":"Show - S00E1401 [Shoko Group=5] [Shoko Series=8] [AniDB Anime=23] [Shoko Episode=81] [Shoko File=801].mkv"}]}`,
		"vfs-unknown": `{"items":[{"file_name":"Show - S03E05 [Shoko Group=5] [Shoko Series=9] [AniDB Anime=99] [Shoko Episode=90] [Shoko File=900].mkv"}]}`,
		"ordinary":    `{"items":[{"file_name":"Show - S03E05.mkv"}]}`,
	}
	reads := map[string]int{}
	var mu sync.Mutex
	silo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer silo-secret" || r.Header.Get("X-Profile-Id") != "profile-a" {
			t.Error("incorrect Silo credential or profile")
		}
		id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v2/catalog/items/"), "/versions")
		mu.Lock()
		reads[id]++
		mu.Unlock()
		switch body, ok := versions[id]; {
		case ok:
			_, _ = w.Write([]byte(body))
		case id == "broken":
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"not_found"}`))
		}
	}))
	defer silo.Close()
	writes, done := fakeAniListWrites(t)
	defer done()

	srv := &server{mappings: mapping.NewClient(&http.Client{Transport: shokoMappingTransport{}})}
	events := []*pluginv1.WatchSyncEvent{
		finishedEpisode("regular", "vfs-regular", 3, 5),
		finishedEpisode("again", "vfs-regular", 3, 5),
		finishedEpisode("special", "vfs-special", 0, 1002),
		finishedEpisode("credits", "vfs-credits", 0, 1401),
		finishedEpisode("unknown", "vfs-unknown", 3, 5),
		finishedEpisode("ordinary", "ordinary", 3, 5),
		finishedEpisode("missing", "gone", 3, 5),
		finishedEpisode("broken", "broken", 3, 5),
	}
	resp, err := srv.ApplyEvents(context.Background(), &pluginv1.WatchSyncApplyEventsRequest{Context: verifiedContext(silo.URL), Events: events})
	if err != nil || resp.GetFault() != nil || len(resp.GetResults()) != len(events) {
		t.Fatalf("response=%v error=%v", resp, err)
	}
	status := map[string]*pluginv1.WatchSyncApplyResult{}
	for _, result := range resp.GetResults() {
		status[result.GetEventId()] = result
	}
	applied := pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED
	rejectedStatus := pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED
	for id, want := range map[string]pluginv1.WatchSyncApplyStatus{
		"regular": applied, "again": applied, "special": applied, "ordinary": applied,
		"credits": rejectedStatus, "unknown": rejectedStatus, "missing": rejectedStatus,
		"broken": pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_RETRY,
	} {
		if status[id].GetStatus() != want {
			t.Errorf("%s: %v want %v", id, status[id], want)
		}
	}
	if !strings.Contains(status["unknown"].GetFault().GetSafeMessage(), "AniDB to AniList") {
		t.Errorf("unknown: message %q", status["unknown"].GetFault().GetSafeMessage())
	}
	if !strings.Contains(status["credits"].GetFault().GetSafeMessage(), "no AniDB episode number") {
		t.Errorf("credits: message %q", status["credits"].GetFault().GetSafeMessage())
	}
	// AniDB 23 episode 5 is AniList 30, its special 2 is AniList 31, and
	// only the ordinary file uses TVDB 100 season 3, which is AniList 11.
	// The unmapped VFS episode must not fall back to that TVDB mapping.
	got := map[float64][]float64{}
	for _, write := range *writes {
		got[write["mediaId"].(float64)] = append(got[write["mediaId"].(float64)], write["progress"].(float64))
	}
	if len(got[30]) != 2 || got[30][0] != 5 || len(got[31]) != 1 || got[31][0] != 2 || len(got[11]) != 1 || len(got) != 3 {
		t.Errorf("AniList writes = %v", got)
	}
	if reads["vfs-regular"] != 1 {
		t.Errorf("a repeated item read its files %d times, want 1", reads["vfs-regular"])
	}
}

func TestStandardExportDoesNotReadSilo(t *testing.T) {
	silo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("standard connection read Silo: %s", r.URL.Path)
	}))
	defer silo.Close()
	writes, done := fakeAniListWrites(t)
	defer done()
	auth := verifiedContext(silo.URL)
	auth.CapabilityId = "anilist"
	srv := &server{mappings: mapping.NewClient(&http.Client{Transport: shokoMappingTransport{}})}
	resp, err := srv.ApplyEvents(context.Background(), &pluginv1.WatchSyncApplyEventsRequest{Context: auth, Events: []*pluginv1.WatchSyncEvent{finishedEpisode("e", "vfs-regular", 3, 5)}})
	if err != nil || resp.GetResults()[0].GetStatus() != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED || len(*writes) != 1 || (*writes)[0]["mediaId"] != float64(11) {
		t.Fatalf("response=%v error=%v writes=%v", resp, err, *writes)
	}
}

// fakeCatalog is a Silo catalog for index tests.
type fakeCatalog struct {
	series   []silostate.Series
	seasons  map[string][]int
	episodes map[string]map[int][]silostate.Episode
	files    map[string][]string
	fail     string
	mu       sync.Mutex
	reads    int
}

func (c *fakeCatalog) count(what string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reads++
	if c.fail == what {
		return errors.New("silo unavailable")
	}
	return nil
}

func (c *fakeCatalog) Series(context.Context) ([]silostate.Series, error) {
	return c.series, c.count("series")
}

func (c *fakeCatalog) Seasons(_ context.Context, id string) ([]int, bool, error) {
	seasons, ok := c.seasons[id]
	return seasons, ok, c.count("seasons")
}

func (c *fakeCatalog) Episodes(_ context.Context, id string, season int) ([]silostate.Episode, bool, error) {
	episodes, ok := c.episodes[id][season]
	return episodes, ok, c.count("episodes")
}

func (c *fakeCatalog) FileNames(_ context.Context, id string) ([]string, bool, error) {
	names, ok := c.files[id]
	return names, ok, c.count("files")
}

func link(anime, rest string) string {
	return "Show - " + rest + " [Shoko Group=5] [AniDB Anime=" + anime + "] [Shoko File=1].mkv"
}

// testCatalog has one VFS show, group 5, whose season 1 is AniDB 23 and
// season 2 AniDB 24, with one special of each, and one ordinary show.
func testCatalog() *fakeCatalog {
	return &fakeCatalog{
		series: []silostate.Series{
			{ContentID: "vfs", Type: "series", TvdbID: "100", TmdbID: "500"},
			{ContentID: "plain", Type: "series", TvdbID: "200"},
		},
		seasons: map[string][]int{"vfs": {0, 1, 2}, "plain": {1, 2}},
		episodes: map[string]map[int][]silostate.Episode{
			"vfs": {
				0: {{ContentID: "v-s1", SeasonNumber: 0, EpisodeNumber: 1, TmdbID: "9001"}, {ContentID: "v-s2", SeasonNumber: 0, EpisodeNumber: 1001, TvdbID: "8002"}, {ContentID: "v-c", SeasonNumber: 0, EpisodeNumber: 401, TvdbID: "8003"}},
				1: {{ContentID: "v-1-1", SeasonNumber: 1, EpisodeNumber: 1, TvdbID: "7001"}, {ContentID: "v-1-2", SeasonNumber: 1, EpisodeNumber: 2, TmdbID: "6002"}},
				2: {{ContentID: "v-2-1", SeasonNumber: 2, EpisodeNumber: 1, ImdbID: "tt0000001"}, {ContentID: "v-2-2", SeasonNumber: 2, EpisodeNumber: 2}},
			},
			"plain": {1: {{ContentID: "p-1-1", EpisodeNumber: 1, TvdbID: "5001"}}, 2: {{ContentID: "p-2-1", EpisodeNumber: 1}}},
		},
		files: map[string][]string{
			"v-1-1": {link("23", "S01E01")},
			"v-2-1": {link("24", "S02E01")},
			"p-1-1": {"Plain - S01E01.mkv"},
		},
	}
}

func TestBuildShokoIndexFindsVFSShowsAndAniDBEpisodes(t *testing.T) {
	catalog := testCatalog()
	index, err := buildShokoIndex(context.Background(), catalog, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	if !index.isShow(map[string]string{"tvdb": "100"}) || !index.isShow(map[string]string{"tmdb": "500"}) || index.isShow(map[string]string{"tvdb": "200"}) {
		t.Fatalf("shows = %v", index.shows)
	}
	for _, tc := range []struct {
		anime, scope string
		number       int
		content      string
	}{
		{"23", "R", 1, "v-1-1"}, {"23", "R", 2, "v-1-2"}, {"24", "R", 1, "v-2-1"}, {"24", "R", 2, "v-2-2"},
		{"23", "S", 1, "v-s1"}, {"24", "S", 1, "v-s2"},
	} {
		episode, ok := index.episode(tc.anime, tc.scope, tc.number)
		if !ok || episode.contentID != tc.content {
			t.Errorf("%s %s %d = %v %v want %s", tc.anime, tc.scope, tc.number, episode, ok, tc.content)
		}
	}
	if _, ok := index.episode("23", "S", 401); ok {
		t.Error("credits were indexed as a special")
	}
	if len(index.episodes) != 6 {
		t.Errorf("episodes = %d want 6", len(index.episodes))
	}
}

func TestBuildShokoIndexFailsWhole(t *testing.T) {
	for _, what := range []string{"series", "seasons", "episodes", "files"} {
		catalog := testCatalog()
		catalog.fail = what
		if index, err := buildShokoIndex(context.Background(), catalog, time.Now()); err == nil || index != nil {
			t.Errorf("%s failure: index=%v err=%v", what, index, err)
		}
	}
}

func TestRemoteStatesForShokoShowsUseAniDBEpisodes(t *testing.T) {
	index, err := buildShokoIndex(context.Background(), testCatalog(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	vfsEntry := anilist.ListEntry{ID: 9, Status: "CURRENT", Progress: 2, UpdatedAt: 1756500000}
	vfsEntry.Media.ID = 42
	vfsEntry.Media.Format = "TV"
	onlyTVDB := anilist.ListEntry{ID: 10, Status: "CURRENT", Progress: 1}
	onlyTVDB.Media.ID = 43
	onlyTVDB.Media.Format = "TV"
	plain := anilist.ListEntry{ID: 11, Status: "CURRENT", Progress: 1}
	plain.Media.ID = 44
	plain.Media.Format = "TV"
	dataset := mapping.Catalog{AniBridge: mapping.Dataset{
		// AniList 42 is AniDB 24, Shoko's season 2, which TVDB numbers as
		// season 1 of show 100.
		"anidb:24:R":       {"anilist:42": {"1-12": "1-12"}},
		"tvdb_show:100:s1": {"anilist:42": {"1-12": "1-12"}, "anilist:43": {"1-12": "1-12"}},
		"tvdb_show:200:s1": {"anilist:44": {"1-12": "1-12"}},
	}}
	states, unmapped, skipped := remoteStatesWithShoko([]anilist.ListEntry{vfsEntry, onlyTVDB, plain}, dataset, index)
	if len(unmapped) != 0 || skipped != 1 {
		t.Fatalf("unmapped=%d skipped=%d", len(unmapped), skipped)
	}
	keys := map[string]*pluginv1.WatchSyncRemoteState{}
	for _, state := range states {
		keys[state.GetProviderItemKey()] = state
	}
	// AniDB 24 episode 2 has no external IDs in Silo, so only episode 1 is
	// emitted; neither entry keeps a TVDB 100 state.
	if len(states) != 2 || keys["anilist:9:anidb:24:r1"] == nil || keys["anilist:11:s1:e1"] == nil {
		t.Fatalf("states = %v", keys)
	}
	shoko := keys["anilist:9:anidb:24:r1"].GetMedia()
	if shoko.GetExternalIds()["imdb"] != "tt0000001" || len(shoko.GetSeriesExternalIds()) != 0 || shoko.GetSeasonNumber() != 0 || shoko.GetEpisodeNumber() != 0 {
		t.Fatalf("Shoko state media = %v", shoko)
	}
	if keys["anilist:9:anidb:24:r1"].GetWatched().GetLastWatchedAt().AsTime().Unix() != 1756500000 {
		t.Fatal("Shoko state lost its watched time")
	}
	if len(shokoSkippedWarnings(skipped)) != 1 {
		t.Fatal("skipped entry has no warning")
	}
	// Without an index, the TVDB states are kept as before.
	states, _, skipped = remoteStatesWithShoko([]anilist.ListEntry{vfsEntry, onlyTVDB, plain}, dataset, nil)
	if len(states) != 4 || skipped != 0 {
		t.Fatalf("states without index = %d skipped=%d", len(states), skipped)
	}
}

func TestShokoIndexCacheWaitsThenAsksSiloToRetry(t *testing.T) {
	cache := newShokoIndexCache()
	cache.wait = 20 * time.Millisecond
	release := make(chan struct{})
	var reads atomic.Int32
	read := func(context.Context) (*shokoIndex, error) {
		reads.Add(1)
		<-release
		index := newShokoIndex()
		index.builtAt = time.Now()
		return index, nil
	}
	if _, err := cache.get(context.Background(), "a", read); !errors.Is(err, errShokoIndexPending) {
		t.Fatalf("err = %v", err)
	}
	if _, err := cache.get(context.Background(), "a", read); !errors.Is(err, errShokoIndexPending) {
		t.Fatalf("second err = %v", err)
	}
	close(release)
	cache.wait = time.Second
	index, err := cache.get(context.Background(), "a", read)
	for err != nil && errors.Is(err, errShokoIndexPending) {
		index, err = cache.get(context.Background(), "a", read)
	}
	if err != nil || index == nil || reads.Load() != 1 {
		t.Fatalf("index=%v err=%v reads=%d", index, err, reads.Load())
	}
	if fault := shokoIndexFault(errShokoIndexPending); fault.GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY || fault.GetRetryAfter() == nil {
		t.Fatalf("pending fault = %v", fault)
	}
}

func TestShokoIndexCacheBacksOffAfterFailure(t *testing.T) {
	cache := newShokoIndexCache()
	now := time.Unix(1000, 0)
	cache.now = func() time.Time { return now }
	reads := 0
	failing := func(context.Context) (*shokoIndex, error) {
		reads++
		return nil, errors.New("silo down")
	}
	if _, err := cache.get(context.Background(), "a", failing); err == nil || errors.Is(err, errShokoIndexPending) {
		t.Fatalf("err = %v", err)
	}
	if _, err := cache.get(context.Background(), "a", failing); err == nil || reads != 1 {
		t.Fatalf("retried during backoff: err=%v reads=%d", err, reads)
	}
	now = now.Add(shokoIndexRetry)
	if _, err := cache.get(context.Background(), "a", failing); err == nil || reads != 2 {
		t.Fatalf("no retry after backoff: err=%v reads=%d", err, reads)
	}
}

func TestVerifiedImportFailsSafeWhenSiloCannotBeRead(t *testing.T) {
	silo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("private body"))
	}))
	defer silo.Close()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("AniList was read before the Shoko shows were known")
	}))
	defer upstream.Close()
	old := anilist.Endpoint
	anilist.Endpoint = upstream.URL
	t.Cleanup(func() { anilist.Endpoint = old })
	srv := &server{mappings: mapping.NewClient(&http.Client{Transport: shokoMappingTransport{}})}
	resp, err := srv.ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{Context: verifiedContext(silo.URL), PageSize: 10})
	fault := resp.GetFault()
	if err != nil || fault == nil || len(resp.GetItems()) != 0 || fault.GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY {
		t.Fatalf("response=%v error=%v", resp, err)
	}
	if strings.Contains(fault.GetSafeMessage(), "private") || strings.Contains(fault.GetSafeMessage(), "silo-secret") {
		t.Fatal("unsafe error")
	}
}

func TestVerifiedImportNamesShokoEpisodesByTheirOwnIDs(t *testing.T) {
	silo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v2/catalog":
			_, _ = w.Write([]byte(`{"items":[{"content_id":"vfs","type":"series","tvdb_id":"100"}]}`))
		case "/api/v2/catalog/series/vfs/seasons":
			_, _ = w.Write([]byte(`{"items":[{"season_number":1},{"season_number":2}]}`))
		case "/api/v2/catalog/series/vfs/seasons/1/episodes":
			_, _ = w.Write([]byte(`{"items":[{"content_id":"v-1-1","season_number":1,"episode_number":1,"tvdb_id":"7001"}]}`))
		case "/api/v2/catalog/series/vfs/seasons/2/episodes":
			_, _ = w.Write([]byte(`{"items":[{"content_id":"v-2-1","season_number":2,"episode_number":1,"tvdb_id":"7101"}]}`))
		case "/api/v2/catalog/items/v-1-1/versions":
			_, _ = w.Write([]byte(`{"items":[{"file_name":"` + link("22", "S01E01") + `"}]}`))
		case "/api/v2/catalog/items/v-2-1/versions":
			_, _ = w.Write([]byte(`{"items":[{"file_name":"` + link("23", "S02E01") + `"}]}`))
		default:
			t.Errorf("unexpected Silo request %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer silo.Close()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"MediaListCollection": map[string]any{
			"lists": []map[string]any{{"entries": []map[string]any{{
				"id": 1, "mediaId": 30, "status": "CURRENT", "progress": 1, "updatedAt": 1756500000,
				"media": map[string]any{"id": 30, "format": "TV", "episodes": 12, "title": map[string]any{"romaji": "Alpha"}},
			}}}},
		}}})
	}))
	defer upstream.Close()
	old := anilist.Endpoint
	anilist.Endpoint = upstream.URL
	t.Cleanup(func() { anilist.Endpoint = old })

	srv := &server{mappings: mapping.NewClient(&http.Client{Transport: shokoMappingTransport{}})}
	resp, err := srv.ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{Context: verifiedContext(silo.URL), PageSize: 10})
	if err != nil || resp.GetFault() != nil {
		t.Fatalf("response=%v error=%v", resp, err)
	}
	// AniList 30 is AniDB 23 episode 1, Shoko's season 2. TVDB 100 season 3
	// is not consulted, and the state names only that episode's own ID.
	items := resp.GetItems()
	if len(items) != 1 || items[0].GetProviderItemKey() != "anilist:1:anidb:23:r1" || items[0].GetMedia().GetExternalIds()["tvdb"] != "7101" || len(items[0].GetMedia().GetSeriesExternalIds()) != 0 {
		t.Fatalf("items = %v", items)
	}
}
