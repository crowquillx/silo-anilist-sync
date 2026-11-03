package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"github.com/crowquillx/silo-anilist-sync/anilist"
	"github.com/crowquillx/silo-anilist-sync/mapping"
)

const featureMappings = `{"$meta":{"schema_version":"3.0.0"},` +
	`"tvdb_show:100:s1":{"anilist:10":{"1-12":"1-12"}},` +
	`"tmdb_movie:500":{"anilist:50":{}},` +
	`"tmdb_movie:600":{"anilist:60":{}},"tvdb_movie:601":{"anilist:61":{}}}`

// mappingTransport serves the given AniBridge dataset and an empty
// Anime-Lists file.
type mappingTransport string

func (body mappingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	status, text := http.StatusNotFound, ""
	switch req.URL.String() {
	case mapping.DefaultURL:
		status, text = http.StatusOK, string(body)
	case mapping.DefaultAnimeListsURL:
		status, text = http.StatusOK, `<anime-list/>`
	}
	return &http.Response{StatusCode: status, Status: http.StatusText(status), Header: make(http.Header), Body: io.NopCloser(strings.NewReader(text)), Request: req}, nil
}

func featureMappingClient() *mapping.Client {
	return mapping.NewClient(&http.Client{Transport: mappingTransport(featureMappings)})
}

// fakeAniList answers GraphQL requests with handle and records them.
type fakeAniList struct {
	mu       sync.Mutex
	requests []graphQLRequest
}

type graphQLRequest struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables"`
}

func (f *fakeAniList) serve(t *testing.T, handle func(graphQLRequest, http.ResponseWriter)) {
	t.Helper()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request graphQLRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		f.mu.Lock()
		f.requests = append(f.requests, request)
		f.mu.Unlock()
		handle(request, w)
	}))
	t.Cleanup(upstream.Close)
	old := anilist.Endpoint
	anilist.Endpoint = upstream.URL
	t.Cleanup(func() { anilist.Endpoint = old })
}

func (f *fakeAniList) mutations() []graphQLRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []graphQLRequest
	for _, request := range f.requests {
		if strings.Contains(request.Query, "SaveMediaListEntry") {
			out = append(out, request)
		}
	}
	return out
}

func writeJSON(w http.ResponseWriter, value any) {
	_ = json.NewEncoder(w).Encode(value)
}

func mediaAnswer(entry map[string]any, episodes int) map[string]any {
	var listEntry any
	if entry != nil {
		listEntry = entry
	}
	return map[string]any{"data": map[string]any{"Media": map[string]any{"episodes": episodes, "mediaListEntry": listEntry}}}
}

func authContext(userID string) *pluginv1.WatchSyncAuthenticatedContext {
	return &pluginv1.WatchSyncAuthenticatedContext{
		CapabilityId: "anilist",
		Credentials:  &pluginv1.WatchSyncCredentials{AccessToken: "token", SecretAttributes: map[string]string{"user_id": userID}},
	}
}

func episodeEvent(id string, episode int32) *pluginv1.WatchSyncEvent {
	return &pluginv1.WatchSyncEvent{
		EventId:   id,
		Operation: pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_WATCHED,
		Origin:    pluginv1.WatchSyncOrigin_WATCH_SYNC_ORIGIN_PLAYBACK_COMPLETION,
		Media: &pluginv1.WatchSyncMedia{
			MediaType: pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE, SeriesTitle: "Alpha",
			SeasonNumber: 1, EpisodeNumber: episode, SeriesExternalIds: map[string]string{"tvdb": "100"},
		},
	}
}

func movieRatingEvent(id, tmdb string, operation pluginv1.WatchSyncOperation, rating int32) *pluginv1.WatchSyncEvent {
	return &pluginv1.WatchSyncEvent{
		EventId: id, Operation: operation, Rating: rating,
		Media: &pluginv1.WatchSyncMedia{MediaType: pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE, Title: "Film", ExternalIds: map[string]string{"tmdb": tmdb}},
	}
}

func newTestStatus() *statusStore {
	return &statusStore{now: time.Now}
}

func TestInvalidTokenFailsTheWholeBatchAndFlagsReconnect(t *testing.T) {
	fake := &fakeAniList{}
	fake.serve(t, func(_ graphQLRequest, w http.ResponseWriter) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"data":null,"errors":[{"message":"Invalid token","status":400}]}`))
	})
	srv := &server{mappings: featureMappingClient(), status: newTestStatus()}
	response, err := srv.ApplyEvents(context.Background(), &pluginv1.WatchSyncApplyEventsRequest{
		Context: authContext("7"),
		Events:  []*pluginv1.WatchSyncEvent{episodeEvent("a", 1), episodeEvent("b", 2)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL || len(response.GetResults()) != 0 {
		t.Fatalf("response = %v", response)
	}
	if len(fake.requests) != 1 {
		t.Fatalf("AniList requests = %d; the batch should stop at the first rejection", len(fake.requests))
	}
	_, accounts := srv.status.snapshot(context.Background())
	if len(accounts) != 1 || !accounts[0].NeedsReconnect {
		t.Fatalf("accounts = %#v", accounts)
	}
}

func TestEventsPastTheDeadlineAreDeferredAsRateLimited(t *testing.T) {
	fake := &fakeAniList{}
	fake.serve(t, func(_ graphQLRequest, w http.ResponseWriter) { writeJSON(w, mediaAnswer(nil, 12)) })
	srv := &server{mappings: featureMappingClient(), status: newTestStatus()}
	ctx, cancel := context.WithTimeout(context.Background(), deadlineMargin/2)
	defer cancel()
	response, err := srv.ApplyEvents(ctx, &pluginv1.WatchSyncApplyEventsRequest{
		Context: authContext("7"),
		Events:  []*pluginv1.WatchSyncEvent{episodeEvent("a", 1), episodeEvent("b", 2)},
	})
	if err != nil || response.GetFault() != nil || len(response.GetResults()) != 2 {
		t.Fatalf("response = %v, err = %v", response, err)
	}
	for _, result := range response.GetResults() {
		if result.GetStatus() != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_RETRY ||
			result.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_RATE_LIMITED ||
			result.GetFault().GetRetryAfter().AsDuration() != deferredRetryAfter {
			t.Fatalf("result = %v", result)
		}
	}
	if len(fake.requests) != 0 {
		t.Fatalf("AniList requests = %d; deferred events must not start", len(fake.requests))
	}
	state, _ := srv.status.snapshot(context.Background())
	if len(state.Problems) != 0 {
		t.Fatalf("deferred events are not problems: %#v", state.Problems)
	}
}

func TestSiloCompletedFlagWinsOverThreshold(t *testing.T) {
	stop := &pluginv1.WatchSyncEvent{Operation: pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_STOP, CompletionPercent: 85}
	behavior := watchBehavior{playbackCompletionPercent: 90}
	if appliesWatchedState(stop, behavior) {
		t.Fatal("85% without completed must not apply at a 90% threshold")
	}
	stop.Completed = true
	if !appliesWatchedState(stop, behavior) {
		t.Fatal("Silo's completed flag must apply")
	}
}

func TestConnectionSettingsTurnSwitchesOn(t *testing.T) {
	auth := &pluginv1.WatchSyncAuthenticatedContext{
		ProviderConfig:     &pluginv1.WatchSyncProviderConfig{Values: map[string]string{"provider.sync_manual_watched": "false"}},
		ConnectionSettings: map[string]string{"sync_manual_watched": "true", "only_existing_entries": "false"},
	}
	behavior := watchBehaviorFromContext(auth)
	if !behavior.syncManualWatched || behavior.onlyExistingEntries {
		t.Fatalf("behavior = %#v", behavior)
	}
	auth.ProviderConfig.Values["provider.only_existing_entries"] = "true"
	if !watchBehaviorFromContext(auth).onlyExistingEntries {
		t.Fatal("the global setting must still apply when the connection leaves it off")
	}
}

func TestMovieRatingExport(t *testing.T) {
	fake := &fakeAniList{}
	fake.serve(t, func(request graphQLRequest, w http.ResponseWriter) {
		if strings.Contains(request.Query, "SaveMediaListEntry") {
			writeJSON(w, map[string]any{"data": map[string]any{"SaveMediaListEntry": map[string]any{"id": 1}}})
			return
		}
		writeJSON(w, mediaAnswer(map[string]any{"id": 9, "progress": 1, "status": "COMPLETED", "score": 40}, 1))
	})
	srv := &server{mappings: featureMappingClient(), status: newTestStatus()}
	set := pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SET_RATING
	remove := pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_RATING
	series := movieRatingEvent("series", "500", set, 8)
	series.Media.MediaType = pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_SERIES
	response, err := srv.ApplyEvents(context.Background(), &pluginv1.WatchSyncApplyEventsRequest{
		Context: authContext("7"),
		Events: []*pluginv1.WatchSyncEvent{
			movieRatingEvent("set", "500", set, 8),
			movieRatingEvent("remove", "500", remove, 0),
			movieRatingEvent("unmapped", "999", set, 8),
			movieRatingEvent("bad", "500", set, 11),
			series,
		},
	})
	if err != nil || response.GetFault() != nil {
		t.Fatalf("response = %v, err = %v", response, err)
	}
	want := []pluginv1.WatchSyncApplyStatus{
		pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED,
		pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED,
		pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED,
		pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED,
		pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED,
	}
	for i, result := range response.GetResults() {
		if result.GetStatus() != want[i] {
			t.Fatalf("result %d = %v", i, result)
		}
	}
	mutations := fake.mutations()
	if len(mutations) != 2 || mutations[0].Variables["scoreRaw"] != float64(80) || mutations[1].Variables["scoreRaw"] != float64(0) {
		t.Fatalf("mutations = %#v", mutations)
	}
	state, accounts := srv.status.snapshot(context.Background())
	if len(state.Problems) != 3 || accounts[0].Exports.Applied != 2 || accounts[0].Exports.Rejected != 3 {
		t.Fatalf("status = %#v, %#v", state.Problems, accounts[0].Exports)
	}
	if !strings.Contains(state.Problems[0].Item, "Film") {
		t.Fatalf("problem item = %q", state.Problems[0].Item)
	}
}

func listAnswer(entries ...map[string]any) map[string]any {
	return map[string]any{"data": map[string]any{"MediaListCollection": map[string]any{
		"lists": []map[string]any{{"entries": entries}},
	}}}
}

func listEntry(id, mediaID int, format, title string, progress int, score float64) map[string]any {
	return map[string]any{
		"id": id, "mediaId": mediaID, "status": "COMPLETED", "progress": progress, "updatedAt": 1000, "score": score,
		"media": map[string]any{"id": mediaID, "format": format, "episodes": max(progress, 1), "title": map[string]any{"romaji": title}, "startDate": map[string]any{"year": 2020}},
	}
}

func TestRatingTraversalIsACompleteMovieSnapshot(t *testing.T) {
	fake := &fakeAniList{}
	fake.serve(t, func(_ graphQLRequest, w http.ResponseWriter) {
		writeJSON(w, listAnswer(
			listEntry(1, 50, "MOVIE", "Scored Film", 1, 85),
			listEntry(2, 99, "MOVIE", "Unmapped Film", 1, 70),
			listEntry(3, 10, "TV", "Scored Show", 12, 90),
			listEntry(4, 50, "MOVIE", "Unscored Film", 1, 0),
		))
	})
	srv := &server{mappings: featureMappingClient(), status: newTestStatus()}
	response, err := srv.ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{
		Context: authContext("7"), PageSize: 10,
		StateKinds: []pluginv1.WatchSyncRemoteStateKind{pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_RATING},
	})
	if err != nil || response.GetFault() != nil {
		t.Fatalf("response = %v, err = %v", response, err)
	}
	if !response.GetCompleteSnapshot() || response.GetNextCursor() != ratingsCursor {
		t.Fatalf("snapshot = %v, cursor = %q", response.GetCompleteSnapshot(), response.GetNextCursor())
	}
	items := response.GetItems()
	if len(items) != 1 || items[0].GetRating().GetRating() != 9 || items[0].GetWatched() != nil ||
		items[0].GetMedia().GetExternalIds()["tmdb"] != "500" || items[0].GetProviderItemKey() != "anilist:1:movie" {
		t.Fatalf("items = %v", items)
	}
}

func TestWatchedImportWarnsAboutUnmappedEntries(t *testing.T) {
	fake := &fakeAniList{}
	fake.serve(t, func(_ graphQLRequest, w http.ResponseWriter) {
		writeJSON(w, listAnswer(
			listEntry(1, 10, "TV", "Mapped Show", 2, 0),
			listEntry(2, 77, "TV", "Unknown Show", 3, 0),
		))
	})
	srv := &server{mappings: featureMappingClient(), status: newTestStatus()}
	response, err := srv.ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{Context: authContext("7"), PageSize: 10})
	if err != nil || response.GetFault() != nil {
		t.Fatalf("response = %v, err = %v", response, err)
	}
	warnings := response.GetWarnings()
	if len(warnings) != 1 || !strings.Contains(warnings[0], "1 AniList entry has no") || !strings.Contains(warnings[0], "Unknown Show") {
		t.Fatalf("warnings = %q", warnings)
	}
	_, accounts := srv.status.snapshot(context.Background())
	if len(accounts) != 1 || accounts[0].UnmappedCount != 1 || accounts[0].UnmappedTitles[0].MediaID != 77 || accounts[0].ImportStates != 2 || !accounts[0].ImportFull {
		t.Fatalf("accounts = %#v", accounts)
	}
}

func TestPageTokenFromAnotherSnapshotRestartsTheImport(t *testing.T) {
	entries := []anilist.ListEntry{{ID: 1, MediaID: 10, Status: "CURRENT", Progress: 2}}
	entries[0].Media.ID, entries[0].Media.Format = 10, "TV"
	fetchedAt := time.Now()
	srv := &server{mappings: featureMappingClient(), imports: seededImports(7, entries, fetchedAt)}
	request := &pluginv1.WatchSyncListRemoteStateRequest{Context: authContext("7"), PageSize: 1}
	first, err := srv.ListRemoteState(context.Background(), request)
	if err != nil || first.GetFault() != nil || !strings.HasPrefix(first.GetNextPageToken(), pageTokenPrefix) {
		t.Fatalf("first page = %v, err = %v", first, err)
	}
	request.PageToken = formatPageToken(fetchedAt.Add(-time.Hour), 1)
	stale, err := srv.ListRemoteState(context.Background(), request)
	if err != nil || stale.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY || len(stale.GetItems()) != 0 {
		t.Fatalf("stale page = %v, err = %v", stale, err)
	}
}

func TestAccountsKeepTheirOwnListSnapshots(t *testing.T) {
	fake := &fakeAniList{}
	fake.serve(t, func(request graphQLRequest, w http.ResponseWriter) {
		if request.Variables["userId"] == float64(7) {
			writeJSON(w, listAnswer(listEntry(1, 10, "TV", "Seven", 2, 0)))
			return
		}
		writeJSON(w, listAnswer(listEntry(2, 10, "TV", "Eight", 3, 0)))
	})
	srv := &server{mappings: featureMappingClient()}
	page := func(user, token string) *pluginv1.WatchSyncListRemoteStateResponse {
		response, err := srv.ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{Context: authContext(user), PageSize: 1, PageToken: token})
		if err != nil || response.GetFault() != nil {
			t.Fatalf("response = %v, err = %v", response, err)
		}
		return response
	}
	seven := page("7", "")
	eight := page("8", "")
	page("7", seven.GetNextPageToken())
	page("8", eight.GetNextPageToken())
	if len(fake.requests) != 2 {
		t.Fatalf("AniList list requests = %d; interleaved traversals must not refetch", len(fake.requests))
	}
}

func TestFullRescanRunsOncePerAccount(t *testing.T) {
	entries := []anilist.ListEntry{{ID: 1, MediaID: 10, Status: "CURRENT", Progress: 2, UpdatedAt: 100}}
	entries[0].Media.ID, entries[0].Media.Format = 10, "TV"
	srv := &server{mappings: featureMappingClient(), status: newTestStatus(), imports: seededImports(7, entries, time.Now())}
	traverse := func(fullScan string) bool {
		auth := authContext("7")
		auth.ProviderConfig = &pluginv1.WatchSyncProviderConfig{Values: map[string]string{"provider.import_full_scan": fullScan}}
		response, err := srv.ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{Context: auth, Cursor: "inc:500", PageSize: 10})
		if err != nil || response.GetFault() != nil {
			t.Fatalf("response = %v, err = %v", response, err)
		}
		return response.GetCompleteSnapshot()
	}
	if traverse("false") {
		t.Fatal("an incremental cursor without a rescan must stay incremental")
	}
	if !traverse("true") {
		t.Fatal("turning the setting on must rescan")
	}
	if traverse("true") {
		t.Fatal("the rescan must run once while the setting stays on")
	}
	srv.status.requestRescan(context.Background())
	if !traverse("true") || traverse("true") {
		t.Fatal("the page's rescan must run once")
	}
	traverse("false")
	if !traverse("true") {
		t.Fatal("turning the setting off and on must rescan again")
	}
}
