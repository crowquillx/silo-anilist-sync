package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"github.com/crowquillx/silo-anilist-sync/anilist"
	"github.com/crowquillx/silo-anilist-sync/mapping"
)

func pendingWith(store instanceStore) *pendingCompletions {
	return &pendingCompletions{store: func() instanceStore { return store }, now: time.Now}
}

// completedStop is the playback stop Silo sends for a finished play.
func completedStop(historyID string, episode int32) *pluginv1.WatchSyncEvent {
	event := episodeEvent("scrobble:STOP:session-"+historyID, episode)
	event.Operation = pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_STOP
	event.Completed = true
	event.WatchHistoryId = historyID
	return event
}

// reconciledMark is the watched mark Silo's history export sends, both for
// manual marks and to retry a finished play.
func reconciledMark(historyID string, episode int32) *pluginv1.WatchSyncEvent {
	event := episodeEvent(historyID, episode)
	event.Origin = pluginv1.WatchSyncOrigin_WATCH_SYNC_ORIGIN_RECONCILIATION
	event.WatchHistoryId = historyID
	return event
}

// flakyAniList serves AniList, failing every request while down is set.
func flakyAniList(t *testing.T, down *atomic.Bool) *fakeAniList {
	t.Helper()
	fake := &fakeAniList{}
	fake.serve(t, func(request graphQLRequest, w http.ResponseWriter) {
		if down.Load() {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		if strings.Contains(request.Query, "SaveMediaListEntry") {
			writeJSON(w, map[string]any{"data": map[string]any{"SaveMediaListEntry": map[string]any{"id": 1}}})
			return
		}
		writeJSON(w, mediaAnswer(nil, 12))
	})
	return fake
}

func applyOne(t *testing.T, srv *server, event *pluginv1.WatchSyncEvent) *pluginv1.WatchSyncApplyResult {
	t.Helper()
	response, err := srv.ApplyEvents(context.Background(), &pluginv1.WatchSyncApplyEventsRequest{
		Context: authContext("7"), Events: []*pluginv1.WatchSyncEvent{event},
	})
	if err != nil || response.GetFault() != nil || len(response.GetResults()) != 1 {
		t.Fatalf("response = %v, err = %v", response, err)
	}
	return response.GetResults()[0]
}

func TestFailedCompletedStopIsAppliedByLaterReconciliation(t *testing.T) {
	var down atomic.Bool
	down.Store(true)
	fake := flakyAniList(t, &down)
	srv := &server{mappings: featureMappingClient(), status: newTestStatus(), pending: pendingWith(&memoryStore{})}

	if result := applyOne(t, srv, completedStop("history-1", 3)); result.GetStatus() != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_RETRY {
		t.Fatalf("stop during the outage = %v", result)
	}
	down.Store(false)
	requests := len(fake.requests)

	// A mark for another history row is a manual mark, which stays out of
	// AniList while sync_manual_watched is off.
	if result := applyOne(t, srv, reconciledMark("history-2", 4)); result.GetStatus() != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE {
		t.Fatalf("unrelated mark = %v", result)
	}
	if len(fake.requests) != requests {
		t.Fatalf("an unrelated manual mark made %d AniList requests", len(fake.requests)-requests)
	}

	if result := applyOne(t, srv, reconciledMark("history-1", 3)); result.GetStatus() != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED {
		t.Fatalf("retried play = %v", result)
	}
	mutations := fake.mutations()
	if len(mutations) != 1 || mutations[0].Variables["progress"] != float64(3) {
		t.Fatalf("mutations = %#v", mutations)
	}
	// Once applied the play is no longer pending, so the same mark sent
	// again is an ordinary manual mark.
	if srv.pending.has(context.Background(), "7:history-1") {
		t.Fatal("an applied play must not stay pending")
	}
	if result := applyOne(t, srv, reconciledMark("history-1", 3)); result.GetStatus() != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE {
		t.Fatalf("mark after the play was applied = %v", result)
	}
}

func TestPendingPlayBelongsToItsAniListAccount(t *testing.T) {
	var down atomic.Bool
	down.Store(true)
	flakyAniList(t, &down)
	srv := &server{mappings: featureMappingClient(), status: newTestStatus(), pending: pendingWith(&memoryStore{})}
	applyOne(t, srv, completedStop("history-1", 3))
	down.Store(false)
	response, err := srv.ApplyEvents(context.Background(), &pluginv1.WatchSyncApplyEventsRequest{
		Context: authContext("8"), Events: []*pluginv1.WatchSyncEvent{reconciledMark("history-1", 3)},
	})
	if err != nil || response.GetResults()[0].GetStatus() != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE {
		t.Fatalf("response = %v, err = %v", response, err)
	}
}

func TestAppliedCompletedStopIsNotLeftPending(t *testing.T) {
	var down atomic.Bool
	flakyAniList(t, &down)
	store := &memoryStore{}
	srv := &server{mappings: featureMappingClient(), status: newTestStatus(), pending: pendingWith(store)}
	if result := applyOne(t, srv, completedStop("history-1", 3)); result.GetStatus() != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED {
		t.Fatalf("stop = %v", result)
	}
	if srv.pending.has(context.Background(), "7:history-1") {
		t.Fatal("an applied stop must not stay pending")
	}
	var saved persistedPending
	if err := json.Unmarshal(store.values[pendingStateKey], &saved); err != nil || len(saved.Entries) != 0 {
		t.Fatalf("saved = %#v, err = %v", saved, err)
	}

	// A rejection no retry can fix ends the play too.
	unmapped := completedStop("history-2", 3)
	unmapped.Media.SeriesExternalIds = map[string]string{"tvdb": "999"}
	if result := applyOne(t, srv, unmapped); result.GetStatus() != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED {
		t.Fatalf("unmapped stop = %v", result)
	}
	if srv.pending.has(context.Background(), "7:history-2") {
		t.Fatal("a rejected stop must not stay pending")
	}
}

func TestPendingPlaySurvivesAPluginRestart(t *testing.T) {
	var down atomic.Bool
	down.Store(true)
	flakyAniList(t, &down)
	store := &memoryStore{}
	before := &server{mappings: featureMappingClient(), status: newTestStatus(), pending: pendingWith(store)}
	if result := applyOne(t, before, completedStop("history-1", 3)); result.GetStatus() != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_RETRY {
		t.Fatalf("stop during the outage = %v", result)
	}
	down.Store(false)

	after := &server{mappings: featureMappingClient(), status: newTestStatus(), pending: pendingWith(store)}
	if result := applyOne(t, after, reconciledMark("history-1", 3)); result.GetStatus() != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED {
		t.Fatalf("retried play after the restart = %v", result)
	}
	var saved persistedPending
	if err := json.Unmarshal(store.values[pendingStateKey], &saved); err != nil || len(saved.Entries) != 0 {
		t.Fatalf("saved = %#v, err = %v", saved, err)
	}
}

// A stop whose batch fails before any event is tried, here because no mapping
// data loaded, must still be remembered.
func TestStopIsPendingWhenTheBatchFailsEarly(t *testing.T) {
	offline := mapping.NewClient(&http.Client{Transport: roundTrip(func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("")), Request: req}, nil
	})})
	srv := &server{mappings: offline, status: newTestStatus(), pending: pendingWith(&memoryStore{})}
	response, err := srv.ApplyEvents(context.Background(), &pluginv1.WatchSyncApplyEventsRequest{
		Context: authContext("7"), Events: []*pluginv1.WatchSyncEvent{completedStop("history-1", 3)},
	})
	if err != nil || response.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY {
		t.Fatalf("response = %v, err = %v", response, err)
	}
	if !srv.pending.has(context.Background(), "7:history-1") {
		t.Fatal("the stop must stay pending")
	}
}

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

// brokenStore is instance state that cannot be reached.
type brokenStore struct{}

func (brokenStore) ReadInstanceState(context.Context, string) ([]byte, bool, error) {
	return nil, false, errors.New("host unavailable")
}

func (brokenStore) WriteInstanceState(context.Context, string, []byte) error {
	return errors.New("host unavailable")
}

func TestPendingPlaysStayInMemoryWithoutInstanceState(t *testing.T) {
	ctx := context.Background()
	for name, pending := range map[string]*pendingCompletions{
		"no host":      {now: time.Now},
		"not attached": pendingWith(nil),
		"failing host": pendingWith(brokenStore{}),
	} {
		pending.add(ctx, []string{"7:a", "7:b"})
		if !pending.has(ctx, "7:a") || !pending.has(ctx, "7:b") {
			t.Fatalf("%s: added plays are not pending", name)
		}
		pending.remove(ctx, []string{"7:a"})
		if pending.has(ctx, "7:a") || !pending.has(ctx, "7:b") {
			t.Fatalf("%s: remove did not apply", name)
		}
	}
	var none *pendingCompletions
	none.add(ctx, []string{"7:a"})
	none.remove(ctx, []string{"7:a"})
	if none.has(ctx, "7:a") {
		t.Fatal("a nil set remembers nothing")
	}
}

func TestPendingPlaysAreCappedAndExpire(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	store := &memoryStore{}
	pending := &pendingCompletions{store: func() instanceStore { return store }, now: func() time.Time { return now }}
	pending.add(ctx, []string{"7:old"})
	now = now.Add(time.Hour)
	keys := make([]string, maxPendingEntries)
	for i := range keys {
		keys[i] = fmt.Sprintf("7:%08d-0000-0000-0000-000000000000", i)
	}
	pending.add(ctx, keys)
	if pending.has(ctx, "7:old") || !pending.has(ctx, keys[0]) || len(pending.entries) != maxPendingEntries {
		t.Fatalf("the oldest play must make room: %d entries", len(pending.entries))
	}
	if size := len(store.values[pendingStateKey]); size == 0 || size > 200<<10 {
		t.Fatalf("saved set is %d bytes", size)
	}
	now = now.Add(pendingMaxAge + time.Hour)
	if pending.has(ctx, keys[0]) {
		t.Fatal("an expired play must not be pending")
	}
	pending.add(ctx, []string{"7:new"})
	if len(pending.entries) != 1 {
		t.Fatalf("expired plays must be pruned: %d entries", len(pending.entries))
	}
}

func TestOversizedAniListResponseIsNotRetried(t *testing.T) {
	fault := faultFromError(fmt.Errorf("list entries: %w", &anilist.ResponseTooLargeError{Limit: 64 << 20}))
	if fault.GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMANENT || fault.GetSafeMessage() != "AniList response too large: over 64 MiB" {
		t.Fatalf("fault = %v", fault)
	}
}

// With AniBridge not loaded nothing maps AniList entries back to Silo. The
// import must fail for now, not report an empty list as the whole state.
func TestImportWaitsForReverseMappings(t *testing.T) {
	fake := &fakeAniList{}
	fake.serve(t, func(_ graphQLRequest, w http.ResponseWriter) {
		writeJSON(w, listAnswer(listEntry(1, 50, "MOVIE", "Scored Film", 1, 85)))
	})
	mappings := mapping.NewClient(&http.Client{Transport: roundTrip(func(req *http.Request) (*http.Response, error) {
		status, body := http.StatusServiceUnavailable, ""
		if req.URL.String() == mapping.DefaultAnimeListsURL {
			status, body = http.StatusOK, `<anime-list/>`
		}
		return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
	})})
	if _, err := mappings.Dataset(context.Background()); err != nil {
		t.Fatalf("exports must keep working from Anime-Lists alone: %v", err)
	}
	srv := &server{mappings: mappings, status: newTestStatus()}
	for _, kinds := range [][]pluginv1.WatchSyncRemoteStateKind{
		nil,
		{pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_RATING},
	} {
		response, err := srv.ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{
			Context: authContext("7"), PageSize: 10, StateKinds: kinds,
		})
		if err != nil {
			t.Fatal(err)
		}
		if response.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY ||
			!strings.Contains(response.GetFault().GetSafeMessage(), "mapping") ||
			response.GetCompleteSnapshot() || response.GetNextCursor() != "" || len(response.GetWarnings()) != 0 {
			t.Fatalf("kinds %v: response = %v", kinds, response)
		}
	}
	_, accounts := srv.status.snapshot(context.Background())
	if len(accounts) != 1 || !accounts[0].LastImportAt.IsZero() || accounts[0].UnmappedCount != 0 {
		t.Fatalf("a failed import must not be recorded: %#v", accounts)
	}
}

func TestUnmappedWarningFitsSiloLimit(t *testing.T) {
	entries := func(count int, title string) []anilist.ListEntry {
		out := make([]anilist.ListEntry, count)
		for i := range out {
			out[i].Media.ID = i + 1
			out[i].Media.Title.Romaji = fmt.Sprintf("%s %d", title, i+1)
		}
		return out
	}
	const lead = "40 AniList entries have no TVDB, TMDB or IMDb mapping and were not imported. The AniList Sync admin page lists them."
	warnings := unmappedWarnings(entries(40, "A Reasonably Long Anime Title: The Second Season"))
	if len(warnings) != 1 || len(warnings[0]) > maxWarningBytes || !strings.HasPrefix(warnings[0], lead) {
		t.Fatalf("warnings = %q", warnings)
	}
	if !strings.Contains(warnings[0], " Not imported: A Reasonably Long Anime Title: The Second Season 1, ") ||
		!strings.HasSuffix(warnings[0], " more.") {
		t.Fatalf("warning = %q", warnings[0])
	}
	// The count of entries left out matches the titles named.
	var more int
	tail := warnings[0][strings.LastIndex(warnings[0], ", and ")+len(", and "):]
	if _, err := fmt.Sscanf(tail, "%d more.", &more); err != nil || more != 40-strings.Count(warnings[0], "The Second Season") {
		t.Fatalf("warning = %q", warnings[0])
	}

	short := unmappedWarnings(entries(12, "Show"))
	if len(short) != 1 || len(short[0]) > maxWarningBytes || !strings.HasSuffix(short[0], "Show 10, and 2 more.") {
		t.Fatalf("warnings = %q", short)
	}
	two := unmappedWarnings(entries(2, "Show"))
	if len(two) != 1 || !strings.HasSuffix(two[0], "lists them. Not imported: Show 1, Show 2.") {
		t.Fatalf("warnings = %q", two)
	}
	// A title too long to fit is left to the admin page.
	huge := unmappedWarnings(entries(1, strings.Repeat("x", 400)))
	if len(huge) != 1 || huge[0] != "1 AniList entry has no TVDB, TMDB or IMDb mapping and was not imported. The AniList Sync admin page lists it." {
		t.Fatalf("warnings = %q", huge)
	}
}
