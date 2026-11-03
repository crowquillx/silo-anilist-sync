package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"github.com/crowquillx/silo-anilist-sync/anilist"
	"github.com/crowquillx/silo-anilist-sync/mapping"
)

type memoryStore struct {
	mu      sync.Mutex
	values  map[string][]byte
	readErr error
	reads   int
}

func (m *memoryStore) ReadInstanceState(_ context.Context, key string) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reads++
	if m.readErr != nil {
		return nil, false, m.readErr
	}
	value, ok := m.values[key]
	return value, ok, nil
}

func (m *memoryStore) WriteInstanceState(_ context.Context, key string, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.values == nil {
		m.values = make(map[string][]byte)
	}
	m.values[key] = append([]byte(nil), value...)
	return nil
}

func storeWith(store *memoryStore, now func() time.Time) *statusStore {
	return &statusStore{store: func() instanceStore { return store }, now: now}
}

func TestStatusSurvivesARestart(t *testing.T) {
	store := &memoryStore{}
	ctx := context.Background()
	first := storeWith(store, time.Now)
	first.recordAccount(ctx, "anilist", anilist.Account{ID: 7, Name: "seven"}, "token")
	first.recordExports(ctx, 7, []exportOutcome{{item: "Alpha S1 E2", status: "rejected", reason: "no mapping"}})
	first.save()

	second := storeWith(store, time.Now)
	second.recordCall(ctx, 8, "anilist", "token", true, "rejected token")
	state, accounts := second.snapshot(ctx)
	if len(accounts) != 2 || len(state.Problems) != 1 || state.Problems[0].Item != "Alpha S1 E2" {
		t.Fatalf("accounts = %#v, problems = %#v", accounts, state.Problems)
	}
	byID := map[int]*accountStatus{}
	for _, account := range accounts {
		byID[account.UserID] = account
	}
	if byID[7].Name != "seven" || byID[7].Exports.Rejected != 1 || !byID[8].NeedsReconnect {
		t.Fatalf("accounts = %#v, %#v", byID[7], byID[8])
	}
}

func TestStatusRetriesAFailedLoadLater(t *testing.T) {
	store := &memoryStore{readErr: errors.New("host not ready")}
	now := time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC)
	status := storeWith(store, func() time.Time { return now })
	ctx := context.Background()
	status.recordAccount(ctx, "anilist", anilist.Account{ID: 7, Name: "seven"}, "token")
	status.recordAccount(ctx, "anilist", anilist.Account{ID: 7, Name: "seven"}, "token")
	if store.reads != 1 {
		t.Fatalf("reads = %d; want one before the retry interval", store.reads)
	}
	saved, _ := json.Marshal(persistedStatus{Accounts: map[string]*accountStatus{"9": {UserID: 9, Name: "nine"}}})
	store.readErr = nil
	store.values = map[string][]byte{statusStateKey: saved}
	now = now.Add(statusLoadInterval)
	_, accounts := status.snapshot(ctx)
	if len(accounts) != 2 {
		t.Fatalf("accounts = %#v; the saved account and the new one should merge", accounts)
	}
}

func TestStatusStaysWithinTheInstanceStateLimit(t *testing.T) {
	status := &statusStore{now: time.Now}
	ctx := context.Background()
	long := strings.Repeat("x", 4000)
	for id := 1; id <= 40; id++ {
		var unmapped []anilist.ListEntry
		for media := range 30 {
			entry := anilist.ListEntry{}
			entry.Media.ID = id*100 + media
			entry.Media.Title.Romaji = long
			unmapped = append(unmapped, entry)
		}
		status.recordImport(ctx, id, importSummary{full: true, unmapped: unmapped}, false)
		var outcomes []exportOutcome
		for range 10 {
			outcomes = append(outcomes, exportOutcome{item: long, status: "rejected", reason: long})
		}
		status.recordExports(ctx, id, outcomes)
	}
	state, accounts := status.snapshot(ctx)
	if len(accounts) != maxAccounts || len(state.Problems) != maxProblems || len(accounts[0].UnmappedTitles) != maxUnmappedTitles || accounts[0].UnmappedCount != 30 {
		t.Fatalf("accounts = %d, problems = %d, titles = %d, count = %d", len(accounts), len(state.Problems), len(accounts[0].UnmappedTitles), accounts[0].UnmappedCount)
	}
	status.mu.Lock()
	data := status.encodeLocked()
	status.mu.Unlock()
	if data == nil || len(data) > maxStatusBytes {
		t.Fatalf("encoded status is %d bytes", len(data))
	}
}

func TestIncrementalImportAddsUnmappedTitlesWithoutForgettingOthers(t *testing.T) {
	status := &statusStore{now: time.Now}
	ctx := context.Background()
	entry := func(id int) anilist.ListEntry {
		e := anilist.ListEntry{}
		e.Media.ID = id
		return e
	}
	status.recordImport(ctx, 7, importSummary{full: true, unmapped: []anilist.ListEntry{entry(1), entry(2)}}, false)
	status.recordImport(ctx, 7, importSummary{unmapped: []anilist.ListEntry{entry(2), entry(3)}}, false)
	_, accounts := status.snapshot(ctx)
	if accounts[0].UnmappedCount != 3 || len(accounts[0].UnmappedTitles) != 3 {
		t.Fatalf("account = %#v", accounts[0])
	}
	status.recordImport(ctx, 7, importSummary{full: true, unmapped: []anilist.ListEntry{entry(3)}}, false)
	_, accounts = status.snapshot(ctx)
	if accounts[0].UnmappedCount != 1 || accounts[0].UnmappedTitles[0].MediaID != 3 {
		t.Fatalf("a full import must replace the list: %#v", accounts[0])
	}
}

func TestStatusPageShowsAccountsAndProblems(t *testing.T) {
	now := time.Now()
	status := &statusStore{now: func() time.Time { return now }}
	ctx := context.Background()
	status.recordAccount(ctx, "anilist", anilist.Account{ID: 7, Name: "seven", ProfileURL: "https://anilist.co/user/seven"}, "token")
	status.recordCall(ctx, 8, siloVerifiedCapability, "token", true, reconnectMessage)
	status.recordExports(ctx, 7, []exportOutcome{{item: "Alpha <S1> E2", status: "rejected", reason: "no unambiguous mapping"}})
	unmapped := anilist.ListEntry{}
	unmapped.Media.ID, unmapped.Media.Title.Romaji, unmapped.Media.Format = 77, "Unknown Show", "TV"
	status.recordImport(ctx, 7, importSummary{full: true, entries: 5, states: 12, unmapped: []anilist.ListEntry{unmapped}}, false)
	srv := &server{status: status, mappings: mapping.NewClient(nil)}

	page, err := srv.Handle(ctx, &pluginv1.HandleHTTPRequest{Method: "GET", Path: "/"})
	if err != nil || page.GetStatusCode() != 200 {
		t.Fatalf("page = %v, err = %v", page, err)
	}
	html := string(page.GetBody())
	for _, want := range []string{
		`<a href="https://anilist.co/user/seven"`,
		"AniList user 8",
		"Needs reconnect",
		"must reconnect AniList in Settings → Watch Providers",
		"Alpha &lt;S1&gt; E2",
		"https://anilist.co/anime/77",
		"full list: 5 entries, 12 watched items",
		"AniList with Silo watched check",
	} {
		if !strings.Contains(html, want) {
			t.Errorf("page lacks %q", want)
		}
	}
	if page.GetHeaders()["Content-Security-Policy"] == "" {
		t.Error("page has no content security policy")
	}

	statusJSON, err := srv.Handle(ctx, &pluginv1.HandleHTTPRequest{Method: "GET", Path: "/status"})
	if err != nil || statusJSON.GetStatusCode() != 200 {
		t.Fatalf("status = %v, err = %v", statusJSON, err)
	}
	var decoded struct {
		Accounts []accountStatus `json:"accounts"`
		Problems []problem       `json:"problems"`
	}
	if err := json.Unmarshal(statusJSON.GetBody(), &decoded); err != nil || len(decoded.Accounts) != 2 || len(decoded.Problems) != 1 {
		t.Fatalf("decoded = %#v, err = %v", decoded, err)
	}
	if strings.Contains(string(statusJSON.GetBody()), "token\"") {
		t.Fatal("status JSON must not carry tokens")
	}
}

func TestStatusPageActions(t *testing.T) {
	status := &statusStore{now: time.Now}
	ctx := context.Background()
	status.recordExports(ctx, 7, []exportOutcome{{item: "Alpha", status: "retry", reason: "temporary"}})
	srv := &server{status: status}

	response, err := srv.Handle(ctx, &pluginv1.HandleHTTPRequest{Method: "POST", Path: "/rescan"})
	if err != nil || response.GetStatusCode() != 303 || response.GetHeaders()["Location"] != "./?saved=rescan" {
		t.Fatalf("rescan = %v, err = %v", response, err)
	}
	state, _ := status.snapshot(ctx)
	if state.Rescan.RequestedAt.IsZero() {
		t.Fatal("rescan was not requested")
	}
	if !status.fullScanDue(ctx, 7, false) {
		t.Fatal("a requested rescan must be due for every account")
	}

	if response, _ := srv.Handle(ctx, &pluginv1.HandleHTTPRequest{Method: "POST", Path: "/clear-problems"}); response.GetStatusCode() != 303 {
		t.Fatalf("clear = %v", response)
	}
	state, accounts := status.snapshot(ctx)
	if len(state.Problems) != 0 || accounts[0].Exports.Retried != 0 {
		t.Fatalf("problems were not cleared: %#v", state)
	}

	for _, request := range []*pluginv1.HandleHTTPRequest{{Method: "DELETE", Path: "/"}, {Method: "GET", Path: "/rescan"}} {
		if response, _ := srv.Handle(ctx, request); response.GetStatusCode() != 405 {
			t.Fatalf("%s %s = %d", request.GetMethod(), request.GetPath(), response.GetStatusCode())
		}
	}
	if response, _ := srv.Handle(ctx, &pluginv1.HandleHTTPRequest{Method: "GET", Path: "/missing"}); response.GetStatusCode() != 404 {
		t.Fatalf("missing = %d", response.GetStatusCode())
	}
	saved, _ := srv.Handle(ctx, &pluginv1.HandleHTTPRequest{Method: "GET", Path: "/", Query: nil})
	if !strings.Contains(string(saved.GetBody()), "No rescan was requested") && !strings.Contains(string(saved.GetBody()), "Last requested") {
		t.Fatal("page lacks the rescan state")
	}
}

func TestStatusPageWorksWithoutStatus(t *testing.T) {
	page, err := (&server{}).Handle(context.Background(), &pluginv1.HandleHTTPRequest{Method: "GET", Path: "/"})
	if err != nil || page.GetStatusCode() != 200 || !strings.Contains(string(page.GetBody()), "No AniList connection has synced yet") {
		t.Fatalf("page = %v, err = %v", page, err)
	}
}
