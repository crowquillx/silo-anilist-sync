package anilist

import (
	"context"
	"encoding/json"
	"errors"

	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestAdvanceProgressDoesNotDecrease(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{"Media": map[string]any{
				"episodes":       12,
				"mediaListEntry": map[string]any{"id": 3, "progress": 8, "status": "CURRENT"},
			}},
		})
	}))
	defer server.Close()
	client := NewClient("token", server.Client())
	client.Endpoint = server.URL
	if err := client.AdvanceProgress(context.Background(), 1, 6); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestAdvanceProgressCompletesNewEntryAtEpisodeCount(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var body struct {
			Variables map[string]any `json:"variables"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if calls == 1 {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": map[string]any{"Media": map[string]any{"episodes": 12, "mediaListEntry": nil}},
			})
			return
		}
		if body.Variables["progress"] != float64(12) || body.Variables["status"] != "COMPLETED" || body.Variables["mediaId"] != float64(1) {
			t.Fatalf("mutation variables = %#v", body.Variables)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"SaveMediaListEntry": map[string]any{"id": 1}}})
	}))
	defer server.Close()
	client := NewClient("token", server.Client())
	client.Endpoint = server.URL
	if err := client.AdvanceProgress(context.Background(), 1, 12); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
}

func TestAdvanceProgressIfExistingSkipsMissingEntry(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		var body struct {
			Query string `json:"query"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(body.Query, "SaveMediaListEntry") {
			t.Fatal("missing entry must not be mutated")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"Media": map[string]any{
			"episodes": 12, "mediaListEntry": nil,
		}}})
	}))
	defer server.Close()
	client := NewClient("token", server.Client())
	client.Endpoint = server.URL
	applied, err := client.AdvanceProgressIfExisting(context.Background(), 1, 13)
	if err != nil {
		t.Fatal(err)
	}
	if applied {
		t.Fatal("missing entry was reported as applied")
	}
	if requests != 1 {
		t.Fatalf("requests = %d, want one authenticated membership lookup", requests)
	}
}

func TestAdvanceProgressIfExistingAllowsExistingEntry(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		var body struct {
			Query     string         `json:"query"`
			Variables map[string]any `json:"variables"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if requests == 1 {
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"Media": map[string]any{
				"episodes": 12, "mediaListEntry": map[string]any{"id": 77, "progress": 1, "status": "PLANNING"},
			}}})
			return
		}
		if !strings.Contains(body.Query, "SaveMediaListEntry") || body.Variables["id"] != float64(77) ||
			body.Variables["progress"] != float64(2) || body.Variables["status"] != "CURRENT" {
			t.Fatalf("mutation = %#v", body)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"SaveMediaListEntry": map[string]any{"id": 77}}})
	}))
	defer server.Close()
	client := NewClient("token", server.Client())
	client.Endpoint = server.URL
	applied, err := client.AdvanceProgressIfExisting(context.Background(), 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !applied || requests != 2 {
		t.Fatalf("applied = %v, requests = %d; want mutation after lookup", applied, requests)
	}
}

func TestAdvanceProgressIfExistingUsesAuthenticatedAccount(t *testing.T) {
	mutations := make(map[string]int)
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		token := r.Header.Get("Authorization")
		var body struct {
			Query string `json:"query"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(body.Query, "SaveMediaListEntry") {
			mutations[token]++
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"SaveMediaListEntry": map[string]any{"id": 42}}})
			return
		}
		var entry any
		if token == "Bearer account-a" {
			entry = map[string]any{"id": 42, "progress": 1, "status": "CURRENT"}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"Media": map[string]any{
			"episodes": 12, "mediaListEntry": entry,
		}}})
	}))
	defer server.Close()

	accountA := NewClient("account-a", server.Client())
	accountA.Endpoint = server.URL
	applied, err := accountA.AdvanceProgressIfExisting(context.Background(), 1, 2)
	if err != nil || !applied {
		t.Fatalf("account A: applied = %v, err = %v", applied, err)
	}
	accountB := NewClient("account-b", server.Client())
	accountB.Endpoint = server.URL
	applied, err = accountB.AdvanceProgressIfExisting(context.Background(), 1, 2)
	if err != nil || applied {
		t.Fatalf("account B: applied = %v, err = %v", applied, err)
	}
	if requests != 3 || mutations["Bearer account-a"] != 1 || mutations["Bearer account-b"] != 0 {
		t.Fatalf("requests = %d, mutations = %#v; want independent account lookups", requests, mutations)
	}
}

// entryServer answers the Media query with entry and records the variables
// of the mutation that follows it.
func entryServer(t *testing.T, episodes any, entry map[string]any, mutation *map[string]any) *Client {
	t.Helper()
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var body struct {
			Variables map[string]any `json:"variables"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if calls == 1 {
			var listEntry any
			if entry != nil {
				listEntry = entry
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"Media": map[string]any{
				"episodes": episodes, "mediaListEntry": listEntry,
			}}})
			return
		}
		if mutation == nil {
			t.Fatalf("unexpected mutation %#v", body.Variables)
		}
		*mutation = body.Variables
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"SaveMediaListEntry": map[string]any{"id": 1}}})
	}))
	t.Cleanup(server.Close)
	client := NewClient("token", server.Client())
	client.Endpoint = server.URL
	return client
}

func TestAdvanceProgressStatusTransitions(t *testing.T) {
	for _, test := range []struct {
		name       string
		entry      map[string]any
		progress   int
		wantStatus any
		wantRepeat any
	}{
		{"paused resumes", map[string]any{"id": 77, "progress": 3, "status": "PAUSED"}, 4, "CURRENT", nil},
		{"dropped resumes", map[string]any{"id": 77, "progress": 3, "status": "DROPPED"}, 4, "CURRENT", nil},
		{"planning starts", map[string]any{"id": 77, "progress": 0, "status": "PLANNING"}, 1, "CURRENT", nil},
		{"planning final completes", map[string]any{"id": 77, "progress": 0, "status": "PLANNING"}, 12, "COMPLETED", nil},
		{"current keeps status", map[string]any{"id": 77, "progress": 3, "status": "CURRENT"}, 4, nil, nil},
		{"current final completes", map[string]any{"id": 77, "progress": 11, "status": "CURRENT"}, 12, "COMPLETED", nil},
		{"rewatch keeps status", map[string]any{"id": 77, "progress": 2, "status": "REPEATING", "repeat": 1}, 3, nil, nil},
		{"rewatch final completes and counts", map[string]any{"id": 77, "progress": 11, "status": "REPEATING", "repeat": 1}, 12, "COMPLETED", float64(2)},
	} {
		t.Run(test.name, func(t *testing.T) {
			var mutation map[string]any
			client := entryServer(t, 12, test.entry, &mutation)
			if err := client.AdvanceProgress(context.Background(), 1, test.progress); err != nil {
				t.Fatal(err)
			}
			if mutation["id"] != float64(77) || mutation["progress"] != float64(test.progress) {
				t.Fatalf("mutation variables = %#v", mutation)
			}
			if mutation["status"] != test.wantStatus || mutation["repeat"] != test.wantRepeat {
				t.Fatalf("status = %#v, repeat = %#v; want %#v, %#v", mutation["status"], mutation["repeat"], test.wantStatus, test.wantRepeat)
			}
		})
	}
}

func TestAdvanceProgressLeavesCompletedEntry(t *testing.T) {
	client := entryServer(t, 12, map[string]any{"id": 77, "progress": 12, "status": "COMPLETED"}, nil)
	if err := client.AdvanceProgress(context.Background(), 1, 3); err != nil {
		t.Fatal(err)
	}
}

func TestMappingPastKnownTotalIsAMismatch(t *testing.T) {
	client := entryServer(t, 12, nil, nil)
	err := client.AdvanceProgress(context.Background(), 1, 13)
	var mismatch *MismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("error = %#v, want MismatchError", err)
	}
}

func TestInvalidTokenIsRecognizedFromErrorBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// AniList's real answer to a revoked or expired token.
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"data":null,"errors":[{"message":"Invalid token","status":400}]}`))
	}))
	defer server.Close()
	client := NewClient("token", server.Client())
	client.Endpoint = server.URL
	_, err := client.Viewer(context.Background())
	var apiErr *Error
	if !errors.As(err, &apiErr) || !apiErr.InvalidToken || apiErr.Status != 400 || apiErr.Message != "AniList GraphQL: Invalid token" {
		t.Fatalf("error = %#v", err)
	}
}

func TestErrorBodyMessageIsKeptForOtherStatuses(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"data":{"Media":null},"errors":[{"message":"Not Found.","status":404}]}`))
	}))
	defer server.Close()
	client := NewClient("token", server.Client())
	client.Endpoint = server.URL
	err := client.AdvanceProgress(context.Background(), 1, 1)
	var apiErr *Error
	if !errors.As(err, &apiErr) || apiErr.InvalidToken || apiErr.Message != "AniList GraphQL: Not Found." {
		t.Fatalf("error = %#v", err)
	}
}

func TestMissingTokenIsAnInvalidCredential(t *testing.T) {
	_, err := NewClient("", nil).SetScore(context.Background(), 1, 5, false)
	var apiErr *Error
	if !errors.As(err, &apiErr) || !apiErr.InvalidToken {
		t.Fatalf("error = %#v", err)
	}
}

func TestSetScoreUpdatesExistingEntry(t *testing.T) {
	var mutation map[string]any
	client := entryServer(t, 1, map[string]any{"id": 9, "progress": 1, "status": "COMPLETED", "score": 60}, &mutation)
	applied, err := client.SetScore(context.Background(), 1, 8, false)
	if err != nil || !applied {
		t.Fatalf("applied = %v, err = %v", applied, err)
	}
	if mutation["id"] != float64(9) || mutation["scoreRaw"] != float64(80) || len(mutation) != 2 {
		t.Fatalf("mutation variables = %#v", mutation)
	}
}

func TestSetScoreSameRatingIsNoChange(t *testing.T) {
	client := entryServer(t, 1, map[string]any{"id": 9, "progress": 1, "status": "COMPLETED", "score": 75}, nil)
	applied, err := client.SetScore(context.Background(), 1, 8, false)
	if err != nil || applied {
		t.Fatalf("applied = %v, err = %v", applied, err)
	}
}

func TestSetScoreAddsWatchedEntry(t *testing.T) {
	var mutation map[string]any
	client := entryServer(t, 1, nil, &mutation)
	applied, err := client.SetScore(context.Background(), 5, 7, false)
	if err != nil || !applied {
		t.Fatalf("applied = %v, err = %v", applied, err)
	}
	if mutation["mediaId"] != float64(5) || mutation["status"] != "COMPLETED" || mutation["scoreRaw"] != float64(70) || mutation["progress"] != float64(1) {
		t.Fatalf("mutation variables = %#v", mutation)
	}
}

func TestSetScoreOnlyExistingSkipsMissingEntry(t *testing.T) {
	client := entryServer(t, 1, nil, nil)
	applied, err := client.SetScore(context.Background(), 5, 7, true)
	if err != nil || applied {
		t.Fatalf("applied = %v, err = %v", applied, err)
	}
}

func TestRemoveScore(t *testing.T) {
	var mutation map[string]any
	client := entryServer(t, 1, map[string]any{"id": 9, "progress": 1, "status": "COMPLETED", "score": 80}, &mutation)
	if applied, err := client.RemoveScore(context.Background(), 1); err != nil || !applied {
		t.Fatalf("applied = %v, err = %v", applied, err)
	}
	if mutation["id"] != float64(9) || mutation["scoreRaw"] != float64(0) {
		t.Fatalf("mutation variables = %#v", mutation)
	}
	unscored := entryServer(t, 1, map[string]any{"id": 9, "progress": 1, "status": "COMPLETED", "score": 0}, nil)
	if applied, err := unscored.RemoveScore(context.Background(), 1); err != nil || applied {
		t.Fatalf("unscored: applied = %v, err = %v", applied, err)
	}
}

func TestRatingFromScore(t *testing.T) {
	for score, want := range map[float64]int{0: 0, 1: 1, 4: 1, 5: 1, 14: 1, 15: 2, 55: 6, 85: 9, 95: 10, 100: 10} {
		if got := RatingFromScore(score); got != want {
			t.Errorf("RatingFromScore(%v) = %d, want %d", score, got, want)
		}
	}
}

func TestAdvanceProgressRejectsMappingPastKnownTotal(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"Media": map[string]any{
			"episodes": 12, "mediaListEntry": nil,
		}}})
	}))
	defer server.Close()
	client := NewClient("token", server.Client())
	client.Endpoint = server.URL
	if err := client.AdvanceProgress(context.Background(), 1, 13); err == nil {
		t.Fatal("expected oversized mapped progress to fail")
	}
}

func TestListEntriesFlattensListsAndDeduplicatesEntries(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Query     string         `json:"query"`
			Variables map[string]any `json:"variables"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.Variables["userId"] != float64(7) {
			t.Fatalf("variables = %#v", body.Variables)
		}
		if body.Query == "" {
			t.Fatal("query is empty")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"MediaListCollection": map[string]any{
			"lists": []map[string]any{
				{"entries": []map[string]any{{
					"id": 9, "mediaId": 42, "status": "COMPLETED", "progress": 0,
					"media": map[string]any{
						"id": 42, "format": "TV", "episodes": 12,
						"title":     map[string]any{"romaji": "Romaji", "english": "English"},
						"startDate": map[string]any{"year": 2024},
					},
				}}},
				{"entries": []map[string]any{
					{
						"id": 10, "mediaId": 7, "status": "CURRENT", "progress": 3,
						"media": map[string]any{
							"id": 7, "format": "TV", "episodes": 24,
							"title":     map[string]any{"romaji": "Early"},
							"startDate": map[string]any{"year": 2023},
						},
					},
					{
						"id": 9, "mediaId": 42, "status": "COMPLETED", "progress": 0,
						"media": map[string]any{
							"id": 42, "format": "TV", "episodes": 12,
							"title":     map[string]any{"romaji": "Romaji", "english": "English"},
							"startDate": map[string]any{"year": 2024},
						},
					},
				}},
			},
		}}})
	}))
	defer server.Close()
	client := NewClient("token", server.Client())
	client.Endpoint = server.URL
	entries, err := client.ListEntries(context.Background(), 7)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("entries = %#v, want 2 after deduplicating entry 9", entries)
	}
	if entries[0].MediaID != 7 || entries[0].ID != 10 {
		t.Fatalf("entries[0] = %#v, want mediaId 7 entry 10 first", entries[0])
	}
	entry := entries[1]
	if entry.MediaID != 42 || entry.ID != 9 || entry.Media.ID != 42 || entry.PreferredTitle() != "English" || entry.CompletedProgress() != 12 {
		t.Fatalf("entries[1] = %#v", entry)
	}
}

func TestClientPacesRequestsFromRateLimitHeaders(t *testing.T) {
	var lastRequest time.Time
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		now := time.Now()
		if !lastRequest.IsZero() && now.Sub(lastRequest) < 75*time.Millisecond {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		lastRequest = now
		w.Header().Set("X-RateLimit-Limit", "600")
		w.Header().Set("X-RateLimit-Remaining", "599")
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"MediaListCollection": map[string]any{
			"lists": []any{},
		}}})
	}))
	defer server.Close()

	client := NewClient("token", server.Client())
	client.Endpoint = server.URL
	for request := 1; request <= 2; request++ {
		if _, err := client.ListEntries(context.Background(), 7); err != nil {
			t.Fatalf("ListEntries(%d): %v", request, err)
		}
	}
}

func TestClientPreservesRateLimitRetryAfter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	client := NewClient("token", server.Client())
	client.Endpoint = server.URL
	_, err := client.ListEntries(context.Background(), 7)
	apiErr, ok := err.(*Error)
	if !ok {
		t.Fatalf("error = %T %v, want *Error", err, err)
	}
	if apiErr.Status != http.StatusTooManyRequests || apiErr.RetryAfter != time.Minute {
		t.Fatalf("error = %#v, want HTTP 429 with 1m retry", apiErr)
	}
}

func TestListEntriesLongBudgetAllowsSlowLargeListImport(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(3 * time.Second)
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"MediaListCollection": map[string]any{
			"lists": []any{},
		}}})
	}))
	defer server.Close()

	// The default 15s budget must be extended for full-list imports.
	client := NewClient("token", nil)
	client.Endpoint = server.URL
	if _, err := client.ListEntries(context.Background(), 7); err != nil {
		t.Fatalf("default-budget client failed on a slow import: %v", err)
	}

	// An explicit caller-configured timeout must still be honored.
	short := NewClient("token", &http.Client{Timeout: time.Second})
	short.Endpoint = server.URL
	started := time.Now()
	if _, err := short.ListEntries(context.Background(), 7); err == nil {
		t.Fatal("explicit 1s timeout should still fail on a 3s import")
	}
	if elapsed := time.Since(started); elapsed < 900*time.Millisecond {
		t.Fatalf("short client failed after %v, want the 1s timeout to fire", elapsed)
	}
}

func TestLimiterStretchesIntervalAcrossRemainingBudget(t *testing.T) {
	limiter := newRateLimiter(defaultRequestsPerMinute)
	resetAt := time.Now().Add(120 * time.Second)
	header := http.Header{}
	header.Set("X-RateLimit-Limit", "30")
	header.Set("X-RateLimit-Remaining", "2")
	header.Set("X-RateLimit-Reset", strconv.FormatInt(resetAt.Unix(), 10))
	limiter.observe(header, http.StatusOK)

	limiter.mu.Lock()
	next := limiter.next
	interval := limiter.interval
	limiter.mu.Unlock()

	// The window has ~120s left and 2 requests of budget, so the limiter must
	// space them ~60s apart instead of the 2s derived from the 30/min limit.
	delay := time.Until(next)
	if delay < 55*time.Second || delay > 65*time.Second {
		t.Fatalf("delay = %v, want roughly 60s (120s reset horizon / 2 remaining)", delay)
	}
	if interval < 55*time.Second || interval > 65*time.Second {
		t.Fatalf("interval = %v, want roughly 60s", interval)
	}
}
