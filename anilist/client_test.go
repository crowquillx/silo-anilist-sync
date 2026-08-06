package anilist

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
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

func TestAdvanceProgressPreservesPausedStatusAndUsesEntryID(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var body struct {
			Variables map[string]any `json:"variables"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if calls == 1 {
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"Media": map[string]any{
				"episodes": 12, "mediaListEntry": map[string]any{"id": 77, "progress": 3, "status": "PAUSED"},
			}}})
			return
		}
		if body.Variables["id"] != float64(77) || body.Variables["progress"] != float64(4) {
			t.Fatalf("mutation variables = %#v", body.Variables)
		}
		if _, ok := body.Variables["status"]; ok {
			t.Fatalf("paused status should be preserved: %#v", body.Variables)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"SaveMediaListEntry": map[string]any{"id": 77}}})
	}))
	defer server.Close()
	client := NewClient("token", server.Client())
	client.Endpoint = server.URL
	if err := client.AdvanceProgress(context.Background(), 1, 4); err != nil {
		t.Fatal(err)
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

func TestListEntriesPageReturnsStableMediaData(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Query     string         `json:"query"`
			Variables map[string]any `json:"variables"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.Variables["userId"] != float64(7) || body.Variables["page"] != float64(2) ||
			body.Variables["perPage"] != float64(25) {
			t.Fatalf("variables = %#v", body.Variables)
		}
		if body.Query == "" {
			t.Fatal("query is empty")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"Page": map[string]any{
			"pageInfo": map[string]any{"hasNextPage": true},
			"mediaList": []map[string]any{{
				"id": 9, "mediaId": 42, "status": "COMPLETED", "progress": 0,
				"media": map[string]any{
					"id": 42, "format": "TV", "episodes": 12,
					"title":     map[string]any{"romaji": "Romaji", "english": "English"},
					"startDate": map[string]any{"year": 2024},
				},
			}},
		}}})
	}))
	defer server.Close()
	client := NewClient("token", server.Client())
	client.Endpoint = server.URL
	page, err := client.ListEntriesPage(context.Background(), 7, 2, 25)
	if err != nil {
		t.Fatal(err)
	}
	if !page.HasNextPage || len(page.Entries) != 1 {
		t.Fatalf("page = %#v", page)
	}
	entry := page.Entries[0]
	if entry.MediaID != 42 || entry.Media.ID != 42 || entry.PreferredTitle() != "English" || entry.CompletedProgress() != 12 {
		t.Fatalf("entry = %#v", entry)
	}
}
