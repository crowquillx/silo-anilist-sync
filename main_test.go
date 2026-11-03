package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	publicmanifest "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginsdk/manifest"
	"github.com/crowquillx/silo-anilist-sync/anilist"
	"github.com/crowquillx/silo-anilist-sync/mapping"
)

func TestExchangeAPIKeyConnectsAndSetsExpiry(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"Viewer": map[string]any{
			"id": 7, "name": "tester",
			"avatar":  map[string]any{"large": "http://x/y.png"},
			"siteUrl": "https://anilist.co/user/tester",
		}}})
	}))
	defer upstream.Close()
	old := anilist.Endpoint
	anilist.Endpoint = upstream.URL
	t.Cleanup(func() { anilist.Endpoint = old })

	resp, err := (&server{}).ExchangeAPIKey(context.Background(), &pluginv1.WatchSyncExchangeAPIKeyRequest{
		ApiKey: "eyJhbGciOiJIUzI1NiJ9.eyJleHAiOjE3MDAwMDAwMDAsInN1YiI6IjEyMyJ9.e30",
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetFault() != nil {
		t.Fatalf("fault = %#v", resp.GetFault())
	}
	credentials := resp.GetCredentials()
	if credentials.GetAccessToken() == "" || credentials.GetTokenType() != "Bearer" {
		t.Fatalf("credentials = %#v", credentials)
	}
	if resp.GetAccount().GetUsername() != "tester" {
		t.Fatalf("account = %#v", resp.GetAccount())
	}
	if got := credentials.GetExpiresAt().AsTime(); !got.Equal(time.Date(2023, 11, 14, 22, 13, 20, 0, time.UTC)) {
		t.Fatalf("expires_at = %v, want 2023-11-14T22:13:20Z", got)
	}
	if credentials.GetSecretAttributes()["user_id"] != "7" {
		t.Fatalf("secret attributes = %#v", credentials.GetSecretAttributes())
	}
}

func TestExchangeAPIKeyRejectsInvalidCredential(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer upstream.Close()
	old := anilist.Endpoint
	anilist.Endpoint = upstream.URL
	t.Cleanup(func() { anilist.Endpoint = old })

	resp, err := (&server{}).ExchangeAPIKey(context.Background(), &pluginv1.WatchSyncExchangeAPIKeyRequest{
		ApiKey: "garbage.token.value",
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL {
		t.Fatalf("fault = %#v", resp.GetFault())
	}
}

func TestResolveTargetsRequiresProviderConvergence(t *testing.T) {
	dataset := mapping.Catalog{AniBridge: mapping.Dataset{
		"tvdb_show:1:s1": {"anilist:10": {"1-12": "1-12"}},
		"tmdb_show:2:s1": {"anilist:11": {"1-12": "1-12"}},
	}}
	_, err := resolveTargets(context.Background(), dataset, &pluginv1.WatchSyncMedia{
		MediaType:         pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE,
		SeasonNumber:      1,
		EpisodeNumber:     3,
		SeriesExternalIds: map[string]string{"tvdb": "1", "tmdb": "2"},
	})
	if err == nil {
		t.Fatal("expected disagreeing mappings to fail")
	}
}

func TestResolveTargetsMapsEpisode(t *testing.T) {
	dataset := mapping.Catalog{AniBridge: mapping.Dataset{
		"tvdb_show:1:s1": {"anilist:10": {"1-12": "1-12"}},
		"tmdb_show:2:s1": {"anilist:10": {"1-12": "1-12"}},
	}}
	targets, err := resolveTargets(context.Background(), dataset, &pluginv1.WatchSyncMedia{
		MediaType:         pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE,
		SeasonNumber:      1,
		EpisodeNumber:     3,
		SeriesExternalIds: map[string]string{"tvdb": "1", "tmdb": "2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if targets[10] != 3 {
		t.Fatalf("targets = %#v", targets)
	}
}

func TestApplyEventRejectsUnwatch(t *testing.T) {
	result := applyEvent(context.Background(), nil, mapping.Catalog{}, &pluginv1.WatchSyncEvent{
		EventId:   "event-1",
		Operation: pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_UNWATCHED,
	}, watchBehavior{})
	if result.GetStatus() != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED {
		t.Fatalf("status = %v", result.GetStatus())
	}
}

func TestMappingServiceFailureRetries(t *testing.T) {
	result := applyErrorResult("event-1", &mapping.TemporaryError{Err: errors.New("ARM offline")})
	if result.GetStatus() != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_RETRY {
		t.Fatalf("status = %v", result.GetStatus())
	}
	if result.GetFault().GetSafeMessage() != "temporary anime mapping service failure (ARM offline)" {
		t.Fatalf("fault = %#v", result.GetFault())
	}
}

func TestManifestAdvertisesWatchedImportAndDeviceCodeConfiguration(t *testing.T) {
	manifest, err := publicmanifest.Load(manifestJSON)
	if err != nil {
		t.Fatal(err)
	}
	capabilities := manifest.GetCapabilities()
	if len(capabilities) != 3 || capabilities[2].GetType() != "http_routes.v1" {
		t.Fatalf("capabilities = %#v", capabilities)
	}
	for _, capability := range capabilities[:2] {
		descriptor := capability.GetWatchSyncProvider()
		if !descriptor.GetImportWatched() || !descriptor.GetScrobblePlayback() ||
			!descriptor.GetImportRatings() || !descriptor.GetExportRatings() || descriptor.GetSyncDropped() ||
			descriptor.GetMaxBatchSize() != 10 {
			t.Fatalf("%s descriptor = %#v", capability.GetId(), descriptor)
		}
		requiresWatched := descriptor.GetRatingExportRequiresWatched()
		if len(requiresWatched) != 1 || requiresWatched[0] != pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE {
			t.Fatalf("%s rating_export_requires_watched = %v", capability.GetId(), requiresWatched)
		}
		var keys []string
		for _, setting := range descriptor.GetConnectionSettings() {
			if setting.GetType() != pluginv1.WatchSyncConnectionSettingType_WATCH_SYNC_CONNECTION_SETTING_TYPE_BOOLEAN || setting.GetDefaultValue().GetBoolValue() {
				t.Fatalf("%s setting %#v", capability.GetId(), setting)
			}
			keys = append(keys, setting.GetKey())
		}
		if strings.Join(keys, ",") != "sync_manual_watched,only_existing_entries" {
			t.Fatalf("%s connection settings = %v", capability.GetId(), keys)
		}
	}
	routes := manifest.GetHttpRoutes()
	if len(routes) != 4 || routes[0].GetPath() != "/" || !routes[0].GetNavigable() || routes[0].GetNavigationKind() != "admin" {
		t.Fatalf("routes = %#v", routes)
	}
	for _, route := range routes {
		if route.GetAccess() != "admin" {
			t.Fatalf("route %s access = %q; the page lists every account", route.GetId(), route.GetAccess())
		}
	}
	descriptor := capabilities[0].GetWatchSyncProvider()
	authMethods := descriptor.GetAuthMethods()
	if len(authMethods) != 1 || authMethods[0] != pluginv1.WatchSyncAuthMethod_WATCH_SYNC_AUTH_METHOD_DEVICE_CODE {
		t.Fatalf("auth methods = %#v", authMethods)
	}
	if len(manifest.GetGlobalConfigSchema()) != 1 {
		t.Fatalf("global config schema = %#v", manifest.GetGlobalConfigSchema())
	}
	schema := manifest.GetGlobalConfigSchema()[0]
	var jsonSchema struct {
		Properties map[string]json.RawMessage `json:"properties"`
		Required   []string                   `json:"required"`
	}
	if err := json.Unmarshal([]byte(schema.GetJsonSchema()), &jsonSchema); err != nil {
		t.Fatal(err)
	}
	if len(jsonSchema.Properties) != 4 {
		t.Fatalf("schema properties = %#v", jsonSchema.Properties)
	}
	if _, ok := jsonSchema.Properties["client_id"]; ok {
		t.Fatal("schema still advertises client_id")
	}
	if _, ok := jsonSchema.Properties["client_secret"]; ok {
		t.Fatal("schema still advertises client_secret")
	}
	var onlyExisting struct {
		Default bool `json:"default"`
	}
	if err := json.Unmarshal(jsonSchema.Properties["only_existing_entries"], &onlyExisting); err != nil {
		t.Fatal(err)
	}
	if onlyExisting.Default {
		t.Fatal("only_existing_entries must default to false")
	}
	if len(jsonSchema.Required) != 0 {
		t.Fatalf("schema required = %#v", jsonSchema.Required)
	}
	fields := schema.GetAdminForm().GetFields()
	if len(fields) != 4 || fields[0].GetKey() != "sync_manual_watched" ||
		fields[1].GetKey() != "playback_completion_percent" ||
		fields[2].GetKey() != "import_full_scan" ||
		fields[3].GetKey() != "only_existing_entries" ||
		fields[3].GetDefaultValue() == nil || fields[3].GetDefaultValue().GetBoolValue() {
		t.Fatalf("provider fields = %#v", fields)
	}
}

func TestWatchBehaviorParsesExistingOnlySetting(t *testing.T) {
	if behavior := watchBehaviorFromConfig(nil); behavior.onlyExistingEntries {
		t.Fatal("only_existing_entries must default to false")
	}
	behavior := watchBehaviorFromConfig(&pluginv1.WatchSyncProviderConfig{Values: map[string]string{
		"provider.only_existing_entries": "true",
	}})
	if !behavior.onlyExistingEntries {
		t.Fatal("only_existing_entries=true was not parsed")
	}
}

func TestRemoteStatesExpandAniListProgressIntoMappedEpisodes(t *testing.T) {
	entry := anilist.ListEntry{ID: 9, MediaID: 42, Status: "CURRENT", Progress: 2, UpdatedAt: 1756500000}
	entry.Media.ID = 42
	entry.Media.Format = "TV"
	entry.Media.Title.English = "Example Anime"
	entry.Media.StartDate.Year = 2024
	states, _ := remoteStates([]anilist.ListEntry{entry}, mapping.Catalog{AniBridge: mapping.Dataset{
		"tvdb_show:100:s2": {"anilist:42": {"1-12": "1-12"}},
		"tmdb_show:200:s2": {"anilist:42": {"1-12": "1-12"}},
	}})
	if len(states) != 2 {
		t.Fatalf("states = %#v", states)
	}
	second := states[1]
	if second.GetProviderItemKey() != "anilist:9:s2:e2" ||
		second.GetMedia().GetMediaType() != pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE ||
		second.GetMedia().GetSeriesExternalIds()["anilist"] != "42" ||
		second.GetMedia().GetSeriesExternalIds()["tvdb"] != "100" ||
		second.GetMedia().GetSeriesExternalIds()["tmdb"] != "200" ||
		second.GetWatched().GetPlayCount() != 1 {
		t.Fatalf("second state = %#v", second)
	}
	lastWatched := second.GetWatched().GetLastWatchedAt()
	if lastWatched == nil || lastWatched.AsTime().Unix() != 1756500000 {
		t.Fatalf("last watched at = %#v", lastWatched)
	}
}

func TestRemoteStatesWithoutEntryTimestampOmitLastWatchedAt(t *testing.T) {
	entry := anilist.ListEntry{ID: 9, MediaID: 42, Status: "CURRENT", Progress: 1}
	entry.Media.ID = 42
	entry.Media.Format = "TV"
	states, _ := remoteStates([]anilist.ListEntry{entry}, mapping.Catalog{AniBridge: mapping.Dataset{
		"tvdb_show:100:s1": {"anilist:42": {"1-12": "1-12"}},
	}})
	if len(states) != 1 || states[0].GetWatched().GetLastWatchedAt() != nil {
		t.Fatalf("states = %#v", states)
	}
}

func TestImportCursorRoundTrip(t *testing.T) {
	if since := parseImportCursor(""); !since.IsZero() {
		t.Fatalf("empty cursor should mean full scan, got %v", since)
	}
	if since := parseImportCursor("full-v1"); !since.IsZero() {
		t.Fatalf("legacy cursor should mean full scan, got %v", since)
	}
	if since := parseImportCursor("inc:notanumber"); !since.IsZero() {
		t.Fatalf("malformed cursor should mean full scan, got %v", since)
	}
	since := parseImportCursor("inc:1756500000")
	if since.Unix() != 1756500000 {
		t.Fatalf("since = %v", since)
	}
	got := formatImportCursor(since)
	if parseImportCursor(got).Unix() != 1756500000 {
		t.Fatalf("formatImportCursor() = %q", got)
	}
	if parseImportCursor(formatImportCursor(time.Time{})).IsZero() {
		t.Fatal("zero time should format to a parseable cursor")
	}
}

func TestEntriesChangedSinceKeepsInclusiveBoundary(t *testing.T) {
	entries := []anilist.ListEntry{
		{MediaID: 1, UpdatedAt: 50},
		{MediaID: 2, UpdatedAt: 100},
		{MediaID: 3, UpdatedAt: 200},
	}
	kept := entriesChangedSince(entries, time.Unix(100, 0).UTC())
	if len(kept) != 2 || kept[0].MediaID != 2 || kept[1].MediaID != 3 {
		t.Fatalf("kept = %#v", kept)
	}
	if all := entriesChangedSince(entries, time.Time{}); len(all) != 3 {
		t.Fatalf("full scan should keep everything, got %#v", all)
	}
}

func TestRemotePageTokenRejectsMalformedValues(t *testing.T) {
	if _, _, err := remotePageToken("1:2:3"); err == nil {
		t.Fatal("expected malformed token to fail")
	}
	// A 0.7 token from a traversal that spans the upgrade still pages on.
	if snapshot, offset, err := remotePageToken("2:7"); err != nil || snapshot != 0 || offset != 7 {
		t.Fatalf("remotePageToken() = %d, %d, %v", snapshot, offset, err)
	}
	if snapshot, offset, err := remotePageToken("v2:1762171200:25"); err != nil || snapshot != 1762171200 || offset != 25 {
		t.Fatalf("remotePageToken() = %d, %d, %v", snapshot, offset, err)
	}
	for _, token := range []string{"v2:0:1", "v2:5", "v2:x:1", "v2:5:-1"} {
		if _, _, err := remotePageToken(token); err == nil {
			t.Fatalf("expected %q to fail", token)
		}
	}
}

func TestOnlyExistingEntriesSkipsMissingTarget(t *testing.T) {
	queries, mutations := 0, 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries++
		var request struct {
			Query string `json:"query"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(request.Query, "SaveMediaListEntry") {
			mutations++
			t.Fatal("missing target must not be mutated")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"Media": map[string]any{
			"episodes": 12, "mediaListEntry": nil,
		}}})
	}))
	defer upstream.Close()
	client := anilist.NewClient("token", upstream.Client())
	client.Endpoint = upstream.URL
	dataset := mapping.Catalog{AniBridge: mapping.Dataset{
		"tvdb_show:1:s1": {"anilist:10": {"1-12": "1-12"}},
	}}
	event := &pluginv1.WatchSyncEvent{
		EventId:   "existing-only-missing",
		Operation: pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_WATCHED,
		Origin:    pluginv1.WatchSyncOrigin_WATCH_SYNC_ORIGIN_PLAYBACK_COMPLETION,
		Media: &pluginv1.WatchSyncMedia{
			MediaType:         pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE,
			SeasonNumber:      1,
			EpisodeNumber:     3,
			SeriesExternalIds: map[string]string{"tvdb": "1"},
		},
	}
	result := applyEvent(context.Background(), client, dataset, event, watchBehavior{onlyExistingEntries: true})
	if result.GetStatus() != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE {
		t.Fatalf("status = %v, want no change; fault = %#v", result.GetStatus(), result.GetFault())
	}
	if queries != 1 || mutations != 0 {
		t.Fatalf("AniList requests = %d queries, %d mutations; want 1 query and no mutations", queries, mutations)
	}
}

func TestOnlyExistingEntriesAppliesMixedTargets(t *testing.T) {
	queries, mutations := 0, 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Query     string         `json:"query"`
			Variables map[string]any `json:"variables"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(request.Query, "SaveMediaListEntry") {
			mutations++
			if request.Variables["id"] != float64(11) || request.Variables["progress"] != float64(3) {
				t.Fatalf("mutation variables = %#v", request.Variables)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"SaveMediaListEntry": map[string]any{"id": 11}}})
			return
		}
		queries++
		if request.Variables["id"] == float64(10) {
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"Media": map[string]any{
				"episodes": 12, "mediaListEntry": nil,
			}}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"Media": map[string]any{
			"episodes": 12, "mediaListEntry": map[string]any{"id": 11, "progress": 1, "status": "CURRENT"},
		}}})
	}))
	defer upstream.Close()
	client := anilist.NewClient("token", upstream.Client())
	client.Endpoint = upstream.URL
	dataset := mapping.Catalog{AniBridge: mapping.Dataset{
		"tvdb_show:1:s1": {
			"anilist:10": {"1-12": "1-12"},
			"anilist:11": {"1-12": "1-12"},
		},
	}}
	event := &pluginv1.WatchSyncEvent{
		EventId:   "existing-only-mixed",
		Operation: pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_WATCHED,
		Origin:    pluginv1.WatchSyncOrigin_WATCH_SYNC_ORIGIN_PLAYBACK_COMPLETION,
		Media: &pluginv1.WatchSyncMedia{
			MediaType:         pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE,
			SeasonNumber:      1,
			EpisodeNumber:     3,
			SeriesExternalIds: map[string]string{"tvdb": "1"},
		},
	}
	result := applyEvent(context.Background(), client, dataset, event, watchBehavior{onlyExistingEntries: true})
	if result.GetStatus() != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED {
		t.Fatalf("status = %v, want applied; fault = %#v", result.GetStatus(), result.GetFault())
	}
	if queries != 2 || mutations != 1 {
		t.Fatalf("AniList requests = %d queries, %d mutations; want 2 queries and 1 mutation", queries, mutations)
	}
}

func TestManualWatchedMarksRequireExplicitToggle(t *testing.T) {
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 1 {
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"Media": map[string]any{
				"episodes": 12, "mediaListEntry": nil,
			}}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"SaveMediaListEntry": map[string]any{"id": 1}}})
	}))
	defer upstream.Close()
	client := anilist.NewClient("token", upstream.Client())
	client.Endpoint = upstream.URL
	dataset := mapping.Catalog{AniBridge: mapping.Dataset{
		"tvdb_show:1:s1": {"anilist:10": {"1-12": "1-12"}},
	}}
	event := &pluginv1.WatchSyncEvent{
		EventId:   "manual-1",
		Operation: pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_WATCHED,
		Origin:    pluginv1.WatchSyncOrigin_WATCH_SYNC_ORIGIN_RECONCILIATION,
		Media: &pluginv1.WatchSyncMedia{
			MediaType:         pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE,
			SeasonNumber:      1,
			EpisodeNumber:     3,
			SeriesExternalIds: map[string]string{"tvdb": "1"},
		},
	}
	if result := applyEvent(context.Background(), client, dataset, event, watchBehavior{}); result.GetStatus() != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE {
		t.Fatalf("disabled status = %v", result.GetStatus())
	}
	if calls != 0 {
		t.Fatalf("disabled manual sync made %d AniList calls", calls)
	}
	if result := applyEvent(context.Background(), client, dataset, event, watchBehavior{syncManualWatched: true}); result.GetStatus() != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED {
		t.Fatalf("enabled status = %v, fault = %#v", result.GetStatus(), result.GetFault())
	}
	if calls != 2 {
		t.Fatalf("enabled manual sync made %d AniList calls, want 2", calls)
	}
}

func TestPlaybackStopUsesConfiguredCompletionThreshold(t *testing.T) {
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 1 {
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"Media": map[string]any{
				"episodes": 12, "mediaListEntry": nil,
			}}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"SaveMediaListEntry": map[string]any{"id": 1}}})
	}))
	defer upstream.Close()
	client := anilist.NewClient("token", upstream.Client())
	client.Endpoint = upstream.URL
	dataset := mapping.Catalog{AniBridge: mapping.Dataset{
		"tvdb_show:1:s1": {"anilist:10": {"1-12": "1-12"}},
	}}
	event := &pluginv1.WatchSyncEvent{
		EventId:           "playback-1",
		Operation:         pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_STOP,
		CompletionPercent: 90,
		Media: &pluginv1.WatchSyncMedia{
			MediaType:         pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE,
			SeasonNumber:      1,
			EpisodeNumber:     3,
			SeriesExternalIds: map[string]string{"tvdb": "1"},
		},
	}
	behavior := watchBehaviorFromConfig(&pluginv1.WatchSyncProviderConfig{Values: map[string]string{
		"provider.playback_completion_percent": "90",
	}})
	if result := applyEvent(context.Background(), client, dataset, event, behavior); result.GetStatus() != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE {
		t.Fatalf("threshold status = %v", result.GetStatus())
	}
	event.CompletionPercent = 90.1
	if result := applyEvent(context.Background(), client, dataset, event, behavior); result.GetStatus() != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED {
		t.Fatalf("completed status = %v, fault = %#v", result.GetStatus(), result.GetFault())
	}
	if calls != 2 {
		t.Fatalf("completed playback made %d AniList calls, want 2", calls)
	}
}

func TestIgnoredManualEventsDoNotLoadMappings(t *testing.T) {
	response, err := (&server{}).ApplyEvents(context.Background(), &pluginv1.WatchSyncApplyEventsRequest{
		Context: &pluginv1.WatchSyncAuthenticatedContext{
			ProviderConfig: &pluginv1.WatchSyncProviderConfig{},
		},
		Events: []*pluginv1.WatchSyncEvent{{
			EventId:   "manual-1",
			Operation: pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_WATCHED,
			Origin:    pluginv1.WatchSyncOrigin_WATCH_SYNC_ORIGIN_RECONCILIATION,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetFault() != nil || len(response.GetResults()) != 1 ||
		response.GetResults()[0].GetStatus() != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE {
		t.Fatalf("response = %#v", response)
	}
}

func TestFaultFromError(t *testing.T) {
	longPending := 90 * time.Second
	tests := []struct {
		name        string
		err         error
		wantCode    pluginv1.WatchSyncFaultCode
		wantMessage string
		wantRetry   time.Duration
	}{
		{
			name:        "graphql too many requests is rate limited",
			err:         &anilist.Error{Status: http.StatusOK, Message: "AniList GraphQL: Too Many Requests"},
			wantCode:    pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_RATE_LIMITED,
			wantMessage: "AniList rate limit reached",
			wantRetry:   60 * time.Second,
		},
		{
			name:        "graphql too many requests keeps retry after",
			err:         &anilist.Error{Status: http.StatusOK, Message: "AniList GraphQL: Too Many Requests", RetryAfter: longPending},
			wantCode:    pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_RATE_LIMITED,
			wantMessage: "AniList rate limit reached",
			wantRetry:   longPending,
		},
		{
			name:        "http 500 is temporary with detail",
			err:         &anilist.Error{Status: http.StatusInternalServerError},
			wantCode:    pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY,
			wantMessage: "temporary AniList request failure (HTTP 500)",
		},
		{
			name:        "graphql validation failure is temporary with detail",
			err:         &anilist.Error{Status: http.StatusOK, Message: "AniList GraphQL: Validation Failed (f.name)."},
			wantCode:    pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY,
			wantMessage: "temporary AniList request failure (AniList GraphQL: Validation Failed (f.name).)",
		},
		{
			name:        "http 404 is permanent with detail",
			err:         &anilist.Error{Status: http.StatusNotFound},
			wantCode:    pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMANENT,
			wantMessage: "AniList rejected the request (HTTP 404)",
		},
		{
			name:        "invalid token answered with 400 needs a reconnect",
			err:         &anilist.Error{Status: http.StatusBadRequest, Message: "AniList GraphQL: Invalid token", InvalidToken: true},
			wantCode:    pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL,
			wantMessage: reconnectMessage,
		},
		{
			name:        "http 401 needs a reconnect",
			err:         &anilist.Error{Status: http.StatusUnauthorized},
			wantCode:    pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL,
			wantMessage: reconnectMessage,
		},
		{
			name:        "http 400 keeps AniList's message",
			err:         &anilist.Error{Status: http.StatusBadRequest, Message: "AniList GraphQL: Variable \"$id\" got invalid value"},
			wantCode:    pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMANENT,
			wantMessage: "AniList rejected the request (HTTP 400; AniList GraphQL: Variable \"$id\" got invalid value)",
		},
		{
			name:        "mapping contradicting AniList is permanent",
			err:         &anilist.MismatchError{Message: "mapped progress 13 exceeds AniList episode count 12"},
			wantCode:    pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMANENT,
			wantMessage: "the anime mapping does not match AniList: mapped progress 13 exceeds AniList episode count 12",
		},
		{
			name:        "non anilist error keeps generic message but carries detail",
			err:         errors.New("network partition"),
			wantCode:    pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY,
			wantMessage: "temporary AniList request failure (network partition)",
		},
		{
			name:        "transport failure carries underlying error text",
			err:         errors.New(`call AniList: Post "https://graphql.anilist.co": context deadline exceeded`),
			wantCode:    pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY,
			wantMessage: `temporary AniList request failure (call AniList: Post "https://graphql.anilist.co": context deadline exceeded)`,
		},
		{
			name:        "long transport error is trimmed",
			err:         errors.New(strings.Repeat("x", 250)),
			wantCode:    pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY,
			wantMessage: "temporary AniList request failure (" + strings.Repeat("x", 200) + "...)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fault := faultFromError(tt.err)
			if fault.GetCode() != tt.wantCode {
				t.Fatalf("code = %v, want %v", fault.GetCode(), tt.wantCode)
			}
			if fault.GetSafeMessage() != tt.wantMessage {
				t.Fatalf("safe_message = %q, want %q", fault.GetSafeMessage(), tt.wantMessage)
			}
			if got := fault.GetRetryAfter().AsDuration(); got != tt.wantRetry {
				t.Fatalf("retry_after = %v, want %v", got, tt.wantRetry)
			}
		})
	}
}

// stubMappingTransport serves canned AniBridge and Anime-Lists artifacts for
// the mapping.Client without touching the network, mirroring how tests stub
// anilist.Endpoint.
type stubMappingTransport struct{}

func (stubMappingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	status, body := http.StatusNotFound, ""
	switch req.URL.String() {
	case mapping.DefaultURL:
		status, body = http.StatusOK, `{"$meta":{"schema_version":"3.0.0"},"tvdb_show:100:s1":{"anilist:10":{"1-12":"1-12"}},"tvdb_show:200:s1":{"anilist:11":{"1-12":"1-12"}}}`
	case mapping.DefaultAnimeListsURL:
		status, body = http.StatusOK, `<anime-list/>`
	}
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}, nil
}

func TestListRemoteStateWalksPaginationWithSingleUpstreamImport(t *testing.T) {
	anilistCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		anilistCalls++
		var body struct {
			Variables map[string]any `json:"variables"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.Variables["userId"] != float64(7) {
			t.Fatalf("variables = %#v", body.Variables)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"MediaListCollection": map[string]any{
			"lists": []map[string]any{{"entries": []map[string]any{
				{
					"id": 1, "mediaId": 10, "status": "CURRENT", "progress": 1,
					"media": map[string]any{
						"id": 10, "format": "TV", "episodes": 12,
						"title":     map[string]any{"romaji": "Alpha"},
						"startDate": map[string]any{"year": 2020},
					},
				},
				{
					"id": 2, "mediaId": 11, "status": "CURRENT", "progress": 1,
					"media": map[string]any{
						"id": 11, "format": "TV", "episodes": 12,
						"title":     map[string]any{"romaji": "Beta"},
						"startDate": map[string]any{"year": 2021},
					},
				},
			}}},
		}}})
	}))
	defer upstream.Close()
	old := anilist.Endpoint
	anilist.Endpoint = upstream.URL
	t.Cleanup(func() { anilist.Endpoint = old })

	srv := &server{mappings: mapping.NewClient(&http.Client{Transport: stubMappingTransport{}})}
	request := func(pageToken string) *pluginv1.WatchSyncListRemoteStateRequest {
		return &pluginv1.WatchSyncListRemoteStateRequest{
			PageToken: pageToken,
			PageSize:  1,
			Context: &pluginv1.WatchSyncAuthenticatedContext{
				Credentials: &pluginv1.WatchSyncCredentials{
					AccessToken:      "token",
					SecretAttributes: map[string]string{"user_id": "7"},
				},
			},
		}
	}

	total := 0
	pageToken := ""
	for pages := 0; ; pages++ {
		response, err := srv.ListRemoteState(context.Background(), request(pageToken))
		if err != nil {
			t.Fatal(err)
		}
		if response.GetFault() != nil {
			t.Fatalf("fault = %#v", response.GetFault())
		}
		total += len(response.GetItems())
		if pages >= 10 {
			t.Fatal("pagination walk did not terminate")
		}
		if response.GetNextPageToken() == "" {
			if !response.GetCompleteSnapshot() {
				t.Fatal("full traversal must be a complete snapshot")
			}
			if parseImportCursor(response.GetNextCursor()).IsZero() {
				t.Fatalf("next_cursor = %q, want a parseable inc cursor", response.GetNextCursor())
			}
			break
		}
		pageToken = response.GetNextPageToken()
	}
	if total != 2 {
		t.Fatalf("total items = %d, want 2", total)
	}
	if anilistCalls != 1 {
		t.Fatalf("AniList upstream calls = %d, want 1 cached import across pages", anilistCalls)
	}
}

func TestListRemoteStateIncrementalCursorSkipsUnchangedEntries(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"MediaListCollection": map[string]any{
			"lists": []map[string]any{{"entries": []map[string]any{
				{
					"id": 1, "mediaId": 10, "status": "CURRENT", "progress": 1, "updatedAt": 100,
					"media": map[string]any{
						"id": 10, "format": "TV", "episodes": 12,
						"title":     map[string]any{"romaji": "Alpha"},
						"startDate": map[string]any{"year": 2020},
					},
				},
				{
					"id": 2, "mediaId": 11, "status": "CURRENT", "progress": 1, "updatedAt": 900,
					"media": map[string]any{
						"id": 11, "format": "TV", "episodes": 12,
						"title":     map[string]any{"romaji": "Beta"},
						"startDate": map[string]any{"year": 2021},
					},
				},
			}}},
		}}})
	}))
	defer upstream.Close()
	old := anilist.Endpoint
	anilist.Endpoint = upstream.URL
	t.Cleanup(func() { anilist.Endpoint = old })

	srv := &server{mappings: mapping.NewClient(&http.Client{Transport: stubMappingTransport{}})}
	response, err := srv.ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{
		Cursor:   "inc:500",
		PageSize: 25,
		Context: &pluginv1.WatchSyncAuthenticatedContext{
			Credentials: &pluginv1.WatchSyncCredentials{
				AccessToken:      "token",
				SecretAttributes: map[string]string{"user_id": "7"},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetFault() != nil {
		t.Fatalf("fault = %#v", response.GetFault())
	}
	if response.GetCompleteSnapshot() {
		t.Fatal("incremental traversal must not claim a complete snapshot")
	}
	items := response.GetItems()
	if len(items) != 1 || items[0].GetProviderItemKey() != "anilist:2:s1:e1" {
		t.Fatalf("items = %#v", items)
	}
	next := parseImportCursor(response.GetNextCursor())
	if next.IsZero() || next.Unix() < 900 {
		t.Fatalf("next_cursor = %q", response.GetNextCursor())
	}
}

func TestListRemoteStateContinuationAvoidsRepeatedExpansion(t *testing.T) {
	entries := make([]anilist.ListEntry, 256)
	for index := range entries {
		entries[index] = anilist.ListEntry{ID: index + 1, MediaID: 10, Status: "CURRENT", Progress: 1}
		entries[index].Media.ID = 10
		entries[index].Media.Format = "TV"
		entries[index].Media.Title.Romaji = "Example Anime"
	}
	mappings := mapping.NewClient(&http.Client{Transport: stubMappingTransport{}})
	newServer := func() *server {
		return &server{
			mappings: mappings,
			imports:  seededImports(7, entries, time.Now()),
		}
	}
	request := func(pageToken string) *pluginv1.WatchSyncListRemoteStateRequest {
		return &pluginv1.WatchSyncListRemoteStateRequest{
			PageToken: pageToken,
			PageSize:  1,
			Context: &pluginv1.WatchSyncAuthenticatedContext{Credentials: &pluginv1.WatchSyncCredentials{
				AccessToken:      "token",
				SecretAttributes: map[string]string{"user_id": "7"},
			}},
		}
	}

	warm := newServer()
	first, err := warm.ListRemoteState(context.Background(), request(""))
	if err != nil || first.GetFault() != nil {
		t.Fatalf("first page: %v, fault = %#v", err, first.GetFault())
	}
	if len(first.GetItems()) != 1 || first.GetNextPageToken() == "" {
		t.Fatalf("first page = %#v", first)
	}
	continuation, err := warm.ListRemoteState(context.Background(), request(first.GetNextPageToken()))
	if err != nil || continuation.GetFault() != nil {
		t.Fatalf("continuation page: %v, fault = %#v", err, continuation.GetFault())
	}
	if len(continuation.GetItems()) != 1 {
		t.Fatalf("continuation page = %#v", continuation)
	}
	continuation.GetItems()[0].GetMedia().Title = "caller mutation"
	unchanged, err := warm.ListRemoteState(context.Background(), request(first.GetNextPageToken()))
	if err != nil || unchanged.GetFault() != nil {
		t.Fatalf("repeated continuation page: %v, fault = %#v", err, unchanged.GetFault())
	}
	if unchanged.GetItems()[0].GetMedia().GetTitle() != "Example Anime" {
		t.Fatalf("cached continuation was mutated through response: %#v", unchanged.GetItems()[0])
	}

	firstAllocs := testing.AllocsPerRun(5, func() {
		response, err := newServer().ListRemoteState(context.Background(), request(""))
		if err != nil || response.GetFault() != nil {
			panic("first page failed")
		}
	})
	continuationAllocs := testing.AllocsPerRun(5, func() {
		response, err := warm.ListRemoteState(context.Background(), request(first.GetNextPageToken()))
		if err != nil || response.GetFault() != nil {
			panic("continuation page failed")
		}
	})
	if continuationAllocs >= firstAllocs/2 {
		t.Fatalf("continuation allocations = %.0f, first-page allocations = %.0f; continuation still expands the full list", continuationAllocs, firstAllocs)
	}
}

func TestListRemoteStatePreparedCacheTracksCursorAndFullScanMode(t *testing.T) {
	entries := []anilist.ListEntry{
		{ID: 1, MediaID: 10, Status: "CURRENT", Progress: 1, UpdatedAt: 100},
		{ID: 2, MediaID: 11, Status: "CURRENT", Progress: 1, UpdatedAt: 900},
	}
	for index := range entries {
		entries[index].Media.ID = entries[index].MediaID
		entries[index].Media.Format = "TV"
		entries[index].Media.Title.Romaji = "Example Anime"
	}
	srv := &server{
		mappings: mapping.NewClient(&http.Client{Transport: stubMappingTransport{}}),
		imports:  seededImports(7, entries, time.Now()),
	}
	request := func(cursor string, fullScan bool) *pluginv1.WatchSyncListRemoteStateRequest {
		return &pluginv1.WatchSyncListRemoteStateRequest{
			Cursor:   cursor,
			PageSize: 25,
			Context: &pluginv1.WatchSyncAuthenticatedContext{
				Credentials: &pluginv1.WatchSyncCredentials{
					AccessToken:      "token",
					SecretAttributes: map[string]string{"user_id": "7"},
				},
				ProviderConfig: &pluginv1.WatchSyncProviderConfig{Values: map[string]string{
					"provider.import_full_scan": strconv.FormatBool(fullScan),
				}},
			},
		}
	}

	response, err := srv.ListRemoteState(context.Background(), request("inc:900", false))
	if err != nil || response.GetFault() != nil {
		t.Fatalf("latest incremental response: %v, fault = %#v", err, response.GetFault())
	}
	if len(response.GetItems()) != 1 || response.GetItems()[0].GetProviderItemKey() != "anilist:2:s1:e1" {
		t.Fatalf("latest incremental items = %#v", response.GetItems())
	}

	response, err = srv.ListRemoteState(context.Background(), request("inc:100", false))
	if err != nil || response.GetFault() != nil {
		t.Fatalf("older incremental response: %v, fault = %#v", err, response.GetFault())
	}
	if len(response.GetItems()) != 2 {
		t.Fatalf("older incremental items = %#v, want both entries", response.GetItems())
	}

	response, err = srv.ListRemoteState(context.Background(), request("inc:900", true))
	if err != nil || response.GetFault() != nil {
		t.Fatalf("full-scan response: %v, fault = %#v", err, response.GetFault())
	}
	if len(response.GetItems()) != 2 || !response.GetCompleteSnapshot() {
		t.Fatalf("full-scan items = %#v, complete = %v", response.GetItems(), response.GetCompleteSnapshot())
	}
}

func TestListRemoteStateCachesEmptyImport(t *testing.T) {
	upstreamCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamCalls++
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"MediaListCollection": map[string]any{
			"lists": []any{},
		}}})
	}))
	defer upstream.Close()
	oldEndpoint := anilist.Endpoint
	anilist.Endpoint = upstream.URL
	t.Cleanup(func() { anilist.Endpoint = oldEndpoint })

	srv := &server{mappings: mapping.NewClient(&http.Client{Transport: stubMappingTransport{}})}
	request := &pluginv1.WatchSyncListRemoteStateRequest{
		PageSize: 25,
		Context: &pluginv1.WatchSyncAuthenticatedContext{Credentials: &pluginv1.WatchSyncCredentials{
			AccessToken:      "token",
			SecretAttributes: map[string]string{"user_id": "7"},
		}},
	}
	for attempt := 0; attempt < 2; attempt++ {
		response, err := srv.ListRemoteState(context.Background(), request)
		if err != nil || response.GetFault() != nil {
			t.Fatalf("empty import attempt %d: %v, fault = %#v", attempt+1, err, response.GetFault())
		}
		if len(response.GetItems()) != 0 || response.GetNextCursor() == "" {
			t.Fatalf("empty import attempt %d response = %#v", attempt+1, response)
		}
	}
	if upstreamCalls != 1 {
		t.Fatalf("empty import upstream calls = %d, want one cached request", upstreamCalls)
	}
}

func TestListRemoteStateConcurrentPagesSharePreparedSnapshot(t *testing.T) {
	entries := make([]anilist.ListEntry, 64)
	for index := range entries {
		entries[index] = anilist.ListEntry{ID: index + 1, MediaID: 10, Status: "CURRENT", Progress: 1}
		entries[index].Media.ID = 10
		entries[index].Media.Format = "TV"
		entries[index].Media.Title.Romaji = "Example Anime"
	}
	srv := &server{
		mappings: mapping.NewClient(&http.Client{Transport: stubMappingTransport{}}),
		imports:  seededImports(7, entries, time.Now()),
	}
	request := func(pageToken string) *pluginv1.WatchSyncListRemoteStateRequest {
		return &pluginv1.WatchSyncListRemoteStateRequest{
			PageToken: pageToken,
			PageSize:  1,
			Context: &pluginv1.WatchSyncAuthenticatedContext{Credentials: &pluginv1.WatchSyncCredentials{
				AccessToken:      "token",
				SecretAttributes: map[string]string{"user_id": "7"},
			}},
		}
	}
	first, err := srv.ListRemoteState(context.Background(), request(""))
	if err != nil || first.GetFault() != nil || first.GetNextPageToken() == "" {
		t.Fatalf("first page: %v, response = %#v", err, first)
	}

	const callers = 8
	responses := make(chan *pluginv1.WatchSyncListRemoteStateResponse, callers)
	errs := make(chan error, callers)
	var waitGroup sync.WaitGroup
	waitGroup.Add(callers)
	for range callers {
		go func() {
			defer waitGroup.Done()
			response, err := srv.ListRemoteState(context.Background(), request(first.GetNextPageToken()))
			if err != nil {
				errs <- err
				return
			}
			responses <- response
		}()
	}
	waitGroup.Wait()
	close(responses)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	for response := range responses {
		if response.GetFault() != nil || len(response.GetItems()) != 1 ||
			response.GetItems()[0].GetProviderItemKey() != "anilist:2:s1:e1" {
			t.Fatalf("concurrent continuation response = %#v", response)
		}
	}
}

// seededImports returns an import cache that already holds an account's list.
func seededImports(userID int, entries []anilist.ListEntry, fetchedAt time.Time) *importCache {
	cache := newImportCache()
	cache.lists[userID] = &listSnapshot{entries: entries, fetchedAt: fetchedAt}
	return cache
}
