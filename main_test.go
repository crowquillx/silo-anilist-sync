package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	publicmanifest "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginsdk/manifest"
	"github.com/crowquillx/silo-anilist-sync/anilist"
	"github.com/crowquillx/silo-anilist-sync/mapping"
)

func TestInitAuthorizeUsesHostStateAndRedirect(t *testing.T) {
	s := &server{}
	resp, err := s.InitAuthorize(context.Background(), &pluginv1.WatchSyncInitAuthorizeRequest{
		ProviderConfig: &pluginv1.WatchSyncProviderConfig{Values: map[string]string{"provider.client_id": "123"}},
		RedirectUri:    "https://silo.example/api/v1/watch-providers/oauth/callback",
		State:          "host-state",
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetFault() != nil {
		t.Fatalf("fault = %#v", resp.GetFault())
	}
	parsed, err := url.Parse(resp.GetAuthorizationUrl())
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Query().Get("client_id") != "123" || parsed.Query().Get("state") != "host-state" || parsed.Query().Get("response_type") != "code" {
		t.Fatalf("authorization query = %v", parsed.Query())
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
	if result.GetFault().GetSafeMessage() != "temporary anime mapping service failure" {
		t.Fatalf("fault = %#v", result.GetFault())
	}
}

func TestManifestAdvertisesWatchedImportAndOAuthConfiguration(t *testing.T) {
	manifest, err := publicmanifest.Load(manifestJSON)
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.GetCapabilities()) != 1 {
		t.Fatalf("capabilities = %#v", manifest.GetCapabilities())
	}
	descriptor := manifest.GetCapabilities()[0].GetWatchSyncProvider()
	if !descriptor.GetImportWatched() || !descriptor.GetScrobblePlayback() {
		t.Fatalf("watch sync descriptor = %#v", descriptor)
	}
	if len(manifest.GetGlobalConfigSchema()) != 1 {
		t.Fatalf("global config schema = %#v", manifest.GetGlobalConfigSchema())
	}
	fields := manifest.GetGlobalConfigSchema()[0].GetAdminForm().GetFields()
	if len(fields) != 4 || fields[0].GetKey() != "client_id" || fields[1].GetKey() != "client_secret" ||
		!fields[1].GetSecret() || fields[2].GetKey() != "sync_manual_watched" ||
		fields[3].GetKey() != "playback_completion_percent" {
		t.Fatalf("provider fields = %#v", fields)
	}
}

func TestRemoteStatesExpandAniListProgressIntoMappedEpisodes(t *testing.T) {
	entry := anilist.ListEntry{ID: 9, MediaID: 42, Status: "CURRENT", Progress: 2}
	entry.Media.ID = 42
	entry.Media.Format = "TV"
	entry.Media.Title.English = "Example Anime"
	entry.Media.StartDate.Year = 2024
	states, err := remoteStates([]anilist.ListEntry{entry}, mapping.Catalog{AniBridge: mapping.Dataset{
		"tvdb_show:100:s2": {"anilist:42": {"1-12": "1-12"}},
		"tmdb_show:200:s2": {"anilist:42": {"1-12": "1-12"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
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
}

func TestRemotePageTokenRejectsMalformedValues(t *testing.T) {
	if _, _, err := remotePageToken("1:2:3"); err == nil {
		t.Fatal("expected malformed token to fail")
	}
	if page, offset, err := remotePageToken("2:7"); err != nil || page != 2 || offset != 7 {
		t.Fatalf("remotePageToken() = %d, %d, %v", page, offset, err)
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
