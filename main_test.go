package main

import (
	"context"
	"net/url"
	"testing"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"github.com/crowquillx/silo-anilist-sync/mapping"
)

func TestInitAuthorizeUsesHostStateAndRedirect(t *testing.T) {
	s := &server{}
	resp, err := s.InitAuthorize(context.Background(), &pluginv1.WatchSyncInitAuthorizeRequest{
		ProviderConfig: &pluginv1.WatchSyncProviderConfig{Values: map[string]string{"client_id": "123"}},
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
	dataset := mapping.Dataset{
		"tvdb_show:1:s1": {"anilist:10": {"1-12": "1-12"}},
		"tmdb_show:2:s1": {"anilist:11": {"1-12": "1-12"}},
	}
	_, err := resolveTargets(dataset, &pluginv1.WatchSyncMedia{
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
	dataset := mapping.Dataset{
		"tvdb_show:1:s1": {"anilist:10": {"1-12": "1-12"}},
		"tmdb_show:2:s1": {"anilist:10": {"1-12": "1-12"}},
	}
	targets, err := resolveTargets(dataset, &pluginv1.WatchSyncMedia{
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
	result := applyEvent(context.Background(), nil, nil, &pluginv1.WatchSyncEvent{
		EventId:   "event-1",
		Operation: pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_UNWATCHED,
	})
	if result.GetStatus() != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED {
		t.Fatalf("status = %v", result.GetStatus())
	}
}
