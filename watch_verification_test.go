package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	publicmanifest "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginsdk/manifest"
	"github.com/crowquillx/silo-anilist-sync/anilist"
	"github.com/crowquillx/silo-anilist-sync/mapping"
)

func TestZeroStopVerifiesSiloWatchedState(t *testing.T) {
	for _, tc := range []struct {
		name         string
		capability   string
		played       bool
		position     float64
		onlyExisting bool
		wantWrites   int
		wantReads    int
	}{
		{"watched zero stop", "anilist-silo", true, 0, false, 1, 2},
		{"unwatched zero stop", "anilist-silo", false, 0, false, 0, 2},
		{"standard provider stays opt out", "anilist", true, 0, false, 0, 0},
		{"partial stop stays partial", "anilist-silo", true, 100, false, 0, 0},
		{"existing entry restriction", "anilist-silo", true, 0, true, 0, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reads, writes := 0, 0
			silo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				reads++
				if r.Header.Get("Authorization") != "Bearer silo-secret" || r.Header.Get("X-Profile-Id") != "profile-a" {
					t.Error("incorrect Silo credential or profile")
				}
				switch r.URL.Path {
				case "/api/v1/profiles":
					_, _ = w.Write([]byte(`{"profiles":[{"id":"profile-a"}]}`))
				case "/api/v1/catalog/items/episode-tvdb-100-1-1":
					_ = json.NewEncoder(w).Encode(map[string]any{"content_id": "episode-tvdb-100-1-1", "user_state": map[string]any{"played": tc.played}})
				default:
					t.Errorf("unexpected Silo request %s", r.URL.Path)
					w.WriteHeader(404)
				}
			}))
			defer silo.Close()
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req struct {
					Query     string         `json:"query"`
					Variables map[string]any `json:"variables"`
				}
				_ = json.NewDecoder(r.Body).Decode(&req)
				if strings.Contains(req.Query, "SaveMediaListEntry") {
					writes++
					if req.Variables["mediaId"] != float64(10) || req.Variables["progress"] != float64(1) {
						t.Errorf("wrong mapped progress: %v", req.Variables)
					}
					_, _ = w.Write([]byte(`{"data":{"SaveMediaListEntry":{"id":1}}}`))
				} else {
					_, _ = w.Write([]byte(`{"data":{"Media":{"id":10,"episodes":12,"mediaListEntry":null}}}`))
				}
			}))
			defer upstream.Close()
			old := anilist.Endpoint
			anilist.Endpoint = upstream.URL
			t.Cleanup(func() { anilist.Endpoint = old })
			event := &pluginv1.WatchSyncEvent{
				EventId: "zero-stop", Operation: pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_STOP,
				PositionSeconds: tc.position, DurationSeconds: 1450,
				Media: &pluginv1.WatchSyncMedia{MediaItemId: "episode-tvdb-100-1-1", MediaType: pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE, SeasonNumber: 1, EpisodeNumber: 1, SeriesExternalIds: map[string]string{"tvdb": "100"}},
			}
			config := &pluginv1.WatchSyncProviderConfig{Values: map[string]string{}}
			if tc.onlyExisting {
				config.Values["provider.only_existing_entries"] = "true"
			}
			srv := &server{mappings: mapping.NewClient(&http.Client{Transport: stubMappingTransport{}})}
			resp, err := srv.ApplyEvents(context.Background(), &pluginv1.WatchSyncApplyEventsRequest{
				Context: &pluginv1.WatchSyncAuthenticatedContext{CapabilityId: tc.capability, ProviderConfig: config, Credentials: &pluginv1.WatchSyncCredentials{AccessToken: "anilist-secret", SecretAttributes: map[string]string{"user_id": "7", "silo.base_url": silo.URL, "silo.api_key": "silo-secret", "silo.profile_id": "profile-a"}}},
				Events:  []*pluginv1.WatchSyncEvent{event},
			})
			if err != nil || resp.GetFault() != nil || len(resp.GetResults()) != 1 {
				t.Fatalf("response=%v error=%v", resp, err)
			}
			if writes != tc.wantWrites || reads != tc.wantReads {
				t.Errorf("AniList writes=%d want %d; Silo reads=%d want %d", writes, tc.wantWrites, reads, tc.wantReads)
			}
			wantStatus := pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE
			if tc.wantWrites > 0 {
				wantStatus = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED
			}
			if resp.Results[0].GetStatus() != wantStatus {
				t.Errorf("result=%v want %v", resp.Results[0], wantStatus)
			}
			if event.GetCompletionPercent() != 0 || event.GetPositionSeconds() != tc.position {
				t.Fatal("input event was modified")
			}
		})
	}
}

func TestVerifiedConnectionStoresOnlyItsOwnScope(t *testing.T) {
	silo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/profiles" || r.Header.Get("Authorization") != "Bearer silo-secret" {
			t.Error("incorrect verification request")
		}
		_, _ = w.Write([]byte(`{"profiles":[{"id":"profile-a"}]}`))
	}))
	defer silo.Close()
	anilistCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		anilistCalls++
		if r.Header.Get("Authorization") != "Bearer anilist-secret" {
			t.Error("incorrect AniList credential")
		}
		_, _ = w.Write([]byte(`{"data":{"Viewer":{"id":7,"name":"tester"}}}`))
	}))
	defer upstream.Close()
	old := anilist.Endpoint
	anilist.Endpoint = upstream.URL
	t.Cleanup(func() { anilist.Endpoint = old })
	for _, profile := range []string{"profile-a", "profile-b"} {
		response, err := (&server{}).ExchangeAPIKey(context.Background(), &pluginv1.WatchSyncExchangeAPIKeyRequest{
			CapabilityId: siloVerifiedCapability, ApiKey: "anilist-secret", ProviderConfig: &pluginv1.WatchSyncProviderConfig{
				Values: map[string]string{"silo.base_url": silo.URL, "silo.profile_id": profile}, SecretValues: map[string]string{"silo.api_key": "silo-secret"},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if profile == "profile-b" {
			if response.GetFault() == nil || response.GetCredentials() != nil {
				t.Fatal("inaccessible profile connected")
			}
			continue
		}
		if response.GetFault() != nil {
			t.Fatal(response.GetFault())
		}
		attrs := response.GetCredentials().GetSecretAttributes()
		if attrs["user_id"] != "7" || attrs["silo.profile_id"] != profile || attrs["silo.api_key"] != "silo-secret" || attrs["silo.base_url"] != silo.URL {
			t.Fatal("connection scope was not saved")
		}
	}
	if anilistCalls != 1 {
		t.Errorf("AniList calls = %d want 1", anilistCalls)
	}
}

func TestVerificationFailuresDoNotAcknowledgeZeroStops(t *testing.T) {
	for _, status := range []int{401, 403, 429, 500} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			silo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
				_, _ = w.Write([]byte("private credential body"))
			}))
			defer silo.Close()
			event := &pluginv1.WatchSyncEvent{EventId: "retry", Operation: pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_STOP, DurationSeconds: 1450, Media: &pluginv1.WatchSyncMedia{MediaItemId: "episode-1"}}
			response, err := (&server{}).ApplyEvents(context.Background(), &pluginv1.WatchSyncApplyEventsRequest{Context: &pluginv1.WatchSyncAuthenticatedContext{CapabilityId: siloVerifiedCapability, Credentials: &pluginv1.WatchSyncCredentials{SecretAttributes: map[string]string{"silo.base_url": silo.URL, "silo.api_key": "silo-secret", "silo.profile_id": "profile-a"}}}, Events: []*pluginv1.WatchSyncEvent{event}})
			if status == http.StatusUnauthorized {
				// A rejected Silo key fails the whole batch, so Silo keeps the
				// events and asks the profile to reconnect.
				fault := response.GetFault()
				if err != nil || fault.GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL || len(response.GetResults()) != 0 {
					t.Fatalf("response=%v error=%v", response, err)
				}
				if strings.Contains(fault.GetSafeMessage(), "private") || strings.Contains(fault.GetSafeMessage(), "silo-secret") {
					t.Fatal("unsafe error")
				}
				return
			}
			if err != nil || response.GetFault() != nil || len(response.GetResults()) != 1 {
				t.Fatalf("response=%v error=%v", response, err)
			}
			result := response.Results[0]
			if result.GetStatus() != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_RETRY {
				t.Fatalf("result=%v", result)
			}
			if strings.Contains(result.GetFault().GetSafeMessage(), "private") || strings.Contains(result.GetFault().GetSafeMessage(), "silo-secret") {
				t.Fatal("unsafe error")
			}
		})
	}
}

func TestVerifiedProviderManifestUsesConnectionSecret(t *testing.T) {
	manifest, err := publicmanifest.Load(manifestJSON)
	if err != nil {
		t.Fatal(err)
	}
	capability := manifest.GetCapabilities()[1]
	if capability.GetId() != siloVerifiedCapability {
		t.Fatal("verified capability missing")
	}
	descriptor := capability.GetWatchSyncProvider()
	if len(descriptor.GetAuthMethods()) != 1 || descriptor.GetAuthMethods()[0] != pluginv1.WatchSyncAuthMethod_WATCH_SYNC_AUTH_METHOD_API_KEY {
		t.Fatal("verified provider requires API-key connection config")
	}
	schemas := capability.GetConfigSchema()
	if len(schemas) != 1 || !schemas[0].GetRequired() || schemas[0].GetKey() != "silo" {
		t.Fatal("Silo connection schema missing")
	}
	for _, field := range schemas[0].GetAdminForm().GetFields() {
		if field.GetKey() == "api_key" && field.GetSecret() && field.GetControl() == pluginv1.AdminFormControl_ADMIN_FORM_CONTROL_PASSWORD {
			return
		}
	}
	t.Fatal("Silo API key must be a connection secret")
}
