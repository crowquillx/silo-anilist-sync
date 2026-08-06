package main

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	publicmanifest "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginsdk/manifest"
	sdkruntime "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginsdk/runtime"
	"github.com/Silo-Server/silo-plugin-sdk/pkg/pluginsdk/runtimedefault"
	"github.com/crowquillx/silo-anilist-sync/anilist"
	"github.com/crowquillx/silo-anilist-sync/mapping"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var version string

//go:embed manifest.json
var manifestJSON []byte

type server struct {
	runtimedefault.Server
	pluginv1.UnimplementedWatchSyncProviderServer

	manifest *pluginv1.PluginManifest
	mappings *mapping.Client
}

func (s *server) GetManifest(context.Context, *pluginv1.GetManifestRequest) (*pluginv1.GetManifestResponse, error) {
	return &pluginv1.GetManifestResponse{Manifest: s.manifest}, nil
}

func (s *server) InitAuthorize(_ context.Context, req *pluginv1.WatchSyncInitAuthorizeRequest) (*pluginv1.WatchSyncInitAuthorizeResponse, error) {
	clientID, _ := providerCredentials(req.GetProviderConfig())
	authorizationURL, err := anilist.AuthorizationURL(clientID, req.GetRedirectUri(), req.GetState())
	if err != nil {
		return &pluginv1.WatchSyncInitAuthorizeResponse{Fault: invalidRequestFault(err)}, nil
	}
	return &pluginv1.WatchSyncInitAuthorizeResponse{AuthorizationUrl: authorizationURL}, nil
}

func (s *server) ExchangeCode(ctx context.Context, req *pluginv1.WatchSyncExchangeCodeRequest) (*pluginv1.WatchSyncCredentialResponse, error) {
	clientID, clientSecret := providerCredentials(req.GetProviderConfig())
	if clientID == "" || clientSecret == "" || req.GetAuthorizationCode() == "" {
		return &pluginv1.WatchSyncCredentialResponse{Fault: invalidRequestFault(errors.New("AniList OAuth client and authorization code are required"))}, nil
	}
	token, err := (&anilist.OAuthClient{}).ExchangeCode(ctx, clientID, clientSecret, req.GetRedirectUri(), req.GetAuthorizationCode())
	if err != nil {
		return &pluginv1.WatchSyncCredentialResponse{Fault: faultFromError(err)}, nil
	}
	return credentialResponse(ctx, token.AccessToken, token.TokenType, token.ExpiresAt)
}

func (s *server) ExchangeAPIKey(ctx context.Context, req *pluginv1.WatchSyncExchangeAPIKeyRequest) (*pluginv1.WatchSyncCredentialResponse, error) {
	token := strings.TrimSpace(req.GetApiKey())
	if token == "" {
		return &pluginv1.WatchSyncCredentialResponse{Fault: invalidRequestFault(errors.New("AniList access token is required"))}, nil
	}
	// Manually issued AniList tokens have the same one-year lifetime, but the
	// exact issue time is unknown. The host will surface reauthorization on 401.
	return credentialResponse(ctx, token, "Bearer", time.Time{})
}

func (s *server) RefreshCredentials(context.Context, *pluginv1.WatchSyncRefreshCredentialsRequest) (*pluginv1.WatchSyncCredentialResponse, error) {
	return &pluginv1.WatchSyncCredentialResponse{Fault: &pluginv1.WatchSyncFault{
		Code:        pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL,
		SafeMessage: "AniList does not issue refresh tokens; reconnect the account",
	}}, nil
}

func (s *server) GetAccount(ctx context.Context, req *pluginv1.WatchSyncGetAccountRequest) (*pluginv1.WatchSyncGetAccountResponse, error) {
	account, err := anilist.NewClient(req.GetContext().GetCredentials().GetAccessToken(), nil).Viewer(ctx)
	if err != nil {
		return &pluginv1.WatchSyncGetAccountResponse{Fault: faultFromError(err)}, nil
	}
	return &pluginv1.WatchSyncGetAccountResponse{Account: accountProto(account)}, nil
}

func (s *server) ApplyEvents(ctx context.Context, req *pluginv1.WatchSyncApplyEventsRequest) (*pluginv1.WatchSyncApplyEventsResponse, error) {
	if len(req.GetEvents()) == 0 {
		return &pluginv1.WatchSyncApplyEventsResponse{}, nil
	}
	dataset, err := s.mappings.Dataset(ctx)
	if err != nil {
		return &pluginv1.WatchSyncApplyEventsResponse{Fault: faultFromError(err)}, nil
	}
	client := anilist.NewClient(req.GetContext().GetCredentials().GetAccessToken(), nil)
	results := make([]*pluginv1.WatchSyncApplyResult, 0, len(req.GetEvents()))
	for _, event := range req.GetEvents() {
		results = append(results, applyEvent(ctx, client, dataset, event))
	}
	return &pluginv1.WatchSyncApplyEventsResponse{Results: results}, nil
}

func applyEvent(ctx context.Context, client *anilist.Client, dataset mapping.Catalog, event *pluginv1.WatchSyncEvent) *pluginv1.WatchSyncApplyResult {
	result := &pluginv1.WatchSyncApplyResult{EventId: event.GetEventId()}
	if event.GetOperation() != pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_WATCHED {
		result.Status = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED
		result.Fault = &pluginv1.WatchSyncFault{
			Code:        pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMANENT,
			SafeMessage: "AniList unwatch synchronization is not supported",
		}
		return result
	}
	targets, err := resolveTargets(ctx, dataset, event.GetMedia())
	if err != nil {
		var mappingErr *mapping.TemporaryError
		if errors.As(err, &mappingErr) {
			return applyErrorResult(event.GetEventId(), err)
		}
		result.Status = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED
		result.Fault = &pluginv1.WatchSyncFault{
			Code:        pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST,
			SafeMessage: err.Error(),
		}
		return result
	}
	if len(targets) == 0 {
		result.Status = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED
		result.Fault = &pluginv1.WatchSyncFault{
			Code:        pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMANENT,
			SafeMessage: "no unambiguous AniBridge, Anime-Lists, or ARM mapping was found",
		}
		return result
	}
	ids := make([]int, 0, len(targets))
	for id := range targets {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	for _, id := range ids {
		if err := client.AdvanceProgress(ctx, id, targets[id]); err != nil {
			return applyErrorResult(event.GetEventId(), err)
		}
	}
	result.Status = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED
	return result
}

func resolveTargets(ctx context.Context, dataset mapping.Catalog, media *pluginv1.WatchSyncMedia) (map[int]int, error) {
	if media == nil {
		return nil, errors.New("watch event has no media identity")
	}
	season, episode := int(media.GetSeasonNumber()), int(media.GetEpisodeNumber())
	ids := media.GetSeriesExternalIds()
	switch media.GetMediaType() {
	case pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE:
		season, episode = -1, 1
		ids = media.GetExternalIds()
	case pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE:
		if season < 0 || episode < 1 {
			return nil, errors.New("episode watch events require season and episode numbers")
		}
	default:
		return nil, fmt.Errorf("unsupported watch media type %q", media.GetMediaType())
	}
	return convergedMappings(ctx, dataset, ids, season, episode)
}

func convergedMappings(ctx context.Context, dataset mapping.Catalog, ids map[string]string, season, episode int) (map[int]int, error) {
	var resolved map[int]int
	for _, provider := range []string{"tvdb", "tmdb"} {
		providerID := strings.TrimSpace(ids[provider])
		if providerID == "" {
			continue
		}
		targets, err := dataset.Resolve(ctx, provider, providerID, season, episode)
		if err != nil {
			return nil, err
		}
		if len(targets) == 0 {
			continue
		}
		candidate := targetsMap(targets)
		if resolved != nil && !sameTargets(resolved, candidate) {
			return nil, fmt.Errorf("anime mappings disagree between provider IDs")
		}
		resolved = candidate
	}
	if resolved == nil && season < 0 && strings.TrimSpace(ids["imdb"]) != "" {
		targets, err := dataset.Resolve(ctx, "imdb", ids["imdb"], season, episode)
		if err != nil {
			return nil, err
		}
		resolved = targetsMap(targets)
	}
	return resolved, nil
}

func targetsMap(targets []mapping.Target) map[int]int {
	out := make(map[int]int, len(targets))
	for _, target := range targets {
		if target.Episode > out[target.AniListID] {
			out[target.AniListID] = target.Episode
		}
	}
	return out
}

func sameTargets(a, b map[int]int) bool {
	if len(a) != len(b) {
		return false
	}
	for id, progress := range a {
		if b[id] != progress {
			return false
		}
	}
	return true
}

func credentialResponse(ctx context.Context, accessToken, tokenType string, expiresAt time.Time) (*pluginv1.WatchSyncCredentialResponse, error) {
	account, err := anilist.NewClient(accessToken, nil).Viewer(ctx)
	if err != nil {
		return &pluginv1.WatchSyncCredentialResponse{Fault: faultFromError(err)}, nil
	}
	credentials := &pluginv1.WatchSyncCredentials{AccessToken: accessToken, TokenType: tokenType}
	if !expiresAt.IsZero() {
		credentials.ExpiresAt = timestamppb.New(expiresAt)
	}
	return &pluginv1.WatchSyncCredentialResponse{Credentials: credentials, Account: accountProto(account)}, nil
}

func accountProto(account anilist.Account) *pluginv1.WatchSyncAccount {
	return &pluginv1.WatchSyncAccount{
		ExternalSubject: strconv.Itoa(account.ID),
		Username:        account.Name,
		DisplayName:     account.Name,
		AvatarUrl:       account.AvatarURL,
		ProfileUrl:      account.ProfileURL,
	}
}

func providerCredentials(config *pluginv1.WatchSyncProviderConfig) (string, string) {
	if config == nil {
		return "", ""
	}
	return strings.TrimSpace(config.GetValues()["client_id"]), strings.TrimSpace(config.GetSecretValues()["client_secret"])
}

func applyErrorResult(eventID string, err error) *pluginv1.WatchSyncApplyResult {
	fault := faultFromError(err)
	result := &pluginv1.WatchSyncApplyResult{EventId: eventID, Fault: fault}
	switch fault.GetCode() {
	case pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST,
		pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMANENT:
		result.Status = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED
	default:
		result.Status = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_RETRY
	}
	return result
}

func invalidRequestFault(err error) *pluginv1.WatchSyncFault {
	return &pluginv1.WatchSyncFault{Code: pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST, SafeMessage: err.Error()}
}

func faultFromError(err error) *pluginv1.WatchSyncFault {
	fault := &pluginv1.WatchSyncFault{Code: pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY, SafeMessage: "temporary AniList request failure"}
	var mappingErr *mapping.TemporaryError
	if errors.As(err, &mappingErr) {
		fault.SafeMessage = "temporary anime mapping service failure"
		return fault
	}
	var apiErr *anilist.Error
	if !errors.As(err, &apiErr) {
		return fault
	}
	switch apiErr.Status {
	case http.StatusUnauthorized:
		fault.Code = pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL
		fault.SafeMessage = "AniList credentials are expired or revoked"
	case http.StatusForbidden:
		fault.Code = pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED
		fault.SafeMessage = "AniList denied the request"
	case http.StatusTooManyRequests:
		fault.Code = pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_RATE_LIMITED
		fault.SafeMessage = "AniList rate limit reached"
		fault.RetryAfter = durationpb.New(apiErr.RetryAfter)
	default:
		if apiErr.Status >= 400 && apiErr.Status < 500 {
			fault.Code = pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMANENT
			fault.SafeMessage = "AniList rejected the request"
		}
	}
	return fault
}

func main() {
	manifest, err := loadManifest()
	if err != nil {
		panic(err)
	}
	srv := &server{manifest: manifest, mappings: mapping.NewClient(nil)}
	sdkruntime.Serve(sdkruntime.ServeConfig{Servers: sdkruntime.CapabilityServers{
		Runtime:           srv,
		WatchSyncProvider: srv,
	}})
}

func loadManifest() (*pluginv1.PluginManifest, error) {
	manifest, err := publicmanifest.Load(manifestJSON)
	if err != nil {
		return nil, fmt.Errorf("load embedded manifest: %w", err)
	}
	if version != "" {
		manifest.Version = version
	}
	path, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("resolve executable: %w", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read executable: %w", err)
	}
	checksum := sha256.Sum256(data)
	manifest.Checksum = hex.EncodeToString(checksum[:])
	return manifest, nil
}
