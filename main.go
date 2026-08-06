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

func (s *server) ListRemoteState(ctx context.Context, req *pluginv1.WatchSyncListRemoteStateRequest) (*pluginv1.WatchSyncListRemoteStateResponse, error) {
	if !requestsWatchedState(req.GetStateKinds()) {
		return &pluginv1.WatchSyncListRemoteStateResponse{CompleteSnapshot: true, NextCursor: "full-v1"}, nil
	}
	page, offset, err := remotePageToken(req.GetPageToken())
	if err != nil {
		return &pluginv1.WatchSyncListRemoteStateResponse{Fault: invalidRequestFault(err)}, nil
	}
	pageSize := int(req.GetPageSize())
	if pageSize < 1 {
		pageSize = 25
	}
	if pageSize > 1000 {
		return &pluginv1.WatchSyncListRemoteStateResponse{
			Fault: invalidRequestFault(errors.New("remote state page size exceeds 1000 items")),
		}, nil
	}
	auth := req.GetContext()
	credentials := auth.GetCredentials()
	client := anilist.NewClient(credentials.GetAccessToken(), nil)
	userID, err := strconv.Atoi(credentials.GetSecretAttributes()["user_id"])
	if err != nil || userID < 1 {
		account, accountErr := client.Viewer(ctx)
		if accountErr != nil {
			return &pluginv1.WatchSyncListRemoteStateResponse{Fault: faultFromError(accountErr)}, nil
		}
		userID = account.ID
	}
	listPage, err := client.ListEntriesPage(ctx, userID, page, 50)
	if err != nil {
		return &pluginv1.WatchSyncListRemoteStateResponse{Fault: faultFromError(err)}, nil
	}
	dataset, err := s.mappings.Dataset(ctx)
	if err != nil {
		return &pluginv1.WatchSyncListRemoteStateResponse{Fault: faultFromError(err)}, nil
	}
	items, err := remoteStates(listPage.Entries, dataset)
	if err != nil {
		return &pluginv1.WatchSyncListRemoteStateResponse{Fault: faultFromError(err)}, nil
	}
	if offset > len(items) {
		return &pluginv1.WatchSyncListRemoteStateResponse{
			Fault: invalidRequestFault(errors.New("remote state page token is out of range")),
		}, nil
	}
	end := min(len(items), offset+pageSize)
	response := &pluginv1.WatchSyncListRemoteStateResponse{
		Items:            items[offset:end],
		CompleteSnapshot: true,
	}
	switch {
	case end < len(items):
		response.NextPageToken = fmt.Sprintf("%d:%d", page, end)
	case listPage.HasNextPage:
		response.NextPageToken = fmt.Sprintf("%d:0", page+1)
	default:
		response.NextCursor = "full-v1"
	}
	return response, nil
}

func requestsWatchedState(kinds []pluginv1.WatchSyncRemoteStateKind) bool {
	if len(kinds) == 0 {
		return true
	}
	for _, kind := range kinds {
		if kind == pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHED {
			return true
		}
	}
	return false
}

func remotePageToken(token string) (int, int, error) {
	if strings.TrimSpace(token) == "" {
		return 1, 0, nil
	}
	parts := strings.Split(token, ":")
	if len(parts) != 2 {
		return 0, 0, errors.New("remote state page token is invalid")
	}
	page, pageErr := strconv.Atoi(parts[0])
	offset, offsetErr := strconv.Atoi(parts[1])
	if pageErr != nil || offsetErr != nil || page < 1 || offset < 0 {
		return 0, 0, errors.New("remote state page token is invalid")
	}
	return page, offset, nil
}

func remoteStates(entries []anilist.ListEntry, dataset mapping.Catalog) ([]*pluginv1.WatchSyncRemoteState, error) {
	var states []*pluginv1.WatchSyncRemoteState
	for _, entry := range entries {
		progress := entry.CompletedProgress()
		if progress < 1 || entry.Media.ID < 1 || entry.ID < 1 {
			continue
		}
		sources, err := dataset.AniBridge.Reverse(entry.Media.ID, progress)
		if err != nil {
			return nil, err
		}
		expectMovie := entry.Media.Format == "MOVIE"
		hasExpectedSource := false
		for _, source := range sources {
			hasExpectedSource = hasExpectedSource || source.Movie == expectMovie
		}
		for _, source := range sources {
			if hasExpectedSource && source.Movie != expectMovie {
				continue
			}
			externalIDs := make(map[string]string, len(source.ExternalIDs)+1)
			for provider, id := range source.ExternalIDs {
				externalIDs[provider] = id
			}
			externalIDs["anilist"] = strconv.Itoa(entry.Media.ID)
			media := &pluginv1.WatchSyncMedia{
				Title: entry.PreferredTitle(),
				Year:  int32(entry.Media.StartDate.Year),
			}
			var itemKey string
			if source.Movie {
				media.MediaType = pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE
				media.ExternalIds = externalIDs
				itemKey = fmt.Sprintf("anilist:%d:movie", entry.ID)
			} else {
				media.MediaType = pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE
				media.SeriesTitle = entry.PreferredTitle()
				media.SeriesYear = int32(entry.Media.StartDate.Year)
				media.SeriesExternalIds = externalIDs
				media.SeasonNumber = int32(source.Season)
				media.EpisodeNumber = int32(source.Episode)
				itemKey = fmt.Sprintf("anilist:%d:s%d:e%d", entry.ID, source.Season, source.Episode)
			}
			states = append(states, &pluginv1.WatchSyncRemoteState{
				ProviderItemKey: itemKey,
				Media:           media,
				Watched:         &pluginv1.WatchSyncRemoteWatchedState{PlayCount: 1},
			})
		}
	}
	return states, nil
}

func (s *server) ApplyEvents(ctx context.Context, req *pluginv1.WatchSyncApplyEventsRequest) (*pluginv1.WatchSyncApplyEventsResponse, error) {
	if len(req.GetEvents()) == 0 {
		return &pluginv1.WatchSyncApplyEventsResponse{}, nil
	}
	behavior := watchBehaviorFromConfig(req.GetContext().GetProviderConfig())
	needsMapping := false
	for _, event := range req.GetEvents() {
		needsMapping = needsMapping || appliesWatchedState(event, behavior)
	}
	var dataset mapping.Catalog
	var client *anilist.Client
	if needsMapping {
		var err error
		dataset, err = s.mappings.Dataset(ctx)
		if err != nil {
			return &pluginv1.WatchSyncApplyEventsResponse{Fault: faultFromError(err)}, nil
		}
		client = anilist.NewClient(req.GetContext().GetCredentials().GetAccessToken(), nil)
	}
	results := make([]*pluginv1.WatchSyncApplyResult, 0, len(req.GetEvents()))
	for _, event := range req.GetEvents() {
		results = append(results, applyEvent(ctx, client, dataset, event, behavior))
	}
	return &pluginv1.WatchSyncApplyEventsResponse{Results: results}, nil
}

type watchBehavior struct {
	syncManualWatched         bool
	playbackCompletionPercent float64
}

func watchBehaviorFromConfig(config *pluginv1.WatchSyncProviderConfig) watchBehavior {
	behavior := watchBehavior{playbackCompletionPercent: 90}
	if config == nil {
		return behavior
	}
	behavior.syncManualWatched, _ = strconv.ParseBool(config.GetValues()["provider.sync_manual_watched"])
	if threshold, err := strconv.ParseFloat(config.GetValues()["provider.playback_completion_percent"], 64); err == nil &&
		threshold >= 1 && threshold <= 99 {
		behavior.playbackCompletionPercent = threshold
	}
	return behavior
}

func appliesWatchedState(event *pluginv1.WatchSyncEvent, behavior watchBehavior) bool {
	switch event.GetOperation() {
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_STOP:
		return event.GetCompletionPercent() > behavior.playbackCompletionPercent
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_WATCHED:
		return event.GetOrigin() == pluginv1.WatchSyncOrigin_WATCH_SYNC_ORIGIN_PLAYBACK_COMPLETION ||
			behavior.syncManualWatched
	default:
		return false
	}
}

func applyEvent(ctx context.Context, client *anilist.Client, dataset mapping.Catalog, event *pluginv1.WatchSyncEvent, behavior watchBehavior) *pluginv1.WatchSyncApplyResult {
	result := &pluginv1.WatchSyncApplyResult{EventId: event.GetEventId()}
	switch event.GetOperation() {
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_START,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_PAUSE:
		result.Status = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE
		return result
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_STOP,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_WATCHED:
		if !appliesWatchedState(event, behavior) {
			result.Status = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE
			return result
		}
	default:
		result.Status = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED
		result.Fault = &pluginv1.WatchSyncFault{
			Code:        pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMANENT,
			SafeMessage: "AniList does not support this watch synchronization operation",
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
	credentials := &pluginv1.WatchSyncCredentials{
		AccessToken:      accessToken,
		TokenType:        tokenType,
		SecretAttributes: map[string]string{"user_id": strconv.Itoa(account.ID)},
	}
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
	return strings.TrimSpace(config.GetValues()["provider.client_id"]),
		strings.TrimSpace(config.GetSecretValues()["provider.client_secret"])
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
