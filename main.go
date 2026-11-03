package main

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	sdkruntime "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginsdk/runtime"
	"github.com/crowquillx/silo-anilist-sync/anilist"
	"github.com/crowquillx/silo-anilist-sync/device"
	"github.com/crowquillx/silo-anilist-sync/mapping"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var version string

//go:embed manifest.json
var manifestJSON []byte

type server struct {
	pluginv1.UnimplementedWatchSyncProviderServer
	pluginv1.UnimplementedHttpRoutesServer

	mappings *mapping.Client
	// status feeds the admin page. A nil status records nothing.
	status *statusStore

	importsOnce sync.Once
	imports     *importCache
}

func (s *server) importCache() *importCache {
	s.importsOnce.Do(func() {
		if s.imports == nil {
			s.imports = newImportCache()
		}
	})
	return s.imports
}

func (s *server) ExchangeAPIKey(ctx context.Context, req *pluginv1.WatchSyncExchangeAPIKeyRequest) (*pluginv1.WatchSyncCredentialResponse, error) {
	token := strings.TrimSpace(req.GetApiKey())
	if token == "" {
		return &pluginv1.WatchSyncCredentialResponse{Fault: invalidRequestFault(errors.New("AniList access token is required"))}, nil
	}
	// credentialResponse runs the Viewer query, which validates the token live
	// and returns the connected account; a bad or revoked token now fails the
	// connect with an INVALID_CREDENTIAL fault instead of at first sync. AniList
	// tokens live ~1 year; the decoded exp lets the host warn before expiry.
	var response *pluginv1.WatchSyncCredentialResponse
	var err error
	if req.GetCapabilityId() == siloVerifiedCapability {
		response, err = s.exchangeVerifiedCredential(ctx, req, token)
	} else {
		response, err = credentialResponse(ctx, token, "Bearer", anilist.TokenExpiresAt(token))
	}
	if err == nil && response.GetFault() == nil {
		s.recordAccount(ctx, req.GetCapabilityId(), response.GetAccount(), token)
	}
	return response, err
}

func (s *server) RefreshCredentials(context.Context, *pluginv1.WatchSyncRefreshCredentialsRequest) (*pluginv1.WatchSyncCredentialResponse, error) {
	return &pluginv1.WatchSyncCredentialResponse{Fault: &pluginv1.WatchSyncFault{
		Code:        pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL,
		SafeMessage: "AniList does not issue refresh tokens; reconnect AniList in Settings → Watch Providers",
	}}, nil
}

func (s *server) GetAccount(ctx context.Context, req *pluginv1.WatchSyncGetAccountRequest) (*pluginv1.WatchSyncGetAccountResponse, error) {
	auth := req.GetContext()
	token := auth.GetCredentials().GetAccessToken()
	account, err := anilist.NewClient(token, nil).Viewer(ctx)
	if err != nil {
		return &pluginv1.WatchSyncGetAccountResponse{Fault: s.callFault(ctx, auth, credentialUserID(auth.GetCredentials()), err)}, nil
	}
	response := accountProto(account)
	s.recordAccount(ctx, auth.GetCapabilityId(), response, token)
	return &pluginv1.WatchSyncGetAccountResponse{Account: response}, nil
}

// recordAccount notes a connected account for the status page.
func (s *server) recordAccount(ctx context.Context, capability string, account *pluginv1.WatchSyncAccount, token string) {
	id, err := strconv.Atoi(account.GetExternalSubject())
	if err != nil || id < 1 {
		return
	}
	s.status.recordAccount(ctx, capability, anilist.Account{ID: id, Name: account.GetUsername(), ProfileURL: account.GetProfileUrl()}, token)
}

// callFault converts a failed AniList call to a fault and records it for the
// account's status.
func (s *server) callFault(ctx context.Context, auth *pluginv1.WatchSyncAuthenticatedContext, userID int, err error) *pluginv1.WatchSyncFault {
	fault := faultFromError(err)
	invalid := fault.GetCode() == pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL
	s.status.recordCall(ctx, userID, auth.GetCapabilityId(), auth.GetCredentials().GetAccessToken(), invalid, fault.GetSafeMessage())
	return fault
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

// reconnectMessage is shown on the profile's Watch Providers page when
// AniList rejects the saved token.
const reconnectMessage = "AniList rejected the saved access token because it expired or was revoked. Reconnect AniList in Settings → Watch Providers."

func faultFromError(err error) *pluginv1.WatchSyncFault {
	fault := &pluginv1.WatchSyncFault{Code: pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY, SafeMessage: "temporary AniList request failure"}
	var mappingErr *mapping.TemporaryError
	if errors.As(err, &mappingErr) {
		fault.SafeMessage = "temporary anime mapping service failure" + transportDetail(mappingErr.Err)
		return fault
	}
	var mismatch *anilist.MismatchError
	if errors.As(err, &mismatch) {
		fault.Code = pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMANENT
		fault.SafeMessage = "the anime mapping does not match AniList: " + mismatch.Message
		return fault
	}
	var apiErr *anilist.Error
	if !errors.As(err, &apiErr) {
		fault.SafeMessage += transportDetail(err)
		return fault
	}
	if apiErr.InvalidToken {
		fault.Code = pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL
		fault.SafeMessage = reconnectMessage
		return fault
	}
	switch apiErr.Status {
	case http.StatusUnauthorized:
		fault.Code = pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL
		fault.SafeMessage = reconnectMessage
	case http.StatusForbidden:
		fault.Code = pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED
		fault.SafeMessage = "AniList denied the request"
	case http.StatusTooManyRequests:
		fault.Code = pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_RATE_LIMITED
		fault.SafeMessage = "AniList rate limit reached"
		fault.RetryAfter = durationpb.New(apiErr.RetryAfter)
	default:
		detail := faultDetail(apiErr)
		if apiErr.Status == http.StatusOK && strings.Contains(strings.ToLower(apiErr.Message), "too many requests") {
			fault.Code = pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_RATE_LIMITED
			fault.SafeMessage = "AniList rate limit reached"
			retryAfter := apiErr.RetryAfter
			if retryAfter == 0 {
				retryAfter = 60 * time.Second
			}
			fault.RetryAfter = durationpb.New(retryAfter)
		} else if apiErr.Status >= 400 && apiErr.Status < 500 {
			fault.Code = pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMANENT
			fault.SafeMessage = "AniList rejected the request" + detail
		} else {
			fault.SafeMessage = "temporary AniList request failure" + detail
		}
	}
	return fault
}

// faultDetail renders the distinguishing parts of an anilist.Error so the
// fault shown in Silo's UI carries the status and message that diagnosis
// needs. Non-anilist errors are rendered by transportDetail instead.
func faultDetail(apiErr *anilist.Error) string {
	var parts []string
	if apiErr.Status > 0 && apiErr.Status != http.StatusOK {
		parts = append(parts, fmt.Sprintf("HTTP %d", apiErr.Status))
	}
	if apiErr.Message != "" {
		parts = append(parts, apiErr.Message)
	}
	if len(parts) > 0 {
		return " (" + strings.Join(parts, "; ") + ")"
	}
	return ""
}

// transportDetail renders the underlying text of a transport-level error
// (timeout, DNS, TLS) as a single-line parenthesized suffix so a temporary
// fault carries diagnosis detail. The text is trimmed so UIs are not flooded.
func transportDetail(err error) string {
	if err == nil {
		return ""
	}
	text := strings.TrimSpace(strings.ReplaceAll(err.Error(), "\n", " "))
	if text == "" {
		return ""
	}
	if len(text) > 200 {
		text = text[:200] + "..."
	}
	return " (" + text + ")"
}

func main() {
	srv := &server{mappings: mapping.NewClient(nil), status: newStatusStore()}
	authorization := device.NewService()
	authorization.OnAuthorized = func(ctx context.Context, account anilist.Account, token string) {
		srv.status.recordAccount(ctx, "anilist", account, token)
	}
	sdkruntime.ServeManifestWithOptions(manifestJSON, version,
		sdkruntime.CapabilityServers{WatchSyncProvider: srv, HttpRoutes: srv},
		sdkruntime.WithWatchSyncDeviceAuthorization(authorization))
}
