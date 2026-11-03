package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"github.com/crowquillx/silo-anilist-sync/anilist"
	"github.com/crowquillx/silo-anilist-sync/mapping"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	// requestsPerEvent is the AniList requests one export usually takes: a
	// read of the list entry and the write.
	requestsPerEvent = 2
	// requestAllowance is the time budgeted for one AniList request itself,
	// on top of the pacing delay before it.
	requestAllowance = 3 * time.Second
	// deadlineMargin is kept free before Silo's call deadline so the response
	// arrives in time.
	deadlineMargin = 10 * time.Second
	// deferredRetryAfter is when Silo should send events deferred because
	// the call ran out of time.
	deferredRetryAfter = 30 * time.Second
)

func (s *server) ApplyEvents(ctx context.Context, req *pluginv1.WatchSyncApplyEventsRequest) (*pluginv1.WatchSyncApplyEventsResponse, error) {
	events := req.GetEvents()
	if len(events) == 0 {
		return &pluginv1.WatchSyncApplyEventsResponse{}, nil
	}
	auth := req.GetContext()
	userID := credentialUserID(auth.GetCredentials())
	behavior := watchBehaviorFromContext(auth)
	verified := make([]bool, len(events))
	results := make([]*pluginv1.WatchSyncApplyResult, len(events))
	needsMapping := false
	for i, event := range events {
		var err error
		verified[i], err = verifyZeroStop(ctx, auth, event)
		if err != nil {
			results[i] = siloVerificationErrorResult(event.GetEventId(), err)
			continue
		}
		needsMapping = needsMapping || needsAniList(event, behavior, verified[i])
	}
	var dataset mapping.Catalog
	var client *anilist.Client
	if needsMapping {
		var err error
		dataset, err = s.mappings.Dataset(ctx)
		if err != nil {
			return &pluginv1.WatchSyncApplyEventsResponse{Fault: faultFromError(err)}, nil
		}
		client = anilist.NewClient(auth.GetCredentials().GetAccessToken(), nil)
	}
	var outcomes []exportOutcome
	failure := ""
	deferred := false
	for i, event := range events {
		reachesAniList := results[i] == nil && needsAniList(event, behavior, verified[i])
		if results[i] == nil {
			// Silo gives each call a deadline. Events that may not finish in
			// time are handed back as rate limited, so Silo sends them again
			// shortly without counting them as failures.
			deferred = deferred || (reachesAniList && deadlineTooClose(ctx))
			if deferred && reachesAniList {
				results[i] = deferredResult(event.GetEventId())
				continue
			}
			results[i] = applyVerifiedEvent(ctx, client, dataset, event, behavior, verified[i])
		}
		if fault := results[i].GetFault(); fault.GetCode() == pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL {
			// A rejected credential fails every event. Report it for the
			// whole batch so Silo keeps the events and asks for a reconnect,
			// instead of recording each one as failed.
			s.status.recordCall(ctx, userID, auth.GetCapabilityId(), auth.GetCredentials().GetAccessToken(), true, fault.GetSafeMessage())
			s.status.recordExports(ctx, userID, outcomes)
			return &pluginv1.WatchSyncApplyEventsResponse{Fault: fault}, nil
		}
		if reachesAniList || results[i].GetStatus() != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE {
			outcome := outcomeFor(event, results[i])
			outcomes = append(outcomes, outcome)
			if outcome.status == "retry" && failure == "" {
				failure = outcome.reason
			}
		}
	}
	s.status.recordExports(ctx, userID, outcomes)
	if needsMapping {
		s.status.recordCall(ctx, userID, auth.GetCapabilityId(), auth.GetCredentials().GetAccessToken(), false, failure)
	}
	return &pluginv1.WatchSyncApplyEventsResponse{Results: results}, nil
}

// deadlineTooClose reports whether one more export might not finish before
// the call's deadline, given the AniList pacing ahead of it.
func deadlineTooClose(ctx context.Context) bool {
	deadline, ok := ctx.Deadline()
	if !ok {
		return false
	}
	needed := anilist.EstimatedDelay(requestsPerEvent) + requestsPerEvent*requestAllowance + deadlineMargin
	return time.Until(deadline) < needed
}

func deferredResult(eventID string) *pluginv1.WatchSyncApplyResult {
	return &pluginv1.WatchSyncApplyResult{
		EventId: eventID,
		Status:  pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_RETRY,
		Fault: &pluginv1.WatchSyncFault{
			Code:        pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_RATE_LIMITED,
			SafeMessage: "AniList requests are paced; the rest of this batch is sent shortly",
			RetryAfter:  durationpb.New(deferredRetryAfter),
		},
	}
}

// needsAniList reports whether an event leads to AniList requests.
func needsAniList(event *pluginv1.WatchSyncEvent, behavior watchBehavior, siloPlayed bool) bool {
	switch event.GetOperation() {
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SET_RATING,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_RATING:
		return true
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_STOP:
		return siloPlayed || appliesWatchedState(event, behavior)
	}
	return appliesWatchedState(event, behavior)
}

type watchBehavior struct {
	syncManualWatched         bool
	playbackCompletionPercent float64
	onlyExistingEntries       bool
}

// watchBehaviorFromContext combines the plugin's global settings with the
// connection's own switches. A switch turned on in either place applies.
func watchBehaviorFromContext(auth *pluginv1.WatchSyncAuthenticatedContext) watchBehavior {
	behavior := watchBehaviorFromConfig(auth.GetProviderConfig())
	settings := auth.GetConnectionSettings()
	if on, _ := strconv.ParseBool(settings["sync_manual_watched"]); on {
		behavior.syncManualWatched = true
	}
	if on, _ := strconv.ParseBool(settings["only_existing_entries"]); on {
		behavior.onlyExistingEntries = true
	}
	return behavior
}

func watchBehaviorFromConfig(config *pluginv1.WatchSyncProviderConfig) watchBehavior {
	behavior := watchBehavior{playbackCompletionPercent: 90}
	if config == nil {
		return behavior
	}
	behavior.syncManualWatched, _ = strconv.ParseBool(config.GetValues()["provider.sync_manual_watched"])
	behavior.onlyExistingEntries, _ = strconv.ParseBool(config.GetValues()["provider.only_existing_entries"])
	if threshold, err := strconv.ParseFloat(config.GetValues()["provider.playback_completion_percent"], 64); err == nil &&
		threshold >= 1 && threshold <= 99 {
		behavior.playbackCompletionPercent = threshold
	}
	return behavior
}

// appliesWatchedState reports whether an event marks its item watched. Silo
// decides itself whether playback finished and says so in completed; the
// plugin's threshold covers Silo versions that do not send it.
func appliesWatchedState(event *pluginv1.WatchSyncEvent, behavior watchBehavior) bool {
	switch event.GetOperation() {
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_STOP:
		return event.GetCompleted() || event.GetCompletionPercent() > behavior.playbackCompletionPercent
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_WATCHED:
		return event.GetOrigin() == pluginv1.WatchSyncOrigin_WATCH_SYNC_ORIGIN_PLAYBACK_COMPLETION ||
			behavior.syncManualWatched
	default:
		return false
	}
}

func applyEvent(ctx context.Context, client *anilist.Client, dataset mapping.Catalog, event *pluginv1.WatchSyncEvent, behavior watchBehavior) *pluginv1.WatchSyncApplyResult {
	return applyVerifiedEvent(ctx, client, dataset, event, behavior, false)
}

func applyVerifiedEvent(ctx context.Context, client *anilist.Client, dataset mapping.Catalog, event *pluginv1.WatchSyncEvent, behavior watchBehavior, siloPlayed bool) *pluginv1.WatchSyncApplyResult {
	result := &pluginv1.WatchSyncApplyResult{EventId: event.GetEventId()}
	switch event.GetOperation() {
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_START,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_PAUSE:
		result.Status = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE
		return result
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_STOP,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_WATCHED:
		if !appliesWatchedState(event, behavior) && !(siloPlayed && event.GetOperation() == pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_STOP) {
			result.Status = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE
			return result
		}
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SET_RATING,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_RATING:
		return applyRating(ctx, client, dataset, event, behavior)
	default:
		return rejected(result, pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMANENT, "AniList does not support this watch synchronization operation")
	}
	targets, failed := mappedTargets(ctx, dataset, event, result)
	if failed != nil {
		return failed
	}
	ids := make([]int, 0, len(targets))
	for id := range targets {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	applied := !behavior.onlyExistingEntries
	for _, id := range ids {
		if behavior.onlyExistingEntries {
			updated, err := client.AdvanceProgressIfExisting(ctx, id, targets[id])
			if err != nil {
				return applyErrorResult(event.GetEventId(), err)
			}
			applied = applied || updated
			continue
		}
		if err := client.AdvanceProgress(ctx, id, targets[id]); err != nil {
			return applyErrorResult(event.GetEventId(), err)
		}
	}
	if !applied {
		result.Status = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE
		return result
	}
	result.Status = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED
	return result
}

// applyRating sets or clears a movie's AniList score.
func applyRating(ctx context.Context, client *anilist.Client, dataset mapping.Catalog, event *pluginv1.WatchSyncEvent, behavior watchBehavior) *pluginv1.WatchSyncApplyResult {
	result := &pluginv1.WatchSyncApplyResult{EventId: event.GetEventId()}
	if event.GetMedia().GetMediaType() != pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE {
		return rejected(result, pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMANENT, "AniList Sync syncs ratings for movies only")
	}
	set := event.GetOperation() == pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SET_RATING
	if set && (event.GetRating() < 1 || event.GetRating() > 10) {
		return rejected(result, pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST, "rating must be from 1 to 10")
	}
	targets, failed := mappedTargets(ctx, dataset, event, result)
	if failed != nil {
		return failed
	}
	if len(targets) != 1 {
		return rejected(result, pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMANENT, "the movie maps to several AniList entries, so its rating is not synced")
	}
	var mediaID int
	for id := range targets {
		mediaID = id
	}
	var changed bool
	var err error
	if set {
		changed, err = client.SetScore(ctx, mediaID, int(event.GetRating()), behavior.onlyExistingEntries)
	} else {
		changed, err = client.RemoveScore(ctx, mediaID)
	}
	if err != nil {
		return applyErrorResult(event.GetEventId(), err)
	}
	result.Status = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE
	if changed {
		result.Status = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED
	}
	return result
}

// mappedTargets resolves an event to AniList entries, or returns the result
// to send when it cannot.
func mappedTargets(ctx context.Context, dataset mapping.Catalog, event *pluginv1.WatchSyncEvent, result *pluginv1.WatchSyncApplyResult) (map[int]int, *pluginv1.WatchSyncApplyResult) {
	targets, err := resolveTargets(ctx, dataset, event.GetMedia())
	if err != nil {
		var mappingErr *mapping.TemporaryError
		if errors.As(err, &mappingErr) {
			return nil, applyErrorResult(event.GetEventId(), err)
		}
		return nil, rejected(result, pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST, err.Error())
	}
	if len(targets) == 0 {
		return nil, rejected(result, pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMANENT, "no unambiguous AniBridge, Anime-Lists, or ARM mapping was found")
	}
	return targets, nil
}

func rejected(result *pluginv1.WatchSyncApplyResult, code pluginv1.WatchSyncFaultCode, message string) *pluginv1.WatchSyncApplyResult {
	result.Status = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED
	result.Fault = &pluginv1.WatchSyncFault{Code: code, SafeMessage: message}
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

// outcomeFor describes an event's result for the status page.
func outcomeFor(event *pluginv1.WatchSyncEvent, result *pluginv1.WatchSyncApplyResult) exportOutcome {
	outcome := exportOutcome{item: describeEvent(event), reason: result.GetFault().GetSafeMessage()}
	switch result.GetStatus() {
	case pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED:
		outcome.status = "applied"
	case pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE:
		outcome.status = "no change"
	case pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED:
		outcome.status = "rejected"
	default:
		outcome.status = "retry"
		if result.GetFault().GetCode() == pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_RATE_LIMITED {
			outcome.status = "deferred"
		}
	}
	return outcome
}

func describeEvent(event *pluginv1.WatchSyncEvent) string {
	media := event.GetMedia()
	var item string
	switch media.GetMediaType() {
	case pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE:
		title := media.GetSeriesTitle()
		if title == "" {
			title = media.GetTitle()
		}
		item = fmt.Sprintf("%s S%d E%d", orUnknown(title), media.GetSeasonNumber(), media.GetEpisodeNumber())
	default:
		item = orUnknown(media.GetTitle())
		if year := media.GetYear(); year > 0 {
			item += fmt.Sprintf(" (%d)", year)
		}
	}
	switch event.GetOperation() {
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SET_RATING:
		item += fmt.Sprintf(", rating %d", event.GetRating())
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_RATING:
		item += ", rating removed"
	}
	return item
}

func orUnknown(title string) string {
	if strings.TrimSpace(title) == "" {
		return "Untitled item"
	}
	return title
}
