package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"github.com/crowquillx/silo-anilist-sync/anilist"
	"github.com/crowquillx/silo-anilist-sync/mapping"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	importCursorPrefix = "inc:"
	// ratingsCursor marks a completed rating traversal. Rating imports are
	// always complete snapshots, so the cursor carries no position.
	ratingsCursor = "ratings-v1"
	// importCacheTTL covers a whole remote-state traversal so every page
	// observes one stable list snapshot; a full traversal pages through
	// Silo's host one batch at a time and can outlive short TTLs.
	importCacheTTL = 30 * time.Minute
	// maxCachedLists bounds the accounts whose lists are kept at once.
	maxCachedLists = 8
	// pageTokenPrefix starts a page token that names its list snapshot.
	pageTokenPrefix = "v2:"
	// maxWarningTitles is how many unmapped titles one warning names.
	maxWarningTitles = 10
)

var errSnapshotChanged = errors.New("AniList list changed during the import; the next sync starts it again")

func (s *server) ListRemoteState(ctx context.Context, req *pluginv1.WatchSyncListRemoteStateRequest) (*pluginv1.WatchSyncListRemoteStateResponse, error) {
	watched, ratings := requestedKinds(req.GetStateKinds())
	if watched && ratings {
		return &pluginv1.WatchSyncListRemoteStateResponse{
			Fault: invalidRequestFault(errors.New("watched and rating state are imported in separate traversals")),
		}, nil
	}
	if !watched && !ratings {
		return &pluginv1.WatchSyncListRemoteStateResponse{CompleteSnapshot: true, NextCursor: req.GetCursor()}, nil
	}
	snapshotUnix, offset, err := remotePageToken(req.GetPageToken())
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
	userID := credentialUserID(credentials)
	if userID < 1 {
		account, accountErr := client.Viewer(ctx)
		if accountErr != nil {
			return &pluginv1.WatchSyncListRemoteStateResponse{Fault: s.callFault(ctx, auth, 0, accountErr)}, nil
		}
		userID = account.ID
	}
	snapshot, err := s.importCache().list(ctx, client, userID)
	if err != nil {
		return &pluginv1.WatchSyncListRemoteStateResponse{Fault: s.callFault(ctx, auth, userID, err)}, nil
	}
	if snapshotUnix != 0 && snapshotUnix != snapshot.fetchedAt.Unix() {
		return &pluginv1.WatchSyncListRemoteStateResponse{Fault: &pluginv1.WatchSyncFault{
			Code:        pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY,
			SafeMessage: errSnapshotChanged.Error(),
		}}, nil
	}

	key := preparedKey{
		userID:     userID,
		credential: sha256.Sum256([]byte(credentials.GetAccessToken())),
		snapshotAt: snapshot.fetchedAt,
		ratings:    ratings,
	}
	incremental := false
	if watched {
		fullSetting, _ := strconv.ParseBool(auth.GetProviderConfig().GetValues()["provider.import_full_scan"])
		since := parseImportCursor(req.GetCursor())
		incremental = !since.IsZero() && !s.status.fullScanDue(ctx, userID, fullSetting)
		if incremental {
			key.sinceUnix = since.Unix()
		}
	}
	prepared, err := s.importCache().prepare(ctx, key, func(dataset mapping.Catalog) *preparedImport {
		if ratings {
			states := ratingStates(snapshot.entries, dataset)
			return &preparedImport{states: states, summary: importSummary{full: true, entries: len(snapshot.entries), ratings: len(states)}}
		}
		entries := snapshot.entries
		if incremental {
			entries = entriesChangedSince(entries, time.Unix(key.sinceUnix, 0))
		}
		states, unmapped := remoteStates(entries, dataset)
		return &preparedImport{states: states, summary: importSummary{
			full: !incremental, entries: len(entries), states: len(states), unmapped: unmapped,
		}}
	}, s.mappings)
	if err != nil {
		return &pluginv1.WatchSyncListRemoteStateResponse{Fault: s.callFault(ctx, auth, userID, err)}, nil
	}
	items := prepared.states
	if offset > len(items) {
		return &pluginv1.WatchSyncListRemoteStateResponse{
			Fault: invalidRequestFault(errors.New("remote state page token is out of range")),
		}, nil
	}
	end := min(len(items), offset+pageSize)
	response := &pluginv1.WatchSyncListRemoteStateResponse{
		Items:            cloneRemoteStates(items[offset:end]),
		CompleteSnapshot: !incremental,
	}
	if end < len(items) {
		response.NextPageToken = formatPageToken(snapshot.fetchedAt, end)
		return response, nil
	}
	if ratings {
		response.NextCursor = ratingsCursor
	} else {
		// The durable checkpoint is the moment the list was fetched, so the
		// next traversal sees every entry updated at or after this snapshot.
		response.NextCursor = formatImportCursor(snapshot.fetchedAt)
		response.Warnings = unmappedWarnings(prepared.summary.unmapped)
	}
	s.status.recordImport(ctx, userID, prepared.summary, ratings)
	return response, nil
}

// requestedKinds reports which supported state kinds a traversal asks for.
// No kinds means watched state, as before kinds existed.
func requestedKinds(kinds []pluginv1.WatchSyncRemoteStateKind) (watched, ratings bool) {
	if len(kinds) == 0 {
		return true, false
	}
	for _, kind := range kinds {
		switch kind {
		case pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHED:
			watched = true
		case pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_RATING:
			ratings = true
		}
	}
	return watched, ratings
}

func credentialUserID(credentials *pluginv1.WatchSyncCredentials) int {
	userID, err := strconv.Atoi(credentials.GetSecretAttributes()["user_id"])
	if err != nil || userID < 1 {
		return 0
	}
	return userID
}

// entriesChangedSince keeps list entries updated at or after the traversal
// checkpoint. The inclusive boundary closes the race where an entry is
// updated in the same second the previous snapshot was fetched. A zero time
// (full scan) keeps everything.
func entriesChangedSince(entries []anilist.ListEntry, since time.Time) []anilist.ListEntry {
	if since.IsZero() {
		return entries
	}
	filtered := make([]anilist.ListEntry, 0, len(entries))
	for _, entry := range entries {
		if int64(entry.UpdatedAt) >= since.Unix() {
			filtered = append(filtered, entry)
		}
	}
	return filtered
}

// parseImportCursor reads the durable traversal checkpoint. Empty or
// unrecognized cursors (first run, pre-0.6.0 "full-v1", another format)
// mean a full traversal.
func parseImportCursor(cursor string) time.Time {
	trimmed, ok := strings.CutPrefix(strings.TrimSpace(cursor), importCursorPrefix)
	if !ok {
		return time.Time{}
	}
	sec, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil || sec <= 0 {
		return time.Time{}
	}
	return time.Unix(sec, 0).UTC()
}

// formatImportCursor renders the checkpoint for the given list snapshot.
func formatImportCursor(fetchedAt time.Time) string {
	if fetchedAt.IsZero() {
		fetchedAt = time.Now().UTC()
	}
	return importCursorPrefix + strconv.FormatInt(fetchedAt.Unix(), 10)
}

// remotePageToken reads a page token. Current tokens name the list snapshot
// they page through, "v2:<snapshot unix>:<offset>". Tokens from 0.7, which
// have only "<page>:<offset>", return a zero snapshot.
func remotePageToken(token string) (int64, int, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return 0, 0, nil
	}
	invalid := errors.New("remote state page token is invalid")
	if rest, ok := strings.CutPrefix(token, pageTokenPrefix); ok {
		snapshotText, offsetText, found := strings.Cut(rest, ":")
		snapshot, snapshotErr := strconv.ParseInt(snapshotText, 10, 64)
		offset, offsetErr := strconv.Atoi(offsetText)
		if !found || snapshotErr != nil || offsetErr != nil || snapshot < 1 || offset < 0 {
			return 0, 0, invalid
		}
		return snapshot, offset, nil
	}
	parts := strings.Split(token, ":")
	if len(parts) != 2 {
		return 0, 0, invalid
	}
	page, pageErr := strconv.Atoi(parts[0])
	offset, offsetErr := strconv.Atoi(parts[1])
	if pageErr != nil || offsetErr != nil || page < 1 || offset < 0 {
		return 0, 0, invalid
	}
	return 0, offset, nil
}

func formatPageToken(snapshotAt time.Time, offset int) string {
	return fmt.Sprintf("%s%d:%d", pageTokenPrefix, snapshotAt.Unix(), offset)
}

// remoteStates expands list progress into mapped episodes and movies. It also
// returns the watched entries that no mapping covers.
func remoteStates(entries []anilist.ListEntry, dataset mapping.Catalog) ([]*pluginv1.WatchSyncRemoteState, []anilist.ListEntry) {
	var states []*pluginv1.WatchSyncRemoteState
	var unmapped []anilist.ListEntry
	for _, entry := range entries {
		progress := entry.CompletedProgress()
		if progress < 1 || entry.Media.ID < 1 || entry.ID < 1 {
			continue
		}
		sources := dataset.Reverse(entry.Media.ID, progress)
		if len(sources) == 0 {
			unmapped = append(unmapped, entry)
			continue
		}
		// Silo's host silently drops watched states without a timestamp, so
		// surface AniList's entry-level update time as the last play. AniList
		// has no per-episode dates; this is when the user last marked progress.
		watched := &pluginv1.WatchSyncRemoteWatchedState{PlayCount: 1}
		if entry.UpdatedAt > 0 {
			watched.LastWatchedAt = timestamppb.New(time.Unix(int64(entry.UpdatedAt), 0).UTC())
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
			state := &pluginv1.WatchSyncRemoteState{Watched: watched}
			state.ProviderItemKey, state.Media = sourceIdentity(entry, source)
			states = append(states, state)
		}
	}
	return states, unmapped
}

// sourceIdentity is the provider key and Silo media identity of one mapped
// source of a list entry.
func sourceIdentity(entry anilist.ListEntry, source mapping.MediaSource) (string, *pluginv1.WatchSyncMedia) {
	externalIDs := make(map[string]string, len(source.ExternalIDs)+1)
	for provider, id := range source.ExternalIDs {
		externalIDs[provider] = id
	}
	externalIDs["anilist"] = strconv.Itoa(entry.Media.ID)
	media := &pluginv1.WatchSyncMedia{
		Title: entry.PreferredTitle(),
		Year:  int32(entry.Media.StartDate.Year),
	}
	if source.Movie {
		media.MediaType = pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE
		media.ExternalIds = externalIDs
		return fmt.Sprintf("anilist:%d:movie", entry.ID), media
	}
	media.MediaType = pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE
	media.SeriesTitle = entry.PreferredTitle()
	media.SeriesYear = int32(entry.Media.StartDate.Year)
	media.SeriesExternalIds = externalIDs
	media.SeasonNumber = int32(source.Season)
	media.EpisodeNumber = int32(source.Episode)
	return fmt.Sprintf("anilist:%d:s%d:e%d", entry.ID, source.Season, source.Episode), media
}

// ratingStates lists scored entries that map to exactly one movie. Series
// ratings are not synced: one Silo series usually spans several AniList
// entries, so no single score belongs to it.
func ratingStates(entries []anilist.ListEntry, dataset mapping.Catalog) []*pluginv1.WatchSyncRemoteState {
	var states []*pluginv1.WatchSyncRemoteState
	for _, entry := range entries {
		rating := anilist.RatingFromScore(entry.Score)
		if rating == 0 || entry.Media.ID < 1 || entry.ID < 1 {
			continue
		}
		var movies []mapping.MediaSource
		hasShow := false
		for _, source := range dataset.Reverse(entry.Media.ID, max(1, entry.CompletedProgress())) {
			if source.Movie {
				movies = append(movies, source)
			} else {
				hasShow = true
			}
		}
		if len(movies) != 1 || (hasShow && entry.Media.Format != "MOVIE") {
			continue
		}
		state := &pluginv1.WatchSyncRemoteState{Rating: &pluginv1.WatchSyncRemoteRatingState{Rating: int32(rating)}}
		if entry.UpdatedAt > 0 {
			state.Rating.RatedAt = timestamppb.New(time.Unix(int64(entry.UpdatedAt), 0).UTC())
		}
		state.ProviderItemKey, state.Media = sourceIdentity(entry, movies[0])
		states = append(states, state)
	}
	return states
}

// unmappedWarnings tells the sync run which watched entries were not
// imported because no mapping covers them.
func unmappedWarnings(unmapped []anilist.ListEntry) []string {
	if len(unmapped) == 0 {
		return nil
	}
	titles := make([]string, 0, min(len(unmapped), maxWarningTitles))
	for _, entry := range unmapped[:min(len(unmapped), maxWarningTitles)] {
		titles = append(titles, entry.PreferredTitle())
	}
	subject := fmt.Sprintf("%d AniList entries have no TVDB, TMDB or IMDb mapping and were", len(unmapped))
	if len(unmapped) == 1 {
		subject = "1 AniList entry has no TVDB, TMDB or IMDb mapping and was"
	}
	warning := subject + " not imported: " + strings.Join(titles, ", ")
	if more := len(unmapped) - len(titles); more > 0 {
		warning += fmt.Sprintf(", and %d more", more)
	}
	return []string{warning + ". The AniList Sync admin page lists them."}
}

func cloneRemoteStates(states []*pluginv1.WatchSyncRemoteState) []*pluginv1.WatchSyncRemoteState {
	if len(states) == 0 {
		return nil
	}
	cloned := make([]*pluginv1.WatchSyncRemoteState, len(states))
	for index, state := range states {
		cloned[index] = proto.Clone(state).(*pluginv1.WatchSyncRemoteState)
	}
	return cloned
}

// importCache keeps recently fetched AniList lists, one per account, so a
// multi-page traversal costs a single AniList request and every page sees one
// stable snapshot. Several accounts can import at once without evicting each
// other's snapshot mid-traversal. Failures are never cached.
type importCache struct {
	mu       sync.Mutex
	lists    map[int]*listSnapshot
	fetching map[int]*sync.Mutex
	prepared map[preparedSlot]*preparedImport
	// preparedMu serializes mapping expansion, which is CPU-heavy, so
	// concurrent pages of one traversal never repeat it.
	preparedMu sync.Mutex
}

type listSnapshot struct {
	entries   []anilist.ListEntry
	fetchedAt time.Time
}

type preparedSlot struct {
	userID  int
	ratings bool
}

// preparedKey names one expansion of a list snapshot. A changed credential,
// snapshot, cursor or scan mode replaces it.
type preparedKey struct {
	userID     int
	credential [sha256.Size]byte
	snapshotAt time.Time
	sinceUnix  int64
	ratings    bool
}

type preparedImport struct {
	key     preparedKey
	states  []*pluginv1.WatchSyncRemoteState
	summary importSummary
}

func newImportCache() *importCache {
	return &importCache{
		lists:    make(map[int]*listSnapshot),
		fetching: make(map[int]*sync.Mutex),
		prepared: make(map[preparedSlot]*preparedImport),
	}
}

// list returns the account's cached list, fetching it when missing or
// expired. Fetches for one account are serialized; other accounts proceed.
func (c *importCache) list(ctx context.Context, client *anilist.Client, userID int) (*listSnapshot, error) {
	c.mu.Lock()
	if snapshot := c.freshLocked(userID); snapshot != nil {
		c.mu.Unlock()
		return snapshot, nil
	}
	fetch := c.fetching[userID]
	if fetch == nil {
		fetch = &sync.Mutex{}
		c.fetching[userID] = fetch
	}
	c.mu.Unlock()

	fetch.Lock()
	defer fetch.Unlock()
	c.mu.Lock()
	if snapshot := c.freshLocked(userID); snapshot != nil {
		c.mu.Unlock()
		return snapshot, nil
	}
	c.mu.Unlock()
	entries, err := client.ListEntries(ctx, userID)
	if err != nil {
		return nil, err
	}
	snapshot := &listSnapshot{entries: entries, fetchedAt: time.Now().UTC()}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lists[userID] = snapshot
	delete(c.prepared, preparedSlot{userID, false})
	delete(c.prepared, preparedSlot{userID, true})
	c.evictLocked()
	return snapshot, nil
}

func (c *importCache) freshLocked(userID int) *listSnapshot {
	snapshot := c.lists[userID]
	if snapshot == nil || time.Since(snapshot.fetchedAt) >= importCacheTTL {
		return nil
	}
	return snapshot
}

// evictLocked drops the oldest lists beyond maxCachedLists.
func (c *importCache) evictLocked() {
	for len(c.lists) > maxCachedLists {
		oldest := 0
		for userID, snapshot := range c.lists {
			if oldest == 0 || snapshot.fetchedAt.Before(c.lists[oldest].fetchedAt) {
				oldest = userID
			}
		}
		delete(c.lists, oldest)
		delete(c.prepared, preparedSlot{oldest, false})
		delete(c.prepared, preparedSlot{oldest, true})
	}
}

// prepare returns the expansion for key, building it with build when the
// cached one is for another key.
func (c *importCache) prepare(ctx context.Context, key preparedKey, build func(mapping.Catalog) *preparedImport, mappings *mapping.Client) (*preparedImport, error) {
	c.preparedMu.Lock()
	defer c.preparedMu.Unlock()
	slot := preparedSlot{key.userID, key.ratings}
	c.mu.Lock()
	cached := c.prepared[slot]
	c.mu.Unlock()
	if cached != nil && cached.key == key {
		return cached, nil
	}
	dataset, err := mappings.Dataset(ctx)
	if err != nil {
		return nil, err
	}
	prepared := build(dataset)
	prepared.key = key
	c.mu.Lock()
	c.prepared[slot] = prepared
	c.mu.Unlock()
	return prepared, nil
}
