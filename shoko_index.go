package main

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"github.com/crowquillx/silo-anilist-sync/silostate"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	// shokoIndexFresh is how long an index is used without reading Silo
	// again, and shokoIndexUsable how long an older one still serves imports
	// while a new one is read in the background.
	shokoIndexFresh  = 6 * time.Hour
	shokoIndexUsable = 24 * time.Hour
	// shokoIndexBuildTimeout bounds one reading of a profile's catalog.
	shokoIndexBuildTimeout = 15 * time.Minute
	// shokoIndexRetry is how long a failed reading waits before the next.
	shokoIndexRetry = 5 * time.Minute
	// shokoIndexWait is the longest an import waits for a reading to finish
	// before it asks Silo to come back.
	shokoIndexWait = 30 * time.Second
	// shokoIndexWorkers is how many series are read at once.
	shokoIndexWorkers = 4
	// maxShokoIndexes bounds the connections whose index is kept.
	maxShokoIndexes = 8
)

// errShokoIndexPending means the profile's Shoko VFS shows are still being
// read. The import is retried shortly instead of emitting states that could
// land on the wrong VFS episode.
var errShokoIndexPending = errors.New("reading the profile's Shoko episodes in Silo; the import continues on the next sync")

// shokoIndex describes a profile's Shoko VFS shows: the external IDs of the
// shows, so TVDB, TMDB and IMDb states never land on their group numbering,
// and each AniDB episode's Silo episode with its own external IDs.
type shokoIndex struct {
	builtAt time.Time
	shows   map[string]map[string]struct{}
	// episodes maps AniDB anime, scope and number to a Silo episode. A
	// number two episodes claim maps to nil.
	episodes map[shokoAniDBKey]*shokoIndexedEpisode
}

type shokoAniDBKey struct {
	anime  string
	scope  string
	number int
}

type shokoIndexedEpisode struct {
	contentID string
	ids       map[string]string
}

func newShokoIndex() *shokoIndex {
	return &shokoIndex{shows: make(map[string]map[string]struct{}), episodes: make(map[shokoAniDBKey]*shokoIndexedEpisode)}
}

// isShow reports whether any of a show's external IDs belongs to a VFS show.
func (x *shokoIndex) isShow(ids map[string]string) bool {
	if x == nil {
		return false
	}
	for provider, id := range ids {
		if _, ok := x.shows[provider][strings.TrimSpace(id)]; ok && strings.TrimSpace(id) != "" {
			return true
		}
	}
	return false
}

// episode returns the Silo episode of an AniDB episode, if exactly one has it.
func (x *shokoIndex) episode(anime, scope string, number int) (*shokoIndexedEpisode, bool) {
	if x == nil {
		return nil, false
	}
	episode := x.episodes[shokoAniDBKey{anime, scope, number}]
	return episode, episode != nil
}

func (x *shokoIndex) addShow(series silostate.Series) {
	for provider, id := range map[string]string{"tvdb": series.TvdbID, "tmdb": series.TmdbID, "imdb": series.ImdbID} {
		if id = strings.TrimSpace(id); id == "" {
			continue
		}
		if x.shows[provider] == nil {
			x.shows[provider] = make(map[string]struct{})
		}
		x.shows[provider][id] = struct{}{}
	}
}

func (x *shokoIndex) addEpisode(anime, scope string, number int, episode silostate.Episode) {
	key := shokoAniDBKey{anime, scope, number}
	if existing, claimed := x.episodes[key]; claimed {
		if existing != nil && existing.contentID != episode.ContentID {
			x.episodes[key] = nil
		}
		return
	}
	ids := make(map[string]string, 3)
	for provider, id := range map[string]string{"tvdb": episode.TvdbID, "tmdb": episode.TmdbID, "imdb": episode.ImdbID} {
		if id = strings.TrimSpace(id); id != "" {
			ids[provider] = id
		}
	}
	x.episodes[key] = &shokoIndexedEpisode{contentID: episode.ContentID, ids: ids}
}

// shokoCatalog is the part of the Silo client the index reads.
type shokoCatalog interface {
	Series(ctx context.Context) ([]silostate.Series, error)
	Seasons(ctx context.Context, seriesID string) ([]int, bool, error)
	Episodes(ctx context.Context, seriesID string, season int) ([]silostate.Episode, bool, error)
	FileNames(ctx context.Context, itemID string) ([]string, bool, error)
}

// buildShokoIndex reads the profile's series and finds its VFS shows. Each
// regular season of a VFS show is one AniDB anime, which one episode's files
// name. A series whose first sampled season is not a VFS link is left after
// that sample. Any failed read fails the whole index, so a partial one never
// lets a VFS show through.
func buildShokoIndex(ctx context.Context, catalog shokoCatalog, now time.Time) (*shokoIndex, error) {
	series, err := catalog.Series(ctx)
	if err != nil {
		return nil, err
	}
	index := newShokoIndex()
	index.builtAt = now
	var mu sync.Mutex
	work := make(chan silostate.Series)
	errs := make(chan error, shokoIndexWorkers)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	for range shokoIndexWorkers {
		wg.Go(func() {
			for item := range work {
				if err := indexShokoSeries(ctx, catalog, item, index, &mu); err != nil {
					errs <- err
					cancel()
					return
				}
			}
		})
	}
feed:
	for _, item := range series {
		select {
		case work <- item:
		case <-ctx.Done():
			break feed
		}
	}
	close(work)
	wg.Wait()
	select {
	case err := <-errs:
		return nil, err
	default:
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return index, nil
}

func indexShokoSeries(ctx context.Context, catalog shokoCatalog, series silostate.Series, index *shokoIndex, mu *sync.Mutex) error {
	seasons, found, err := catalog.Seasons(ctx, series.ContentID)
	if err != nil || !found {
		return err
	}
	sort.Ints(seasons)
	type seasonEpisodes struct {
		anime    string
		episodes []silostate.Episode
	}
	regular := make(map[int]seasonEpisodes)
	vfs, sampled := false, false
	var specials []silostate.Episode
	for _, season := range seasons {
		if season < 0 {
			continue
		}
		episodes, found, err := catalog.Episodes(ctx, series.ContentID, season)
		if err != nil {
			return err
		}
		if !found || len(episodes) == 0 {
			continue
		}
		if season == 0 {
			specials = episodes
			continue
		}
		names, found, err := catalog.FileNames(ctx, episodes[0].ContentID)
		if err != nil {
			return err
		}
		anime := ""
		if found {
			anime = shokoAnime(names)
		}
		if anime == "" && !vfs && !sampled {
			// The first regular season is not a VFS link: an ordinary show.
			return nil
		}
		sampled = true
		if anime != "" {
			vfs = true
			regular[season] = seasonEpisodes{anime: anime, episodes: episodes}
		}
	}
	if !vfs && len(specials) > 0 && !sampled {
		// A show of extras only is a VFS show when its first extra is a link.
		names, found, err := catalog.FileNames(ctx, specials[0].ContentID)
		if err != nil {
			return err
		}
		vfs = found && shokoAnime(names) != ""
	}
	if !vfs {
		return nil
	}
	// Specials carry their series' member index in their number; the
	// series is the season at member+1. When that season is not in Silo,
	// or the index is capped, the special's own files name its anime.
	type special struct {
		anime   string
		number  int
		episode silostate.Episode
	}
	var placed []special
	for _, episode := range specials {
		_, number, ok := shokoAniDBEpisode(0, episode.EpisodeNumber)
		if !ok {
			continue
		}
		member := episode.EpisodeNumber / shokoSpecialsBlock
		anime := ""
		if member < shokoMaxMember {
			anime = regular[member+1].anime
		}
		if anime == "" {
			names, found, err := catalog.FileNames(ctx, episode.ContentID)
			if err != nil {
				return err
			}
			if !found {
				continue
			}
			anime = shokoAnime(names)
		}
		if anime != "" {
			placed = append(placed, special{anime, number, episode})
		}
	}
	mu.Lock()
	defer mu.Unlock()
	index.addShow(series)
	for _, season := range regular {
		for _, episode := range season.episodes {
			if episode.EpisodeNumber > 0 {
				index.addEpisode(season.anime, "R", episode.EpisodeNumber, episode)
			}
		}
	}
	for _, s := range placed {
		index.addEpisode(s.anime, "S", s.number, s.episode)
	}
	return nil
}

// shokoIndexCache keeps one index per verified connection. Reading a large
// catalog takes longer than one import call, so it runs in the background:
// an import waits briefly, then asks Silo to retry. An index older than
// shokoIndexFresh is replaced in the background while it keeps serving.
type shokoIndexCache struct {
	mu      sync.Mutex
	now     func() time.Time
	wait    time.Duration
	entries map[string]*shokoIndexEntry
}

type shokoIndexEntry struct {
	index   *shokoIndex
	build   *shokoIndexBuild
	err     error
	retryAt time.Time
	usedAt  time.Time
}

type shokoIndexBuild struct {
	done  chan struct{}
	index *shokoIndex
	err   error
}

func newShokoIndexCache() *shokoIndexCache {
	return &shokoIndexCache{now: time.Now, wait: shokoIndexWait, entries: make(map[string]*shokoIndexEntry)}
}

func (s *server) shokoIndexCache() *shokoIndexCache {
	s.shokoIndexesOnce.Do(func() {
		if s.shokoIndexes == nil {
			s.shokoIndexes = newShokoIndexCache()
		}
	})
	return s.shokoIndexes
}

// get returns the connection's index, starting a reading when there is none
// or it is old.
func (c *shokoIndexCache) get(ctx context.Context, key string, read func(context.Context) (*shokoIndex, error)) (*shokoIndex, error) {
	c.mu.Lock()
	now := c.now()
	entry := c.entries[key]
	if entry == nil {
		c.evictLocked()
		entry = &shokoIndexEntry{}
		c.entries[key] = entry
	}
	entry.usedAt = now
	if entry.index != nil && now.Sub(entry.index.builtAt) < shokoIndexFresh {
		index := entry.index
		c.mu.Unlock()
		return index, nil
	}
	if entry.build == nil && (entry.err == nil || !now.Before(entry.retryAt)) {
		entry.build = c.startLocked(key, entry, read)
	}
	if entry.index != nil && now.Sub(entry.index.builtAt) < shokoIndexUsable {
		index := entry.index
		c.mu.Unlock()
		return index, nil
	}
	build, failure := entry.build, entry.err
	c.mu.Unlock()
	if build == nil {
		return nil, failure
	}
	wait := c.wait
	if deadline, ok := ctx.Deadline(); ok {
		wait = min(wait, time.Until(deadline)-deadlineMargin)
	}
	if wait <= 0 {
		return nil, errShokoIndexPending
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-build.done:
		return build.index, build.err
	case <-timer.C:
		return nil, errShokoIndexPending
	case <-ctx.Done():
		return nil, errShokoIndexPending
	}
}

func (c *shokoIndexCache) startLocked(key string, entry *shokoIndexEntry, read func(context.Context) (*shokoIndex, error)) *shokoIndexBuild {
	build := &shokoIndexBuild{done: make(chan struct{})}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), shokoIndexBuildTimeout)
		defer cancel()
		index, err := read(ctx)
		c.mu.Lock()
		build.index, build.err = index, err
		if c.entries[key] == entry {
			entry.build = nil
			if err != nil {
				entry.err, entry.retryAt = err, c.now().Add(shokoIndexRetry)
			} else {
				entry.index, entry.err = index, nil
			}
		}
		c.mu.Unlock()
		close(build.done)
	}()
	return build
}

// evictLocked drops the least recently used connection beyond the bound.
func (c *shokoIndexCache) evictLocked() {
	for len(c.entries) >= maxShokoIndexes {
		oldest := ""
		for key, entry := range c.entries {
			if oldest == "" || entry.usedAt.Before(c.entries[oldest].usedAt) {
				oldest = key
			}
		}
		delete(c.entries, oldest)
	}
}

// importShokoIndex returns a verified connection's index, or nil for a
// connection without Silo access.
func (s *server) importShokoIndex(ctx context.Context, auth *pluginv1.WatchSyncAuthenticatedContext) (*shokoIndex, error) {
	if auth.GetCapabilityId() != siloVerifiedCapability {
		return nil, nil
	}
	config := siloConfig(auth)
	if err := config.Validate(); err != nil {
		return nil, err
	}
	cache := s.shokoIndexCache()
	client := silostate.NewClient(config)
	return cache.get(ctx, siloConnectionKey(config), func(ctx context.Context) (*shokoIndex, error) {
		return buildShokoIndex(ctx, client, cache.now())
	})
}

// shokoIndexFault describes why an import could not read the VFS shows.
func shokoIndexFault(err error) *pluginv1.WatchSyncFault {
	if errors.Is(err, errShokoIndexPending) {
		return &pluginv1.WatchSyncFault{
			Code:        pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY,
			SafeMessage: err.Error(),
			RetryAfter:  durationpb.New(time.Minute),
		}
	}
	return siloVerificationFault(err)
}

// shokoEpisodeState is the remote state of one VFS episode an AniList entry
// covers. It names the episode only by the episode's own external IDs and has
// no episode number, so Silo matches that episode or nothing: it cannot fall
// back to the show's IDs and the group's season numbering.
func shokoEpisodeState(entryID int, title string, source shokoAniDBKey, episode *shokoIndexedEpisode, watched *pluginv1.WatchSyncRemoteWatchedState) *pluginv1.WatchSyncRemoteState {
	ids := make(map[string]string, len(episode.ids))
	for provider, id := range episode.ids {
		ids[provider] = id
	}
	return &pluginv1.WatchSyncRemoteState{
		ProviderItemKey: "anilist:" + strconv.Itoa(entryID) + ":anidb:" + source.anime + ":" + strings.ToLower(source.scope) + strconv.Itoa(source.number),
		Watched:         watched,
		Media: &pluginv1.WatchSyncMedia{
			MediaType:   pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE,
			Title:       title,
			SeriesTitle: title,
			ExternalIds: ids,
		},
	}
}
