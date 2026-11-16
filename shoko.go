package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"github.com/crowquillx/silo-anilist-sync/mapping"
	"github.com/crowquillx/silo-anilist-sync/silostate"
)

// The ShokoAnime VFS plugin writes the AniDB anime of every link into its
// name as "[AniDB Anime=<id>]" and documents the marker as a stable contract.
// Its seasons follow the Shoko group, not TVDB or TMDB: each series of the
// group is one season, numbered in Shoko's order. A VFS episode is therefore
// mapped by its AniDB anime and episode number, never by its show's TVDB or
// TMDB ID and season.
var shokoAniDBAnimeMarker = regexp.MustCompile(`\[AniDB Anime=([1-9][0-9]*)\]`)

const (
	// shokoSpecialsBlock is the range of season-zero numbers each series of a
	// group gets: the VFS numbers an extra memberIndex*1000 + offset + n.
	shokoSpecialsBlock = 1000
	// shokoMaxSpecial is the highest number in a block that is a special.
	// Credits start at 400, trailers at 600, parodies at 800 and other
	// extras at 900; none of those has an AniDB episode to map.
	shokoMaxSpecial = 399
	// shokoMaxMember is the last member index the VFS numbers on its own;
	// later series share block 99, so their specials are not attributed by
	// number.
	shokoMaxMember = 99
)

// shokoAnime returns the AniDB anime every file name names, or "" when a
// name has no marker or two names disagree, such as an item that merges a
// VFS link with a file from an ordinary library.
func shokoAnime(fileNames []string) string {
	anime := ""
	for _, name := range fileNames {
		match := shokoAniDBAnimeMarker.FindStringSubmatch(name)
		if match == nil {
			return ""
		}
		if anime != "" && anime != match[1] {
			return ""
		}
		anime = match[1]
	}
	return anime
}

// shokoAniDBEpisode places a VFS episode in its AniDB anime. A regular season
// holds the anime's regular episodes under their AniDB numbers. Season zero
// holds the group's extras; only specials have an AniDB episode, numbered
// within the series' block.
func shokoAniDBEpisode(season, episode int) (scope string, number int, ok bool) {
	switch {
	case season > 0 && episode > 0:
		return "R", episode, true
	case season == 0 && episode > 0:
		if n := episode % shokoSpecialsBlock; n >= 1 && n <= shokoMaxSpecial {
			return "S", n, true
		}
	}
	return "", 0, false
}

// shokoEpisode is what Silo's files say about one episode item.
type shokoEpisode struct {
	// anime is the AniDB anime the item's VFS links name, or "" for an item
	// outside the Shoko VFS.
	anime string
}

// shokoLookup reads an episode item's files from Silo.
type shokoLookup func(ctx context.Context, mediaItemID string) (shokoEpisode, error)

// errShokoItemMissing is returned when Silo no longer has an event's item, so
// its files cannot show whether it is a VFS link.
var errShokoItemMissing = errors.New("Silo no longer has this episode, so it cannot be checked for a Shoko link")

// shokoTargets maps a VFS episode to AniList progress through its AniDB
// anime. It never falls back to the show's TVDB or TMDB numbering.
func shokoTargets(ctx context.Context, dataset mapping.Catalog, anime string, season, episode int) (map[int]int, error) {
	scope, number, ok := shokoAniDBEpisode(season, episode)
	if !ok {
		return nil, nil
	}
	targets, err := dataset.ResolveAniDB(ctx, anime, scope, number)
	if err != nil {
		return nil, err
	}
	return targetsMap(targets), nil
}

// shokoMappingMessage explains why a VFS episode was not synced.
func shokoMappingMessage(season, episode int) string {
	if _, _, ok := shokoAniDBEpisode(season, episode); !ok {
		return fmt.Sprintf("Shoko extra S%02dE%02d has no AniDB episode number, so it is not synced", season, episode)
	}
	return "Shoko episode has no AniDB to AniList mapping"
}

// siloConfig reads the Silo connection a verified AniList connection keeps
// with its encrypted credential.
func siloConfig(auth *pluginv1.WatchSyncAuthenticatedContext) silostate.Config {
	attrs := auth.GetCredentials().GetSecretAttributes()
	return silostate.Config{BaseURL: attrs["silo.base_url"], APIKey: attrs["silo.api_key"], ProfileID: attrs["silo.profile_id"]}
}

// siloConnectionKey names a Silo connection in caches without keeping its key.
func siloConnectionKey(config silostate.Config) string {
	sum := sha256.Sum256([]byte(config.BaseURL + "\x00" + config.ProfileID + "\x00" + config.APIKey))
	return string(sum[:])
}

// exportShokoLookup returns how a batch reads its episodes' files, or nil for
// a connection without Silo access.
func (s *server) exportShokoLookup(auth *pluginv1.WatchSyncAuthenticatedContext) shokoLookup {
	if auth.GetCapabilityId() != siloVerifiedCapability {
		return nil
	}
	config := siloConfig(auth)
	client := silostate.NewClient(config)
	connection := siloConnectionKey(config)
	cache := s.shokoItemCache()
	return func(ctx context.Context, mediaItemID string) (shokoEpisode, error) {
		key := connection + "\x00" + mediaItemID
		if cached, ok := cache.get(key); ok {
			return cached, nil
		}
		names, found, err := client.FileNames(ctx, mediaItemID)
		if err != nil {
			return shokoEpisode{}, err
		}
		if !found {
			return shokoEpisode{}, errShokoItemMissing
		}
		episode := shokoEpisode{anime: shokoAnime(names)}
		cache.put(key, episode)
		return episode, nil
	}
}

func (s *server) shokoItemCache() *shokoItemCache {
	s.shokoItemsOnce.Do(func() {
		if s.shokoItems == nil {
			s.shokoItems = newShokoItemCache()
		}
	})
	return s.shokoItems
}

const (
	shokoItemTTL      = 6 * time.Hour
	maxShokoItemCache = 4096
)

// shokoItemCache remembers which episode items are VFS links, so retried and
// repeated events do not read their files again. A link keeps its anime for
// its whole life, so a few hours of staleness only delay a moved file.
type shokoItemCache struct {
	mu      sync.Mutex
	now     func() time.Time
	entries map[string]shokoItemEntry
}

type shokoItemEntry struct {
	episode shokoEpisode
	at      time.Time
}

func newShokoItemCache() *shokoItemCache {
	return &shokoItemCache{now: time.Now, entries: make(map[string]shokoItemEntry)}
}

func (c *shokoItemCache) get(key string) (shokoEpisode, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	if !ok || c.now().Sub(entry.at) >= shokoItemTTL {
		return shokoEpisode{}, false
	}
	return entry.episode, true
}

func (c *shokoItemCache) put(key string, episode shokoEpisode) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	if len(c.entries) >= maxShokoItemCache {
		for k, entry := range c.entries {
			if now.Sub(entry.at) >= shokoItemTTL {
				delete(c.entries, k)
			}
		}
		for k := range c.entries {
			if len(c.entries) < maxShokoItemCache {
				break
			}
			delete(c.entries, k)
		}
	}
	c.entries[key] = shokoItemEntry{episode: episode, at: now}
}

// isShokoEpisodeEvent reports whether an event names an episode item whose
// files can be read.
func isShokoEpisodeEvent(event *pluginv1.WatchSyncEvent) bool {
	media := event.GetMedia()
	return media.GetMediaType() == pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE &&
		strings.TrimSpace(media.GetMediaItemId()) != ""
}

// mappedEventTargets resolves an event to AniList entries, reading a verified
// connection's episode files first. A Shoko VFS episode maps only through its
// AniDB anime; any other item keeps the TVDB, TMDB and IMDb mapping.
func mappedEventTargets(ctx context.Context, dataset mapping.Catalog, event *pluginv1.WatchSyncEvent, result *pluginv1.WatchSyncApplyResult, shoko shokoLookup) (map[int]int, *pluginv1.WatchSyncApplyResult) {
	if shoko == nil || !isShokoEpisodeEvent(event) {
		return mappedTargets(ctx, dataset, event, result)
	}
	episode, err := shoko(ctx, strings.TrimSpace(event.GetMedia().GetMediaItemId()))
	if errors.Is(err, errShokoItemMissing) {
		return nil, rejected(result, pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMANENT, err.Error())
	}
	if err != nil {
		return nil, siloVerificationErrorResult(event.GetEventId(), err)
	}
	if episode.anime == "" {
		return mappedTargets(ctx, dataset, event, result)
	}
	media := event.GetMedia()
	season, number := int(media.GetSeasonNumber()), int(media.GetEpisodeNumber())
	targets, err := shokoTargets(ctx, dataset, episode.anime, season, number)
	if err != nil {
		var mappingErr *mapping.TemporaryError
		if errors.As(err, &mappingErr) {
			return nil, applyErrorResult(event.GetEventId(), err)
		}
		return nil, rejected(result, pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMANENT, err.Error())
	}
	if len(targets) == 0 {
		return nil, rejected(result, pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMANENT, shokoMappingMessage(season, number))
	}
	return targets, nil
}
