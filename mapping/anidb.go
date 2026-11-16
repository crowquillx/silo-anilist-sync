package mapping

import (
	"context"
	"sort"
	"strconv"
	"strings"
)

// AniDBSource is one AniDB episode that completed AniList progress covers.
// Scope is "R" for a regular episode and "S" for a special, as in the
// dataset's anidb:<id>:<scope> descriptors.
type AniDBSource struct {
	AnimeID string
	Scope   string
	Episode int
}

// ResolveAniDB maps one AniDB episode to AniList progress. AniBridge is read
// first; a regular episode it does not cover falls back to ARM's AniDB to
// AniList link, as the Anime-Lists fallback does.
func (c Catalog) ResolveAniDB(ctx context.Context, animeID, scope string, episode int) ([]Target, error) {
	return c.resolveAniDB(ctx, aniDBTarget{ID: strings.TrimSpace(animeID), Scope: strings.ToUpper(strings.TrimSpace(scope)), Episode: episode})
}

// ReverseAniDB resolves completed AniList progress to the AniDB episodes it
// covers. Like Reverse, it reads only AniBridge.
func (c Catalog) ReverseAniDB(anilistID, progress int) []AniDBSource {
	if anilistID < 1 || progress < 1 {
		return nil
	}
	target := "anilist:" + strconv.Itoa(anilistID)
	var descriptors []string
	if c.reverse != nil {
		descriptors = c.reverse[target]
	} else {
		for descriptor, targets := range c.AniBridge {
			if _, ok := targets[target]; ok {
				descriptors = append(descriptors, descriptor)
			}
		}
	}
	seen := make(map[AniDBSource]struct{})
	var out []AniDBSource
	for _, descriptor := range descriptors {
		animeID, scope, ok := parseAniDBDescriptor(descriptor)
		if !ok {
			continue
		}
		ranges, ok := c.AniBridge[descriptor][target]
		if !ok {
			continue
		}
		for _, episode := range completedSourceEpisodes(ranges, progress) {
			source := AniDBSource{AnimeID: animeID, Scope: scope, Episode: episode}
			if _, dup := seen[source]; dup {
				continue
			}
			seen[source] = struct{}{}
			out = append(out, source)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].AnimeID != out[j].AnimeID {
			return out[i].AnimeID < out[j].AnimeID
		}
		if out[i].Scope != out[j].Scope {
			return out[i].Scope < out[j].Scope
		}
		return out[i].Episode < out[j].Episode
	})
	return out
}

// parseAniDBDescriptor reads anidb:<id>:R or anidb:<id>:S. Other scopes, such
// as O for other episodes, are not episodes a library numbers.
func parseAniDBDescriptor(descriptor string) (string, string, bool) {
	parts := strings.Split(descriptor, ":")
	if len(parts) != 3 || parts[0] != "anidb" {
		return "", "", false
	}
	id, err := strconv.Atoi(parts[1])
	if err != nil || id < 1 {
		return "", "", false
	}
	switch parts[2] {
	case "R", "S":
		return parts[1], parts[2], true
	}
	return "", "", false
}
