package mapping

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type Target struct {
	AniListID int
	Episode   int
}

type Dataset map[string]map[string]map[string]string

type MediaSource struct {
	Movie       bool
	Season      int
	Episode     int
	ExternalIDs map[string]string
}

type sourceCandidate struct {
	provider string
	id       string
	movie    bool
	season   int
	episode  int
}

// Reverse resolves completed AniList progress back to stable catalog identities.
// AniBridge is authoritative here: the title-only fallbacks used for export do
// not carry enough episode identity to import history safely.
func (d Dataset) Reverse(anilistID, progress int) ([]MediaSource, error) {
	if anilistID < 1 || progress < 1 {
		return nil, nil
	}
	targetDescriptor := "anilist:" + strconv.Itoa(anilistID)
	var candidates []sourceCandidate
	for descriptor, targets := range d {
		ranges, ok := targets[targetDescriptor]
		if !ok {
			continue
		}
		source, ok := parseSourceCandidate(descriptor)
		if !ok {
			continue
		}
		if source.movie {
			candidates = append(candidates, source)
			continue
		}
		episodes, err := completedSourceEpisodes(ranges, progress)
		if err != nil {
			return nil, fmt.Errorf("reverse map %s to %s: %w", targetDescriptor, descriptor, err)
		}
		for _, episode := range episodes {
			candidate := source
			candidate.episode = episode
			candidates = append(candidates, candidate)
		}
	}
	return preferredMediaSources(candidates), nil
}

func parseSourceCandidate(descriptor string) (sourceCandidate, bool) {
	parts := strings.Split(descriptor, ":")
	if len(parts) == 2 && strings.HasSuffix(parts[0], "_movie") {
		provider := strings.TrimSuffix(parts[0], "_movie")
		if (provider == "tvdb" || provider == "tmdb" || provider == "imdb") && strings.TrimSpace(parts[1]) != "" {
			return sourceCandidate{provider: provider, id: parts[1], movie: true}, true
		}
		return sourceCandidate{}, false
	}
	if len(parts) != 3 || !strings.HasSuffix(parts[0], "_show") || !strings.HasPrefix(parts[2], "s") {
		return sourceCandidate{}, false
	}
	provider := strings.TrimSuffix(parts[0], "_show")
	if provider != "tvdb" && provider != "tmdb" && provider != "imdb" {
		return sourceCandidate{}, false
	}
	season, err := strconv.Atoi(strings.TrimPrefix(parts[2], "s"))
	if err != nil || season < 0 || strings.TrimSpace(parts[1]) == "" {
		return sourceCandidate{}, false
	}
	return sourceCandidate{provider: provider, id: parts[1], season: season}, true
}

func completedSourceEpisodes(ranges map[string]string, progress int) ([]int, error) {
	if len(ranges) == 0 {
		episodes := make([]int, progress)
		for index := range episodes {
			episodes[index] = index + 1
		}
		return episodes, nil
	}
	seen := make(map[int]struct{})
	for sourceSpec, targetSpec := range ranges {
		sourceStart, sourceEnd, err := parseRange(sourceSpec)
		if err != nil {
			return nil, err
		}
		targetStart, targetEnd, ratio, err := parseProjection(targetSpec)
		if err != nil {
			return nil, err
		}
		completedTargets := progress - targetStart + 1
		if targetEnd > 0 {
			completedTargets = min(completedTargets, targetEnd-targetStart+1)
		}
		if completedTargets <= 0 {
			continue
		}
		completedSources := completedTargets
		switch {
		case ratio > 1:
			completedSources *= ratio
		case ratio < 0:
			completedSources /= -ratio
		}
		if sourceEnd > 0 {
			completedSources = min(completedSources, sourceEnd-sourceStart+1)
		}
		for offset := range completedSources {
			seen[sourceStart+offset] = struct{}{}
		}
	}
	episodes := make([]int, 0, len(seen))
	for episode := range seen {
		episodes = append(episodes, episode)
	}
	sort.Ints(episodes)
	return episodes, nil
}

func parseProjection(spec string) (int, int, int, error) {
	parts := strings.SplitN(spec, "|", 2)
	if strings.Contains(parts[0], ",") {
		return 0, 0, 0, fmt.Errorf("non-contiguous target range %q is not reversible", spec)
	}
	start, end, err := parseRange(parts[0])
	if err != nil {
		return 0, 0, 0, err
	}
	ratio := 1
	if len(parts) == 2 {
		ratio, err = strconv.Atoi(parts[1])
		if err != nil || ratio == 0 {
			return 0, 0, 0, fmt.Errorf("invalid target ratio %q", spec)
		}
	}
	return start, end, ratio, nil
}

func preferredMediaSources(candidates []sourceCandidate) []MediaSource {
	var result []MediaSource
	for _, movie := range []bool{false, true} {
		group := preferredSourceGroup(candidates, movie)
		if group == "" {
			continue
		}
		byEpisode := make(map[[2]int]*MediaSource)
		for _, candidate := range candidates {
			if candidate.movie != movie {
				continue
			}
			if sourceGroup(candidate) == group {
				key := [2]int{candidate.season, candidate.episode}
				byEpisode[key] = &MediaSource{
					Movie:       movie,
					Season:      candidate.season,
					Episode:     candidate.episode,
					ExternalIDs: map[string]string{candidate.provider: candidate.id},
				}
			}
		}
		for _, candidate := range candidates {
			if candidate.movie != movie {
				continue
			}
			key := [2]int{candidate.season, candidate.episode}
			source := byEpisode[key]
			if source == nil {
				continue
			}
			if existing := source.ExternalIDs[candidate.provider]; existing == "" || existing == candidate.id {
				source.ExternalIDs[candidate.provider] = candidate.id
			}
		}
		for _, source := range byEpisode {
			result = append(result, *source)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Movie != result[j].Movie {
			return !result[i].Movie
		}
		if result[i].Season != result[j].Season {
			return result[i].Season < result[j].Season
		}
		return result[i].Episode < result[j].Episode
	})
	return result
}

func preferredSourceGroup(candidates []sourceCandidate, movie bool) string {
	counts := make(map[string]int)
	for _, candidate := range candidates {
		if candidate.movie == movie {
			counts[sourceGroup(candidate)]++
		}
	}
	best, bestCount, bestPriority := "", 0, 100
	for group, count := range counts {
		provider := strings.SplitN(group, ":", 2)[0]
		priority := sourceProviderPriority(provider, movie)
		if count > bestCount || count == bestCount && (priority < bestPriority || priority == bestPriority && group < best) {
			best, bestCount, bestPriority = group, count, priority
		}
	}
	return best
}

func sourceGroup(candidate sourceCandidate) string {
	return candidate.provider + ":" + candidate.id
}

func sourceProviderPriority(provider string, movie bool) int {
	order := []string{"tvdb", "tmdb", "imdb"}
	if movie {
		order = []string{"tmdb", "imdb", "tvdb"}
	}
	for index, candidate := range order {
		if provider == candidate {
			return index
		}
	}
	return len(order)
}

func (d Dataset) Resolve(provider, id string, season, episode int) ([]Target, error) {
	descriptor := sourceDescriptor(provider, id, season)
	if descriptor == "" {
		return nil, nil
	}
	return d.resolveDescriptor(descriptor, episode)
}

func (d Dataset) resolveAniDB(id, scope string, episode int) ([]Target, error) {
	id = strings.TrimSpace(id)
	scope = strings.ToUpper(strings.TrimSpace(scope))
	if id == "" || (scope != "R" && scope != "S") {
		return nil, nil
	}
	return d.resolveDescriptor(fmt.Sprintf("anidb:%s:%s", id, scope), episode)
}

func (d Dataset) resolveDescriptor(descriptor string, episode int) ([]Target, error) {
	targets := d[descriptor]
	out := make([]Target, 0, len(targets))
	for targetDescriptor, ranges := range targets {
		if !strings.HasPrefix(targetDescriptor, "anilist:") {
			continue
		}
		anilistID, err := strconv.Atoi(strings.TrimPrefix(targetDescriptor, "anilist:"))
		if err != nil {
			continue
		}
		mapped, ok, err := mapEpisode(ranges, episode)
		if err != nil {
			return nil, fmt.Errorf("map %s to %s: %w", descriptor, targetDescriptor, err)
		}
		if ok {
			out = append(out, Target{AniListID: anilistID, Episode: mapped})
		}
	}
	return out, nil
}

func sourceDescriptor(provider, id string, season int) string {
	id = strings.TrimSpace(id)
	if id == "" {
		return ""
	}
	switch strings.ToLower(provider) {
	case "tvdb":
		if season < 0 {
			return "tvdb_movie:" + id
		}
		return fmt.Sprintf("tvdb_show:%s:s%d", id, season)
	case "tmdb":
		if season < 0 {
			return "tmdb_movie:" + id
		}
		return fmt.Sprintf("tmdb_show:%s:s%d", id, season)
	case "imdb":
		if season < 0 {
			return "imdb_movie:" + id
		}
		return fmt.Sprintf("imdb_show:%s:s%d", id, season)
	default:
		return ""
	}
}

func mapEpisode(ranges map[string]string, episode int) (int, bool, error) {
	if episode < 1 {
		return 0, false, nil
	}
	if len(ranges) == 0 {
		return episode, true, nil
	}
	for source, target := range ranges {
		start, end, err := parseRange(source)
		if err != nil {
			return 0, false, err
		}
		if episode < start || (end > 0 && episode > end) {
			continue
		}
		return projectEpisode(target, episode-start)
	}
	return 0, false, nil
}

func parseRange(value string) (int, int, error) {
	parts := strings.SplitN(strings.TrimSpace(value), "-", 2)
	start, err := strconv.Atoi(parts[0])
	if err != nil || start < 1 {
		return 0, 0, fmt.Errorf("invalid range %q", value)
	}
	if len(parts) == 1 || parts[1] == "" {
		return start, 0, nil
	}
	end, err := strconv.Atoi(parts[1])
	if err != nil || end < start {
		return 0, 0, fmt.Errorf("invalid range %q", value)
	}
	return start, end, nil
}

func projectEpisode(spec string, sourceOffset int) (int, bool, error) {
	parts := strings.SplitN(spec, "|", 2)
	ratio := 1
	if len(parts) == 2 {
		var err error
		ratio, err = strconv.Atoi(parts[1])
		if err != nil || ratio == 0 {
			return 0, false, fmt.Errorf("invalid target ratio %q", spec)
		}
	}
	if strings.Contains(parts[0], ",") {
		return 0, false, fmt.Errorf("non-contiguous target range %q is not representable as AniList progress", spec)
	}
	start, end, err := parseRange(parts[0])
	if err != nil {
		return 0, false, err
	}

	index := sourceOffset
	switch {
	case ratio > 1:
		consumed := sourceOffset + 1
		if consumed%ratio != 0 {
			return 0, false, nil
		}
		index = consumed/ratio - 1
	case ratio < 0:
		index = (sourceOffset+1)*(-ratio) - 1
	}
	mapped := start + index
	if end > 0 && mapped > end {
		return 0, false, nil
	}
	return mapped, true, nil
}
