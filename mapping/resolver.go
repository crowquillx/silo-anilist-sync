package mapping

import (
	"fmt"
	"strconv"
	"strings"
)

type Target struct {
	AniListID int
	Episode   int
}

type Dataset map[string]map[string]map[string]string

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
