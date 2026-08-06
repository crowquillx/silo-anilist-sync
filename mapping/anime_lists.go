package mapping

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"strconv"
	"strings"
)

const DefaultAnimeListsURL = "https://raw.githubusercontent.com/Anime-Lists/anime-lists/master/anime-list-full.xml"

type AnimeListsDataset map[string][]animeListsRule

type animeListsRule struct {
	AniDBID    string
	AniDBScope string
	SourceFrom int
	SourceTo   int
	TargetFrom int
	Explicit   bool
}

type animeListsDocument struct {
	XMLName xml.Name          `xml:"anime-list"`
	Entries []animeListsEntry `xml:"anime"`
}

type animeListsEntry struct {
	AniDBID           string              `xml:"anidbid,attr"`
	TVDBID            string              `xml:"tvdbid,attr"`
	DefaultTVDBSeason string              `xml:"defaulttvdbseason,attr"`
	EpisodeOffset     string              `xml:"episodeoffset,attr"`
	TMDBTV            string              `xml:"tmdbtv,attr"`
	TMDBSeason        string              `xml:"tmdbseason,attr"`
	TMDBOffset        string              `xml:"tmdboffset,attr"`
	TMDBID            string              `xml:"tmdbid,attr"`
	IMDbID            string              `xml:"imdbid,attr"`
	Mappings          []animeListsMapping `xml:"mapping-list>mapping"`
}

type animeListsMapping struct {
	AniDBSeason string `xml:"anidbseason,attr"`
	TVDBSeason  string `xml:"tvdbseason,attr"`
	TMDBSeason  string `xml:"tmdbseason,attr"`
	Start       string `xml:"start,attr"`
	End         string `xml:"end,attr"`
	Offset      string `xml:"offset,attr"`
	Text        string `xml:",chardata"`
}

type aniDBTarget struct {
	ID      string
	Scope   string
	Episode int
}

func parseAnimeLists(data []byte) (AnimeListsDataset, error) {
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return nil, fmt.Errorf("decode Anime-Lists mappings: empty document")
	}
	var document animeListsDocument
	if err := xml.Unmarshal(data, &document); err != nil {
		return nil, fmt.Errorf("decode Anime-Lists mappings: %w", err)
	}
	if document.XMLName.Local != "anime-list" {
		return nil, fmt.Errorf("decode Anime-Lists mappings: root element must be anime-list")
	}

	dataset := make(AnimeListsDataset)
	for _, entry := range document.Entries {
		if err := dataset.addEntry(entry); err != nil {
			return nil, err
		}
	}
	return dataset, nil
}

func (d AnimeListsDataset) addEntry(entry animeListsEntry) error {
	aniDBID := strings.TrimSpace(entry.AniDBID)
	if !decimalID(aniDBID) {
		return fmt.Errorf("decode Anime-Lists mappings: invalid AniDB ID %q", entry.AniDBID)
	}

	if decimalID(entry.TVDBID) {
		if err := d.addDefaultSeries("tvdb", entry.TVDBID, entry.DefaultTVDBSeason, entry.EpisodeOffset, aniDBID); err != nil {
			return err
		}
	}
	if decimalID(entry.TMDBTV) {
		if err := d.addDefaultSeries("tmdb", entry.TMDBTV, entry.TMDBSeason, entry.TMDBOffset, aniDBID); err != nil {
			return err
		}
	}

	for _, row := range entry.Mappings {
		scope := "R"
		if strings.TrimSpace(row.AniDBSeason) == "0" {
			scope = "S"
		}
		if decimalID(entry.TVDBID) {
			season := firstValue(row.TVDBSeason, entry.DefaultTVDBSeason)
			if err := d.addExplicitSeries("tvdb", entry.TVDBID, season, aniDBID, scope, row); err != nil {
				return err
			}
		}
		if decimalID(entry.TMDBTV) {
			season := firstValue(row.TMDBSeason, entry.TMDBSeason)
			if err := d.addExplicitSeries("tmdb", entry.TMDBTV, season, aniDBID, scope, row); err != nil {
				return err
			}
		}
	}

	for _, id := range splitIDs(entry.TMDBID) {
		if decimalID(id) {
			d.addRule(sourceDescriptor("tmdb", id, -1), animeListsRule{
				AniDBID: aniDBID, AniDBScope: "R", SourceFrom: 1, SourceTo: 1, TargetFrom: 1,
			})
		}
	}
	for _, id := range splitIDs(entry.IMDbID) {
		if id != "" {
			d.addRule(sourceDescriptor("imdb", id, -1), animeListsRule{
				AniDBID: aniDBID, AniDBScope: "R", SourceFrom: 1, SourceTo: 1, TargetFrom: 1,
			})
		}
	}
	return nil
}

func (d AnimeListsDataset) addDefaultSeries(provider, id, seasonRaw, offsetRaw, aniDBID string) error {
	season, ok := positiveOrZeroInt(seasonRaw)
	if !ok {
		return nil
	}
	offset, err := optionalInt(offsetRaw)
	if err != nil {
		return fmt.Errorf("decode Anime-Lists mapping for AniDB %s: %w", aniDBID, err)
	}
	aniDBStart := 1
	externalStart := 1 + offset
	if externalStart < 1 {
		aniDBStart += 1 - externalStart
		externalStart = 1
	}
	d.addRule(sourceDescriptor(provider, id, season), animeListsRule{
		AniDBID: aniDBID, AniDBScope: "R", SourceFrom: externalStart, TargetFrom: aniDBStart,
	})
	return nil
}

func (d AnimeListsDataset) addExplicitSeries(provider, id, seasonRaw, aniDBID, scope string, row animeListsMapping) error {
	season, ok := positiveOrZeroInt(seasonRaw)
	if !ok {
		return nil
	}
	descriptor := sourceDescriptor(provider, id, season)
	for _, token := range strings.Split(strings.TrimSpace(row.Text), ";") {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}
		parts := strings.SplitN(token, "-", 2)
		if len(parts) != 2 || strings.Contains(parts[0], "+") {
			return fmt.Errorf("decode Anime-Lists mapping for AniDB %s: invalid episode pair %q", aniDBID, token)
		}
		aniDBEpisode, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil || aniDBEpisode < 1 {
			return fmt.Errorf("decode Anime-Lists mapping for AniDB %s: invalid AniDB episode %q", aniDBID, parts[0])
		}
		for _, externalRaw := range strings.Split(parts[1], "+") {
			externalEpisode, err := strconv.Atoi(strings.TrimSpace(externalRaw))
			if err != nil || externalEpisode < 0 {
				return fmt.Errorf("decode Anime-Lists mapping for AniDB %s: invalid external episode %q", aniDBID, externalRaw)
			}
			if externalEpisode == 0 {
				continue
			}
			d.addRule(descriptor, animeListsRule{
				AniDBID: aniDBID, AniDBScope: scope, SourceFrom: externalEpisode,
				SourceTo: externalEpisode, TargetFrom: aniDBEpisode, Explicit: true,
			})
		}
	}
	if strings.TrimSpace(row.Start) == "" {
		return nil
	}
	start, err := strconv.Atoi(strings.TrimSpace(row.Start))
	if err != nil || start < 1 {
		return fmt.Errorf("decode Anime-Lists mapping for AniDB %s: invalid start %q", aniDBID, row.Start)
	}
	end := start
	if strings.TrimSpace(row.End) != "" {
		end, err = strconv.Atoi(strings.TrimSpace(row.End))
		if err != nil || end < start {
			return fmt.Errorf("decode Anime-Lists mapping for AniDB %s: invalid end %q", aniDBID, row.End)
		}
	}
	offset, err := optionalInt(row.Offset)
	if err != nil {
		return fmt.Errorf("decode Anime-Lists mapping for AniDB %s: %w", aniDBID, err)
	}
	externalStart, externalEnd := start+offset, end+offset
	if externalEnd < 1 {
		return nil
	}
	targetStart := start
	if externalStart < 1 {
		targetStart += 1 - externalStart
		externalStart = 1
	}
	d.addRule(descriptor, animeListsRule{
		AniDBID: aniDBID, AniDBScope: scope, SourceFrom: externalStart,
		SourceTo: externalEnd, TargetFrom: targetStart, Explicit: true,
	})
	return nil
}

func (d AnimeListsDataset) addRule(descriptor string, rule animeListsRule) {
	if descriptor != "" {
		d[descriptor] = append(d[descriptor], rule)
	}
}

func (d AnimeListsDataset) Resolve(provider, id string, season, episode int) []aniDBTarget {
	descriptor := sourceDescriptor(provider, id, season)
	if descriptor == "" || episode < 1 {
		return nil
	}
	var fallback, explicit []aniDBTarget
	for _, rule := range d[descriptor] {
		if episode < rule.SourceFrom || (rule.SourceTo > 0 && episode > rule.SourceTo) {
			continue
		}
		target := aniDBTarget{
			ID: rule.AniDBID, Scope: rule.AniDBScope,
			Episode: rule.TargetFrom + episode - rule.SourceFrom,
		}
		if rule.Explicit {
			explicit = appendUniqueAniDBTarget(explicit, target)
		} else {
			fallback = appendUniqueAniDBTarget(fallback, target)
		}
	}
	if len(explicit) > 0 {
		return explicit
	}
	return fallback
}

func appendUniqueAniDBTarget(targets []aniDBTarget, candidate aniDBTarget) []aniDBTarget {
	for _, target := range targets {
		if target == candidate {
			return targets
		}
	}
	return append(targets, candidate)
}

func optionalInt(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid offset %q", raw)
	}
	return value, nil
}

func positiveOrZeroInt(raw string) (int, bool) {
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	return value, err == nil && value >= 0
}

func decimalID(raw string) bool {
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	return err == nil && value > 0
}

func splitIDs(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if id := strings.TrimSpace(part); id != "" {
			out = append(out, id)
		}
	}
	return out
}

func firstValue(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
