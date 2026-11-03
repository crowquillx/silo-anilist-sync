package mapping

import "testing"

func TestResolveEpisode(t *testing.T) {
	dataset := Dataset{
		"tvdb_show:100:s2": {
			"anilist:42": {"1-12": "1-12"},
		},
	}
	targets, err := dataset.Resolve("tvdb", "100", 2, 7)
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 1 || targets[0] != (Target{AniListID: 42, Episode: 7}) {
		t.Fatalf("targets = %#v", targets)
	}
}

func TestResolveIdentityMapping(t *testing.T) {
	dataset := Dataset{"tmdb_show:55:s1": {"anilist:9": {}}}
	targets, err := dataset.Resolve("tmdb", "55", 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 1 || targets[0].Episode != 3 {
		t.Fatalf("targets = %#v", targets)
	}
}

func TestResolveRatios(t *testing.T) {
	cases := []struct {
		name    string
		ranges  map[string]string
		episode int
		want    int
		ok      bool
	}{
		{"two sources per target waits for boundary", map[string]string{"1-4": "20-21|2"}, 1, 0, false},
		{"two sources per target", map[string]string{"1-4": "20-21|2"}, 2, 20, true},
		{"three targets per source", map[string]string{"1-2": "20-25|-3"}, 1, 22, true},
		{"open ended", map[string]string{"4-": "10-"}, 6, 12, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok, err := mapEpisode(tc.ranges, tc.episode)
			if err != nil {
				t.Fatal(err)
			}
			if ok != tc.ok || got != tc.want {
				t.Fatalf("mapEpisode() = %d, %v; want %d, %v", got, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestRejectsNonContiguousTarget(t *testing.T) {
	if _, _, err := mapEpisode(map[string]string{"1-6": "1-3,5-7"}, 4); err == nil {
		t.Fatal("expected non-contiguous target to be rejected")
	}
}

func TestZeroRatioTargetMapsToNothing(t *testing.T) {
	got, ok, err := mapEpisode(map[string]string{"1-19": "1-19|0"}, 5)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("zero-ratio row should map to no target, got %d", got)
	}
}

func TestMovieDescriptor(t *testing.T) {
	for _, provider := range []string{"tmdb", "tvdb"} {
		dataset := Dataset{provider + "_movie:1": {"anilist:2": {}}}
		targets, err := dataset.Resolve(provider, "1", -1, 1)
		if err != nil || len(targets) != 1 || targets[0].AniListID != 2 {
			t.Fatalf("%s targets = %#v, err = %v", provider, targets, err)
		}
	}
}

func TestReversePrefersCompleteTVDBSeriesAndMergesMatchingIDs(t *testing.T) {
	dataset := Dataset{
		"tvdb_show:100:s2": {"anilist:42": {"1-12": "1-12"}},
		"tmdb_show:200:s2": {"anilist:42": {"1-12": "1-12"}},
	}
	sources := dataset.Reverse(42, 2)
	if len(sources) != 2 {
		t.Fatalf("sources = %#v", sources)
	}
	if sources[1].Season != 2 || sources[1].Episode != 2 ||
		sources[1].ExternalIDs["tvdb"] != "100" || sources[1].ExternalIDs["tmdb"] != "200" {
		t.Fatalf("second source = %#v", sources[1])
	}
}

func TestReverseRatiosRepresentCompletedSourceEpisodes(t *testing.T) {
	dataset := Dataset{
		"tvdb_show:100:s1": {"anilist:42": {"1-4": "1-2|2"}},
		"tvdb_show:200:s1": {"anilist:43": {"1-2": "1-6|-3"}},
	}
	manySources := dataset.Reverse(42, 1)
	if len(manySources) != 2 || manySources[1].Episode != 2 {
		t.Fatalf("two-source projection = %#v", manySources)
	}
	manyTargets := dataset.Reverse(43, 2)
	if len(manyTargets) != 0 {
		t.Fatalf("partial target group should not complete a source episode: %#v", manyTargets)
	}
	manyTargets = dataset.Reverse(43, 3)
	if len(manyTargets) != 1 || manyTargets[0].Episode != 1 {
		t.Fatalf("completed target group = %#v", manyTargets)
	}
}

func TestReversePrefersTMDBMovieIdentity(t *testing.T) {
	dataset := Dataset{
		"tvdb_movie:100": {"anilist:42": {}},
		"tmdb_movie:200": {"anilist:42": {}},
	}
	sources := dataset.Reverse(42, 1)
	if len(sources) != 1 || !sources[0].Movie ||
		sources[0].ExternalIDs["tmdb"] != "200" || sources[0].ExternalIDs["tvdb"] != "100" {
		t.Fatalf("sources = %#v", sources)
	}
}

// Regression: the upstream AniBridge dataset ships rows like
// tvdb_show:305089:s1 -> anilist:189046 {"1-19": "1-19|0"} (Re:Zero S4).
// A zero-ratio row implies no source progress, and an unreadable row must
// never fail the whole watched import.
func TestReverseIgnoresZeroRatioAndUnreadableRows(t *testing.T) {
	dataset := Dataset{
		"tvdb_show:305089:s1": {"anilist:189046": {"1-19": "1-19|0"}},
		"tvdb_show:305089:s4": {"anilist:189046": {"1-19": "1-19"}},
		"tmdb_show:65942:s1":  {"anilist:189046": {"67-85": "1-19"}},
	}
	sources := dataset.Reverse(189046, 3)
	if len(sources) != 3 || sources[0].Season != 4 || sources[0].Episode != 1 || sources[2].Episode != 3 {
		t.Fatalf("sources = %#v", sources)
	}

	only := Dataset{"tvdb_show:305089:s1": {"anilist:189046": {"1-19": "1-19|0"}}}
	if got := only.Reverse(189046, 19); len(got) != 0 {
		t.Fatalf("zero-ratio-only dataset should yield no sources, got %#v", got)
	}

	corrupt := Dataset{
		"tvdb_show:1:s1": {"anilist:42": {"1-4": "not-a-range"}},
		"tvdb_show:2:s1": {"anilist:42": {"garbage": "1-4"}},
		"tmdb_show:3:s1": {"anilist:42": {"1-4": "1-4"}},
	}
	sources = corrupt.Reverse(42, 2)
	if len(sources) != 2 || sources[1].ExternalIDs["tmdb"] != "3" {
		t.Fatalf("unreadable rows should be skipped, got %#v", sources)
	}
}

func TestMapEpisodeSkipsUnreadableRowsDeterministically(t *testing.T) {
	ranges := map[string]string{"1-12": "1-12", "x-y": "13-24", "13-": "garbage|z"}
	for range 200 {
		got, ok, err := mapEpisode(ranges, 3)
		if err != nil || !ok || got != 3 {
			t.Fatalf("mapEpisode() = %d, %v, %v; want 3, true, nil", got, ok, err)
		}
	}
	// The matching row's own target is still validated.
	if _, _, err := mapEpisode(ranges, 14); err == nil {
		t.Fatal("expected the unreadable matching target to fail")
	}
}

func TestReverseIndexedMatchesReverse(t *testing.T) {
	dataset := Dataset{
		"tvdb_show:100:s1": {"anilist:42": {"1-12": "1-12"}, "anilist:43": {"13-24": "1-12"}},
		"tmdb_show:55:s1":  {"anilist:42": {}},
		"tmdb_movie:7":     {"anilist:44": {}},
	}
	index := dataset.BuildReverseIndex()
	for _, test := range []struct{ id, progress int }{{42, 5}, {43, 2}, {44, 1}, {45, 1}} {
		want := dataset.Reverse(test.id, test.progress)
		got := dataset.ReverseIndexed(index, test.id, test.progress)
		if len(want) != len(got) {
			t.Fatalf("anilist %d: indexed %#v, scan %#v", test.id, got, want)
		}
		for i := range want {
			if want[i].Movie != got[i].Movie || want[i].Season != got[i].Season || want[i].Episode != got[i].Episode || len(want[i].ExternalIDs) != len(got[i].ExternalIDs) {
				t.Fatalf("anilist %d: indexed %#v, scan %#v", test.id, got, want)
			}
		}
	}
}
