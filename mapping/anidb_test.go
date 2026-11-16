package mapping

import (
	"context"
	"reflect"
	"testing"
)

func TestReverseAniDBListsCompletedAniDBEpisodes(t *testing.T) {
	catalog := Catalog{AniBridge: Dataset{
		"anidb:23:R":       {"anilist:42": {"1-12": "1-12"}},
		"anidb:24:R":       {"anilist:42": {"1-2": "13-14"}},
		"anidb:23:S":       {"anilist:42": {}},
		"anidb:23:O":       {"anilist:42": {"1": "1"}},
		"tvdb_show:100:s1": {"anilist:42": {"1-14": "1-14"}},
		"anidb:99:R":       {"anilist:7": {"1-12": "1-12"}},
	}}
	got := catalog.ReverseAniDB(42, 13)
	want := []AniDBSource{}
	for episode := 1; episode <= 12; episode++ {
		want = append(want, AniDBSource{"23", "R", episode})
	}
	for episode := 1; episode <= 13; episode++ {
		want = append(want, AniDBSource{"23", "S", episode})
	}
	want = append(want, AniDBSource{"24", "R", 1})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ReverseAniDB = %v\nwant %v", got, want)
	}
	indexed := catalog
	indexed.reverse = catalog.AniBridge.BuildReverseIndex()
	if !reflect.DeepEqual(indexed.ReverseAniDB(42, 13), want) {
		t.Fatal("indexed lookup differs")
	}
	if catalog.ReverseAniDB(42, 0) != nil || catalog.ReverseAniDB(0, 3) != nil {
		t.Fatal("empty progress mapped")
	}
}

func TestResolveAniDBMapsRegularEpisodesAndSpecials(t *testing.T) {
	catalog := Catalog{AniBridge: Dataset{
		"anidb:23:R": {"anilist:30": {"1-12": "1-12"}},
		"anidb:23:S": {"anilist:31": {"1-2": "1-2"}},
	}}
	for _, tc := range []struct {
		scope   string
		episode int
		want    []Target
	}{
		{"R", 5, []Target{{30, 5}}},
		{"r", 5, []Target{{30, 5}}},
		{"S", 2, []Target{{31, 2}}},
		{"S", 3, nil},
	} {
		got, err := catalog.ResolveAniDB(context.Background(), "23", tc.scope, tc.episode)
		if err != nil || !reflect.DeepEqual(got, tc.want) && !(len(got) == 0 && len(tc.want) == 0) {
			t.Errorf("%s %d = %v %v want %v", tc.scope, tc.episode, got, err, tc.want)
		}
	}
}
