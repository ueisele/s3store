package s3parquet

import "testing"

type dedupRec struct {
	entity string
	ver    int64
	payload string
}

func TestDedupLatest_PicksMaxVersionPerEntity(t *testing.T) {
	recs := []dedupRec{
		{"a", 1, "a-1"},
		{"b", 5, "b-5"},
		{"a", 3, "a-3"},
		{"b", 2, "b-2"},
		{"c", 0, "c-0"},
	}
	got := dedupLatest(recs,
		func(r dedupRec) string { return r.entity },
		func(r dedupRec) int64 { return r.ver })

	if len(got) != 3 {
		t.Fatalf("got %d records, want 3", len(got))
	}

	// First-seen order is preserved: a (first seen at idx 0),
	// then b (idx 1), then c (idx 4).
	want := []string{"a-3", "b-5", "c-0"}
	for i, r := range got {
		if r.payload != want[i] {
			t.Errorf("[%d] got payload %q, want %q",
				i, r.payload, want[i])
		}
	}
}

func TestDedupLatest_EmptyInput(t *testing.T) {
	got := dedupLatest([]dedupRec{},
		func(r dedupRec) string { return r.entity },
		func(r dedupRec) int64 { return r.ver })
	if len(got) != 0 {
		t.Errorf("expected empty, got %d", len(got))
	}
}

func TestDedupLatest_TieKeepsFirst(t *testing.T) {
	// Equal versions: the first occurrence wins. Second
	// occurrence's payload is NOT picked.
	recs := []dedupRec{
		{"a", 5, "first"},
		{"a", 5, "second"},
	}
	got := dedupLatest(recs,
		func(r dedupRec) string { return r.entity },
		func(r dedupRec) int64 { return r.ver })
	if len(got) != 1 {
		t.Fatalf("got %d records, want 1", len(got))
	}
	if got[0].payload != "first" {
		t.Errorf("got %q, want %q", got[0].payload, "first")
	}
}
