package s3parquet

import (
	"testing"
	"time"
)

type dedupRec struct {
	entity  string
	ver     int64
	payload string
}

func vr(e string, v int64, p string, insertedAt time.Time) versionedRecord[dedupRec] {
	return versionedRecord[dedupRec]{
		rec:        dedupRec{entity: e, ver: v, payload: p},
		insertedAt: insertedAt,
	}
}

// TestDedupLatest_PicksMaxVersionPerEntity uses an explicit
// VersionOf that reads the record's own ver field (ignores the
// file timestamp).
func TestDedupLatest_PicksMaxVersionPerEntity(t *testing.T) {
	now := time.UnixMicro(1_000_000)
	recs := []versionedRecord[dedupRec]{
		vr("a", 1, "a-1", now),
		vr("b", 5, "b-5", now),
		vr("a", 3, "a-3", now),
		vr("b", 2, "b-2", now),
		vr("c", 0, "c-0", now),
	}
	got := dedupLatest(recs,
		func(r dedupRec) string { return r.entity },
		func(r dedupRec, _ time.Time) int64 { return r.ver })

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

// TestDedupLatest_UsesInsertedAtFromFile exercises the
// DefaultVersionOf-style path: VersionOf ignores the record
// and returns insertedAt's micros, so the library's per-file
// timestamp decides the winner.
func TestDedupLatest_UsesInsertedAtFromFile(t *testing.T) {
	earlier := time.UnixMicro(1_000_000)
	later := time.UnixMicro(2_000_000)

	recs := []versionedRecord[dedupRec]{
		vr("a", 0, "earlier", earlier),
		vr("a", 0, "later", later),
	}
	got := dedupLatest(recs,
		func(r dedupRec) string { return r.entity },
		DefaultVersionOf[dedupRec])

	if len(got) != 1 {
		t.Fatalf("got %d records, want 1", len(got))
	}
	if got[0].payload != "later" {
		t.Errorf("got %q, want %q", got[0].payload, "later")
	}
}

// TestDedupLatest_HybridVersion mixes a business timestamp with
// insertedAt fallback: records with a non-zero ver win on ver,
// records with zero ver fall back to insertedAt.
func TestDedupLatest_HybridVersion(t *testing.T) {
	earlier := time.UnixMicro(1_000_000)
	later := time.UnixMicro(2_000_000)

	hybrid := func(r dedupRec, insertedAt time.Time) int64 {
		if r.ver != 0 {
			return r.ver * 10_000_000 // push into "newer than file time" range
		}
		return insertedAt.UnixMicro()
	}

	recs := []versionedRecord[dedupRec]{
		// entity a: explicit ver=1 beats a later file
		vr("a", 1, "explicit", earlier),
		vr("a", 0, "filetime", later),
	}
	got := dedupLatest(recs,
		func(r dedupRec) string { return r.entity },
		hybrid)
	if len(got) != 1 {
		t.Fatalf("got %d records, want 1", len(got))
	}
	if got[0].payload != "explicit" {
		t.Errorf("got %q, want explicit", got[0].payload)
	}
}

func TestDedupLatest_EmptyInput(t *testing.T) {
	got := dedupLatest(nil,
		func(r dedupRec) string { return r.entity },
		func(r dedupRec, _ time.Time) int64 { return r.ver })
	if len(got) != 0 {
		t.Errorf("expected empty, got %d", len(got))
	}
}

func TestDedupLatest_TieKeepsFirst(t *testing.T) {
	now := time.UnixMicro(1_000_000)
	recs := []versionedRecord[dedupRec]{
		vr("a", 5, "first", now),
		vr("a", 5, "second", now),
	}
	got := dedupLatest(recs,
		func(r dedupRec) string { return r.entity },
		func(r dedupRec, _ time.Time) int64 { return r.ver })
	if len(got) != 1 {
		t.Fatalf("got %d records, want 1", len(got))
	}
	if got[0].payload != "first" {
		t.Errorf("got %q, want first", got[0].payload)
	}
}

func TestDefaultVersionOf(t *testing.T) {
	ts := time.UnixMicro(1_710_684_000_000_000)
	got := DefaultVersionOf(dedupRec{}, ts)
	if got != 1_710_684_000_000_000 {
		t.Errorf("got %d, want %d", got, 1_710_684_000_000_000)
	}
}
