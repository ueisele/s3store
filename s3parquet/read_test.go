package s3parquet

import (
	"sort"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/ueisele/s3store/internal/core"
)

// dedupRec is a pure-Go scratch record used to exercise
// dedupLatest without paying for parquet encode / decode.
type dedupRec struct {
	entity  string
	ver     int64
	payload string
}

// vr is a terse constructor for versionedRecord[dedupRec].
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
// records with zero ver fall back to insertedAt. Hard to set up
// via integration without µs-precision timing, so lives here.
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

// TestDedupLatest_EmptyInput guards the nil/empty-slice fast
// path. Integration can't easily produce a genuinely-empty
// record batch because every ref corresponds to a file with
// at least one row.
func TestDedupLatest_EmptyInput(t *testing.T) {
	got := dedupLatest(nil,
		func(r dedupRec) string { return r.entity },
		func(r dedupRec, _ time.Time) int64 { return r.ver })
	if len(got) != 0 {
		t.Errorf("expected empty, got %d", len(got))
	}
}

// TestStripVersions drops the per-record insertedAt metadata
// while preserving order and record values. It's used on the
// non-dedup path so a regression here would show up as a
// missing field in user-visible output.
func TestStripVersions(t *testing.T) {
	now := time.UnixMicro(1)
	in := []versionedRecord[dedupRec]{
		{rec: dedupRec{entity: "a", payload: "first"}, insertedAt: now},
		{rec: dedupRec{entity: "b", payload: "second"}, insertedAt: now},
	}
	got := stripVersions(in)
	if len(got) != 2 {
		t.Fatalf("got %d, want 2", len(got))
	}
	if got[0].payload != "first" || got[1].payload != "second" {
		t.Errorf("order or values wrong: %+v", got)
	}

	// Nil input returns nil, not a zero-length allocation.
	if v := stripVersions[dedupRec](nil); v != nil {
		t.Errorf("stripVersions(nil) = %v, want nil", v)
	}
}

// narrowRec / wideRec simulate schema evolution: a file written
// with the narrow type must still decode cleanly into the wide
// type, with missing columns zero-filled.
type narrowRec struct {
	Period   string `parquet:"period"`
	Customer string `parquet:"customer"`
	Value    int64  `parquet:"value"`
}

type wideRec struct {
	Period   string  `parquet:"period"`
	Customer string  `parquet:"customer"`
	Value    int64   `parquet:"value"`
	Amount   float64 `parquet:"amount"`   // added after file written
	Currency string  `parquet:"currency"` // added after file written
}

// TestDecodeParquet_MissingColumnsZeroFill guards the public
// contract of the read path: when a parquet file lacks a column
// present in T, parquet-go must zero-fill rather than error. If
// this ever regresses, "added a new column to T" becomes a
// breaking change against older files on disk.
func TestDecodeParquet_MissingColumnsZeroFill(t *testing.T) {
	in := []narrowRec{
		{Period: "2026-03-17", Customer: "abc", Value: 1},
		{Period: "2026-03-17", Customer: "def", Value: 2},
	}
	data, err := encodeParquet(in, &parquet.Snappy)
	if err != nil {
		t.Fatalf("encodeParquet: %v", err)
	}

	out, err := decodeParquet[wideRec](data)
	if err != nil {
		t.Fatalf("decodeParquet[wideRec]: %v", err)
	}
	if len(out) != len(in) {
		t.Fatalf("got %d rows, want %d", len(out), len(in))
	}
	for i, r := range out {
		if r.Period != in[i].Period ||
			r.Customer != in[i].Customer ||
			r.Value != in[i].Value {
			t.Errorf("[%d] carried fields wrong: got %+v", i, r)
		}
		if r.Amount != 0 {
			t.Errorf("[%d] Amount=%v, want 0", i, r.Amount)
		}
		if r.Currency != "" {
			t.Errorf("[%d] Currency=%q, want \"\"", i, r.Currency)
		}
	}
}

// TestDedupePatterns guards the literal-duplicate dedup applied
// before plan construction in the *Many functions. Tests the
// fast paths (nil, single) and order preservation on duplicates.
func TestDedupePatterns(t *testing.T) {
	cases := []struct {
		name string
		in   []string
		want []string
	}{
		{"nil", nil, nil},
		{"single", []string{"a"}, []string{"a"}},
		{"distinct", []string{"a", "b", "c"}, []string{"a", "b", "c"}},
		{"mixed dups", []string{"a", "b", "a", "c", "b"},
			[]string{"a", "b", "c"}},
		{"all same", []string{"a", "a", "a"}, []string{"a"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := core.DedupePatterns(tc.in)
			if len(got) != len(tc.want) {
				t.Fatalf("got %v (len %d), want %v (len %d)",
					got, len(got), tc.want, len(tc.want))
			}
			for i, w := range tc.want {
				if got[i] != w {
					t.Errorf("got[%d] = %q, want %q",
						i, got[i], w)
				}
			}
		})
	}
}

// TestDedupReplicas_CollapsesIdenticalEntityVersion covers the
// primary case: two records with the same (entity, version)
// collapse to the first occurrence, while distinct versions of
// the same entity are preserved untouched.
func TestDedupReplicas_CollapsesIdenticalEntityVersion(t *testing.T) {
	now := time.UnixMicro(1_000_000)
	recs := []versionedRecord[dedupRec]{
		vr("a", 1, "a-1-first", now),
		vr("a", 1, "a-1-dup", now),
		vr("a", 2, "a-2", now),
		vr("b", 1, "b-1-first", now),
		vr("b", 1, "b-1-dup", now),
	}
	got := dedupReplicas(recs,
		func(r dedupRec) string { return r.entity },
		func(r dedupRec, _ time.Time) int64 { return r.ver })

	if len(got) != 3 {
		t.Fatalf("got %d records, want 3", len(got))
	}
	wantPayloads := []string{"a-1-first", "a-2", "b-1-first"}
	for i, want := range wantPayloads {
		if got[i].rec.payload != want {
			t.Errorf("[%d] got %q, want %q",
				i, got[i].rec.payload, want)
		}
	}
}

// TestDedupReplicas_EmptyInput guards the nil/empty-slice fast
// path. Mirrors TestDedupLatest_EmptyInput.
func TestDedupReplicas_EmptyInput(t *testing.T) {
	got := dedupReplicas(nil,
		func(r dedupRec) string { return r.entity },
		func(r dedupRec, _ time.Time) int64 { return r.ver })
	if len(got) != 0 {
		t.Errorf("expected empty, got %d", len(got))
	}
}

// TestDedupReplicas_NoOpOnDistinctVersions confirms that when
// every (entity, version) pair is unique, dedupReplicas returns
// its input unchanged — the function only collapses true
// replicas, never distinct versions.
func TestDedupReplicas_NoOpOnDistinctVersions(t *testing.T) {
	now := time.UnixMicro(1_000_000)
	recs := []versionedRecord[dedupRec]{
		vr("a", 1, "a-1", now),
		vr("a", 2, "a-2", now),
		vr("a", 3, "a-3", now),
		vr("b", 1, "b-1", now),
	}
	got := dedupReplicas(recs,
		func(r dedupRec) string { return r.entity },
		func(r dedupRec, _ time.Time) int64 { return r.ver })
	if len(got) != 4 {
		t.Fatalf("got %d records, want 4 (no collapse)", len(got))
	}
}

// TestDedupLatest_TieKeepsFirst documents the
// stability-on-tie invariant: when two records share the same
// version, the first occurrence wins. Integration tests can't
// reliably reproduce this (same-µs file timestamps are rare on
// fast hardware).
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

// vrf is a terse versionedRecord[dedupRec] constructor for the
// sort-resolver tests. fileName isn't carried on versionedRecord
// any more — equal-key ties are resolved by sort.SliceStable
// preserving input order, which in production is set up by
// preparePartitions sorting files by S3 key first.
func vrf(e string, v int64, p string, insertedAt time.Time) versionedRecord[dedupRec] {
	return versionedRecord[dedupRec]{
		rec:        dedupRec{entity: e, ver: v, payload: p},
		insertedAt: insertedAt,
	}
}

// TestResolveSortCmp_EntityAndExplicitVersion covers the
// "EntityKeyOf set, explicit VersionOf" branch: records group
// per entity and ascend by the explicit version so newer lands
// after older within each entity.
func TestResolveSortCmp_EntityAndExplicitVersion(t *testing.T) {
	cmp := resolveSortCmp[dedupRec](
		func(r dedupRec) string { return r.entity },
		func(r dedupRec, _ time.Time) int64 { return r.ver })

	now := time.UnixMicro(1_000_000)
	recs := []versionedRecord[dedupRec]{
		vrf("b", 1, "b1", now),
		vrf("a", 2, "a2", now),
		vrf("a", 1, "a1", now),
		vrf("b", 2, "b2", now),
	}
	sortWith(recs, cmp)
	wantOrder := []string{"a1", "a2", "b1", "b2"}
	for i, want := range wantOrder {
		if recs[i].rec.payload != want {
			t.Errorf("[%d] got %q, want %q",
				i, recs[i].rec.payload, want)
		}
	}
}

// TestResolveSortCmp_EntityAndDefaultVersion covers the
// "EntityKeyOf set, VersionOf defaulted to DefaultVersionOf"
// branch: the sort picks insertedAt (= LastModified) as version,
// so within each entity the newer LastModified lands later.
func TestResolveSortCmp_EntityAndDefaultVersion(t *testing.T) {
	cmp := resolveSortCmp[dedupRec](
		func(r dedupRec) string { return r.entity },
		DefaultVersionOf[dedupRec])

	early := time.UnixMicro(1_000_000)
	late := time.UnixMicro(2_000_000)
	recs := []versionedRecord[dedupRec]{
		vrf("b", 0, "b-late", late),
		vrf("a", 0, "a-late", late),
		vrf("a", 0, "a-early", early),
		vrf("b", 0, "b-early", early),
	}
	sortWith(recs, cmp)
	wantOrder := []string{"a-early", "a-late", "b-early", "b-late"}
	for i, want := range wantOrder {
		if recs[i].rec.payload != want {
			t.Errorf("[%d] got %q, want %q",
				i, recs[i].rec.payload, want)
		}
	}
}

// TestResolveSortCmp_LastModifiedFallback covers the
// "EntityKeyOf nil" branch: per-file chronological. Equal
// LastModified values resolve via sort.SliceStable preserving
// input order — in production, preparePartitions ensures input
// order is file lex order, so the effective tiebreak is fileName
// ASC even though no fileName field exists on versionedRecord.
func TestResolveSortCmp_LastModifiedFallback(t *testing.T) {
	cmp := resolveSortCmp[dedupRec](nil, nil)

	early := time.UnixMicro(1_000_000)
	late := time.UnixMicro(2_000_000)
	// Input order simulates production's file-lex-sorted feed:
	// for each LastModified group, the lexically-first filename's
	// records appear first.
	recs := []versionedRecord[dedupRec]{
		vrf("x", 0, "early-a", early),
		vrf("x", 0, "early-b", early),
		vrf("x", 0, "late-a", late),
		vrf("x", 0, "late-b", late),
	}
	sortWith(recs, cmp)
	// Stable sort preserves input order within each timestamp.
	wantOrder := []string{"early-a", "early-b", "late-a", "late-b"}
	for i, want := range wantOrder {
		if recs[i].rec.payload != want {
			t.Errorf("[%d] got %q, want %q",
				i, recs[i].rec.payload, want)
		}
	}
}

// sortWith is a tiny test helper that applies a cmp-returning
// comparator through sort.SliceStable, matching how the reader
// invokes its sortCmp in the hot path.
func sortWith(recs []versionedRecord[dedupRec],
	cmpFn func(a, b versionedRecord[dedupRec]) int,
) {
	sort.SliceStable(recs, func(i, j int) bool {
		return cmpFn(recs[i], recs[j]) < 0
	})
}
