package s3parquet

import (
	"slices"
	"sort"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/ueisele/s3store/internal/core"
)

// dedupRec is a pure-Go scratch record used to exercise the dedup
// helpers without paying for parquet encode / decode.
type dedupRec struct {
	entity  string
	ver     int64
	payload string
}

// dedupRecCmp is the dedup-path sort: (entity, ver) ascending.
// Mirrors what resolveSortCmp produces in production.
func dedupRecCmp(a, b dedupRec) int {
	if a.entity != b.entity {
		if a.entity < b.entity {
			return -1
		}
		return 1
	}
	if a.ver < b.ver {
		return -1
	}
	if a.ver > b.ver {
		return 1
	}
	return 0
}

// sortDedup sorts in-place by (entity, ver) ascending — the same
// preconditioning sortAndIterate applies before invoking the
// dedup helpers in production.
func sortDedup(recs []dedupRec) {
	sort.SliceStable(recs, func(i, j int) bool {
		return dedupRecCmp(recs[i], recs[j]) < 0
	})
}

// TestDedupLatestSeq_PicksMaxVersionPerEntity feeds an unsorted
// slice through the same sort+dedup the production pipeline runs
// (sort by (entity, ver) ascending, then dedupLatestSeq picks the
// last record of each entity group). Verifies output is one
// record per entity, in entity-sort order, with the max version.
func TestDedupLatestSeq_PicksMaxVersionPerEntity(t *testing.T) {
	recs := []dedupRec{
		{"a", 1, "a-1"},
		{"b", 5, "b-5"},
		{"a", 3, "a-3"},
		{"b", 2, "b-2"},
		{"c", 0, "c-0"},
	}
	sortDedup(recs)
	got := slices.Collect(dedupLatestSeq(recs,
		func(r dedupRec) string { return r.entity }))

	if len(got) != 3 {
		t.Fatalf("got %d records, want 3", len(got))
	}
	// Entities in sort order: a, b, c. Each yields its
	// max-version record (sort places it last in the group).
	want := []string{"a-3", "b-5", "c-0"}
	for i, r := range got {
		if r.payload != want[i] {
			t.Errorf("[%d] got payload %q, want %q",
				i, r.payload, want[i])
		}
	}
}

// TestDedupLatestSeq_EmptyInput guards the nil/empty-slice fast
// path.
func TestDedupLatestSeq_EmptyInput(t *testing.T) {
	got := slices.Collect(dedupLatestSeq[dedupRec](nil,
		func(r dedupRec) string { return r.entity }))
	if len(got) != 0 {
		t.Errorf("expected empty, got %d", len(got))
	}
}

// TestDedupLatestSeq_TieKeepsFirst documents the
// stability-on-tie invariant: when two records share (entity,
// version), the first one (in sorted input order) wins. With
// stable sort + the one-pass walker, that ends up being the
// LAST tied record actually — pending is overwritten on each
// equal element. Verify the documented behaviour.
//
// Note: with stable sort, ties stay in input order. The walker
// updates `pending` on every iteration, so the "winner" of a
// tie is the LAST record in the tied group. Document that.
func TestDedupLatestSeq_TieKeepsLast(t *testing.T) {
	// Already sorted by (entity, ver); two records tie on (a, 5).
	recs := []dedupRec{
		{"a", 5, "first"},
		{"a", 5, "second"},
	}
	got := slices.Collect(dedupLatestSeq(recs,
		func(r dedupRec) string { return r.entity }))
	if len(got) != 1 {
		t.Fatalf("got %d records, want 1", len(got))
	}
	// pending advances to "second" before the loop ends, so it
	// wins. Last-write-wins on ties.
	if got[0].payload != "second" {
		t.Errorf("got %q, want second", got[0].payload)
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

// TestDedupReplicasSeq_CollapsesIdenticalEntityVersion covers
// the primary case: two records with the same (entity, version)
// collapse to the first occurrence per (entity, version) group;
// distinct versions of the same entity are preserved.
func TestDedupReplicasSeq_CollapsesIdenticalEntityVersion(t *testing.T) {
	recs := []dedupRec{
		{"a", 1, "a-1-first"},
		{"a", 1, "a-1-dup"},
		{"a", 2, "a-2"},
		{"b", 1, "b-1-first"},
		{"b", 1, "b-1-dup"},
	}
	sortDedup(recs)
	got := slices.Collect(dedupReplicasSeq(recs,
		func(r dedupRec) string { return r.entity },
		func(r dedupRec) int64 { return r.ver }))

	if len(got) != 3 {
		t.Fatalf("got %d records, want 3", len(got))
	}
	wantPayloads := []string{"a-1-first", "a-2", "b-1-first"}
	for i, want := range wantPayloads {
		if got[i].payload != want {
			t.Errorf("[%d] got %q, want %q",
				i, got[i].payload, want)
		}
	}
}

// TestDedupReplicasSeq_EmptyInput guards the nil/empty-slice
// fast path.
func TestDedupReplicasSeq_EmptyInput(t *testing.T) {
	got := slices.Collect(dedupReplicasSeq[dedupRec](nil,
		func(r dedupRec) string { return r.entity },
		func(r dedupRec) int64 { return r.ver }))
	if len(got) != 0 {
		t.Errorf("expected empty, got %d", len(got))
	}
}

// TestDedupReplicasSeq_NoOpOnDistinctVersions confirms that when
// every (entity, version) pair is unique, dedupReplicasSeq emits
// every input record — only true replicas collapse.
func TestDedupReplicasSeq_NoOpOnDistinctVersions(t *testing.T) {
	recs := []dedupRec{
		{"a", 1, "a-1"},
		{"a", 2, "a-2"},
		{"a", 3, "a-3"},
		{"b", 1, "b-1"},
	}
	sortDedup(recs)
	got := slices.Collect(dedupReplicasSeq(recs,
		func(r dedupRec) string { return r.entity },
		func(r dedupRec) int64 { return r.ver }))
	if len(got) != 4 {
		t.Fatalf("got %d records, want 4 (no collapse)", len(got))
	}
}

// TestResolveSortCmp_EntityAndExplicitVersion covers the
// "EntityKeyOf set, explicit VersionOf" branch: records group
// per entity and ascend by the explicit version so newer lands
// after older within each entity.
func TestResolveSortCmp_EntityAndExplicitVersion(t *testing.T) {
	cmp := resolveSortCmp[dedupRec](
		func(r dedupRec) string { return r.entity },
		func(r dedupRec) int64 { return r.ver })

	recs := []dedupRec{
		{"b", 1, "b1"},
		{"a", 2, "a2"},
		{"a", 1, "a1"},
		{"b", 2, "b2"},
	}
	sort.SliceStable(recs, func(i, j int) bool {
		return cmp(recs[i], recs[j]) < 0
	})
	wantOrder := []string{"a1", "a2", "b1", "b2"}
	for i, want := range wantOrder {
		if recs[i].payload != want {
			t.Errorf("[%d] got %q, want %q",
				i, recs[i].payload, want)
		}
	}
}

// TestResolveSortCmp_NilWhenNoDedup pins the contract that
// EntityKeyOf == nil produces a nil comparator — sortAndIterate
// uses that signal to skip the sort entirely and emit records in
// input (decode) order.
func TestResolveSortCmp_NilWhenNoDedup(t *testing.T) {
	if cmp := resolveSortCmp[dedupRec](nil, nil); cmp != nil {
		t.Errorf("resolveSortCmp(nil, nil) = non-nil; want nil")
	}
}
