package s3parquet

import (
	"bytes"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

// rgRec is the record shape used across these tests — string and
// int64 fields so we can exercise StringBounds, Int64Bounds, and
// the type-guard short-circuits from each accessor.
type rgRec struct {
	Customer string `parquet:"customer"`
	Amount   int64  `parquet:"amount"`
}

// buildFile writes recs into an in-memory parquet file with one
// row group per distinct customer (mirrors what
// WriteWithKeyRowGroupsBy produces), then re-opens it so the page
// index is available for tests. No S3, no sorting: callers pass
// recs already grouped by customer.
func buildFile(t *testing.T, recs []rgRec) *parquet.File {
	t.Helper()
	data, err := encodeParquetWithFlush(
		recs, &parquet.Snappy,
		func(r rgRec) string { return r.Customer })
	if err != nil {
		t.Fatalf("encodeParquetWithFlush: %v", err)
	}
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("parquet.OpenFile: %v", err)
	}
	return f
}

// wrapGroup bundles a parquet row group behind the public
// RowGroup wrapper so tests can exercise predicates the same way
// callers do.
func wrapGroup(f *parquet.File, i int) RowGroup {
	return RowGroup{inner: f.RowGroups()[i], schema: f.Schema()}
}

// TestStringBounds_TightPerGroup guards the happy path that the
// rest of the ReadIterWhere pruning story depends on: with one
// customer per row group, StringBounds returns min == max == the
// customer's ID for each group.
func TestStringBounds_TightPerGroup(t *testing.T) {
	f := buildFile(t, []rgRec{
		{Customer: "abc", Amount: 1},
		{Customer: "def", Amount: 2},
		{Customer: "def", Amount: 3},
		{Customer: "ghi", Amount: 4},
	})
	wantGroups := []string{"abc", "def", "ghi"}
	if got := len(f.RowGroups()); got != len(wantGroups) {
		t.Fatalf("row groups: got %d, want %d", got, len(wantGroups))
	}
	for i, want := range wantGroups {
		stats, ok := wrapGroup(f, i).Column("customer")
		if !ok {
			t.Fatalf("row group %d: Column(customer) not found", i)
		}
		lo, hi, ok := stats.StringBounds()
		if !ok {
			t.Fatalf("row group %d: StringBounds ok=false", i)
		}
		if lo != want || hi != want {
			t.Errorf("row group %d: got [%q, %q], want [%q, %q]",
				i, lo, hi, want, want)
		}
	}
}

// TestInt64Bounds_TightPerGroup mirrors the above for the
// numeric typed accessor, guarding that Int64Bounds works when
// the column is actually INT64.
func TestInt64Bounds_TightPerGroup(t *testing.T) {
	f := buildFile(t, []rgRec{
		{Customer: "abc", Amount: 10},
		{Customer: "def", Amount: 20},
	})
	// Row group 0 = abc (Amount 10), row group 1 = def (Amount 20).
	cases := []struct {
		group    int
		min, max int64
	}{{0, 10, 10}, {1, 20, 20}}
	for _, tc := range cases {
		stats, ok := wrapGroup(f, tc.group).Column("amount")
		if !ok {
			t.Fatalf("group %d: Column(amount) not found", tc.group)
		}
		lo, hi, ok := stats.Int64Bounds()
		if !ok {
			t.Fatalf("group %d: Int64Bounds ok=false", tc.group)
		}
		if lo != tc.min || hi != tc.max {
			t.Errorf("group %d: got [%d, %d], want [%d, %d]",
				tc.group, lo, hi, tc.min, tc.max)
		}
	}
}

// TestBounds_TypeGuard is the regression test for the silent-
// wrong-answer bug: calling Int64Bounds on a string column must
// return ok=false, not (0, 0, true) from Value.Int64's zero-value
// fallback. Same symmetric guard for StringBounds on an int64
// column.
func TestBounds_TypeGuard(t *testing.T) {
	f := buildFile(t, []rgRec{
		{Customer: "abc", Amount: 42},
	})
	customerStats, _ := wrapGroup(f, 0).Column("customer")
	amountStats, _ := wrapGroup(f, 0).Column("amount")

	// Int64Bounds on a ByteArray (customer) column — must be ok=false.
	if _, _, ok := customerStats.Int64Bounds(); ok {
		t.Error("Int64Bounds on string column: ok=true, want false")
	}
	// StringBounds on an Int64 (amount) column — same.
	if _, _, ok := amountStats.StringBounds(); ok {
		t.Error("StringBounds on int64 column: ok=true, want false")
	}
}

// TestBounds_MissingColumn guards the other ok=false path: a
// column the caller asks for but the schema doesn't carry.
func TestBounds_MissingColumn(t *testing.T) {
	f := buildFile(t, []rgRec{{Customer: "abc", Amount: 1}})
	if _, ok := wrapGroup(f, 0).Column("nonexistent"); ok {
		t.Error("Column(nonexistent) ok=true, want false")
	}
}

// TestEqualsString_PrunesAndAccepts exercises the predicate's
// three paths: matching value on a tight group (accept), non-
// matching value on a tight group (reject), and missing column
// (safe fallback — accept).
func TestEqualsString_PrunesAndAccepts(t *testing.T) {
	f := buildFile(t, []rgRec{
		{Customer: "abc", Amount: 1},
		{Customer: "def", Amount: 2},
		{Customer: "ghi", Amount: 3},
	})
	pred := EqualsString("customer", "def")
	wantAccept := []bool{false, true, false}
	for i, want := range wantAccept {
		if got := pred(wrapGroup(f, i)); got != want {
			t.Errorf("group %d (%q): got accept=%v, want %v",
				i, []string{"abc", "def", "ghi"}[i], got, want)
		}
	}
	// Missing column → safe fallback accepts.
	if got := EqualsString("nonexistent", "x")(wrapGroup(f, 0)); !got {
		t.Error("missing-column fallback: got reject, want accept")
	}
}

// TestInStrings_EmptyRejects guards the documented corner case:
// InStrings with no values rejects every row group.
func TestInStrings_EmptyRejects(t *testing.T) {
	f := buildFile(t, []rgRec{{Customer: "abc", Amount: 1}})
	pred := InStrings("customer", nil)
	if pred(wrapGroup(f, 0)) {
		t.Error("InStrings(nil): got accept, want reject")
	}
}

// TestInStrings_SomeMatch covers the multi-value match path and
// the "none match" rejection.
func TestInStrings_SomeMatch(t *testing.T) {
	f := buildFile(t, []rgRec{
		{Customer: "abc", Amount: 1},
		{Customer: "def", Amount: 2},
		{Customer: "ghi", Amount: 3},
	})
	pred := InStrings("customer", []string{"abc", "ghi"})
	wantAccept := []bool{true, false, true}
	for i, want := range wantAccept {
		if got := pred(wrapGroup(f, i)); got != want {
			t.Errorf("group %d: got accept=%v, want %v",
				i, got, want)
		}
	}
}

// TestAndOr covers the short-circuit composition helpers: And
// rejects on first false, Or accepts on first true, zero-args
// match their identity-element defaults.
func TestAndOr(t *testing.T) {
	accept := RowGroupPredicate(func(RowGroup) bool { return true })
	reject := RowGroupPredicate(func(RowGroup) bool { return false })
	f := buildFile(t, []rgRec{{Customer: "abc", Amount: 1}})
	rg := wrapGroup(f, 0)

	if !And()(rg) {
		t.Error("And() with no args: want accept (identity element)")
	}
	if Or()(rg) {
		t.Error("Or() with no args: want reject (identity element)")
	}
	if And(accept, reject, accept)(rg) {
		t.Error("And(... reject ...): want reject")
	}
	if !Or(reject, accept, reject)(rg) {
		t.Error("Or(... accept ...): want accept")
	}
}

// TestLess_PanicsOnUnknownType guards the fail-loud behavior:
// future *Bounds accessors that forget to update less() must
// surface at the first comparison, not silently fall through.
func TestLess_PanicsOnUnknownType(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("less(float32, float32): expected panic, got none")
		}
	}()
	// less uses a concrete-type switch; float32 isn't covered, so
	// this call must panic. If a future CR adds float64 support,
	// swap in a still-unsupported type here.
	less(float32(1), float32(2))
}

// TestTimestampRange_HalfOpen verifies the predicate is
// inclusive on "from" and exclusive on "to", so [from, to).
// Edge cases: from equal to group min (accept), to equal to
// group min (reject — group lies at or above to), to just after
// group min (accept).
func TestTimestampRange_HalfOpen(t *testing.T) {
	type tsRec struct {
		Ts int64 `parquet:"ts"`
	}
	data, err := encodeParquetWithFlush(
		[]tsRec{{Ts: 1000}, {Ts: 2000}, {Ts: 3000}},
		&parquet.Snappy,
		func(r tsRec) string { return "" }, // single row group
	)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	rg := RowGroup{inner: f.RowGroups()[0], schema: f.Schema()}

	// Only row group has Ts bounds [1000, 3000] microseconds.
	cases := []struct {
		name       string
		from, to   time.Time
		wantAccept bool
	}{
		{"window fully before", micros(0), micros(500), false},
		{"window fully after", micros(4000), micros(5000), false},
		{"window touches min (to exclusive)", micros(0), micros(1000), false},
		{"window starts at min (from inclusive)", micros(1000), micros(1500), true},
		{"window overlaps middle", micros(1500), micros(2500), true},
		{"window ends at max inclusive", micros(2500), micros(3001), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pred := TimestampRange("ts", tc.from, tc.to)
			if got := pred(rg); got != tc.wantAccept {
				t.Errorf("got accept=%v, want %v", got, tc.wantAccept)
			}
		})
	}
}

func micros(us int64) time.Time {
	return time.UnixMicro(us).UTC()
}
