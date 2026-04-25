package s3sql

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"
)

// TestScanAll_EndToEnd opens an in-memory DuckDB, runs a query
// that returns three rows with varied types (string, int, float,
// NULL, time), and verifies ScanAll produces the right []T —
// including NULL → Go zero, columns absent from T are silently
// dropped, and T-fields absent from the result set stay zero.
func TestScanAll_EndToEnd(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	rows, err := db.Query(`
		SELECT 'a' AS name, 1 AS qty, 2.5 AS price, 'extra' AS ignored
		UNION ALL
		SELECT 'b', 2, NULL, 'extra2'
		UNION ALL
		SELECT NULL, 3, 7.5, 'extra3'
		ORDER BY qty
	`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	type Out struct {
		Name    string  `parquet:"name"`
		Qty     int64   `parquet:"qty"`
		Price   float64 `parquet:"price"`
		Missing string  `parquet:"missing"` // never in result set
		// `ignored` column has no field — must be discarded.
	}
	got, err := ScanAll[Out](rows)
	if err != nil {
		t.Fatalf("ScanAll: %v", err)
	}

	want := []Out{
		{Name: "a", Qty: 1, Price: 2.5},
		{Name: "b", Qty: 2, Price: 0},  // NULL Price → zero
		{Name: "", Qty: 3, Price: 7.5}, // NULL Name → ""
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v\nwant %+v", got, want)
	}
}

// TestScanAll_EmptyResult guards the no-rows path: ScanAll
// returns (nil, nil) on a query that yields zero rows.
func TestScanAll_EmptyResult(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT 'x' AS name WHERE 1=0")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	type Out struct {
		Name string `parquet:"name"`
	}
	got, err := ScanAll[Out](rows)
	if err != nil {
		t.Fatalf("ScanAll: %v", err)
	}
	if got != nil {
		t.Errorf("got %v, want nil", got)
	}
}

// TestScanAll_RejectsNonStruct guards that the type-shape error
// from buildScanBinder surfaces through ScanAll's wrapper.
func TestScanAll_RejectsNonStruct(t *testing.T) {
	db, _ := sql.Open("duckdb", "")
	defer db.Close()
	rows, _ := db.Query("SELECT 1")
	_, err := ScanAll[int](rows)
	if err == nil || !strings.Contains(err.Error(), "must be a struct") {
		t.Errorf("got %v, want must-be-struct error", err)
	}
}

// mustBuildScanBinder fails fast on a bad build so the rest of
// the test can dereference b.byName[...] without nil-deref
// panics masking the real cause.
func mustBuildScanBinder(t *testing.T, typ reflect.Type) *scanBinder {
	t.Helper()
	b, err := buildScanBinder(typ)
	if err != nil {
		t.Fatalf("buildScanBinder: %v", err)
	}
	return b
}

// TestBuildScanBinder_BasicTypes verifies a vanilla struct with
// primitive + string + time fields binds cleanly.
func TestBuildScanBinder_BasicTypes(t *testing.T) {
	type R struct {
		Period string    `parquet:"period"`
		Amount float64   `parquet:"amount"`
		Ts     time.Time `parquet:"ts"`
	}
	b, err := buildScanBinder(reflect.TypeOf(R{}))
	if err != nil {
		t.Fatalf("buildScanBinder: %v", err)
	}
	for _, col := range []string{"period", "amount", "ts"} {
		if _, ok := b.byName[col]; !ok {
			t.Errorf("byName missing %q", col)
		}
	}
}

// TestBuildScanBinder_SkipsUntagged_Unexported_Dash checks the
// three exclusion rules: no parquet tag, explicit `parquet:"-"`,
// and unexported fields (even when tagged). Only tagged exported
// fields enter the binder.
func TestBuildScanBinder_SkipsUntagged_Unexported_Dash(t *testing.T) {
	type R struct {
		Keep   string `parquet:"keep"`
		Skip   string // no tag
		Dashed string `parquet:"-"`
		hidden string `parquet:"hidden"` // unexported
	}
	b := mustBuildScanBinder(t, reflect.TypeOf(R{}))
	if len(b.byName) != 1 {
		t.Errorf("byName: got %d entries, want 1: %v", len(b.byName), b.byName)
	}
	if _, ok := b.byName["keep"]; !ok {
		t.Errorf("byName missing 'keep'")
	}
	_ = R{hidden: ""}.hidden
}

// TestBuildScanBinder_RejectsNonStruct enforces that T must be a
// struct (after dereferencing pointers).
func TestBuildScanBinder_RejectsNonStruct(t *testing.T) {
	_, err := buildScanBinder(reflect.TypeOf(42))
	if err == nil || !strings.Contains(err.Error(), "must be a struct") {
		t.Errorf("expected must-be-struct error, got %v", err)
	}
}

// TestBuildScanBinder_AllowsEmpty: a struct with zero parquet
// tags is a valid (no-op) binder. ScanAll then discards every
// column from the result set — useful when the caller wants to
// drain rows for their side effect (e.g. an INSERT RETURNING)
// without materializing them.
func TestBuildScanBinder_AllowsEmpty(t *testing.T) {
	type R struct {
		A int
		B string
	}
	b, err := buildScanBinder(reflect.TypeOf(R{}))
	if err != nil {
		t.Fatalf("buildScanBinder: %v", err)
	}
	if len(b.byName) != 0 {
		t.Errorf("byName: got %d entries, want 0", len(b.byName))
	}
}

// TestBuildScanBinder_RejectsUnsupportedType guards the error
// path for a field whose type has no NULL-safe Scan destination
// and doesn't implement sql.Scanner.
func TestBuildScanBinder_RejectsUnsupportedType(t *testing.T) {
	type R struct {
		// complex128 isn't in the Kind switch and doesn't
		// implement sql.Scanner. It's also not slice/map/struct
		// so the composite path doesn't catch it either.
		X complex128 `parquet:"x"`
	}
	_, err := buildScanBinder(reflect.TypeOf(R{}))
	if err == nil || !strings.Contains(err.Error(), "unsupported type") {
		t.Errorf("expected unsupported-type error, got %v", err)
	}
}

// TestBuildScanBinder_CompositeTypes guards that slice, map, and
// nested-struct fields build a binder without error. Runtime
// behaviour is covered by TestCompositeScanner_*.
func TestBuildScanBinder_CompositeTypes(t *testing.T) {
	type Addr struct {
		City string `parquet:"city"`
	}
	type R struct {
		Tags    []string       `parquet:"tags"`
		Attrs   map[string]int `parquet:"attrs"`
		Address Addr           `parquet:"address"`
	}
	b, err := buildScanBinder(reflect.TypeOf(R{}))
	if err != nil {
		t.Fatalf("buildScanBinder: %v", err)
	}
	for _, col := range []string{"tags", "attrs", "address"} {
		if b.byName[col] == nil {
			t.Errorf("byName missing %q", col)
		}
	}
}

// TestCompositeScanner_SliceOfPrimitives exercises the LIST
// path: driver returns []any, mapstructure decodes into []string.
func TestCompositeScanner_SliceOfPrimitives(t *testing.T) {
	type R struct {
		Tags []string `parquet:"tags"`
	}
	b := mustBuildScanBinder(t, reflect.TypeOf(R{}))
	fb := b.byName["tags"]

	dest := fb.makeDest()
	cs := dest.(*compositeScanner)
	if err := cs.Scan([]any{"alpha", "beta", "gamma"}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	var r R
	rv := reflect.ValueOf(&r).Elem()
	fb.assign(rv.FieldByIndex(fb.fieldIndex), dest)
	want := []string{"alpha", "beta", "gamma"}
	if !reflect.DeepEqual(r.Tags, want) {
		t.Errorf("Tags: got %v, want %v", r.Tags, want)
	}
}

// TestCompositeScanner_MapFromOrderedMap guards the MAP path
// with the OrderedMap decode hook. The driver returns MAPs as
// duckdb.OrderedMap; the hook turns it into map[any]any, and
// mapstructure then fits it into map[string]int.
func TestCompositeScanner_MapFromOrderedMap(t *testing.T) {
	type R struct {
		Attrs map[string]int `parquet:"attrs"`
	}
	b := mustBuildScanBinder(t, reflect.TypeOf(R{}))
	fb := b.byName["attrs"]

	var om duckdb.OrderedMap
	om.Set("a", int64(1))
	om.Set("b", int64(2))

	dest := fb.makeDest()
	cs := dest.(*compositeScanner)
	if err := cs.Scan(om); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	var r R
	rv := reflect.ValueOf(&r).Elem()
	fb.assign(rv.FieldByIndex(fb.fieldIndex), dest)
	if len(r.Attrs) != 2 || r.Attrs["a"] != 1 || r.Attrs["b"] != 2 {
		t.Errorf("Attrs: got %v, want map[a:1 b:2]", r.Attrs)
	}
}

// TestCompositeScanner_NestedStruct guards the STRUCT path: the
// driver returns map[string]any; mapstructure decodes into a
// user struct using the parquet tag on each nested field.
func TestCompositeScanner_NestedStruct(t *testing.T) {
	type Addr struct {
		Street string `parquet:"street"`
		City   string `parquet:"city"`
	}
	type R struct {
		Address Addr `parquet:"address"`
	}
	b := mustBuildScanBinder(t, reflect.TypeOf(R{}))
	fb := b.byName["address"]

	dest := fb.makeDest()
	cs := dest.(*compositeScanner)
	if err := cs.Scan(map[string]any{
		"street": "1 Main",
		"city":   "Berlin",
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	var r R
	rv := reflect.ValueOf(&r).Elem()
	fb.assign(rv.FieldByIndex(fb.fieldIndex), dest)
	if r.Address.Street != "1 Main" || r.Address.City != "Berlin" {
		t.Errorf("Address: got %+v, want {1 Main, Berlin}", r.Address)
	}
}

// TestCompositeScanner_NamedInt8InStruct guards the realistic
// "named int8 enum inside a nested struct inside a list" shape
// (e.g. ProcessLog.Field where Field is go-enum-generated). The
// read side relies on mapstructure decoding int64 values into
// the named int8 field.
func TestCompositeScanner_NamedInt8InStruct(t *testing.T) {
	type Field int8
	const (
		FieldUnknown Field = iota
		FieldPrimary
		FieldSecondary
	)
	type Inner struct {
		Name  string `parquet:"name"`
		Field Field  `parquet:"field"`
	}
	type R struct {
		Items []Inner `parquet:"items"`
	}
	b := mustBuildScanBinder(t, reflect.TypeOf(R{}))
	fb := b.byName["items"]

	dest := fb.makeDest()
	if err := dest.(interface{ Scan(any) error }).Scan([]any{
		map[string]any{"name": "a", "field": int64(FieldPrimary)},
		map[string]any{"name": "b", "field": int64(FieldSecondary)},
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	var r R
	rv := reflect.ValueOf(&r).Elem()
	fb.assign(rv.FieldByIndex(fb.fieldIndex), dest)
	want := []Inner{
		{Name: "a", Field: FieldPrimary},
		{Name: "b", Field: FieldSecondary},
	}
	if !reflect.DeepEqual(r.Items, want) {
		t.Errorf("got %+v, want %+v", r.Items, want)
	}
	_ = FieldUnknown
}

// TestCompositeScanner_NULLLeavesZero guards that a NULL
// composite leaves the field at Go zero — matching the rest of
// the binder's contract.
func TestCompositeScanner_NULLLeavesZero(t *testing.T) {
	type R struct {
		Tags []string `parquet:"tags"`
	}
	b := mustBuildScanBinder(t, reflect.TypeOf(R{}))
	fb := b.byName["tags"]

	dest := fb.makeDest()
	cs := dest.(*compositeScanner)
	if err := cs.Scan(nil); err != nil {
		t.Fatalf("Scan(nil): %v", err)
	}
	var r R
	rv := reflect.ValueOf(&r).Elem()
	fb.assign(rv.FieldByIndex(fb.fieldIndex), dest)
	if r.Tags != nil {
		t.Errorf("Tags: got %v, want nil", r.Tags)
	}
}

// TestCompositeScanner_TypeMismatchErrors guards that an
// undecodable value (e.g. scanning []any{"notanint"} into
// []int64) surfaces as a Scan error rather than silent zero.
func TestCompositeScanner_TypeMismatchErrors(t *testing.T) {
	type R struct {
		Counts []int64 `parquet:"counts"`
	}
	b := mustBuildScanBinder(t, reflect.TypeOf(R{}))
	fb := b.byName["counts"]

	dest := fb.makeDest()
	cs := dest.(*compositeScanner)
	if err := cs.Scan([]any{"notanumber"}); err == nil {
		t.Error("expected decode error, got nil")
	}
}

// TestBuildScanBinder_TagStripsOptions guards that tag
// sub-options (e.g. "timestamp(microsecond)") are stripped —
// only the first comma-separated part is the column name.
func TestBuildScanBinder_TagStripsOptions(t *testing.T) {
	type R struct {
		Ts time.Time `parquet:"ts,timestamp(millisecond)"`
	}
	b, err := buildScanBinder(reflect.TypeOf(R{}))
	if err != nil {
		t.Fatalf("buildScanBinder: %v", err)
	}
	if _, ok := b.byName["ts"]; !ok {
		t.Error("byName missing bare 'ts' after tag options stripped")
	}
}

// testScanner is a sql.Scanner used to prove the generic
// custom-type path works end-to-end.
type testScanner struct {
	Text string
}

func (s *testScanner) Scan(src any) error {
	switch v := src.(type) {
	case string:
		s.Text = v
	case []byte:
		s.Text = string(v)
	case nil:
		return errors.New("testScanner: NULL not expected at this layer")
	default:
		return fmt.Errorf("testScanner: unexpected type %T", v)
	}
	return nil
}

// TestBuildScanBinder_CustomSQLScannerType guards that a field
// whose *T implements sql.Scanner gets the generic scanner path,
// and that the returned wrapper delegates Scan to the inner type.
func TestBuildScanBinder_CustomSQLScannerType(t *testing.T) {
	type R struct {
		Custom testScanner `parquet:"custom"`
	}
	b, err := buildScanBinder(reflect.TypeOf(R{}))
	if err != nil {
		t.Fatalf("buildScanBinder: %v", err)
	}
	fb := b.byName["custom"]
	if fb == nil {
		t.Fatal("binder missing 'custom' field")
	}

	dest := fb.makeDest()
	ns, ok := dest.(*nullScanner)
	if !ok {
		t.Fatalf("makeDest returned %T, want *nullScanner", dest)
	}
	if err := ns.Scan("hello"); err != nil {
		t.Fatalf("nullScanner.Scan: %v", err)
	}
	if !ns.valid {
		t.Error("ns.valid should be true after non-NULL Scan")
	}

	var r R
	rv := reflect.ValueOf(&r).Elem()
	fb.assign(rv.FieldByIndex(fb.fieldIndex), dest)
	if r.Custom.Text != "hello" {
		t.Errorf("r.Custom.Text = %q, want %q", r.Custom.Text, "hello")
	}
}

// TestNullScanner_NULL guards that src == nil sets valid=false
// without touching the inner scanner — critical for the
// zero-value contract on user types.
func TestNullScanner_NULL(t *testing.T) {
	ts := &testScanner{Text: "preset"}
	ns := &nullScanner{inner: ts}
	if err := ns.Scan(nil); err != nil {
		t.Fatalf("Scan(nil): %v", err)
	}
	if ns.valid {
		t.Error("ns.valid should be false after NULL")
	}
	if ts.Text != "preset" {
		t.Errorf("inner scanner modified on NULL: Value=%q", ts.Text)
	}
}

// TestFieldScanner_NullableTypes runs every supported Go
// primitive type through a NULL scan and a valued scan, and
// verifies the assign function writes (resp. skips) correctly.
func TestFieldScanner_NullableTypes(t *testing.T) {
	type R struct {
		S   string  `parquet:"s"`
		I   int     `parquet:"i"`
		I8  int8    `parquet:"i8"`
		I16 int16   `parquet:"i16"`
		I32 int32   `parquet:"i32"`
		I64 int64   `parquet:"i64"`
		U   uint    `parquet:"u"`
		U8  uint8   `parquet:"u8"`
		U16 uint16  `parquet:"u16"`
		U32 uint32  `parquet:"u32"`
		U64 uint64  `parquet:"u64"`
		F32 float32 `parquet:"f32"`
		F64 float64 `parquet:"f64"`
		B   bool    `parquet:"b"`
	}
	b := mustBuildScanBinder(t, reflect.TypeOf(R{}))

	cases := []struct {
		col  string
		val  any
		want func(r R) bool
	}{
		{"s", "hello", func(r R) bool { return r.S == "hello" }},
		{"i", int64(42), func(r R) bool { return r.I == 42 }},
		{"i8", int64(7), func(r R) bool { return r.I8 == 7 }},
		{"i16", int64(300), func(r R) bool { return r.I16 == 300 }},
		{"i32", int64(70000), func(r R) bool { return r.I32 == 70000 }},
		{"i64", int64(1 << 40), func(r R) bool { return r.I64 == 1<<40 }},
		{"u", int64(42), func(r R) bool { return r.U == 42 }},
		{"u8", int64(200), func(r R) bool { return r.U8 == 200 }},
		{"u16", int64(40000), func(r R) bool { return r.U16 == 40000 }},
		{"u32", int64(99), func(r R) bool { return r.U32 == 99 }},
		{"u64", int64(1 << 40), func(r R) bool { return r.U64 == 1<<40 }},
		{"f32", 2.5, func(r R) bool { return r.F32 == 2.5 }},
		{"f64", 3.14, func(r R) bool { return r.F64 == 3.14 }},
		{"b", true, func(r R) bool { return r.B }},
	}
	for _, tc := range cases {
		t.Run("valued_"+tc.col, func(t *testing.T) {
			fb := b.byName[tc.col]
			dest := fb.makeDest()
			if err := dest.(interface{ Scan(any) error }).Scan(tc.val); err != nil {
				t.Fatalf("Scan: %v", err)
			}
			var r R
			rv := reflect.ValueOf(&r).Elem()
			fb.assign(rv.FieldByIndex(fb.fieldIndex), dest)
			if !tc.want(r) {
				t.Errorf("%s not set correctly: %+v", tc.col, r)
			}
		})

		t.Run("null_"+tc.col, func(t *testing.T) {
			fb := b.byName[tc.col]
			dest := fb.makeDest()
			if err := dest.(interface{ Scan(any) error }).Scan(nil); err != nil {
				t.Fatalf("Scan(nil): %v", err)
			}
			var r R
			rv := reflect.ValueOf(&r).Elem()
			fb.assign(rv.FieldByIndex(fb.fieldIndex), dest)
			var zero R
			if !reflect.DeepEqual(r, zero) {
				t.Errorf("NULL left non-zero state: %+v", r)
			}
		})
	}
}

// TestFieldScanner_Time covers the time.Time special case —
// it's a struct and would otherwise fall through to the
// composite path, but makeFieldScanner handles it with a
// sql.NullTime wrapper.
func TestFieldScanner_Time(t *testing.T) {
	type R struct {
		Ts time.Time `parquet:"ts"`
	}
	b := mustBuildScanBinder(t, reflect.TypeOf(R{}))
	fb := b.byName["ts"]

	want := time.Date(2026, 3, 17, 10, 0, 0, 0, time.UTC)
	dest := fb.makeDest()
	if err := dest.(interface{ Scan(any) error }).Scan(want); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	var r R
	rv := reflect.ValueOf(&r).Elem()
	fb.assign(rv.FieldByIndex(fb.fieldIndex), dest)
	if !r.Ts.Equal(want) {
		t.Errorf("valued: got %v, want %v", r.Ts, want)
	}

	dest = fb.makeDest()
	if err := dest.(interface{ Scan(any) error }).Scan(nil); err != nil {
		t.Fatalf("Scan(nil): %v", err)
	}
	r = R{}
	fb.assign(rv.FieldByIndex(fb.fieldIndex), dest)
	if !r.Ts.IsZero() {
		t.Errorf("NULL: got %v, want zero time", r.Ts)
	}
}

// TestBuildScanBinder_PointerInput guards the
// pointer-dereferencing loop at the top of buildScanBinder:
// callers may pass reflect.TypeOf((*T)(nil)) and should get the
// same binder as reflect.TypeOf(T{}).
func TestBuildScanBinder_PointerInput(t *testing.T) {
	type R struct {
		Keep string `parquet:"keep"`
	}
	b := mustBuildScanBinder(t, reflect.TypeFor[*R]())
	if _, ok := b.byName["keep"]; !ok {
		t.Error("byName missing 'keep' after pointer-input unwrap")
	}
	b = mustBuildScanBinder(t, reflect.TypeFor[**R]())
	if _, ok := b.byName["keep"]; !ok {
		t.Error("byName missing 'keep' after double-pointer unwrap")
	}
}

// TestCompositeScanner_SliceOfStructs exercises the LIST<STRUCT>
// shape: driver returns []any where each element is
// map[string]any, and mapstructure recursively decodes into
// []Inner.
func TestCompositeScanner_SliceOfStructs(t *testing.T) {
	type Inner struct {
		Name  string `parquet:"name"`
		Count int32  `parquet:"count"`
	}
	type R struct {
		Items []Inner `parquet:"items"`
	}
	b := mustBuildScanBinder(t, reflect.TypeOf(R{}))
	fb := b.byName["items"]

	dest := fb.makeDest()
	if err := dest.(interface{ Scan(any) error }).Scan([]any{
		map[string]any{"name": "alpha", "count": int64(1)},
		map[string]any{"name": "beta", "count": int64(2)},
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	var r R
	rv := reflect.ValueOf(&r).Elem()
	fb.assign(rv.FieldByIndex(fb.fieldIndex), dest)
	want := []Inner{{Name: "alpha", Count: 1}, {Name: "beta", Count: 2}}
	if !reflect.DeepEqual(r.Items, want) {
		t.Errorf("Items: got %+v, want %+v", r.Items, want)
	}
}

// TestFieldScanner_NamedPrimitive guards that a named primitive
// type still routes through the Kind-switch path.
func TestFieldScanner_NamedPrimitive(t *testing.T) {
	type Status int16
	type R struct {
		S Status `parquet:"s"`
	}
	b := mustBuildScanBinder(t, reflect.TypeOf(R{}))
	fb := b.byName["s"]

	dest := fb.makeDest()
	if err := dest.(interface{ Scan(any) error }).Scan(int64(7)); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	var r R
	rv := reflect.ValueOf(&r).Elem()
	fb.assign(rv.FieldByIndex(fb.fieldIndex), dest)
	if r.S != Status(7) {
		t.Errorf("got %d, want %d", r.S, 7)
	}
}

// TestFieldScanner_Int8Enum mirrors the shape go-enum produces
// for `//go:generate go tool go-enum --marshal` on a
// `type Field int8` — a named int8 with named constants.
func TestFieldScanner_Int8Enum(t *testing.T) {
	type Field int8
	const (
		FieldPayingCustomer Field = iota
		FieldListPrice
		FieldBasePrice
		FieldNetPrice
	)
	type R struct {
		F Field `parquet:"f"`
	}
	b := mustBuildScanBinder(t, reflect.TypeOf(R{}))
	fb := b.byName["f"]

	dest := fb.makeDest()
	if err := dest.(interface{ Scan(any) error }).Scan(
		int64(FieldBasePrice),
	); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	var r R
	rv := reflect.ValueOf(&r).Elem()
	fb.assign(rv.FieldByIndex(fb.fieldIndex), dest)
	if r.F != FieldBasePrice {
		t.Errorf("got %d, want %d (FieldBasePrice)", r.F, FieldBasePrice)
	}
	_ = FieldPayingCustomer
	_ = FieldListPrice
	_ = FieldNetPrice
}

// TestFieldScanner_ByteSlice covers the raw *[]byte path that
// makeFieldScanner uses for []byte fields — no NullXxx wrapper,
// relies on nil-slice-is-NULL semantics of database/sql.
func TestFieldScanner_ByteSlice(t *testing.T) {
	type R struct {
		By []byte `parquet:"by"`
	}
	b := mustBuildScanBinder(t, reflect.TypeOf(R{}))
	fb := b.byName["by"]

	dest := fb.makeDest()
	*(dest.(*[]byte)) = []byte{1, 2, 3}
	var r R
	rv := reflect.ValueOf(&r).Elem()
	fb.assign(rv.FieldByIndex(fb.fieldIndex), dest)
	if len(r.By) != 3 || r.By[0] != 1 {
		t.Errorf("valued: got %v, want [1 2 3]", r.By)
	}

	dest = fb.makeDest()
	r = R{}
	fb.assign(rv.FieldByIndex(fb.fieldIndex), dest)
	if r.By != nil {
		t.Errorf("NULL: got %v, want nil", r.By)
	}
}
