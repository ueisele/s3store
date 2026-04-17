package s3sql

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"
)

// TestBuildBinder_BasicTypes verifies a vanilla struct with a
// primitive + string + time field binds cleanly and exposes the
// tag-declaration order via b.columns.
func TestBuildBinder_BasicTypes(t *testing.T) {
	type R struct {
		Period string    `parquet:"period"`
		Amount float64   `parquet:"amount"`
		Ts     time.Time `parquet:"ts"`
	}
	b, err := buildBinder(reflect.TypeOf(R{}))
	if err != nil {
		t.Fatalf("buildBinder: %v", err)
	}
	want := []string{"period", "amount", "ts"}
	if !reflect.DeepEqual(b.columns, want) {
		t.Errorf("columns: got %v, want %v", b.columns, want)
	}
	for _, col := range want {
		if _, ok := b.byName[col]; !ok {
			t.Errorf("byName missing %q", col)
		}
	}
}

// TestBuildBinder_SkipsUntagged_Unexported_Dash checks the three
// exclusion rules: no parquet tag, unexported field, and explicit
// `parquet:"-"`. Only tagged exported fields enter the binder.
func TestBuildBinder_SkipsUntagged_Unexported_Dash(t *testing.T) {
	type R struct {
		Keep   string `parquet:"keep"`
		Skip   string // no tag
		Dashed string `parquet:"-"`
		hidden string `parquet:"hidden"` //nolint:unused
	}
	b, err := buildBinder(reflect.TypeOf(R{}))
	if err != nil {
		t.Fatalf("buildBinder: %v", err)
	}
	if len(b.columns) != 1 || b.columns[0] != "keep" {
		t.Errorf("columns: got %v, want [keep]", b.columns)
	}
	_ = R{hidden: ""} // silence unused warning
}

// TestBuildBinder_RejectsNonStruct enforces that T must be a
// struct (after dereferencing pointers).
func TestBuildBinder_RejectsNonStruct(t *testing.T) {
	_, err := buildBinder(reflect.TypeOf(42))
	if err == nil || !strings.Contains(err.Error(), "must be a struct") {
		t.Errorf("expected must-be-struct error, got %v", err)
	}
}

// TestBuildBinder_RejectsEmpty enforces that at least one field
// must carry a parquet tag — otherwise the library can't build
// a SELECT list or map results back to T.
func TestBuildBinder_RejectsEmpty(t *testing.T) {
	type R struct {
		A int
		B string
	}
	_, err := buildBinder(reflect.TypeOf(R{}))
	if err == nil ||
		!strings.Contains(err.Error(), "no parquet-tagged") {
		t.Errorf("expected no-tags error, got %v", err)
	}
}

// TestBuildBinder_RejectsUnsupportedType guards the error path
// for a field whose type has no NULL-safe Scan destination and
// doesn't implement sql.Scanner.
func TestBuildBinder_RejectsUnsupportedType(t *testing.T) {
	type R struct {
		// complex128 isn't in the Kind switch and doesn't
		// implement sql.Scanner on *R. It's also not a
		// slice/map/struct so it can't go through the
		// composite path.
		X complex128 `parquet:"x"`
	}
	_, err := buildBinder(reflect.TypeOf(R{}))
	if err == nil || !strings.Contains(err.Error(), "unsupported type") {
		t.Errorf("expected unsupported-type error, got %v", err)
	}
}

// TestBuildBinder_CompositeTypes guards that slice, map, and
// nested-struct fields build a binder without error. Runtime
// behavior is covered by TestCompositeScanner_*.
func TestBuildBinder_CompositeTypes(t *testing.T) {
	type Addr struct {
		City string `parquet:"city"`
	}
	type R struct {
		Tags    []string       `parquet:"tags"`
		Attrs   map[string]int `parquet:"attrs"`
		Address Addr           `parquet:"address"`
	}
	b, err := buildBinder(reflect.TypeOf(R{}))
	if err != nil {
		t.Fatalf("buildBinder: %v", err)
	}
	for _, col := range []string{"tags", "attrs", "address"} {
		if b.byName[col] == nil {
			t.Errorf("byName missing %q", col)
		}
	}
}

// TestCompositeScanner_SliceOfPrimitives exercises the LIST
// path end-to-end: driver returns []any, mapstructure decodes
// into []string.
func TestCompositeScanner_SliceOfPrimitives(t *testing.T) {
	type R struct {
		Tags []string `parquet:"tags"`
	}
	b, _ := buildBinder(reflect.TypeOf(R{}))
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
	b, _ := buildBinder(reflect.TypeOf(R{}))
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
	b, _ := buildBinder(reflect.TypeOf(R{}))
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

// TestCompositeScanner_NULLLeavesZero guards that a NULL
// composite leaves the field at Go zero — matching the rest of
// the binder's contract.
func TestCompositeScanner_NULLLeavesZero(t *testing.T) {
	type R struct {
		Tags []string `parquet:"tags"`
	}
	b, _ := buildBinder(reflect.TypeOf(R{}))
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
	b, _ := buildBinder(reflect.TypeOf(R{}))
	fb := b.byName["counts"]

	dest := fb.makeDest()
	cs := dest.(*compositeScanner)
	if err := cs.Scan([]any{"notanumber"}); err == nil {
		t.Error("expected decode error, got nil")
	}
}

// TestBuildBinder_TagStripsOptions guards that the "timestamp"
// sub-option in a parquet tag is stripped — only the first
// comma-separated part is the column name.
func TestBuildBinder_TagStripsOptions(t *testing.T) {
	type R struct {
		Ts time.Time `parquet:"ts,timestamp(millisecond)"`
	}
	b, err := buildBinder(reflect.TypeOf(R{}))
	if err != nil {
		t.Fatalf("buildBinder: %v", err)
	}
	if _, ok := b.byName["ts"]; !ok {
		t.Error("byName missing bare 'ts' after tag options stripped")
	}
}

// testScanner is a sql.Scanner implementation used to prove the
// generic custom-type path works end-to-end. Its Scan stores the
// raw driver value as a string.
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

// TestBuildBinder_CustomSQLScannerType guards that a field whose
// *T implements sql.Scanner gets the generic scanner path, and
// that the returned wrapper delegates Scan to the inner type.
func TestBuildBinder_CustomSQLScannerType(t *testing.T) {
	type R struct {
		Custom testScanner `parquet:"custom"`
	}
	b, err := buildBinder(reflect.TypeOf(R{}))
	if err != nil {
		t.Fatalf("buildBinder: %v", err)
	}
	fb := b.byName["custom"]
	if fb == nil {
		t.Fatal("binder missing 'custom' field")
	}

	// Exercise makeDest + the inner Scan path.
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

	// Now exercise assign: a fresh R, then assign writes Custom
	// from the scanner's captured value.
	var r R
	rv := reflect.ValueOf(&r).Elem()
	fb.assign(rv.FieldByIndex(fb.fieldIndex), dest)
	if r.Custom.Text != "hello" {
		t.Errorf("r.Custom.Text = %q, want %q", r.Custom.Text, "hello")
	}
}

// TestNullScanner_NULL guards that src == nil sets valid=false
// without touching the inner scanner — critical for the zero-
// value contract on user types.
func TestNullScanner_NULL(t *testing.T) {
	ts := &testScanner{Text: "preset"}
	ns := &nullScanner{inner: ts, value: reflect.ValueOf(ts).Elem()}
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

// TestFieldScanner_NullableTypes runs each of the supported Go
// primitive types through a NULL scan and a valued scan, and
// verifies the assign function writes (resp. skips) correctly.
// This is the comprehensive guard for the Kind-switch in
// makeFieldScanner.
func TestFieldScanner_NullableTypes(t *testing.T) {
	type R struct {
		S  string  `parquet:"s"`
		I  int64   `parquet:"i"`
		I8 int8    `parquet:"i8"`
		U  uint32  `parquet:"u"`
		F  float64 `parquet:"f"`
		B  bool    `parquet:"b"`
		By []byte  `parquet:"by"`
	}
	b, err := buildBinder(reflect.TypeOf(R{}))
	if err != nil {
		t.Fatalf("buildBinder: %v", err)
	}

	cases := []struct {
		col  string
		val  any
		want func(r R) bool
	}{
		{"s", "hello", func(r R) bool { return r.S == "hello" }},
		{"i", int64(42), func(r R) bool { return r.I == 42 }},
		{"i8", int64(7), func(r R) bool { return r.I8 == 7 }},
		{"u", int64(99), func(r R) bool { return r.U == 99 }},
		{"f", 3.14, func(r R) bool { return r.F == 3.14 }},
		{"b", true, func(r R) bool { return r.B }},
		{"by", []byte{1, 2, 3},
			func(r R) bool {
				return len(r.By) == 3 && r.By[0] == 1
			}},
	}
	for _, tc := range cases {
		t.Run("valued_"+tc.col, func(t *testing.T) {
			fb := b.byName[tc.col]
			dest := fb.makeDest()
			// Exercise via sql.Scanner interface.
			if s, ok := dest.(interface{ Scan(any) error }); ok {
				if err := s.Scan(tc.val); err != nil {
					t.Fatalf("Scan: %v", err)
				}
			} else if bp, ok := dest.(*[]byte); ok {
				*bp = tc.val.([]byte)
			} else {
				t.Fatalf("dest %T lacks Scan or *[]byte", dest)
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
			// NULL path: valid stays false; assign is a no-op
			// and the field stays at Go zero.
			if s, ok := dest.(interface{ Scan(any) error }); ok {
				if err := s.Scan(nil); err != nil {
					t.Fatalf("Scan(nil): %v", err)
				}
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
