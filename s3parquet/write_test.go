package s3parquet

import (
	"reflect"
	"testing"
)

// TestGroupByKey guards that groupByKey partitions records by
// PartitionKeyOf and preserves every record in its group.
func TestGroupByKey(t *testing.T) {
	s := &Store[testRec]{cfg: Config[testRec]{
		PartitionKeyParts: []string{"period", "customer"},
		PartitionKeyOf: func(r testRec) string {
			return "period=" + r.Period + "/customer=" + r.Customer
		},
	}}

	records := []testRec{
		{Period: "p1", Customer: "a", Value: 1},
		{Period: "p1", Customer: "a", Value: 2},
		{Period: "p1", Customer: "b", Value: 3},
		{Period: "p2", Customer: "a", Value: 4},
	}
	got := s.groupByKey(records)

	if len(got) != 3 {
		t.Fatalf("got %d groups, want 3", len(got))
	}
	cases := []struct {
		key  string
		want int
	}{
		{"period=p1/customer=a", 2},
		{"period=p1/customer=b", 1},
		{"period=p2/customer=a", 1},
	}
	for _, tc := range cases {
		if n := len(got[tc.key]); n != tc.want {
			t.Errorf("group %q: got %d records, want %d",
				tc.key, n, tc.want)
		}
	}
}

// TestEncodeParquet guards the write-side encode path: the
// produced byte stream has the parquet magic markers and
// decodes back to the same records.
func TestEncodeParquet(t *testing.T) {
	in := []testRec{
		{Period: "p1", Customer: "a", Value: 1},
		{Period: "p1", Customer: "b", Value: 2},
		{Period: "p2", Customer: "c", Value: 3},
	}
	data, err := encodeParquet(in)
	if err != nil {
		t.Fatalf("encodeParquet: %v", err)
	}
	if len(data) < 8 ||
		string(data[:4]) != "PAR1" ||
		string(data[len(data)-4:]) != "PAR1" {
		t.Fatalf("not a valid parquet file: len=%d", len(data))
	}

	out, err := decodeParquet[testRec](data)
	if err != nil {
		t.Fatalf("decodeParquet: %v", err)
	}
	if len(out) != len(in) {
		t.Fatalf("got %d rows, want %d", len(out), len(in))
	}
	for i := range in {
		if out[i] != in[i] {
			t.Errorf("row %d: got %+v, want %+v", i, out[i], in[i])
		}
	}
}

// TestEncodeParquet_NamedInt8EnumInNestedStruct guards that a
// named int8 enum (e.g. go-enum output) inside a nested struct
// inside a list round-trips cleanly through parquet-go — both
// the write and read side. Requires parquet-go v0.29+'s
// Kind-based small-int dispatch. Fast unit-level equivalent of
// the s3sql integration test for the same shape.
func TestEncodeParquet_NamedInt8EnumInNestedStruct(t *testing.T) {
	type Field int8
	const (
		FieldUnknown Field = iota
		FieldPrimary
		FieldSecondary
	)
	type Log struct {
		Processor string            `parquet:"processor"`
		Field     Field             `parquet:"field"`
		Attrs     map[string]string `parquet:"attrs"`
	}
	type Rec struct {
		ID   string `parquet:"id"`
		Logs []Log  `parquet:"logs"`
	}

	in := []Rec{{
		ID: "r1",
		Logs: []Log{
			{
				Processor: "ingest",
				Field:     FieldPrimary,
				Attrs:     map[string]string{"stage": "raw"},
			},
			{
				Processor: "enrich",
				Field:     FieldSecondary,
				Attrs:     map[string]string{"model": "v2"},
			},
		},
	}}

	data, err := encodeParquet(in)
	if err != nil {
		t.Fatalf("encodeParquet: %v", err)
	}
	out, err := decodeParquet[Rec](data)
	if err != nil {
		t.Fatalf("decodeParquet: %v", err)
	}
	if len(out) != len(in) {
		t.Fatalf("got %d rows, want %d", len(out), len(in))
	}
	if !reflect.DeepEqual(out[0], in[0]) {
		t.Errorf("round-trip mismatch:\n got  %+v\n want %+v",
			out[0], in[0])
	}
	_ = FieldUnknown
}
