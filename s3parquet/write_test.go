package s3parquet

import (
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
)

// TestGroupByKey guards that groupByKey partitions records by
// PartitionKeyOf and preserves every record in its group.
func TestGroupByKey(t *testing.T) {
	s := &writer[testRec]{cfg: Config[testRec]{
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
	data, err := encodeParquet(in, nil, &parquet.Snappy)
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

// TestEncodeParquet_Compression covers every supported codec:
// the produced bytes round-trip cleanly, and snappy / zstd /
// gzip each produce meaningfully smaller output than
// uncompressed for a compressible payload. Guards that the
// CompressionCodec → parquet-go codec mapping is wired up on
// the write path.
func TestEncodeParquet_Compression(t *testing.T) {
	// Highly compressible: one value repeated across 10k rows.
	in := make([]testRec, 10_000)
	for i := range in {
		in[i] = testRec{Period: "p", Customer: "c", Value: 42}
	}

	uncompressed, err := encodeParquet(in, nil, &parquet.Uncompressed)
	if err != nil {
		t.Fatalf("uncompressed encode: %v", err)
	}

	cases := []struct {
		name  string
		codec compress.Codec
	}{
		{"snappy", &parquet.Snappy},
		{"zstd", &parquet.Zstd},
		{"gzip", &parquet.Gzip},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := encodeParquet(in, nil, tc.codec)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if len(data) >= len(uncompressed) {
				t.Errorf("%s encoded %d bytes; not smaller "+
					"than uncompressed %d (codec didn't "+
					"fire)",
					tc.name, len(data), len(uncompressed))
			}
			out, err := decodeParquet[testRec](data)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if len(out) != len(in) {
				t.Fatalf("round-trip lost rows: got %d, want %d",
					len(out), len(in))
			}
		})
	}
}

// TestNew_CompressionValidation guards that New() rejects an
// unknown Compression value before any S3 work.
func TestNew_CompressionValidation(t *testing.T) {
	_, err := New(Config[testRec]{
		Bucket:            "b",
		Prefix:            "p",
		PartitionKeyParts: []string{"period", "customer"},
		S3Client:          &s3.Client{},
		PartitionKeyOf:    func(r testRec) string { return "" },
		Compression:       "brotli", // not supported
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "unknown Compression") {
		t.Errorf("error %q did not mention unknown Compression", err)
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

	data, err := encodeParquet(in, nil, &parquet.Snappy)
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
