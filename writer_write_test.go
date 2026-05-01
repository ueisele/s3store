package s3store

import (
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
)

// TestGroupByPartition_PartitionsAndPreservesRecords guards that
// GroupByPartition partitions records by PartitionKeyOf and
// preserves every record in its group.
func TestGroupByPartition_PartitionsAndPreservesRecords(t *testing.T) {
	s := &Writer[testRec]{cfg: WriterConfig[testRec]{
		Target: newS3TargetSkipConfig(S3TargetConfig{
			PartitionKeyParts: []string{"period", "customer"},
		}),
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
	got := s.GroupByPartition(records)

	if len(got) != 3 {
		t.Fatalf("got %d partitions, want 3", len(got))
	}
	wantSizes := map[string]int{
		"period=p1/customer=a": 2,
		"period=p1/customer=b": 1,
		"period=p2/customer=a": 1,
	}
	for _, p := range got {
		want, ok := wantSizes[p.Key]
		if !ok {
			t.Errorf("unexpected partition key %q", p.Key)
			continue
		}
		if n := len(p.Rows); n != want {
			t.Errorf("partition %q: got %d records, want %d",
				p.Key, n, want)
		}
	}
}

// TestGroupByPartition_LexOrder pins the public contract that
// partitions emit in lex-ascending order of Key regardless of
// the order in which they appear in the input slice. Mirrors
// the read-side TestReadIter_PartitionLexOrder — see
// "Deterministic emission order across read and write paths"
// in CLAUDE.md.
func TestGroupByPartition_LexOrder(t *testing.T) {
	s := &Writer[testRec]{cfg: WriterConfig[testRec]{
		Target: newS3TargetSkipConfig(S3TargetConfig{
			PartitionKeyParts: []string{"period", "customer"},
		}),
		PartitionKeyOf: func(r testRec) string {
			return "period=" + r.Period + "/customer=" + r.Customer
		},
	}}

	// Input in reverse-lex order so any accidental
	// insertion-order or map-iteration emission would produce
	// (c, b, a) instead of (a, b, c).
	records := []testRec{
		{Period: "p1", Customer: "c", Value: 1},
		{Period: "p1", Customer: "b", Value: 2},
		{Period: "p1", Customer: "a", Value: 3},
	}
	got := s.GroupByPartition(records)

	wantKeys := []string{
		"period=p1/customer=a",
		"period=p1/customer=b",
		"period=p1/customer=c",
	}
	if len(got) != len(wantKeys) {
		t.Fatalf("got %d partitions, want %d", len(got), len(wantKeys))
	}
	for i, want := range wantKeys {
		if got[i].Key != want {
			t.Errorf("[%d] Key=%q, want %q (lex)",
				i, got[i].Key, want)
		}
	}
}

// TestGroupByPartition_InsertionOrderWithinPartition pins that
// records that share a partition key retain their input order in
// Rows — the writer doesn't sort within a partition (no
// EntityKeyOf to compare on).
func TestGroupByPartition_InsertionOrderWithinPartition(t *testing.T) {
	s := &Writer[testRec]{cfg: WriterConfig[testRec]{
		Target: newS3TargetSkipConfig(S3TargetConfig{
			PartitionKeyParts: []string{"period", "customer"},
		}),
		PartitionKeyOf: func(r testRec) string {
			return "period=" + r.Period + "/customer=" + r.Customer
		},
	}}

	records := []testRec{
		{Period: "p1", Customer: "a", Value: 30},
		{Period: "p1", Customer: "a", Value: 10},
		{Period: "p1", Customer: "a", Value: 20},
	}
	got := s.GroupByPartition(records)
	if len(got) != 1 {
		t.Fatalf("got %d partitions, want 1", len(got))
	}
	wantValues := []int64{30, 10, 20}
	for i, v := range wantValues {
		if got[0].Rows[i].Value != v {
			t.Errorf("[%d] Value=%d, want %d (insertion order)",
				i, got[0].Rows[i].Value, v)
		}
	}
}

// TestGroupByPartition_EmptyInput guards the nil-return fast
// path.
func TestGroupByPartition_EmptyInput(t *testing.T) {
	s := &Writer[testRec]{cfg: WriterConfig[testRec]{
		Target: newS3TargetSkipConfig(S3TargetConfig{
			PartitionKeyParts: []string{"period", "customer"},
		}),
		PartitionKeyOf: func(r testRec) string {
			return "period=" + r.Period + "/customer=" + r.Customer
		},
	}}
	if got := s.GroupByPartition(nil); got != nil {
		t.Errorf("nil input: got %v, want nil", got)
	}
	if got := s.GroupByPartition([]testRec{}); got != nil {
		t.Errorf("empty input: got %v, want nil", got)
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
	data, err := encodeParquet(in, &parquet.Snappy)
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

	uncompressed, err := encodeParquet(in, &parquet.Uncompressed)
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
			data, err := encodeParquet(in, tc.codec)
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
	cfg := StoreConfig[testRec]{
		S3TargetConfig: S3TargetConfig{
			Bucket:            "b",
			Prefix:            "p",
			PartitionKeyParts: []string{"period", "customer"},
			S3Client:          &s3.Client{},
		},
		PartitionKeyOf: func(r testRec) string { return "" },
		Compression:    "brotli", // not supported
	}
	_, err := newStoreFromTarget(cfg,
		newS3TargetSkipConfig(cfg.S3TargetConfig))
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
// Kind-based small-int dispatch.
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

	data, err := encodeParquet(in, &parquet.Snappy)
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
