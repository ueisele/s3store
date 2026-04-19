package s3parquet

import (
	"bytes"
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"
	"github.com/ueisele/s3store/internal/core"
)

type testRec struct {
	Period   string `parquet:"period"`
	Customer string `parquet:"customer"`
	Value    int64  `parquet:"value"`
}

func validConfig() Config[testRec] {
	return Config[testRec]{
		Bucket:            "b",
		Prefix:            "p",
		PartitionKeyParts: []string{"period", "customer"},
		S3Client:          &s3.Client{},
	}
}

func TestNew_Validation(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Config[testRec])
		wantSub string
	}{
		{"missing Bucket", func(c *Config[testRec]) { c.Bucket = "" }, "Bucket is required"},
		{"missing Prefix", func(c *Config[testRec]) { c.Prefix = "" }, "Prefix is required"},
		{"missing S3Client", func(c *Config[testRec]) { c.S3Client = nil }, "S3Client is required"},
		{"missing PartitionKeyParts", func(c *Config[testRec]) { c.PartitionKeyParts = nil }, "PartitionKeyParts is required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConfig()
			tc.mutate(&cfg)
			_, err := New(cfg)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("error %q did not contain %q",
					err.Error(), tc.wantSub)
			}
		})
	}
}

func TestValidateKey(t *testing.T) {
	s := &Writer[testRec]{cfg: WriterConfig[testRec]{
		PartitionKeyParts: []string{"period", "customer"},
	}}
	cases := []struct {
		name    string
		key     string
		wantErr bool
	}{
		{"valid", "period=X/customer=Y", false},
		{"wrong count", "period=X", true},
		{"extra segment", "period=X/customer=Y/extra=Z", true},
		{"wrong name", "x=X/customer=Y", true},
		{"empty value", "period=/customer=Y", true},
		{"double-dot in value", "period=2026..03-17/customer=Y", true},
		{"double-dot in tail value", "period=X/customer=a..b", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := s.validateKey(tc.key)
			if tc.wantErr != (err != nil) {
				t.Errorf("wantErr=%v got %v", tc.wantErr, err)
			}
		})
	}
}

// TestEncodeDecodeRoundTrip exercises the parquet encode +
// decode path so the pure-Go read path is known good without
// touching S3.
func TestEncodeDecodeRoundTrip(t *testing.T) {
	in := []testRec{
		{Period: "2026-03-17", Customer: "abc", Value: 1},
		{Period: "2026-03-17", Customer: "abc", Value: 2},
		{Period: "2026-03-17", Customer: "def", Value: 3},
	}
	data, err := encodeParquet(in, nil, &parquet.Snappy)
	if err != nil {
		t.Fatalf("encodeParquet: %v", err)
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

// TestEncodeParquet_BloomFilter guards that when
// BloomFilterColumns is non-empty, encodeParquet emits a
// per-row-group split-block bloom filter for every requested
// column. Looks at the raw parquet metadata so the check is
// independent of any reader-side pruning.
func TestEncodeParquet_BloomFilter(t *testing.T) {
	in := []testRec{
		{Period: "2026-03-17", Customer: "abc", Value: 1},
		{Period: "2026-03-17", Customer: "def", Value: 2},
		{Period: "2026-03-18", Customer: "abc", Value: 3},
	}
	data, err := encodeParquet(in, []string{"customer"}, &parquet.Snappy)
	if err != nil {
		t.Fatalf("encodeParquet: %v", err)
	}

	f, err := parquet.OpenFile(
		bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}

	// Each row group must carry exactly one bloom filter (on
	// the requested "customer" column), and that filter must
	// accept the values we wrote and reject one we didn't.
	leafNames := leafColumnNames(f.Schema())
	for rgIdx, rg := range f.RowGroups() {
		var bfCount int
		for colIdx, col := range rg.ColumnChunks() {
			bf := col.BloomFilter()
			if bf == nil {
				continue
			}
			bfCount++
			if leafNames[colIdx] != "customer" {
				t.Errorf("row group %d: bloom filter on %q, want customer",
					rgIdx, leafNames[colIdx])
			}
			for _, want := range []string{"abc", "def"} {
				ok, err := bf.Check(parquet.ValueOf(want))
				if err != nil {
					t.Fatalf("bloom Check(%q): %v", want, err)
				}
				if !ok {
					t.Errorf("bloom filter rejects written value %q", want)
				}
			}
			// A value we never wrote should (with very high
			// probability) be rejected. 10 bpv → ~1% false
			// positive; "zzz-not-present" is well outside the
			// written set.
			ok, _ := bf.Check(parquet.ValueOf("zzz-not-present"))
			if ok {
				t.Logf("false-positive on unseen value (rare but " +
					"legal for bloom filters)")
			}
		}
		if bfCount != 1 {
			t.Errorf("row group %d: got %d bloom filters, want 1",
				rgIdx, bfCount)
		}
	}
}

// leafColumnNames returns the top-level leaf column names in the
// order they appear in a row group's ColumnChunks().
func leafColumnNames(s *parquet.Schema) []string {
	paths := s.Columns()
	out := make([]string, len(paths))
	for i, p := range paths {
		out[i] = p[len(p)-1]
	}
	return out
}

// TestNew_BloomFilterColumnsValidation guards the typo guard:
// a column name that doesn't exist on T must fail at New()
// rather than silently producing files without the filter.
func TestNew_BloomFilterColumnsValidation(t *testing.T) {
	cfg := Config[testRec]{
		Bucket:             "b",
		Prefix:             "p",
		S3Client:           &s3.Client{},
		PartitionKeyParts:  []string{"period", "customer"},
		BloomFilterColumns: []string{"typo_not_a_column"},
	}
	if _, err := New(cfg); err == nil {
		t.Error("expected error for unknown BloomFilterColumns entry, got nil")
	}

	cfg.BloomFilterColumns = []string{"customer", ""}
	if _, err := New(cfg); err == nil {
		t.Error("expected error for empty BloomFilterColumns entry, got nil")
	}

	cfg.BloomFilterColumns = []string{"customer"}
	if _, err := New(cfg); err != nil {
		t.Errorf("valid BloomFilterColumns: unexpected error: %v", err)
	}
}

func TestSettleWindowDefault(t *testing.T) {
	var c Config[testRec]
	if got := c.settleWindow(); got.String() != "5s" {
		t.Errorf("default: got %v, want 5s", got)
	}
}

// TestWriteEmptyRecords guards that Write and WriteWithKey
// return (nil, nil) for empty input instead of erroring. This
// lets callers forward their batch pipelines without a manual
// length check before every Write call. No S3 is contacted
// because the empty-records fast path returns before any
// method touches s.s3.
func TestWriteEmptyRecords(t *testing.T) {
	s := &Writer[testRec]{cfg: WriterConfig[testRec]{
		PartitionKeyParts: []string{"period", "customer"},
		PartitionKeyOf: func(r testRec) string {
			return "period=" + r.Period + "/customer=" + r.Customer
		},
	}}

	got, err := s.Write(context.Background(), nil)
	if err != nil {
		t.Errorf("Write(nil): unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("Write(nil): expected nil slice, got %v", got)
	}

	got, err = s.Write(context.Background(), []testRec{})
	if err != nil {
		t.Errorf("Write([]): unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("Write([]): expected nil slice, got %v", got)
	}

	ptr, err := s.WriteWithKey(context.Background(),
		"period=X/customer=Y", nil)
	if err != nil {
		t.Errorf("WriteWithKey(nil): unexpected error: %v", err)
	}
	if ptr != nil {
		t.Errorf("WriteWithKey(nil): expected nil result, got %+v", ptr)
	}
}

// TestDedupEnabled guards that dedup is gated on EntityKeyOf
// only. New() populates VersionOf with DefaultVersionOf when
// the user leaves it nil, so by the time dedupEnabled is
// consulted the gating fact is whether the user asked for
// dedup at all (by providing an entity key).
func TestDedupEnabled(t *testing.T) {
	c := Config[testRec]{}
	if c.dedupEnabled() {
		t.Error("dedupEnabled: no EntityKeyOf, want false")
	}
	c.EntityKeyOf = func(r testRec) string { return r.Customer }
	if !c.dedupEnabled() {
		t.Error("dedupEnabled: with EntityKeyOf, want true")
	}
}

// TestNewPopulatesDefaultVersionOf guards that New assigns
// DefaultVersionOf when the user set EntityKeyOf but left
// VersionOf nil — that's the "sensible default" behaviour.
func TestNewPopulatesDefaultVersionOf(t *testing.T) {
	cfg := validConfig()
	cfg.EntityKeyOf = func(r testRec) string { return r.Customer }
	// VersionOf deliberately nil

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if s.Reader.cfg.VersionOf == nil {
		t.Fatal("VersionOf still nil after New; expected DefaultVersionOf")
	}
	// Spot-check: DefaultVersionOf returns insertedAt.UnixMicro().
	ts := time.UnixMicro(1_710_684_000_000_000)
	if got := s.Reader.cfg.VersionOf(testRec{}, ts); got != 1_710_684_000_000_000 {
		t.Errorf("default VersionOf returned %d, want %d",
			got, 1_710_684_000_000_000)
	}
}

// TestNewLeavesUserVersionOfAlone guards that New does not
// overwrite a user-supplied VersionOf.
func TestNewLeavesUserVersionOfAlone(t *testing.T) {
	cfg := validConfig()
	cfg.EntityKeyOf = func(r testRec) string { return r.Customer }
	cfg.VersionOf = func(r testRec, _ time.Time) int64 {
		return r.Value * 2
	}

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	got := s.Reader.cfg.VersionOf(testRec{Value: 21}, time.Time{})
	if got != 42 {
		t.Errorf("user VersionOf was replaced; got %d, want 42", got)
	}
}

// TestStoreClose guards that Close is a clean no-op for the
// pure-Go Store. Integration tests verify this too, but a unit
// smoke test catches a regression where Close starts doing
// something (e.g. closing S3Client) and returning an error.
func TestStoreClose(t *testing.T) {
	s, err := New(validConfig())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// TestDefaultVersionOf guards that the exported helper returns
// insertedAt in microseconds, matching what the doc comment
// promises. Users who reference the helper explicitly rely on
// this contract.
func TestDefaultVersionOf(t *testing.T) {
	ts := time.UnixMicro(1_710_684_000_000_000)
	got := DefaultVersionOf(testRec{}, ts)
	if got != 1_710_684_000_000_000 {
		t.Errorf("got %d, want %d", got, 1_710_684_000_000_000)
	}
}

// TestOffsetAt guards the lex-cursor semantics: a ref encoded
// before time t must sort strictly less than OffsetAt(t); a ref
// encoded at exactly time t must sort at or after it. These
// are the invariants Poll+WithUntilOffset relies on to turn a
// time window into the correct half-open offset range.
func TestOffsetAt(t *testing.T) {
	s := &Reader[testRec]{
		cfg:     readerConfigFrom(validConfig()),
		refPath: "p/_stream/refs",
	}
	anchor := time.UnixMicro(2_000_000_000_000_000)
	off := s.OffsetAt(anchor)

	earlier := core.EncodeRefKey(
		s.refPath, anchor.Add(-time.Millisecond).UnixMicro(),
		"abcd1234", "period=X/customer=y")
	if earlier >= string(off) {
		t.Errorf("earlier ref %q should sort before offset %q",
			earlier, off)
	}
	same := core.EncodeRefKey(
		s.refPath, anchor.UnixMicro(),
		"abcd1234", "period=X/customer=y")
	if same < string(off) {
		t.Errorf("same-time ref %q should sort >= offset %q",
			same, off)
	}
	later := core.EncodeRefKey(
		s.refPath, anchor.Add(time.Second).UnixMicro(),
		"abcd1234", "period=X/customer=y")
	if later <= string(off) {
		t.Errorf("later ref %q should sort after offset %q",
			later, off)
	}
}

// TestReaderExtrasMirrorsReaderConfig guards the drift-guard:
// every read-side field on ReaderExtras must also appear on
// ReaderConfig with the same name and type. Without this, adding
// a new read knob to one struct and forgetting the other would
// silently go unnoticed — Writer.Reader / NewView users would
// see different behavior from direct NewReader users.
func TestReaderExtrasMirrorsReaderConfig(t *testing.T) {
	extras := reflect.TypeFor[ReaderExtras[testRec]]()
	cfg := reflect.TypeFor[ReaderConfig[testRec]]()

	for i := range extras.NumField() {
		ef := extras.Field(i)
		cf, ok := cfg.FieldByName(ef.Name)
		if !ok {
			t.Errorf("ReaderExtras field %q missing from ReaderConfig",
				ef.Name)
			continue
		}
		if ef.Type != cf.Type {
			t.Errorf("ReaderExtras.%s type %s != ReaderConfig.%s type %s",
				ef.Name, ef.Type, cf.Name, cf.Type)
		}
	}
}

// TestNewSkipsDefaultWhenNoEntityKey guards that New does not
// assign VersionOf when the user hasn't asked for dedup
// (EntityKeyOf nil). Unnecessary allocation and a subtle
// invariant for dedupEnabled.
func TestNewSkipsDefaultWhenNoEntityKey(t *testing.T) {
	cfg := validConfig()
	// both EntityKeyOf and VersionOf left nil

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if s.Reader.cfg.VersionOf != nil {
		t.Error("VersionOf set despite no EntityKeyOf")
	}
}
