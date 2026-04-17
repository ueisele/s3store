package s3parquet

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type testRec struct {
	Period   string `parquet:"period"`
	Customer string `parquet:"customer"`
	Value    int64  `parquet:"value"`
}

func validConfig() Config[testRec] {
	return Config[testRec]{
		Bucket:   "b",
		Prefix:   "p",
		KeyParts: []string{"period", "customer"},
		S3Client: &s3.Client{},
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
		{"missing KeyParts", func(c *Config[testRec]) { c.KeyParts = nil }, "KeyParts is required"},
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
	s := &Store[testRec]{cfg: Config[testRec]{
		KeyParts: []string{"period", "customer"},
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
	data, err := encodeParquet(in)
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
	s := &Store[testRec]{cfg: Config[testRec]{
		KeyParts: []string{"period", "customer"},
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
	if s.cfg.VersionOf == nil {
		t.Fatal("VersionOf still nil after New; expected DefaultVersionOf")
	}
	// Spot-check: DefaultVersionOf returns insertedAt.UnixMicro().
	ts := time.UnixMicro(1_710_684_000_000_000)
	if got := s.cfg.VersionOf(testRec{}, ts); got != 1_710_684_000_000_000 {
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
	got := s.cfg.VersionOf(testRec{Value: 21}, time.Time{})
	if got != 42 {
		t.Errorf("user VersionOf was replaced; got %d, want 42", got)
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
	if s.cfg.VersionOf != nil {
		t.Error("VersionOf set despite no EntityKeyOf")
	}
}
