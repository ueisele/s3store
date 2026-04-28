package s3store

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"
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
			_, err := New(t.Context(), cfg)
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
		Target: newS3TargetSkipConfig(S3TargetConfig{
			PartitionKeyParts: []string{"period", "customer"},
		}),
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
	data, err := encodeParquet(in, &parquet.Snappy)
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

// TestWriteEmptyRecords guards that Write and WriteWithKey
// return (nil, nil) for empty input instead of erroring. This
// lets callers forward their batch pipelines without a manual
// length check before every Write call. No S3 is contacted
// because the empty-records fast path returns before any
// method touches s.s3.
func TestWriteEmptyRecords(t *testing.T) {
	s := &Writer[testRec]{cfg: WriterConfig[testRec]{
		Target: newS3TargetSkipConfig(S3TargetConfig{
			PartitionKeyParts: []string{"period", "customer"},
		}),
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

// TestNewRejectsPartialDedupConfig guards that New rejects
// EntityKeyOf without VersionOf (and vice versa) — both are
// required for dedup to be meaningful.
func TestNewRejectsPartialDedupConfig(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Config[testRec])
	}{
		{
			name: "EntityKeyOf without VersionOf",
			mutate: func(c *Config[testRec]) {
				c.EntityKeyOf = func(r testRec) string { return r.Customer }
			},
		},
		{
			name: "VersionOf without EntityKeyOf",
			mutate: func(c *Config[testRec]) {
				c.VersionOf = func(r testRec) int64 { return r.Value }
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConfig()
			tc.mutate(&cfg)
			if _, err := newStoreFromTarget(cfg,
				newS3TargetSkipConfig(s3TargetConfigFrom(cfg))); err == nil {
				t.Error("expected error, got nil")
			}
		})
	}
}

// TestNewKeepsUserVersionOf guards that New does not mutate a
// user-supplied VersionOf.
func TestNewKeepsUserVersionOf(t *testing.T) {
	cfg := validConfig()
	cfg.EntityKeyOf = func(r testRec) string { return r.Customer }
	cfg.VersionOf = func(r testRec) int64 { return r.Value * 2 }

	s, err := newStoreFromTarget(cfg,
		newS3TargetSkipConfig(s3TargetConfigFrom(cfg)))
	if err != nil {
		t.Fatalf("newStoreFromTarget: %v", err)
	}
	got := s.Reader.cfg.VersionOf(testRec{Value: 21})
	if got != 42 {
		t.Errorf("user VersionOf was replaced; got %d, want 42", got)
	}
}

// TestOffsetAt guards the lex-cursor semantics: a ref encoded
// before time t must sort strictly less than OffsetAt(t); a ref
// encoded at exactly time t must sort at or after it. These
// are the invariants Poll+WithUntilOffset relies on to turn a
// time window into the correct half-open offset range.
func TestOffsetAt(t *testing.T) {
	cfg := validConfig()
	target := newS3TargetSkipConfig(s3TargetConfigFrom(cfg))
	s := &Reader[testRec]{
		cfg:     readerConfigFrom(cfg, target),
		refPath: "p/_ref",
	}
	anchor := time.UnixMicro(2_000_000_000_000_000)
	off := s.OffsetAt(anchor)

	earlier := encodeRefKey(
		s.refPath, anchor.Add(-time.Millisecond).UnixMicro(),
		"abcd1234", anchor.Add(-time.Millisecond).UnixMicro(),
		"period=X/customer=y")
	if earlier >= string(off) {
		t.Errorf("earlier ref %q should sort before offset %q",
			earlier, off)
	}
	same := encodeRefKey(
		s.refPath, anchor.UnixMicro(),
		"abcd1234", anchor.UnixMicro(),
		"period=X/customer=y")
	if same < string(off) {
		t.Errorf("same-time ref %q should sort >= offset %q",
			same, off)
	}
	later := encodeRefKey(
		s.refPath, anchor.Add(time.Second).UnixMicro(),
		"abcd1234", anchor.Add(time.Second).UnixMicro(),
		"period=X/customer=y")
	if later <= string(off) {
		t.Errorf("later ref %q should sort after offset %q",
			later, off)
	}
}

// TestWriterConfigMirroredInConfig guards the projection from the
// unified Config onto WriterConfig: every non-Target field on
// WriterConfig must also appear on Config with the same name and
// type, so writerConfigFrom can forward it. Target is excluded
// because Config flattens its fields to the top level. Without
// this, adding a new write knob to WriterConfig and forgetting
// Config would silently go unnoticed — Store users would see the
// default, direct NewWriter users the override.
func TestWriterConfigMirroredInConfig(t *testing.T) {
	wc := reflect.TypeFor[WriterConfig[testRec]]()
	c := reflect.TypeFor[Config[testRec]]()

	for i := range wc.NumField() {
		wf := wc.Field(i)
		if wf.Name == "Target" {
			continue
		}
		cf, ok := c.FieldByName(wf.Name)
		if !ok {
			t.Errorf("WriterConfig field %q missing from Config",
				wf.Name)
			continue
		}
		if wf.Type != cf.Type {
			t.Errorf("WriterConfig.%s type %s != Config.%s type %s",
				wf.Name, wf.Type, cf.Name, cf.Type)
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

	s, err := newStoreFromTarget(cfg,
		newS3TargetSkipConfig(s3TargetConfigFrom(cfg)))
	if err != nil {
		t.Fatalf("newStoreFromTarget: %v", err)
	}
	if s.Reader.cfg.VersionOf != nil {
		t.Error("VersionOf set despite no EntityKeyOf")
	}
}

// TestEffectiveMaxInflightRequests guards the fan-out cap
// resolution: positive user values win, zero falls back to the
// default (32). Negative gets the default too — NewS3Target sizes
// the semaphore from the same value, so the cap shows up at every
// fan-out site uniformly.
func TestEffectiveMaxInflightRequests(t *testing.T) {
	cases := []struct {
		name string
		set  int
		want int
	}{
		{"zero uses default", 0, 32},
		{"negative uses default", -1, 32},
		{"positive wins", 64, 64},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := S3TargetConfig{MaxInflightRequests: tc.set}
			if got := cfg.EffectiveMaxInflightRequests(); got != tc.want {
				t.Errorf("EffectiveMaxInflightRequests: got %d, want %d",
					got, tc.want)
			}
		})
	}
}
