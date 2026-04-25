package s3sql

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ueisele/s3store/s3parquet"
)

type testRec struct {
	Period   string `parquet:"period"`
	Customer string `parquet:"customer"`
	Value    int64  `parquet:"value"`
}

func validTarget() s3parquet.S3Target {
	return s3parquet.NewS3Target(s3parquet.S3TargetConfig{
		Bucket:            "b",
		Prefix:            "p",
		PartitionKeyParts: []string{"period", "customer"},
		S3Client:          &s3.Client{},
	})
}

func validConfig() ReaderConfig[testRec] {
	return ReaderConfig[testRec]{
		Target:     validTarget(),
		TableAlias: "t",
	}
}

func TestNewReader_Validation(t *testing.T) {
	validTargetCfg := s3parquet.S3TargetConfig{
		Bucket:            "b",
		Prefix:            "p",
		PartitionKeyParts: []string{"period", "customer"},
		S3Client:          &s3.Client{},
	}
	cases := []struct {
		name    string
		mutate  func(*s3parquet.S3TargetConfig, *ReaderConfig[testRec])
		wantSub string
	}{
		{"missing Bucket", func(tc *s3parquet.S3TargetConfig, _ *ReaderConfig[testRec]) { tc.Bucket = "" }, "Bucket is required"},
		{"missing Prefix", func(tc *s3parquet.S3TargetConfig, _ *ReaderConfig[testRec]) { tc.Prefix = "" }, "Prefix is required"},
		{"missing TableAlias", func(_ *s3parquet.S3TargetConfig, c *ReaderConfig[testRec]) { c.TableAlias = "" }, "TableAlias is required"},
		{"missing S3Client", func(tc *s3parquet.S3TargetConfig, _ *ReaderConfig[testRec]) { tc.S3Client = nil }, "S3Client is required"},
		{"missing PartitionKeyParts", func(tc *s3parquet.S3TargetConfig, _ *ReaderConfig[testRec]) { tc.PartitionKeyParts = nil }, "PartitionKeyParts is required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			targetCfg := validTargetCfg
			cfg := validConfig()
			tc.mutate(&targetCfg, &cfg)
			cfg.Target = s3parquet.NewS3Target(targetCfg)
			_, err := NewReader(cfg)
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

func TestSQLQuote(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"", "''"},
		{"plain", "'plain'"},
		{"o'brien", "'o''brien'"},
		{"'", "''''"},
		{"s3://x/data/customer=o'brien/abc.parquet",
			"'s3://x/data/customer=o''brien/abc.parquet'"},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			if got := sqlQuote(tc.in); got != tc.want {
				t.Errorf("sqlQuote(%q) = %q, want %q",
					tc.in, got, tc.want)
			}
		})
	}
}

// TestDedupEnabled guards the dedup gate: explicit opt-in via
// EntityKeyColumns, no default.
func TestDedupEnabled(t *testing.T) {
	var c ReaderConfig[testRec]
	if c.dedupEnabled() {
		t.Error("empty config: dedupEnabled should be false")
	}
	c.EntityKeyColumns = []string{"customer_id", "sku"}
	if !c.dedupEnabled() {
		t.Error("with EntityKeyColumns: dedupEnabled should be true")
	}
}

// TestNewReader_DedupValidation guards the both-or-neither rule:
// a VersionColumn without EntityKeyColumns (or vice versa) is a
// misconfiguration NewReader must reject at construction time.
func TestNewReader_DedupValidation(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*ReaderConfig[testRec])
		wantSub string
	}{
		{
			name: "VersionColumn without EntityKeyColumns",
			mutate: func(c *ReaderConfig[testRec]) {
				c.VersionColumn = "ts"
			},
			wantSub: "EntityKeyColumns is required",
		},
		{
			name: "EntityKeyColumns without VersionColumn",
			mutate: func(c *ReaderConfig[testRec]) {
				c.EntityKeyColumns = []string{"customer_id"}
			},
			wantSub: "VersionColumn is required",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConfig()
			tc.mutate(&cfg)
			_, err := NewReader(cfg)
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

// TestNewReader_FilenameColumnCollision guards the guard: a T
// with a `parquet:"filename"` field collides with DuckDB's
// read_parquet(filename=true), which we use for the dedup CTE's
// tie-breaker. NewReader must reject the configuration up front
// instead of producing wrong rows at query time.
//
// (After Phase 1, InsertedAtField is a real parquet column
// decoded natively — no filename plumbing — so the historical
// "InsertedAtField set" branch of this test no longer applies.)
func TestNewReader_FilenameColumnCollision(t *testing.T) {
	type recWithFilename struct {
		Period   string `parquet:"period"`
		Customer string `parquet:"customer"`
		Filename string `parquet:"filename"`
	}
	cfg := ReaderConfig[recWithFilename]{
		Target:           validTarget(),
		TableAlias:       "t",
		EntityKeyColumns: []string{"period"},
		VersionColumn:    "customer",
	}
	_, err := NewReader(cfg)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "dedup tie-breaker") {
		t.Errorf("error %q did not contain %q",
			err.Error(), "dedup tie-breaker")
	}
}

// TestSettleWindowDefault guards the 5s default propagated via
// the Target. Redundant with s3parquet's own test for
// EffectiveSettleWindow, but locks the cross-package contract.
func TestSettleWindowDefault(t *testing.T) {
	var target s3parquet.S3Target
	if got := target.EffectiveSettleWindow(); got.String() != "5s" {
		t.Errorf("default: got %v", got)
	}
}
