package s3sql

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ueisele/s3store/s3parquet"
)

func validTarget() s3parquet.S3Target {
	return s3parquet.NewS3Target(s3parquet.S3TargetConfig{
		Bucket:            "b",
		Prefix:            "p",
		PartitionKeyParts: []string{"period", "customer"},
		S3Client:          &s3.Client{},
	})
}

func validConfig() ReaderConfig {
	return ReaderConfig{
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
		mutate  func(*s3parquet.S3TargetConfig, *ReaderConfig)
		wantSub string
	}{
		{"missing Bucket", func(tc *s3parquet.S3TargetConfig, _ *ReaderConfig) { tc.Bucket = "" }, "Bucket is required"},
		{"missing Prefix", func(tc *s3parquet.S3TargetConfig, _ *ReaderConfig) { tc.Prefix = "" }, "Prefix is required"},
		{"missing TableAlias", func(_ *s3parquet.S3TargetConfig, c *ReaderConfig) { c.TableAlias = "" }, "TableAlias is required"},
		{"missing S3Client", func(tc *s3parquet.S3TargetConfig, _ *ReaderConfig) { tc.S3Client = nil }, "S3Client is required"},
		{"missing PartitionKeyParts", func(tc *s3parquet.S3TargetConfig, _ *ReaderConfig) { tc.PartitionKeyParts = nil }, "PartitionKeyParts is required"},
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
	var c ReaderConfig
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
		mutate  func(*ReaderConfig)
		wantSub string
	}{
		{
			name: "VersionColumn without EntityKeyColumns",
			mutate: func(c *ReaderConfig) {
				c.VersionColumn = "ts"
			},
			wantSub: "EntityKeyColumns is required",
		},
		{
			name: "EntityKeyColumns without VersionColumn",
			mutate: func(c *ReaderConfig) {
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

// TestSettleWindowDefault guards the 5s default propagated via
// the Target. Redundant with s3parquet's own test for
// EffectiveSettleWindow, but locks the cross-package contract.
func TestSettleWindowDefault(t *testing.T) {
	var target s3parquet.S3Target
	if got := target.EffectiveSettleWindow(); got.String() != "5s" {
		t.Errorf("default: got %v", got)
	}
}
