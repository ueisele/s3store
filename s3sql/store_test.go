package s3sql

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type testRec struct {
	Period   string
	Customer string
	Value    int64
}

func validConfig() Config[testRec] {
	return Config[testRec]{
		Bucket:     "b",
		Prefix:     "p",
		KeyParts:   []string{"period", "customer"},
		S3Client:   &s3.Client{},
		TableAlias: "t",
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
		{"missing TableAlias", func(c *Config[testRec]) { c.TableAlias = "" }, "TableAlias is required"},
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

// newTestStore builds a Store without going through New(): no
// DuckDB connection, no S3. Used for unit tests that only
// exercise pure helpers.
func newTestStore() *Store[testRec] {
	cfg := validConfig()
	return &Store[testRec]{
		cfg:      cfg,
		dataPath: cfg.Prefix + "/data",
		refPath:  cfg.Prefix + "/_stream/refs",
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

func TestBuildParquetURI(t *testing.T) {
	s := newTestStore()
	cases := []struct {
		name    string
		pattern string
		want    string
		wantErr bool
	}{
		{"exact", "period=2026-03-17/customer=abc",
			"s3://b/p/data/period=2026-03-17/customer=abc/*.parquet", false},
		{"match all empty", "",
			"s3://b/p/data/**/*.parquet", false},
		{"match all star", "*",
			"s3://b/p/data/**/*.parquet", false},
		{"tail star", "period=X/*",
			"s3://b/p/data/period=X/customer=*/*.parquet", false},
		{"head star", "*/customer=abc",
			"s3://b/p/data/period=*/customer=abc/*.parquet", false},
		{"partial value glob", "period=2026-03-*/customer=abc",
			"s3://b/p/data/period=2026-03-*/customer=abc/*.parquet", false},

		{"truncated", "period=X", "", true},
		{"extra segment", "period=X/customer=Y/extra=Z", "", true},
		{"wrong part name", "ustomer=X/period=Y", "", true},
		{"middle wildcard rejected", "period=2026-*-17/customer=abc", "", true},
		{"char class rejected", "period=[0-9]/customer=abc", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := s.buildParquetURI(tc.pattern)
			if tc.wantErr {
				if err == nil {
					t.Errorf("expected error, got %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("\ngot  %q\nwant %q", got, tc.want)
			}
		})
	}
}

func TestDedupColumns(t *testing.T) {
	c := Config[testRec]{KeyParts: []string{"period", "customer"}}
	got := c.dedupColumns()
	if len(got) != 2 || got[0] != "period" || got[1] != "customer" {
		t.Errorf("default: got %v", got)
	}
	c.DeduplicateBy = []string{"customer_id", "sku"}
	got = c.dedupColumns()
	if len(got) != 2 || got[0] != "customer_id" || got[1] != "sku" {
		t.Errorf("override: got %v", got)
	}
}

// buildColumnTransforms is exhaustively covered in
// transforms_test.go's TestBuildColumnTransformsAllCases.

// TestSettleWindowDefault guards the 5s default.
func TestSettleWindowDefault(t *testing.T) {
	var c Config[testRec]
	if got := c.settleWindow(); got.String() != "5s" {
		t.Errorf("default: got %v", got)
	}
}

// TestScanAllNilScanFunc guards that the missing-ScanFunc error
// surfaces correctly instead of panicking.
func TestScanAllNilScanFunc(t *testing.T) {
	s := newTestStore()
	// Construct a zero-value *sql.Rows via nil; scanAll should
	// return an error on the nil-ScanFunc check before touching
	// rows.
	var rows *sql.Rows
	_, err := s.scanAll(rows)
	if err == nil || !strings.Contains(err.Error(), "ScanFunc is required") {
		t.Errorf("expected ScanFunc-required error, got %v", err)
	}
}
