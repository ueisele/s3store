package s3sql

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
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
		TableAlias:        "t",
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
		{"range narrows to common prefix glob",
			"period=2026-03-01..2026-04-01/customer=abc",
			"s3://b/p/data/period=2026-0*/customer=abc/*.parquet", false},
		{"range no common prefix glob",
			"period=a..z/customer=abc",
			"s3://b/p/data/period=*/customer=abc/*.parquet", false},
		{"range unbounded upper glob",
			"period=2026-03-01../customer=abc",
			"s3://b/p/data/period=*/customer=abc/*.parquet", false},
		{"range + prefix same pattern",
			"period=2026-03-01..2026-04-01/customer=ab*",
			"s3://b/p/data/period=2026-0*/customer=ab*/*.parquet", false},

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

// TestBuildRangeWhere covers the WHERE-clause body that
// scanExprForPattern appends when a pattern contains range
// segments. No ranges → empty string; each bounded side becomes
// one predicate; multiple ranges are ANDed.
func TestBuildRangeWhere(t *testing.T) {
	s := newTestStore()
	cases := []struct {
		name    string
		pattern string
		want    string
	}{
		{"no range", "period=X/customer=Y", ""},
		{"match all", "*", ""},
		{"range both sides",
			"period=2026-03-01..2026-04-01/customer=abc",
			`"period" >= '2026-03-01' AND "period" < '2026-04-01'`},
		{"range unbounded upper",
			"period=2026-03-01../customer=abc",
			`"period" >= '2026-03-01'`},
		{"range unbounded lower",
			"period=..2026-04-01/customer=abc",
			`"period" < '2026-04-01'`},
		{"range both segments",
			"period=2026-03-01..2026-04-01/customer=a..m",
			`"period" >= '2026-03-01' AND "period" < '2026-04-01' ` +
				`AND "customer" >= 'a' AND "customer" < 'm'`},
		{"range with apostrophe escapes",
			"period=o'brien..o'connor/customer=abc",
			`"period" >= 'o''brien' AND "period" < 'o''connor'`},
		{"range + prefix in same pattern — only the range emits a predicate",
			"period=2026-03-01..2026-04-01/customer=ab*",
			`"period" >= '2026-03-01' AND "period" < '2026-04-01'`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := s.buildRangeWhere(tc.pattern)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("\ngot  %q\nwant %q", got, tc.want)
			}
		})
	}
}

// TestScanExprForPattern_Range confirms ranges produce both a
// common-prefix glob and a WHERE clause in the final scan, and
// that the filename option is emitted only when the caller
// requested it (for dedup tie-breaking).
func TestScanExprForPattern_Range(t *testing.T) {
	s := newTestStore()
	cases := []struct {
		name         string
		withFilename bool
		wantOpt      string
	}{
		{"no filename", false, ""},
		{"with filename", true, ", filename=true"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := s.scanExprForPattern(
				"period=2026-03-01..2026-04-01/customer=abc",
				tc.withFilename)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			want := `SELECT * FROM read_parquet(` +
				`'s3://b/p/data/period=2026-0*/customer=abc/*.parquet', ` +
				`hive_partitioning=true, hive_types_autocast=false, ` +
				`union_by_name=true` + tc.wantOpt + `) ` +
				`WHERE "period" >= '2026-03-01' AND "period" < '2026-04-01'`
			if got != want {
				t.Errorf("\ngot  %q\nwant %q", got, want)
			}
		})
	}
}

// TestDedupEnabled guards the dedup gate: explicit opt-in via
// EntityKeyColumns, no default.
func TestDedupEnabled(t *testing.T) {
	var c Config[testRec]
	if c.dedupEnabled() {
		t.Error("empty config: dedupEnabled should be false")
	}
	c.EntityKeyColumns = []string{"customer_id", "sku"}
	if !c.dedupEnabled() {
		t.Error("with EntityKeyColumns: dedupEnabled should be true")
	}
}

// TestNew_DedupValidation guards the both-or-neither rule: a
// VersionColumn without EntityKeyColumns (or vice versa) is a
// misconfiguration New() must reject at construction time.
func TestNew_DedupValidation(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Config[testRec])
		wantSub string
	}{
		{
			name: "VersionColumn without EntityKeyColumns",
			mutate: func(c *Config[testRec]) {
				c.VersionColumn = "ts"
			},
			wantSub: "EntityKeyColumns is required",
		},
		{
			name: "EntityKeyColumns without VersionColumn",
			mutate: func(c *Config[testRec]) {
				c.EntityKeyColumns = []string{"customer_id"}
			},
			wantSub: "VersionColumn is required",
		},
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

// TestSettleWindowDefault guards the 5s default.
func TestSettleWindowDefault(t *testing.T) {
	var c Config[testRec]
	if got := c.settleWindow(); got.String() != "5s" {
		t.Errorf("default: got %v", got)
	}
}
