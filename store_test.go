package s3store

import (
	"testing"
)

// testRecord is the shared record type used by unit tests. The
// Parquet tags are present for integration tests that need real
// parquet encoding; unit tests only exercise pure helpers.
type testRecord struct {
	Customer string `parquet:"customer"`
	Period   string `parquet:"period"`
	Value    int64  `parquet:"value"`
}

// newTestStore constructs a Store[testRecord] without going
// through New(): no DuckDB connection, no S3 client. Used for
// unit tests that exercise pure helpers (encode/parse ref keys,
// glob compilation, key validation, transform building). Any
// method that touches s.db or s.s3 will panic — that's by
// design; those paths belong in integration tests.
func newTestStore(keyParts ...string) *Store[testRecord] {
	if len(keyParts) == 0 {
		keyParts = []string{"period", "customer"}
	}
	cfg := Config[testRecord]{
		Bucket:     "test-bucket",
		Prefix:     "test-prefix",
		KeyParts:   keyParts,
		TableAlias: "t",
	}
	return &Store[testRecord]{
		cfg:      cfg,
		dataPath: cfg.Prefix + "/data",
		refPath:  cfg.Prefix + "/_stream/refs",
	}
}

func TestRefKeyRoundTrip(t *testing.T) {
	s := newTestStore()
	const ts int64 = 1710684000000000
	const shortID = "a3f2e1b4"

	cases := []struct {
		name string
		key  string
	}{
		{"simple", "period=2026-03-17/customer=abc"},
		{"hyphen in value", "period=2026-03-17/customer=foo-bar"},
		{"double hyphen in value", "period=X/customer=foo--bar"},
		{"semicolon in value", "period=X/customer=a;b"},
		{"percent in value", "period=X/customer=50%off"},
		{"slash in value", "period=X/customer=a/b"},
		{"question mark in value", "period=X/customer=who?"},
		{"unicode", "period=X/customer=日本"},
		{"space in value", "period=X/customer=hello world"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			encoded := s.encodeRefKey(ts, shortID, tc.key)

			gotKey, gotID, err := s.parseRefKey(encoded)
			if err != nil {
				t.Fatalf("parseRefKey(%q): %v", encoded, err)
			}
			if gotKey != tc.key {
				t.Errorf("key: got %q want %q", gotKey, tc.key)
			}
			if gotID != shortID {
				t.Errorf("id: got %q want %q", gotID, shortID)
			}
		})
	}
}

func TestParseRefKeyInvalid(t *testing.T) {
	s := newTestStore()
	cases := []string{
		"not-a-ref-key",
		"refs/garbage.ref",
		"refs/1710684000000000.ref",                    // no separator
		"refs/1710684000000000;period=X.ref",           // no '-' between ts and id
		"refs/1710684000000000-id;period=X%ZZabc.ref",  // invalid percent escape
	}
	for _, raw := range cases {
		t.Run(raw, func(t *testing.T) {
			if _, _, err := s.parseRefKey(raw); err == nil {
				t.Errorf("parseRefKey(%q): expected error", raw)
			}
		})
	}
}

func TestBuildParquetURI(t *testing.T) {
	s := newTestStore("period", "customer")

	cases := []struct {
		name    string
		pattern string
		want    string
		wantErr bool
	}{
		{
			name:    "exact key",
			pattern: "period=2026-03-17/customer=abc",
			want:    "s3://test-bucket/test-prefix/data/period=2026-03-17/customer=abc/*.parquet",
		},
		{
			name:    "literal star means everything",
			pattern: "*",
			want:    "s3://test-bucket/test-prefix/data/**/*.parquet",
		},
		{
			name:    "empty means everything",
			pattern: "",
			want:    "s3://test-bucket/test-prefix/data/**/*.parquet",
		},
		{
			name:    "bare star in tail position",
			pattern: "period=X/*",
			want:    "s3://test-bucket/test-prefix/data/period=X/customer=*/*.parquet",
		},
		{
			name:    "bare star in head position",
			pattern: "*/customer=abc",
			want:    "s3://test-bucket/test-prefix/data/period=*/customer=abc/*.parquet",
		},
		{
			name:    "bare star in both positions",
			pattern: "*/*",
			want:    "s3://test-bucket/test-prefix/data/period=*/customer=*/*.parquet",
		},
		{
			name:    "partial value glob",
			pattern: "period=2026-03-*/customer=abc",
			want:    "s3://test-bucket/test-prefix/data/period=2026-03-*/customer=abc/*.parquet",
		},
		{
			name:    "explicit value star",
			pattern: "period=X/customer=*",
			want:    "s3://test-bucket/test-prefix/data/period=X/customer=*/*.parquet",
		},
		{
			name:    "truncated pattern errors",
			pattern: "period=2026-03-17",
			wantErr: true,
		},
		{
			name:    "extra segment errors",
			pattern: "period=X/customer=Y/extra=Z",
			wantErr: true,
		},
		{
			name:    "wrong part name at position 0",
			pattern: "ustomer=abc/period=X",
			wantErr: true,
		},
		{
			name:    "wrong part name at position 1",
			pattern: "period=X/extra=Y",
			wantErr: true,
		},
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

func TestBuildDataPath(t *testing.T) {
	s := newTestStore()
	got := s.buildDataPath("period=2026-03-17/customer=abc", "a3f2e1b4")
	want := "test-prefix/data/period=2026-03-17/customer=abc/a3f2e1b4.parquet"
	if got != want {
		t.Errorf("\ngot  %q\nwant %q", got, want)
	}
}

func TestDedupColumns(t *testing.T) {
	s := newTestStore("period", "customer")

	// default: falls back to KeyParts
	got := s.dedupColumns()
	want := []string{"period", "customer"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("default: got %v want %v", got, want)
	}

	// explicit DeduplicateBy
	s.cfg.DeduplicateBy = []string{"customer_id", "sku"}
	got = s.dedupColumns()
	want = []string{"customer_id", "sku"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("override: got %v want %v", got, want)
	}
}
