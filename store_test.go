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

// TestRefKeyLexicalOrdering verifies that ref filenames sort
// in timestamp order via plain byte comparison. Poll relies on
// this via S3 ListObjectsV2's StartAfter parameter to walk the
// stream chronologically, so any encoding change that broke
// lexical = chronological would silently corrupt streaming.
func TestRefKeyLexicalOrdering(t *testing.T) {
	s := newTestStore()
	const key = "period=2026-03-17/customer=abc"

	// Three timestamps spanning a few orders of magnitude to
	// defend against future encoding changes that break on
	// varying-width integers.
	timestamps := []int64{
		1_000_000_000_000_000,
		1_710_684_000_000_000,
		9_000_000_000_000_000,
	}

	var encoded []string
	for _, ts := range timestamps {
		encoded = append(encoded, s.encodeRefKey(ts, "abcd1234", key))
	}

	// Every adjacent pair must satisfy earlier < later as
	// bytewise-compared strings.
	for i := 0; i < len(encoded)-1; i++ {
		if encoded[i] >= encoded[i+1] {
			t.Errorf("lexical order violated:\n %q\n !< %q",
				encoded[i], encoded[i+1])
		}
	}

	// And encoded keys within the same timestamp sort by id
	// (or at least are mutually distinct).
	a := s.encodeRefKey(1_710_684_000_000_000, "aaaaaaaa", key)
	b := s.encodeRefKey(1_710_684_000_000_000, "bbbbbbbb", key)
	if a >= b {
		t.Errorf("same-ts id ordering: %q !< %q", a, b)
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
	cases := []struct {
		name    string
		key     string
		shortID string
		want    string
	}{
		{
			name:    "standard key",
			key:     "period=2026-03-17/customer=abc",
			shortID: "a3f2e1b4",
			want:    "test-prefix/data/period=2026-03-17/customer=abc/a3f2e1b4.parquet",
		},
		{
			name:    "value with hyphen",
			key:     "period=2026-03-17/customer=foo-bar",
			shortID: "c7d9f0e2",
			want:    "test-prefix/data/period=2026-03-17/customer=foo-bar/c7d9f0e2.parquet",
		},
		{
			name:    "value with double hyphen",
			key:     "period=X/customer=foo--bar",
			shortID: "11111111",
			want:    "test-prefix/data/period=X/customer=foo--bar/11111111.parquet",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := s.buildDataPath(tc.key, tc.shortID)
			if got != tc.want {
				t.Errorf("\ngot  %q\nwant %q", got, tc.want)
			}
		})
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
