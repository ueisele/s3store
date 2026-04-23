package core

import (
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestRefKeyRoundTrip(t *testing.T) {
	const refPath = "test-prefix/_stream/refs"
	const refTs int64 = 1710684000000000
	const dataTs int64 = 1710683999500000
	const shortID = "a3f2e1b4"

	cases := []struct {
		name string
		key  string
	}{
		{"simple", "period=2026-03-17/customer=abc"},
		{"hyphen in value", "period=2026-03-17/customer=foo-bar"},
		{"semicolon in value", "period=X/customer=a;b"},
		{"percent in value", "period=X/customer=50%off"},
		{"slash in value", "period=X/customer=a/b"},
		{"question mark in value", "period=X/customer=who?"},
		{"unicode", "period=X/customer=日本"},
		{"space in value", "period=X/customer=hello world"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			encoded := EncodeRefKey(refPath, refTs, shortID, dataTs, tc.key)

			gotKey, gotRefTs, gotID, gotDataTs, err := ParseRefKey(encoded)
			if err != nil {
				t.Fatalf("ParseRefKey(%q): %v", encoded, err)
			}
			if gotKey != tc.key {
				t.Errorf("key: got %q want %q", gotKey, tc.key)
			}
			if gotRefTs != refTs {
				t.Errorf("refTs: got %d want %d", gotRefTs, refTs)
			}
			if gotID != shortID {
				t.Errorf("id: got %q want %q", gotID, shortID)
			}
			if gotDataTs != dataTs {
				t.Errorf("dataTs: got %d want %d", gotDataTs, dataTs)
			}
		})
	}
}

// TestRefKeyLexicalOrdering guards that ref filenames sort in
// timestamp order via plain byte comparison. Poll relies on this
// via S3 ListObjectsV2's StartAfter to walk the stream
// chronologically.
func TestRefKeyLexicalOrdering(t *testing.T) {
	const refPath = "test-prefix/_stream/refs"
	const key = "period=2026-03-17/customer=abc"

	timestamps := []int64{
		1_000_000_000_000_000,
		1_710_684_000_000_000,
		9_000_000_000_000_000,
	}

	var encoded []string
	for _, ts := range timestamps {
		encoded = append(encoded, EncodeRefKey(refPath, ts, "abcd1234", ts-1, key))
	}
	for i := 0; i < len(encoded)-1; i++ {
		if encoded[i] >= encoded[i+1] {
			t.Errorf("lexical order violated:\n %q\n !< %q",
				encoded[i], encoded[i+1])
		}
	}

	a := EncodeRefKey(refPath, 1_710_684_000_000_000, "aaaaaaaa", 1_710_683_999_000_000, key)
	b := EncodeRefKey(refPath, 1_710_684_000_000_000, "bbbbbbbb", 1_710_683_999_000_000, key)
	if a >= b {
		t.Errorf("same-ts id ordering: %q !< %q", a, b)
	}
}

func TestParseRefKeyInvalid(t *testing.T) {
	cases := []string{
		"not-a-ref-key",
		"refs/garbage.ref",
		"refs/1710684000000000.ref",                                    // no separator
		"refs/1710684000000000;period=X.ref",                           // no '-' between fields
		"refs/1710684000000000-id;period=X.ref",                        // only 2 dash-fields
		"refs/notanumber-id-1710683999500000;period=X.ref",             // non-numeric ref ts
		"refs/1710684000000000-id-notanumber;period=X.ref",             // non-numeric data ts
		"refs/1710684000000000-id-1710683999500000;period=X%ZZabc.ref", // invalid percent escape
	}
	for _, raw := range cases {
		t.Run(raw, func(t *testing.T) {
			if _, _, _, _, err := ParseRefKey(raw); err == nil {
				t.Errorf("ParseRefKey(%q): expected error", raw)
			}
		})
	}
}

func TestBuildDataFilePath(t *testing.T) {
	const dataPath = "test-prefix/data"
	cases := []struct {
		name string
		key  string
		id   string
		want string
	}{
		{
			name: "auto id (library default)",
			key:  "period=2026-03-17/customer=abc",
			id:   MakeAutoID(1710684000000000, "a3f2e1b4"),
			want: "test-prefix/data/period=2026-03-17/customer=abc/1710684000000000-a3f2e1b4.parquet",
		},
		{
			name: "value with hyphen",
			key:  "period=X/customer=foo-bar",
			id:   MakeAutoID(1710770400000000, "c7d9f0e2"),
			want: "test-prefix/data/period=X/customer=foo-bar/1710770400000000-c7d9f0e2.parquet",
		},
		{
			name: "caller-supplied token id",
			key:  "period=X/customer=Y",
			id:   "2026-04-22T10:15:00Z-batch42",
			want: "test-prefix/data/period=X/customer=Y/2026-04-22T10:15:00Z-batch42.parquet",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := BuildDataFilePath(dataPath, tc.key, tc.id)
			if got != tc.want {
				t.Errorf("\ngot  %q\nwant %q", got, tc.want)
			}
		})
	}
}

func TestParseDataFileName(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		wantID  string
		wantErr bool
	}{
		{
			name:   "auto-id format",
			in:     "1710684000000000-a3f2e1b4.parquet",
			wantID: "1710684000000000-a3f2e1b4",
		},
		{
			name:   "caller-token id with dashes",
			in:     "2026-04-22T10:15:00Z-batch42.parquet",
			wantID: "2026-04-22T10:15:00Z-batch42",
		},
		{
			name:    "missing .parquet suffix",
			in:      "1710684000000000-a3f2e1b4",
			wantErr: true,
		},
		{
			name:    "empty id",
			in:      ".parquet",
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id, err := ParseDataFileName(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Errorf("expected error for %q, got id=%q",
						tc.in, id)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if id != tc.wantID {
				t.Errorf("id: got %q want %q", id, tc.wantID)
			}
		})
	}
}

// TestBuildParseDataFileNameRoundTrip guards that
// ParseDataFileName is the inverse of BuildDataFilePath (for
// the filename portion) — works for both auto-id and caller-
// token shapes.
func TestBuildParseDataFileNameRoundTrip(t *testing.T) {
	const (
		dataPath = "p/data"
		hiveKey  = "period=X/customer=Y"
	)
	cases := []string{
		MakeAutoID(1_710_684_000_000_000, "a3f2e1b4"),
		"2026-04-22T10:15:00Z-batch42",
		"job.uuid.0e0e0e0e",
	}
	for _, id := range cases {
		t.Run(id, func(t *testing.T) {
			full := BuildDataFilePath(dataPath, hiveKey, id)
			filename := full[len(dataPath)+len(hiveKey)+2:]
			gotID, err := ParseDataFileName(filename)
			if err != nil {
				t.Fatalf("ParseDataFileName(%q): %v", filename, err)
			}
			if gotID != id {
				t.Errorf("round-trip: got id=%q want %q", gotID, id)
			}
		})
	}
}

// TestBuildDataFilePathLexicalOrdering guards that two
// auto-id writes to the same partition sort chronologically
// in S3 LIST by filename — the property MakeAutoID's tsMicros
// prefix preserves.
func TestBuildDataFilePathLexicalOrdering(t *testing.T) {
	const (
		dataPath = "p/data"
		key      = "period=X/customer=Y"
	)
	earlier := BuildDataFilePath(dataPath, key,
		MakeAutoID(1_000_000_000_000, "aaaaaaaa"))
	later := BuildDataFilePath(dataPath, key,
		MakeAutoID(2_000_000_000_000, "aaaaaaaa"))
	if earlier >= later {
		t.Errorf("earlier %q should sort before later %q", earlier, later)
	}
}

func TestIndexPath(t *testing.T) {
	got := IndexPath("store", "sku_period_idx")
	want := "store/_index/sku_period_idx"
	if got != want {
		t.Errorf("IndexPath = %q, want %q", got, want)
	}
}

func TestBuildAndParseIndexMarkerKey(t *testing.T) {
	const indexPath = "store/_index/sku_period_idx"
	columns := []string{
		"sku_id", "charge_period_start",
		"causing_customer", "charge_period_end",
	}
	values := []string{
		"SKU-123", "2026-03-01T00",
		"abc", "2026-04-01T00",
	}

	key := BuildIndexMarkerPath(indexPath, columns, values)

	if !strings.HasSuffix(key, "/m.idx") {
		t.Errorf("BuildIndexMarkerPath %q missing /m.idx suffix", key)
	}
	if !strings.HasPrefix(key, indexPath+"/") {
		t.Errorf("BuildIndexMarkerPath %q missing %q prefix",
			key, indexPath)
	}

	got, err := ParseIndexMarkerKey(key, indexPath, columns)
	if err != nil {
		t.Fatalf("ParseIndexMarkerKey: %v", err)
	}
	if !reflect.DeepEqual(got, values) {
		t.Errorf("round-trip: got %v, want %v", got, values)
	}
}

func TestParseIndexMarkerKey_Rejects(t *testing.T) {
	const indexPath = "store/_index/idx"
	columns := []string{"a", "b"}

	cases := []struct {
		name, key string
	}{
		{"wrong prefix", "other/_index/idx/a=1/b=2/m.idx"},
		{"wrong suffix", "store/_index/idx/a=1/b=2/other.txt"},
		{"wrong segment count", "store/_index/idx/a=1/m.idx"},
		{"wrong column name", "store/_index/idx/x=1/b=2/m.idx"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParseIndexMarkerKey(
				tc.key, indexPath, columns,
			); err == nil {
				t.Errorf("expected error, got nil for %q", tc.key)
			}
		})
	}
}

func TestDataPathAndRefPath(t *testing.T) {
	if got := DataPath("pre"); got != "pre/data" {
		t.Errorf("DataPath: got %q", got)
	}
	if got := RefPath("pre"); got != "pre/_stream/refs" {
		t.Errorf("RefPath: got %q", got)
	}
}

// TestRefCutoff guards that the cutoff prefix is lexically less
// than any ref encoded at a timestamp strictly after the cutoff,
// and lexically greater than or equal to any ref encoded at the
// cutoff timestamp. That invariant is what lets Poll compare the
// cutoff against full ref keys byte-wise.
func TestRefCutoff(t *testing.T) {
	const refPath = "p/_stream/refs"
	now := time.UnixMicro(2_000_000_000_000_000)
	settle := 100 * time.Millisecond

	cutoff := RefCutoff(refPath, now, settle)

	// Encoded earlier than cutoff must sort < cutoff.
	earlier := EncodeRefKey(refPath,
		now.Add(-settle).Add(-time.Second).UnixMicro(),
		"abcd1234",
		now.Add(-settle).Add(-time.Second).UnixMicro(),
		"period=X/customer=y")
	if earlier >= cutoff {
		t.Errorf("earlier ref %q should sort before cutoff %q",
			earlier, cutoff)
	}

	// Encoded later than cutoff must sort > cutoff.
	later := EncodeRefKey(refPath,
		now.UnixMicro(),
		"abcd1234",
		now.UnixMicro(),
		"period=X/customer=y")
	if later <= cutoff {
		t.Errorf("later ref %q should sort after cutoff %q",
			later, cutoff)
	}
}

func TestValidateIdempotencyToken(t *testing.T) {
	cases := []struct {
		name    string
		token   string
		wantErr bool
	}{
		{"ok simple", "batch42", false},
		{"ok with dashes", "2026-04-22T10:15:00Z-batch42", false},
		{"ok with digits", "job.1234567890", false},
		{"ok max length", strings.Repeat("a", 200), false},
		{"empty", "", true},
		{"too long", strings.Repeat("a", 201), true},
		{"contains slash", "ns/job42", true},
		{"contains dotdot", "job..42", true},
		{"contains space", "job 42", true},
		{"contains tab", "job\t42", true},
		{"contains null", "job\x0042", true},
		{"contains unicode", "jöb42", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateIdempotencyToken(tc.token)
			if tc.wantErr && err == nil {
				t.Errorf("want error for %q, got nil", tc.token)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("want nil error for %q, got %v",
					tc.token, err)
			}
		})
	}
}

func TestExtractRefID(t *testing.T) {
	const refPath = "test/_stream/refs"
	cases := []struct {
		name    string
		id      string
		wantErr bool
	}{
		{"library-shortID", "a1b2c3d4", false},
		{"caller-token", "2026-04-22T10:15:00Z-batch42", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Build a ref key and verify ExtractRefID round-trips
			// the id field (shortID slot).
			key := EncodeRefKey(
				refPath, 1_000_000, tc.id, 999_999,
				"period=X/customer=alice")
			got, err := ExtractRefID(key)
			if tc.wantErr && err == nil {
				t.Fatalf("want error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.id {
				t.Errorf("ExtractRefID = %q, want %q", got, tc.id)
			}
		})
	}
}

func TestExtractRefID_InvalidKey(t *testing.T) {
	_, err := ExtractRefID("not-a-ref-key")
	if err == nil {
		t.Fatal("want error on unparseable ref key")
	}
}

func TestRefRangeForRetry(t *testing.T) {
	const refPath = "test/_stream/refs"
	// Pick a reference instant far from the epoch so we can see
	// the subtraction in the lower bound clearly.
	now := time.Date(2026, 4, 22, 10, 15, 0, 0, time.UTC)

	lo, hi := RefRangeForRetry(refPath, now, time.Hour)
	loExpected := refPath + "/" +
		itoa(now.Add(-time.Hour).UnixMicro())
	hiExpected := refPath + "/" + itoa(now.UnixMicro())
	if lo != loExpected {
		t.Errorf("lo = %q, want %q", lo, loExpected)
	}
	if hi != hiExpected {
		t.Errorf("hi = %q, want %q", hi, hiExpected)
	}

	// A ref published at now-30m must sort into [lo, hi].
	within := EncodeRefKey(refPath,
		now.Add(-30*time.Minute).UnixMicro(),
		"abc12345",
		now.Add(-30*time.Minute).UnixMicro(),
		"period=X/customer=y")
	if within < lo || within > hi {
		t.Errorf("ref inside window %q sorted outside [%q, %q]",
			within, lo, hi)
	}

	// A ref published two hours before now (outside the 1h
	// window) must sort below lo.
	before := EncodeRefKey(refPath,
		now.Add(-2*time.Hour).UnixMicro(),
		"abc12345",
		now.Add(-2*time.Hour).UnixMicro(),
		"period=X/customer=y")
	if before >= lo {
		t.Errorf("ref before window %q should sort below lo %q",
			before, lo)
	}
}

func itoa(n int64) string {
	return strconv.FormatInt(n, 10)
}
