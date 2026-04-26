package s3parquet

import (
	"strconv"
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
			encoded := encodeRefKey(refPath, refTs, shortID, dataTs, tc.key)

			gotKey, gotRefTs, gotID, gotDataTs, err := parseRefKey(encoded)
			if err != nil {
				t.Fatalf("parseRefKey(%q): %v", encoded, err)
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
		encoded = append(encoded, encodeRefKey(refPath, ts, "abcd1234", ts-1, key))
	}
	for i := 0; i < len(encoded)-1; i++ {
		if encoded[i] >= encoded[i+1] {
			t.Errorf("lexical order violated:\n %q\n !< %q",
				encoded[i], encoded[i+1])
		}
	}

	a := encodeRefKey(refPath, 1_710_684_000_000_000, "aaaaaaaa", 1_710_683_999_000_000, key)
	b := encodeRefKey(refPath, 1_710_684_000_000_000, "bbbbbbbb", 1_710_683_999_000_000, key)
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
			if _, _, _, _, err := parseRefKey(raw); err == nil {
				t.Errorf("parseRefKey(%q): expected error", raw)
			}
		})
	}
}

// TestRefCutoff guards that the cutoff prefix is lexically less
// than any ref encoded at a timestamp strictly after the cutoff,
// and lexically greater than or equal to any ref encoded at the
// cutoff timestamp.
func TestRefCutoff(t *testing.T) {
	const refPath = "p/_stream/refs"
	now := time.UnixMicro(2_000_000_000_000_000)
	settle := 100 * time.Millisecond

	cutoff := refCutoff(refPath, now, settle)

	earlier := encodeRefKey(refPath,
		now.Add(-settle).Add(-time.Second).UnixMicro(),
		"abcd1234",
		now.Add(-settle).Add(-time.Second).UnixMicro(),
		"period=X/customer=y")
	if earlier >= cutoff {
		t.Errorf("earlier ref %q should sort before cutoff %q",
			earlier, cutoff)
	}

	later := encodeRefKey(refPath,
		now.UnixMicro(),
		"abcd1234",
		now.UnixMicro(),
		"period=X/customer=y")
	if later <= cutoff {
		t.Errorf("later ref %q should sort after cutoff %q",
			later, cutoff)
	}
}

func TestRefRangeForRetry(t *testing.T) {
	const refPath = "test/_stream/refs"
	now := time.Date(2026, 4, 22, 10, 15, 0, 0, time.UTC)

	lo, hi := refRangeForRetry(refPath, now, time.Hour)
	loExpected := refPath + "/" +
		strconv.FormatInt(now.Add(-time.Hour).UnixMicro(), 10)
	hiExpected := refPath + "/" +
		strconv.FormatInt(now.UnixMicro(), 10)
	if lo != loExpected {
		t.Errorf("lo = %q, want %q", lo, loExpected)
	}
	if hi != hiExpected {
		t.Errorf("hi = %q, want %q", hi, hiExpected)
	}

	within := encodeRefKey(refPath,
		now.Add(-30*time.Minute).UnixMicro(),
		"abc12345",
		now.Add(-30*time.Minute).UnixMicro(),
		"period=X/customer=y")
	if within < lo || within > hi {
		t.Errorf("ref inside window %q sorted outside [%q, %q]",
			within, lo, hi)
	}

	before := encodeRefKey(refPath,
		now.Add(-2*time.Hour).UnixMicro(),
		"abc12345",
		now.Add(-2*time.Hour).UnixMicro(),
		"period=X/customer=y")
	if before >= lo {
		t.Errorf("ref before window %q should sort below lo %q",
			before, lo)
	}
}
