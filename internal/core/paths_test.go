package core

import (
	"testing"
	"time"
)

func TestRefKeyRoundTrip(t *testing.T) {
	const refPath = "test-prefix/_stream/refs"
	const ts int64 = 1710684000000000
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
			encoded := EncodeRefKey(refPath, ts, shortID, tc.key)

			gotKey, gotTs, gotID, err := ParseRefKey(encoded)
			if err != nil {
				t.Fatalf("ParseRefKey(%q): %v", encoded, err)
			}
			if gotKey != tc.key {
				t.Errorf("key: got %q want %q", gotKey, tc.key)
			}
			if gotTs != ts {
				t.Errorf("ts: got %d want %d", gotTs, ts)
			}
			if gotID != shortID {
				t.Errorf("id: got %q want %q", gotID, shortID)
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
		encoded = append(encoded, EncodeRefKey(refPath, ts, "abcd1234", key))
	}
	for i := 0; i < len(encoded)-1; i++ {
		if encoded[i] >= encoded[i+1] {
			t.Errorf("lexical order violated:\n %q\n !< %q",
				encoded[i], encoded[i+1])
		}
	}

	a := EncodeRefKey(refPath, 1_710_684_000_000_000, "aaaaaaaa", key)
	b := EncodeRefKey(refPath, 1_710_684_000_000_000, "bbbbbbbb", key)
	if a >= b {
		t.Errorf("same-ts id ordering: %q !< %q", a, b)
	}
}

func TestParseRefKeyInvalid(t *testing.T) {
	cases := []string{
		"not-a-ref-key",
		"refs/garbage.ref",
		"refs/1710684000000000.ref",                   // no separator
		"refs/1710684000000000;period=X.ref",          // no '-' between ts and id
		"refs/notanumber-id;period=X.ref",             // non-numeric ts
		"refs/1710684000000000-id;period=X%ZZabc.ref", // invalid percent escape
	}
	for _, raw := range cases {
		t.Run(raw, func(t *testing.T) {
			if _, _, _, err := ParseRefKey(raw); err == nil {
				t.Errorf("ParseRefKey(%q): expected error", raw)
			}
		})
	}
}

func TestBuildDataFilePath(t *testing.T) {
	const dataPath = "test-prefix/data"
	cases := []struct {
		name     string
		key      string
		tsMicros int64
		shortID  string
		want     string
	}{
		{
			name:     "standard key",
			key:      "period=2026-03-17/customer=abc",
			tsMicros: 1710684000000000,
			shortID:  "a3f2e1b4",
			want:     "test-prefix/data/period=2026-03-17/customer=abc/1710684000000000-a3f2e1b4.parquet",
		},
		{
			name:     "value with hyphen",
			key:      "period=X/customer=foo-bar",
			tsMicros: 1710770400000000,
			shortID:  "c7d9f0e2",
			want:     "test-prefix/data/period=X/customer=foo-bar/1710770400000000-c7d9f0e2.parquet",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := BuildDataFilePath(dataPath, tc.key, tc.tsMicros, tc.shortID)
			if got != tc.want {
				t.Errorf("\ngot  %q\nwant %q", got, tc.want)
			}
		})
	}
}

func TestParseDataFileName(t *testing.T) {
	cases := []struct {
		name       string
		in         string
		wantTs     int64
		wantShort  string
		wantErr    bool
	}{
		{
			name:      "standard",
			in:        "1710684000000000-a3f2e1b4.parquet",
			wantTs:    1710684000000000,
			wantShort: "a3f2e1b4",
		},
		{
			name:    "missing dash",
			in:      "a3f2e1b4.parquet",
			wantErr: true,
		},
		{
			name:    "non-numeric ts",
			in:      "notanumber-a3f2e1b4.parquet",
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ts, short, err := ParseDataFileName(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Errorf("expected error for %q, got ts=%d id=%q",
						tc.in, ts, short)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ts != tc.wantTs {
				t.Errorf("ts: got %d want %d", ts, tc.wantTs)
			}
			if short != tc.wantShort {
				t.Errorf("shortID: got %q want %q", short, tc.wantShort)
			}
		})
	}
}

// TestBuildParseDataFileNameRoundTrip guards that
// ParseDataFileName is the inverse of BuildDataFilePath (for
// the filename portion).
func TestBuildParseDataFileNameRoundTrip(t *testing.T) {
	const (
		dataPath = "p/data"
		hiveKey  = "period=X/customer=Y"
		ts       = int64(1_710_684_000_000_000)
		shortID  = "a3f2e1b4"
	)
	full := BuildDataFilePath(dataPath, hiveKey, ts, shortID)
	// strip the dataPath + "/" + hiveKey + "/" prefix to get
	// the filename part.
	filename := full[len(dataPath)+len(hiveKey)+2:]
	gotTs, gotID, err := ParseDataFileName(filename)
	if err != nil {
		t.Fatalf("ParseDataFileName(%q): %v", filename, err)
	}
	if gotTs != ts || gotID != shortID {
		t.Errorf("round-trip: got (ts=%d, id=%q), want (ts=%d, id=%q)",
			gotTs, gotID, ts, shortID)
	}
}

// TestBuildDataFilePathLexicalOrdering guards that two writes
// to the same partition key sort chronologically in S3 LIST by
// filename — the primary reason we embed tsMicros in the data
// filename.
func TestBuildDataFilePathLexicalOrdering(t *testing.T) {
	const (
		dataPath = "p/data"
		key      = "period=X/customer=Y"
	)
	earlier := BuildDataFilePath(dataPath, key, 1_000_000_000_000, "aaaaaaaa")
	later := BuildDataFilePath(dataPath, key, 2_000_000_000_000, "aaaaaaaa")
	if earlier >= later {
		t.Errorf("earlier %q should sort before later %q", earlier, later)
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
		"period=X/customer=y")
	if earlier >= cutoff {
		t.Errorf("earlier ref %q should sort before cutoff %q",
			earlier, cutoff)
	}

	// Encoded later than cutoff must sort > cutoff.
	later := EncodeRefKey(refPath,
		now.UnixMicro(),
		"abcd1234",
		"period=X/customer=y")
	if later <= cutoff {
		t.Errorf("later ref %q should sort after cutoff %q",
			later, cutoff)
	}
}
