package s3store

import (
	"testing"
	"time"
)

func TestRefKeyRoundTrip(t *testing.T) {
	const refPath = "test-prefix/_ref"
	const dataLM int64 = 1710684000000000
	const tsMicros int64 = 1710683999500000
	const shortID = "a3f2e1b4"

	cases := []struct {
		name  string
		key   string
		token string
	}{
		{"simple, no token", "period=2026-03-17/customer=abc", ""},
		{"simple, with token", "period=2026-03-17/customer=abc", "tok42"},
		{"hyphen in value", "period=2026-03-17/customer=foo-bar", "tok42"},
		{"hyphen in token", "period=X/customer=y", "2026-04-22T10:15:00Z-batch42"},
		{"semicolon in value", "period=X/customer=a;b", ""},
		{"percent in value", "period=X/customer=50%off", "tok42"},
		{"slash in value", "period=X/customer=a/b", ""},
		{"question mark in value", "period=X/customer=who?", "tok42"},
		{"unicode", "period=X/customer=日本", ""},
		{"space in value", "period=X/customer=hello world", "tok42"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			encoded := encodeRefKey(refPath, dataLM, tsMicros,
				shortID, tc.token, tc.key)

			gotKey, gotDataLM, gotTs, gotShort, gotTok, err := parseRefKey(encoded)
			if err != nil {
				t.Fatalf("parseRefKey(%q): %v", encoded, err)
			}
			if gotKey != tc.key {
				t.Errorf("key: got %q want %q", gotKey, tc.key)
			}
			if gotDataLM != dataLM {
				t.Errorf("dataLM: got %d want %d", gotDataLM, dataLM)
			}
			if gotTs != tsMicros {
				t.Errorf("tsMicros: got %d want %d", gotTs, tsMicros)
			}
			if gotShort != shortID {
				t.Errorf("shortID: got %q want %q", gotShort, shortID)
			}
			if gotTok != tc.token {
				t.Errorf("token: got %q want %q", gotTok, tc.token)
			}
		})
	}
}

// TestRefKeyLexicalOrdering guards that ref filenames sort in
// timestamp order via plain byte comparison: dataLM is the
// primary lex key (Poll's refCutoff relies on this), and tsMicros
// is the within-second tiebreaker so refs from same-second
// writes have stable sub-second order.
func TestRefKeyLexicalOrdering(t *testing.T) {
	const refPath = "test-prefix/_ref"
	const key = "period=2026-03-17/customer=abc"

	// Primary axis: dataLM. Different dataLMs sort by dataLM
	// regardless of tsMicros.
	dataLMs := []int64{
		1_000_000_000_000_000,
		1_710_684_000_000_000,
		9_000_000_000_000_000,
	}
	var byLM []string
	for _, lm := range dataLMs {
		byLM = append(byLM,
			encodeRefKey(refPath, lm, lm-1, "abcd1234", "", key))
	}
	for i := 0; i < len(byLM)-1; i++ {
		if byLM[i] >= byLM[i+1] {
			t.Errorf("lexical order violated by dataLM:\n %q\n !< %q",
				byLM[i], byLM[i+1])
		}
	}

	// Secondary axis: tsMicros tiebreaker within same dataLM.
	const sameLM int64 = 1_710_684_000_000_000
	a := encodeRefKey(refPath, sameLM, 1_710_684_000_000_111,
		"aaaaaaaa", "", key)
	b := encodeRefKey(refPath, sameLM, 1_710_684_000_000_999,
		"bbbbbbbb", "", key)
	if a >= b {
		t.Errorf("same-dataLM tsMicros ordering: %q !< %q", a, b)
	}

	// Token writes and auto writes share the same lex-relevant
	// dataLM-tsMicros-shortID prefix. Within-prefix ordering
	// (auto vs token) is implementation-defined and not part of
	// the Poll contract — Poll's refCutoff only inspects the
	// 16-digit dataLM portion.
}

func TestParseRefKeyInvalid(t *testing.T) {
	cases := []string{
		"not-a-ref-key",
		"refs/garbage.ref",
		"refs/1710684000000000.ref",                                          // no separator
		"refs/1710684000000000;period=X.ref",                                 // pre-sep too short
		"refs/1710684000000000-1710683999500000;period=X.ref",                // missing shortID
		"refs/notanumber-1710683999500000-abcd1234;period=X.ref",             // non-numeric dataLM
		"refs/1710684000000000-notanumber-abcd1234;period=X.ref",             // non-numeric tsMicros
		"refs/1710684000000000-1710683999500000-abcd1234;period=X%ZZabc.ref", // invalid percent escape
	}
	for _, raw := range cases {
		t.Run(raw, func(t *testing.T) {
			if _, _, _, _, _, err := parseRefKey(raw); err == nil {
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
	const refPath = "p/_ref"
	now := time.UnixMicro(2_000_000_000_000_000)
	settle := 100 * time.Millisecond

	cutoff := refCutoff(refPath, now, settle)

	earlierTs := now.Add(-settle).Add(-time.Second).UnixMicro()
	earlier := encodeRefKey(refPath, earlierTs, earlierTs,
		"abcd1234", "", "period=X/customer=y")
	if earlier >= cutoff {
		t.Errorf("earlier ref %q should sort before cutoff %q",
			earlier, cutoff)
	}

	later := encodeRefKey(refPath, now.UnixMicro(), now.UnixMicro(),
		"abcd1234", "", "period=X/customer=y")
	if later <= cutoff {
		t.Errorf("later ref %q should sort after cutoff %q",
			later, cutoff)
	}
}

// TestParseID covers the round-trip and corner cases of the
// data-file id helpers shared by the writer, the reader, and
// the upfront-LIST dedup gate.
func TestParseID(t *testing.T) {
	cases := []struct {
		name      string
		token     string
		tsMicros  int64
		shortID   string
		wantParse bool // true = should round-trip
	}{
		{"auto-id (no token)", "", 1_700_000_000_000_000, "deadbeef", true},
		{"with token", "tok42", 1_700_000_000_000_000, "deadbeef", true},
		{"token with dashes", "2026-04-22T10:15:00Z-batch42",
			1_700_000_000_000_000, "abcdef01", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id := makeID(tc.token, tc.tsMicros, tc.shortID)
			gotTok, gotTs, gotShort, err := parseID(id)
			if !tc.wantParse {
				if err == nil {
					t.Errorf("parseID(%q): want error", id)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseID(%q): %v", id, err)
			}
			if gotTok != tc.token || gotTs != tc.tsMicros ||
				gotShort != tc.shortID {
				t.Errorf("round-trip drift: id=%q parsed=(%q, %d, %q), want (%q, %d, %q)",
					id, gotTok, gotTs, gotShort,
					tc.token, tc.tsMicros, tc.shortID)
			}
		})
	}
}

func TestParseIDInvalid(t *testing.T) {
	cases := []string{
		"deadbeef",                     // too short, no separator
		"1234-deadbeef",                // tsMicros wrong width
		"17000000000000-deadbeef",      // tsMicros 14 digits
		"1700000000000000-deadbe",      // shortID 6 chars
		"1700000000000000-DEADBEEF",    // shortID uppercase (we require lowercase)
		"foo1700000000000000-deadbeef", // missing - between token and tsMicros
		"170000000000000a-deadbeef",    // non-digit in tsMicros
		"1700000000000000-deadbeeg",    // non-hex in shortID
	}
	for _, raw := range cases {
		t.Run(raw, func(t *testing.T) {
			if _, _, _, err := parseID(raw); err == nil {
				t.Errorf("parseID(%q): want error", raw)
			}
		})
	}
}
