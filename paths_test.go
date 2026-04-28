package s3store

import (
	"strings"
	"testing"
	"time"
)

const (
	testAttemptIDA = "0190a1b23c4d7e5f8a9bcdef01234567"
	testAttemptIDB = "0190a1b23c4d7e5f8a9bcdef01234568"
)

func TestRefKeyRoundTrip(t *testing.T) {
	const refPath = "test-prefix/_ref"
	const refMicroTs int64 = 1710684000000000

	cases := []struct {
		name      string
		token     string
		attemptID string
		hiveKey   string
	}{
		{"idempotent simple", "tok42", testAttemptIDA,
			"period=2026-03-17/customer=abc"},
		{"idempotent, hyphen in value", "tok42", testAttemptIDA,
			"period=2026-03-17/customer=foo-bar"},
		{"idempotent, hyphen in token", "2026-04-22T10:15:00Z-batch42",
			testAttemptIDA, "period=X/customer=y"},
		{"idempotent, semicolon in value (escaped)", "tok42", testAttemptIDA,
			"period=X/customer=a;b"},
		{"idempotent, percent in value", "tok42", testAttemptIDA,
			"period=X/customer=50%off"},
		{"idempotent, slash in value", "tok42", testAttemptIDA,
			"period=X/customer=a/b"},
		{"idempotent, question mark in value", "tok42", testAttemptIDA,
			"period=X/customer=who?"},
		{"idempotent, unicode in value", "tok42", testAttemptIDA,
			"period=X/customer=日本"},
		{"idempotent, space in value", "tok42", testAttemptIDA,
			"period=X/customer=hello world"},
		// Auto-token (token == attemptID, same UUIDv7 used for both)
		// — the path stays uniform, parser still has one case.
		{"auto-token (token == attemptID)", testAttemptIDA, testAttemptIDA,
			"period=2026-03-17/customer=abc"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			encoded := encodeRefKey(refPath, refMicroTs,
				tc.token, tc.attemptID, tc.hiveKey)

			gotHive, gotTs, gotTok, gotAtt, err := parseRefKey(encoded)
			if err != nil {
				t.Fatalf("parseRefKey(%q): %v", encoded, err)
			}
			if gotHive != tc.hiveKey {
				t.Errorf("hiveKey: got %q want %q", gotHive, tc.hiveKey)
			}
			if gotTs != refMicroTs {
				t.Errorf("refMicroTs: got %d want %d", gotTs, refMicroTs)
			}
			if gotTok != tc.token {
				t.Errorf("token: got %q want %q", gotTok, tc.token)
			}
			if gotAtt != tc.attemptID {
				t.Errorf("attemptID: got %q want %q", gotAtt, tc.attemptID)
			}
		})
	}
}

// TestRefKeyLexicalOrdering guards that ref filenames sort in
// timestamp order via plain byte comparison: refMicroTs is the
// primary lex key, and Poll's refCutoff relies on this for the
// settle-window cutoff to be a clean prefix bound.
func TestRefKeyLexicalOrdering(t *testing.T) {
	const refPath = "test-prefix/_ref"
	const hiveKey = "period=2026-03-17/customer=abc"

	timestamps := []int64{
		1_000_000_000_000_000,
		1_710_684_000_000_000,
		9_000_000_000_000_000,
	}
	var byTs []string
	for _, ts := range timestamps {
		byTs = append(byTs,
			encodeRefKey(refPath, ts, "tok42", testAttemptIDA, hiveKey))
	}
	for i := 0; i < len(byTs)-1; i++ {
		if byTs[i] >= byTs[i+1] {
			t.Errorf("lexical order violated by refMicroTs:\n %q\n !< %q",
				byTs[i], byTs[i+1])
		}
	}

	// Within the same refMicroTs, ordering between concurrent
	// writers' refs is implementation-defined (depends on
	// alphanumeric collation of token + attemptID); the change-
	// stream contract already tolerates this via SettleWindow.
	// Refs at the same refMicroTs must at least be distinct keys
	// when token or attemptID differs.
	const sameTs int64 = 1_710_684_000_000_000
	a := encodeRefKey(refPath, sameTs, "tok42", testAttemptIDA, hiveKey)
	b := encodeRefKey(refPath, sameTs, "tok42", testAttemptIDB, hiveKey)
	if a == b {
		t.Errorf("distinct attemptIDs produced identical ref keys: %q", a)
	}
}

func TestParseRefKeyInvalid(t *testing.T) {
	cases := []string{
		"not-a-ref-key",
		"refs/garbage.ref",
		"refs/1710684000000000.ref",          // no separator
		"refs/1710684000000000;period=X.ref", // pre-sep too short
		"refs/1710684000000000-tok-" + testAttemptIDA[:30] + // attemptID too short
			";period=X.ref",
		"refs/1710684000000000-tok-" + testAttemptIDA + "Z" + // attemptID has non-hex
			";period=X.ref",
		"refs/notanumber0000000-tok-" + testAttemptIDA + // refMicroTs non-digit
			";period=X.ref",
		// missing token segment between refMicroTs and attemptID
		"refs/1710684000000000-" + testAttemptIDA + ";period=X.ref",
		// invalid percent escape in hiveKey
		"refs/1710684000000000-tok-" + testAttemptIDA + ";period=X%ZZabc.ref",
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
	const refPath = "p/_ref"
	now := time.UnixMicro(2_000_000_000_000_000)
	settle := 100 * time.Millisecond

	cutoff := refCutoff(refPath, now, settle)

	earlierTs := now.Add(-settle).Add(-time.Second).UnixMicro()
	earlier := encodeRefKey(refPath, earlierTs,
		"tok42", testAttemptIDA, "period=X/customer=y")
	if earlier >= cutoff {
		t.Errorf("earlier ref %q should sort before cutoff %q",
			earlier, cutoff)
	}

	later := encodeRefKey(refPath, now.UnixMicro(),
		"tok42", testAttemptIDA, "period=X/customer=y")
	if later <= cutoff {
		t.Errorf("later ref %q should sort after cutoff %q",
			later, cutoff)
	}
}

// TestParseAttemptID covers the round-trip and corner cases of
// the data-file id helpers shared by the writer, the reader, and
// the upfront-HEAD dedup gate.
func TestParseAttemptID(t *testing.T) {
	cases := []struct {
		name      string
		token     string
		attemptID string
	}{
		{"idempotent token", "tok42", testAttemptIDA},
		{"token with dashes", "2026-04-22T10:15:00Z-batch42", testAttemptIDA},
		{"auto-token (same UUIDv7 in both halves)", testAttemptIDA, testAttemptIDA},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id := makeID(tc.token, tc.attemptID)
			gotTok, gotAtt, err := parseAttemptID(id)
			if err != nil {
				t.Fatalf("parseAttemptID(%q): %v", id, err)
			}
			if gotTok != tc.token || gotAtt != tc.attemptID {
				t.Errorf("round-trip drift: id=%q parsed=(%q, %q), want (%q, %q)",
					id, gotTok, gotAtt, tc.token, tc.attemptID)
			}
		})
	}
}

func TestParseAttemptIDInvalid(t *testing.T) {
	cases := []string{
		"deadbeef",                               // way too short
		"tok-deadbeef",                           // attemptID too short
		"tok-" + testAttemptIDA[:31],             // attemptID 31 chars
		"tok-" + strings.ToUpper(testAttemptIDA), // uppercase hex
		"tok-" + testAttemptIDA + "z",            // 33 chars including non-hex
		"tok" + testAttemptIDA,                   // missing - between token and attemptID
	}
	for _, raw := range cases {
		t.Run(raw, func(t *testing.T) {
			if _, _, err := parseAttemptID(raw); err == nil {
				t.Errorf("parseAttemptID(%q): want error", raw)
			}
		})
	}
}

// TestNewAttemptID confirms the writer-side generator produces a
// 32-char lowercase-hex string (the round-trip-able shape
// parseAttemptID expects). UUIDv7's 48-bit timestamp prefix is
// time-sortable; we don't assert on the value beyond shape.
func TestNewAttemptID(t *testing.T) {
	a, err := newAttemptID()
	if err != nil {
		t.Fatalf("newAttemptID: %v", err)
	}
	if len(a) != attemptIDHexLen {
		t.Errorf("len = %d, want %d", len(a), attemptIDHexLen)
	}
	if !isLowerHex(a) {
		t.Errorf("%q is not lowercase hex", a)
	}
	// Two consecutive calls must produce distinct ids — the random
	// suffix is 74 bits; collisions are practically impossible.
	b, err := newAttemptID()
	if err != nil {
		t.Fatalf("newAttemptID: %v", err)
	}
	if a == b {
		t.Errorf("two newAttemptID calls returned identical %q", a)
	}
}
