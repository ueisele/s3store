package s3store

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

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
			err := validateIdempotencyToken(tc.token)
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

// TestApplyIdempotentRead_NoTokenNoFilter guards the
// fast-path: empty token means the helper returns its input.
func TestApplyIdempotentRead_NoTokenNoFilter(t *testing.T) {
	in := []KeyMeta{
		{Key: "p/data/period=A/1.parquet", InsertedAt: time.UnixMicro(1)},
	}
	got := applyIdempotentRead(in, "p/data", "")
	if len(got) != len(in) {
		t.Fatalf("empty token filtered: got %d, want %d", len(got), len(in))
	}
}

// TestApplyIdempotentRead_FirstAttempt guards that when no file
// in any partition matches the token (i.e. first attempt), the
// full list passes through unchanged.
func TestApplyIdempotentRead_FirstAttempt(t *testing.T) {
	now := time.UnixMicro(100)
	in := []KeyMeta{
		{Key: "p/data/period=A/other-1.parquet", InsertedAt: now},
		{Key: "p/data/period=B/other-2.parquet", InsertedAt: now},
	}
	got := applyIdempotentRead(in, "p/data", "tok42")
	if len(got) != 2 {
		t.Fatalf("no token match expected pass-through, got %d", len(got))
	}
}

// tokenAttempt builds a per-attempt data file basename of shape
// "{token}-{tsMicros}-{shortID}.parquet" — the format the writer
// produces under WithIdempotencyToken (see Phase 4 plan). tsMicros
// is rendered as 16 fixed-width decimal digits (the operating-
// range invariant refTsKey relies on); shortID is supplied by the
// caller so multiple attempts in one partition are distinct.
func tokenAttempt(token string, tsMicros int64, shortID string) string {
	return fmt.Sprintf("%s-%016d-%s.parquet", token, tsMicros, shortID)
}

// TestApplyIdempotentRead_SelfExclusion guards that token-matching
// files are dropped even when no other files in the partition
// would be filtered.
func TestApplyIdempotentRead_SelfExclusion(t *testing.T) {
	now := time.UnixMicro(100)
	earlier := now.Add(-time.Second)
	tokFile := "p/data/period=A/" + tokenAttempt("tok42", 100, "deadbeef")
	in := []KeyMeta{
		{Key: tokFile, InsertedAt: now},
		{Key: "p/data/period=A/x.parquet", InsertedAt: earlier},
	}
	got := applyIdempotentRead(in, "p/data", "tok42")
	if len(got) != 1 {
		t.Fatalf("got %d, want 1 (pre-barrier only)", len(got))
	}
	if got[0].Key != "p/data/period=A/x.parquet" {
		t.Errorf("wrong survivor: %q", got[0].Key)
	}
}

// TestApplyIdempotentRead_LaterWriteExclusion exercises both
// filters: the barrier is the own-file's LastModified, and a
// later file in the same partition is excluded.
func TestApplyIdempotentRead_LaterWriteExclusion(t *testing.T) {
	ownTs := time.UnixMicro(1_000)
	laterTs := time.UnixMicro(2_000)
	earlierTs := time.UnixMicro(500)

	tokFile := "p/data/period=A/" + tokenAttempt("tok42", 1000, "deadbeef")
	in := []KeyMeta{
		{Key: tokFile, InsertedAt: ownTs},
		{Key: "p/data/period=A/later.parquet", InsertedAt: laterTs},
		{Key: "p/data/period=A/earlier.parquet", InsertedAt: earlierTs},
	}
	got := applyIdempotentRead(in, "p/data", "tok42")

	if len(got) != 1 {
		t.Fatalf("got %d survivors, want 1", len(got))
	}
	if got[0].Key != "p/data/period=A/earlier.parquet" {
		t.Errorf("wrong survivor: %q", got[0].Key)
	}
}

// TestApplyIdempotentRead_PerPartitionIsolation guards that
// each partition's barrier is computed independently.
func TestApplyIdempotentRead_PerPartitionIsolation(t *testing.T) {
	ownTs := time.UnixMicro(1_000)
	laterTs := time.UnixMicro(2_000)

	tokFile := "p/data/period=A/" + tokenAttempt("tok42", 1000, "deadbeef")
	in := []KeyMeta{
		{Key: tokFile, InsertedAt: ownTs},
		{Key: "p/data/period=A/blocked.parquet", InsertedAt: laterTs},
		{Key: "p/data/period=B/unfiltered.parquet", InsertedAt: laterTs},
	}
	got := applyIdempotentRead(in, "p/data", "tok42")

	if len(got) != 1 {
		t.Fatalf("got %d, want 1", len(got))
	}
	if got[0].Key != "p/data/period=B/unfiltered.parquet" {
		t.Errorf("wrong survivor: %q", got[0].Key)
	}
}

// TestApplyIdempotentRead_MultipleOwnAttempts handles the case
// where the same token produced two per-attempt files in the same
// partition (Phase 4 retry overlap). The barrier is
// min(LastModified of own files) so the earliest attempt defines
// the cutoff.
func TestApplyIdempotentRead_MultipleOwnAttempts(t *testing.T) {
	t1 := time.UnixMicro(1_000)
	t2 := time.UnixMicro(2_000)

	pre := time.UnixMicro(500)
	atBar := time.UnixMicro(1_000)
	postBar := time.UnixMicro(1_500)

	in := []KeyMeta{
		{Key: "p/data/period=A/" + tokenAttempt("tok42", 1000, "11111111"), InsertedAt: t1},
		{Key: "p/data/period=A/" + tokenAttempt("tok42", 2000, "22222222"), InsertedAt: t2},
		{Key: "p/data/period=A/pre.parquet", InsertedAt: pre},
		{Key: "p/data/period=A/at.parquet", InsertedAt: atBar},
		{Key: "p/data/period=A/post.parquet", InsertedAt: postBar},
	}
	got := applyIdempotentRead(in, "p/data", "tok42")

	if len(got) != 1 {
		t.Fatalf("got %d, want 1", len(got))
	}
	if got[0].Key != "p/data/period=A/pre.parquet" {
		t.Errorf("wrong survivor: %q", got[0].Key)
	}
}

// TestApplyIdempotentRead_NonDataFileKeysPassThrough guards that
// non-data-file keys (shouldn't appear at this stage, but defend)
// pass through unfiltered so the helper doesn't mask an upstream
// bug.
func TestApplyIdempotentRead_NonDataFileKeysPassThrough(t *testing.T) {
	tokFile := "p/data/period=A/" + tokenAttempt("tok42", 1000, "deadbeef")
	in := []KeyMeta{
		{Key: tokFile, InsertedAt: time.UnixMicro(1_000)},
		{Key: "p/data/period=A/blocked.parquet", InsertedAt: time.UnixMicro(2_000)},
		{Key: "some/other/path.txt", InsertedAt: time.UnixMicro(5_000)},
	}
	got := applyIdempotentRead(in, "p/data", "tok42")

	if len(got) != 1 {
		t.Fatalf("got %d, want 1", len(got))
	}
	if got[0].Key != "some/other/path.txt" {
		t.Errorf("wrong survivor: %q", got[0].Key)
	}
}

// TestApplyIdempotentRead_TokenWithDashes guards that token
// values containing dashes are matched correctly under per-
// attempt-paths: the matcher anchors on the trailing
// "-{16 digits}-{8 hex}.parquet" suffix, so a dashy token doesn't
// confuse the auto-id portion.
func TestApplyIdempotentRead_TokenWithDashes(t *testing.T) {
	t1 := time.UnixMicro(1_000)
	tLater := time.UnixMicro(2_000)
	token := "2026-04-22T10:15:00Z-batch42"

	in := []KeyMeta{
		{Key: "p/data/period=A/" + tokenAttempt(token, 1000, "deadbeef"), InsertedAt: t1},
		{Key: "p/data/period=A/blocked.parquet", InsertedAt: tLater},
	}
	got := applyIdempotentRead(in, "p/data", token)

	if len(got) != 0 {
		t.Fatalf("got %d survivors, want 0 (both filtered)", len(got))
	}
}

// TestDataFileBasenameMatchesToken pins the per-attempt-id shape
// matcher down to its corner cases. Critical for the
// applyIdempotentRead behaviour above and for any future caller
// that needs to recognise "this data file belongs to {token}".
func TestDataFileBasenameMatchesToken(t *testing.T) {
	cases := []struct {
		base  string
		token string
		want  bool
	}{
		// Happy paths.
		{tokenAttempt("tok42", 1700_000_000_000_000, "deadbeef"), "tok42", true},
		{tokenAttempt("2026-04-22T10:15:00Z-batch42", 1700_000_000_000_000, "abcdef01"),
			"2026-04-22T10:15:00Z-batch42", true},

		// Wrong token (prefix mismatch).
		{tokenAttempt("tok42", 1700_000_000_000_000, "deadbeef"), "tok99", false},
		// Wrong suffix shape: bare token (Phase 3 shape, not Phase 4).
		{"tok42.parquet", "tok42", false},
		// Wrong suffix shape: missing shortID.
		{"tok42-1700000000000000.parquet", "tok42", false},
		// Wrong tsMicros width (15 digits instead of 16).
		{"tok42-170000000000000-deadbeef.parquet", "tok42", false},
		// Wrong shortID width (7 chars).
		{"tok42-1700000000000000-deadbee.parquet", "tok42", false},
		// Non-hex chars in shortID.
		{"tok42-1700000000000000-deadbeeg.parquet", "tok42", false},
		// Non-digit in tsMicros.
		{"tok42-170000000000000a-deadbeef.parquet", "tok42", false},
		// Wrong extension.
		{"tok42-1700000000000000-deadbeef.txt", "tok42", false},
	}
	for _, tc := range cases {
		t.Run(tc.base, func(t *testing.T) {
			got := dataFileBasenameMatchesToken(tc.base, tc.token)
			if got != tc.want {
				t.Errorf("dataFileBasenameMatchesToken(%q, %q) = %v, want %v",
					tc.base, tc.token, got, tc.want)
			}
		})
	}
}
