package core

import (
	"testing"
	"time"
)

// TestApplyIdempotentRead_NoTokenNoFilter guards the
// fast-path: empty token means the helper returns its input.
func TestApplyIdempotentRead_NoTokenNoFilter(t *testing.T) {
	in := []KeyMeta{
		{Key: "p/data/period=A/1.parquet", InsertedAt: time.UnixMicro(1)},
	}
	got := ApplyIdempotentRead(in, "p/data", "")
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
	got := ApplyIdempotentRead(in, "p/data", "tok42")
	if len(got) != 2 {
		t.Fatalf("no token match expected pass-through, got %d", len(got))
	}
}

// TestApplyIdempotentRead_SelfExclusion guards that token-matching
// files are dropped even when no other files in the partition
// would be filtered.
func TestApplyIdempotentRead_SelfExclusion(t *testing.T) {
	now := time.UnixMicro(100)
	earlier := now.Add(-time.Second)
	in := []KeyMeta{
		{Key: "p/data/period=A/tok42.parquet", InsertedAt: now},
		{Key: "p/data/period=A/x.parquet", InsertedAt: earlier}, // pre-barrier → kept
	}
	got := ApplyIdempotentRead(in, "p/data", "tok42")
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

	in := []KeyMeta{
		{Key: "p/data/period=A/tok42.parquet", InsertedAt: ownTs},
		{Key: "p/data/period=A/later.parquet", InsertedAt: laterTs},
		{Key: "p/data/period=A/earlier.parquet", InsertedAt: earlierTs},
	}
	got := ApplyIdempotentRead(in, "p/data", "tok42")

	if len(got) != 1 {
		t.Fatalf("got %d survivors, want 1", len(got))
	}
	if got[0].Key != "p/data/period=A/earlier.parquet" {
		t.Errorf("wrong survivor: %q", got[0].Key)
	}
}

// TestApplyIdempotentRead_PerPartitionIsolation guards that
// each partition's barrier is computed independently: partition
// A has a token; partition B does not. B's files flow through.
func TestApplyIdempotentRead_PerPartitionIsolation(t *testing.T) {
	ownTs := time.UnixMicro(1_000)
	laterTs := time.UnixMicro(2_000)

	in := []KeyMeta{
		{Key: "p/data/period=A/tok42.parquet", InsertedAt: ownTs},
		{Key: "p/data/period=A/blocked.parquet", InsertedAt: laterTs},
		{Key: "p/data/period=B/unfiltered.parquet", InsertedAt: laterTs},
	}
	got := ApplyIdempotentRead(in, "p/data", "tok42")

	if len(got) != 1 {
		t.Fatalf("got %d, want 1", len(got))
	}
	if got[0].Key != "p/data/period=B/unfiltered.parquet" {
		t.Errorf("wrong survivor: %q", got[0].Key)
	}
}

// TestApplyIdempotentRead_MultipleOwnAttempts handles the case
// where the same token produced two files in the same partition
// (e.g. at-least-once retry without overwrite-prevention). The
// barrier is min(LastModified of own files) so the earliest
// attempt defines the cutoff, and the later attempt is also
// self-excluded.
func TestApplyIdempotentRead_MultipleOwnAttempts(t *testing.T) {
	t1 := time.UnixMicro(1_000)
	t2 := time.UnixMicro(2_000)

	// A pre-barrier record (< t1) should survive; one record at
	// the barrier, one after it — both must go.
	pre := time.UnixMicro(500)
	atBar := time.UnixMicro(1_000)
	postBar := time.UnixMicro(1_500)

	in := []KeyMeta{
		{Key: "p/data/period=A/tok42.parquet", InsertedAt: t1},
		// At-least-once retry produced another token file at t2.
		// With overwrite-prevention this is impossible, but defend
		// against it anyway — ApplyIdempotentRead is a pure filter.
		{Key: "p/data/period=A/tok42.parquet", InsertedAt: t2},
		{Key: "p/data/period=A/pre.parquet", InsertedAt: pre},
		{Key: "p/data/period=A/at.parquet", InsertedAt: atBar},
		{Key: "p/data/period=A/post.parquet", InsertedAt: postBar},
	}
	got := ApplyIdempotentRead(in, "p/data", "tok42")

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
	in := []KeyMeta{
		{Key: "p/data/period=A/tok42.parquet", InsertedAt: time.UnixMicro(1_000)},
		{Key: "p/data/period=A/blocked.parquet", InsertedAt: time.UnixMicro(2_000)},
		{Key: "some/other/path.txt", InsertedAt: time.UnixMicro(5_000)},
	}
	got := ApplyIdempotentRead(in, "p/data", "tok42")

	// Only "some/other/path.txt" should survive among non-token,
	// non-partition entries; "blocked.parquet" is dropped.
	if len(got) != 1 {
		t.Fatalf("got %d, want 1", len(got))
	}
	if got[0].Key != "some/other/path.txt" {
		t.Errorf("wrong survivor: %q", got[0].Key)
	}
}

// TestApplyIdempotentRead_TokenWithDashes guards that token
// values containing dashes (e.g. the "{ISO-time}-{suffix}"
// convention callers often use) are matched via exact basename
// comparison rather than any parsing that might misread the
// internal dashes.
func TestApplyIdempotentRead_TokenWithDashes(t *testing.T) {
	t1 := time.UnixMicro(1_000)
	tLater := time.UnixMicro(2_000)
	token := "2026-04-22T10:15:00Z-batch42"

	in := []KeyMeta{
		{Key: "p/data/period=A/" + token + ".parquet", InsertedAt: t1},
		{Key: "p/data/period=A/blocked.parquet", InsertedAt: tLater},
	}
	got := ApplyIdempotentRead(in, "p/data", token)

	if len(got) != 0 {
		t.Fatalf("got %d survivors, want 0 (both filtered)", len(got))
	}
}
