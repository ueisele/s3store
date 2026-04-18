package refstream

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ueisele/s3store/internal/core"
)

// TestPollAll_DrainsUntilEmpty guards the loop contract: PollAll
// keeps calling its callback, advancing since to the next offset,
// until the callback returns an empty batch. The callback is
// responsible for honouring any upper bound — PollAll treats
// "empty batch" as the sole termination signal.
func TestPollAll_DrainsUntilEmpty(t *testing.T) {
	// Three batches of two records, then an empty batch.
	batches := [][]int{
		{1, 2},
		{3, 4},
		{5, 6},
		{},
	}
	offsets := []core.Offset{"o1", "o2", "o3", "o3"}

	var callCount int
	poll := func(
		_ context.Context, since core.Offset, max int32,
	) ([]int, core.Offset, error) {
		if max != PollAllBatch {
			t.Errorf("callback called with max=%d, want %d",
				max, PollAllBatch)
		}
		// Check since advances: after first call it should be o1, etc.
		if callCount > 0 && since != offsets[callCount-1] {
			t.Errorf("call %d: since=%q, want %q",
				callCount, since, offsets[callCount-1])
		}
		b := batches[callCount]
		off := offsets[callCount]
		callCount++
		return b, off, nil
	}

	got, err := PollAll(context.Background(), "", poll)
	if err != nil {
		t.Fatalf("PollAll: %v", err)
	}

	want := []int{1, 2, 3, 4, 5, 6}
	if len(got) != len(want) {
		t.Fatalf("got %d items, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("got[%d]=%d, want %d", i, got[i], want[i])
		}
	}
	if callCount != len(batches) {
		t.Errorf("callback called %d times, want %d",
			callCount, len(batches))
	}
}

// TestPollAll_PropagatesError guards that a callback error stops
// the loop and is returned unwrapped to the caller. Partial
// results accumulated before the error are discarded (caller
// retries with the last known-good offset externally).
func TestPollAll_PropagatesError(t *testing.T) {
	boom := errors.New("boom")
	var callCount int
	poll := func(
		_ context.Context, _ core.Offset, _ int32,
	) ([]int, core.Offset, error) {
		callCount++
		if callCount == 1 {
			return []int{1, 2}, "o1", nil
		}
		return nil, "", boom
	}

	got, err := PollAll(context.Background(), "", poll)
	if !errors.Is(err, boom) {
		t.Errorf("got err=%v, want %v", err, boom)
	}
	if got != nil {
		t.Errorf("got %v, want nil on error", got)
	}
}

// TestPollAll_EmptyFromStart handles the no-op case: callback
// returns empty immediately, PollAll returns nil without looping.
func TestPollAll_EmptyFromStart(t *testing.T) {
	var callCount int
	poll := func(
		_ context.Context, _ core.Offset, _ int32,
	) ([]int, core.Offset, error) {
		callCount++
		return nil, "", nil
	}

	got, err := PollAll(context.Background(), "since-x", poll)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("got %v, want nil", got)
	}
	if callCount != 1 {
		t.Errorf("callback called %d times, want 1", callCount)
	}
}

// TestOffsetAt guards the pure-computation part of the ref-stream
// helpers: OffsetAt produces an offset that sorts before any ref
// written strictly after t and at-or-after any ref written at or
// before t. The actual RefCutoff / EncodeRefKey invariant is
// tested in internal/core, so here we just check the contract is
// preserved through OffsetAt's thin wrapper.
func TestOffsetAt(t *testing.T) {
	const refPath = "p/_stream/refs"
	const hiveKey = "period=X/customer=Y"

	now := time.UnixMicro(2_000_000_000_000_000)
	offset := OffsetAt(refPath, now)

	earlier := core.EncodeRefKey(refPath,
		now.Add(-time.Second).UnixMicro(), "abcd1234", hiveKey)
	if string(offset) <= earlier {
		t.Errorf("offset %q should sort after earlier ref %q",
			offset, earlier)
	}

	later := core.EncodeRefKey(refPath,
		now.Add(time.Second).UnixMicro(), "abcd1234", hiveKey)
	if string(offset) >= later {
		t.Errorf("offset %q should sort before later ref %q",
			offset, later)
	}
}
