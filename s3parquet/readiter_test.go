package s3parquet

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ueisele/s3store/internal/core"
)

func newTestStreamState() *streamState {
	s := &streamState{}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// TestReserveBytes_NoCap verifies the cap=0 fast path returns
// immediately and does not touch bufferedBytes.
func TestReserveBytes_NoCap(t *testing.T) {
	s := newTestStreamState()
	if !s.reserveBytes(context.Background(), 1<<30, 0) {
		t.Fatal("reserveBytes with cap=0 should return true")
	}
	if s.bufferedBytes != 0 {
		t.Errorf("bufferedBytes = %d, want 0 (cap=0 should not reserve)",
			s.bufferedBytes)
	}
}

// TestReserveBytes_FitsImmediately reserves a chunk inside the
// cap and verifies the running total is updated.
func TestReserveBytes_FitsImmediately(t *testing.T) {
	s := newTestStreamState()
	if !s.reserveBytes(context.Background(), 100, 1000) {
		t.Fatal("reserveBytes should succeed when fits")
	}
	if s.bufferedBytes != 100 {
		t.Errorf("bufferedBytes = %d, want 100", s.bufferedBytes)
	}
}

// TestReserveBytes_BlocksUntilRelease verifies the gate: a
// second reservation that would exceed the cap blocks until the
// first one is released.
func TestReserveBytes_BlocksUntilRelease(t *testing.T) {
	s := newTestStreamState()
	// Pre-fill so the buffer is non-empty (otherwise the
	// oversized-partition escape lets anything through).
	if !s.reserveBytes(context.Background(), 700, 1000) {
		t.Fatal("first reserve should succeed")
	}

	// Second reservation: 700 + 500 = 1200 > 1000. Must block.
	done := make(chan struct{})
	go func() {
		s.reserveBytes(context.Background(), 500, 1000)
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("second reserveBytes should have blocked")
	case <-time.After(20 * time.Millisecond):
		// Expected: still blocked.
	}

	// Releasing the first slot frees enough room for the second.
	s.releaseBytes(700)

	select {
	case <-done:
		// Good — second reservation completed.
	case <-time.After(time.Second):
		t.Fatal("second reserveBytes should have unblocked after release")
	}

	if s.bufferedBytes != 500 {
		t.Errorf("bufferedBytes = %d, want 500 (only second still held)",
			s.bufferedBytes)
	}
}

// TestReserveBytes_OversizedSinglePartitionFlows guards the
// escape clause: when the buffer is empty, an over-cap
// reservation still proceeds (otherwise a single oversized
// partition would deadlock the pipeline).
func TestReserveBytes_OversizedSinglePartitionFlows(t *testing.T) {
	s := newTestStreamState()
	// 2000 > cap 1000 BUT buffer is empty → reservation goes
	// through immediately.
	if !s.reserveBytes(context.Background(), 2000, 1000) {
		t.Fatal("oversized reserve into empty buffer should succeed")
	}
	if s.bufferedBytes != 2000 {
		t.Errorf("bufferedBytes = %d, want 2000", s.bufferedBytes)
	}
}

// TestReserveBytes_CtxCancellation guards that a blocked
// reservation returns false when ctx is cancelled — needed so
// the pipeline can shut down cleanly when the caller breaks out
// of the iter loop.
func TestReserveBytes_CtxCancellation(t *testing.T) {
	s := newTestStreamState()
	if !s.reserveBytes(context.Background(), 700, 1000) {
		t.Fatal("first reserve should succeed")
	}

	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan bool, 1)
	go func() {
		got <- s.reserveBytes(ctx, 500, 1000) // would block
	}()

	// Give the goroutine a moment to enter Wait().
	time.Sleep(10 * time.Millisecond)

	// Cancellation alone won't wake Wait(); the cond needs a
	// broadcast. The downloadAndDecodeIter pipeline pairs cancel() with a
	// watchdog goroutine that broadcasts; here we do it inline.
	cancel()
	s.mu.Lock()
	s.cond.Broadcast()
	s.mu.Unlock()

	select {
	case ok := <-got:
		if ok {
			t.Error("reserveBytes should have returned false on ctx cancel")
		}
	case <-time.After(time.Second):
		t.Fatal("reserveBytes did not return after ctx cancel")
	}
}

// TestAcquireBodySlot_BlocksUntilRelease verifies the body-pool
// back-pressure: once cap slots are held, the next acquire blocks
// until releaseBodySlots returns one.
func TestAcquireBodySlot_BlocksUntilRelease(t *testing.T) {
	s := newTestStreamState()
	const cap = 2

	if !s.acquireBodySlot(context.Background(), cap) {
		t.Fatal("first acquire should succeed")
	}
	if !s.acquireBodySlot(context.Background(), cap) {
		t.Fatal("second acquire should succeed")
	}

	// Third must block — pool is full.
	done := make(chan struct{})
	go func() {
		s.acquireBodySlot(context.Background(), cap)
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("third acquire should have blocked")
	case <-time.After(20 * time.Millisecond):
	}

	// Release one — third unblocks.
	s.releaseBodySlots(1)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("third acquire should have unblocked after release")
	}

	if s.outstandingBodies != 2 {
		t.Errorf("outstandingBodies = %d, want 2", s.outstandingBodies)
	}
}

// TestAcquireBodySlot_NoCap returns true immediately and does
// not touch the counter.
func TestAcquireBodySlot_NoCap(t *testing.T) {
	s := newTestStreamState()
	if !s.acquireBodySlot(context.Background(), 0) {
		t.Fatal("acquireBodySlot with cap=0 should return true")
	}
	if s.outstandingBodies != 0 {
		t.Errorf("outstandingBodies = %d, want 0", s.outstandingBodies)
	}
}

// TestAcquireBodySlot_CtxCancellation guards that a blocked
// acquire returns false when ctx is cancelled (paired with the
// watchdog broadcast in the live pipeline).
func TestAcquireBodySlot_CtxCancellation(t *testing.T) {
	s := newTestStreamState()
	const cap = 1
	if !s.acquireBodySlot(context.Background(), cap) {
		t.Fatal("first acquire should succeed")
	}

	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan bool, 1)
	go func() {
		got <- s.acquireBodySlot(ctx, cap)
	}()
	time.Sleep(10 * time.Millisecond)

	cancel()
	s.mu.Lock()
	s.cond.Broadcast()
	s.mu.Unlock()

	select {
	case ok := <-got:
		if ok {
			t.Error("acquireBodySlot should return false on ctx cancel")
		}
	case <-time.After(time.Second):
		t.Fatal("acquireBodySlot did not return after ctx cancel")
	}
}

// TestWaitForPartition_BlocksUntilComplete verifies that the
// decoder's wait actually unblocks when downloaders finish.
func TestWaitForPartition_BlocksUntilComplete(t *testing.T) {
	s := newTestStreamState()
	s.parts = []*partState{
		{
			files:  make([]core.KeyMeta, 3), // only len matters
			bodies: make([][]byte, 3),
			errs:   make([]error, 3),
		},
	}

	done := make(chan bool, 1)
	go func() {
		done <- s.waitForPartition(context.Background(), 0)
	}()

	// Should still be blocked.
	select {
	case <-done:
		t.Fatal("waitForPartition should have blocked on 0/3 complete")
	case <-time.After(20 * time.Millisecond):
	}

	// Mark all three files complete.
	for fi := range 3 {
		s.markComplete(0, fi, []byte("x"), nil)
	}

	select {
	case ok := <-done:
		if !ok {
			t.Error("waitForPartition should have returned true on completion")
		}
	case <-time.After(time.Second):
		t.Fatal("waitForPartition did not return after completion")
	}
}
