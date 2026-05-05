package s3store

import (
	"context"
	"sync"
	"testing"
	"time"
)

func newTestStreamState() *streamState {
	s := &streamState{}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// withSlotCap attaches a body-slot semaphore of the given
// capacity. Mirrors how downloadAndDecodeIter wires up slotCh in
// production. cap == 0 leaves slotCh nil — the no-back-pressure
// path that the live pipeline never takes but that test cases
// exercise to confirm the disabled-cap branch returns immediately.
func (s *streamState) withSlotCap(cap int) *streamState {
	if cap > 0 {
		s.slotCh = make(chan struct{}, cap)
	}
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
	s := newTestStreamState().withSlotCap(2)

	if !s.acquireBodySlot(context.Background()) {
		t.Fatal("first acquire should succeed")
	}
	if !s.acquireBodySlot(context.Background()) {
		t.Fatal("second acquire should succeed")
	}

	// Third must block — pool is full.
	done := make(chan struct{})
	go func() {
		s.acquireBodySlot(context.Background())
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

	if got := len(s.slotCh); got != 2 {
		t.Errorf("slotCh occupancy = %d, want 2", got)
	}
}

// TestAcquireBodySlot_NoCap returns true immediately when slotCh
// is nil (semaphore disabled — the test-helper path).
func TestAcquireBodySlot_NoCap(t *testing.T) {
	s := newTestStreamState()
	if !s.acquireBodySlot(context.Background()) {
		t.Fatal("acquireBodySlot with nil slotCh should return true")
	}
	if s.slotCh != nil {
		t.Errorf("slotCh = %v, want nil (no-cap path should not allocate)",
			s.slotCh)
	}
}

// TestAcquireBodySlot_CtxCancellation guards that a blocked
// acquire returns false when ctx is cancelled. Channel-based
// semaphore observes ctx.Done directly via select, so no watchdog
// broadcast is needed (in contrast to reserveBytes / waitForPartition,
// which still rely on the cond and need the watchdog wakeup).
func TestAcquireBodySlot_CtxCancellation(t *testing.T) {
	s := newTestStreamState().withSlotCap(1)
	if !s.acquireBodySlot(context.Background()) {
		t.Fatal("first acquire should succeed")
	}

	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan bool, 1)
	go func() {
		got <- s.acquireBodySlot(ctx)
	}()
	time.Sleep(10 * time.Millisecond)

	cancel()

	select {
	case ok := <-got:
		if ok {
			t.Error("acquireBodySlot should return false on ctx cancel")
		}
	case <-time.After(time.Second):
		t.Fatal("acquireBodySlot did not return after ctx cancel")
	}
}

// TestDeadlockObserver_FiresOnStall verifies the watchdog
// surfaces a stalled pipeline by incrementing the stall counter.
// Pipeline state is left untouched (no markComplete, no slot
// release) so lastProgressNs stays at the seed timestamp and
// the observer trips on its first tick past the threshold.
func TestDeadlockObserver_FiresOnStall(t *testing.T) {
	m, reader := newTestMetrics(t, "")
	s := newTestStreamState().withSlotCap(2)
	s.m = m
	// Seed lastProgressNs with a time well past the threshold so
	// the very first tick observes a stall (otherwise the seed-of-
	// zero short-circuit would skip the alert).
	s.lastProgressNs.Store(time.Now().Add(-time.Second).UnixNano())
	scope := &methodScope{method: methodReadIter}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runDeadlockObserver(ctx, scope, 5*time.Millisecond, 50*time.Millisecond)
	}()

	// Give the watchdog enough ticks to fire at least once. With
	// a 5ms tick and a 50ms threshold, the first qualifying tick
	// lands shortly after the threshold; 200ms is comfortably
	// above timing noise.
	time.Sleep(200 * time.Millisecond)
	cancel()
	<-done

	rm := collectMetrics(t, reader)
	dp := findCounterDP(rm, "s3store.read.iter.stall.count",
		map[string]string{"s3store.method": string(methodReadIter)})
	if dp == nil {
		t.Fatalf("stall counter has no data point for method=%s",
			methodReadIter)
	}
	if dp.Value < 1 {
		t.Errorf("stall counter = %d, want >= 1", dp.Value)
	}
}

// TestDeadlockObserver_NoSignalWhenProgress verifies the
// watchdog stays quiet when the pipeline is making forward
// progress. lastProgressNs is bumped on every iteration so no
// tick ever observes a stale window.
func TestDeadlockObserver_NoSignalWhenProgress(t *testing.T) {
	m, reader := newTestMetrics(t, "")
	s := newTestStreamState().withSlotCap(2)
	s.m = m
	scope := &methodScope{method: methodReadIter}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runDeadlockObserver(ctx, scope, 5*time.Millisecond, 50*time.Millisecond)
	}()

	// Drive forward progress every 5ms — well within the 50ms
	// threshold — for 200ms total. Stall counter must stay at
	// zero throughout.
	for range 40 {
		s.lastProgressNs.Store(time.Now().UnixNano())
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done

	rm := collectMetrics(t, reader)
	dp := findCounterDP(rm, "s3store.read.iter.stall.count",
		map[string]string{"s3store.method": string(methodReadIter)})
	if dp != nil && dp.Value > 0 {
		t.Errorf("stall counter = %d, want 0 (pipeline was making progress)",
			dp.Value)
	}
}

// TestDeadlockObserver_ExitsOnCtxDone verifies the watchdog
// goroutine returns promptly when its ctx is cancelled. Required
// so downloadAndDecodeIter's deferred wg.Wait() doesn't leak the
// observer past the pipeline's lifetime.
func TestDeadlockObserver_ExitsOnCtxDone(t *testing.T) {
	s := newTestStreamState().withSlotCap(2)
	scope := &methodScope{method: methodReadIter}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runDeadlockObserver(ctx, scope, time.Hour, time.Hour)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runDeadlockObserver did not return after ctx cancel")
	}
}

// TestWaitForPartition_BlocksUntilComplete verifies that the
// decoder's wait actually unblocks when downloaders finish.
func TestWaitForPartition_BlocksUntilComplete(t *testing.T) {
	s := newTestStreamState()
	s.parts = []*partState{
		{
			files:  make([]keyMeta, 3), // only len matters
			bodies: make([][]byte, 3),
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
		s.markComplete(0, fi, []byte("x"))
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
