package core

import (
	"context"
	"errors"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// goid returns the calling goroutine's runtime ID. Used by
// FanOut tests to assert that the single-item fast path runs work
// in the caller's goroutine instead of spawning. Lifted from the
// standard "parse runtime.Stack header" trick — there's no public
// API for goroutine ID and the test cost is marginal.
func goid() int {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	s := strings.TrimPrefix(string(buf[:n]), "goroutine ")
	s = s[:strings.IndexByte(s, ' ')]
	id, _ := strconv.Atoi(s)
	return id
}

// TestFanOut_Empty pins the empty-input fast path: no work, no
// goroutines, nil error.
func TestFanOut_Empty(t *testing.T) {
	called := false
	err := FanOut(context.Background(), []int{}, 4,
		func(_ context.Context, _ int, _ int) error {
			called = true
			return nil
		})
	if err != nil {
		t.Fatalf("got err %v, want nil", err)
	}
	if called {
		t.Fatalf("work called on empty input")
	}
}

// TestFanOut_SingleItemDirect verifies len(items)==1 runs work in
// the caller's goroutine without spawning. Compares goroutine IDs
// directly — robust against background goroutines (GC, scheduler)
// that would make a runtime.NumGoroutine() comparison flaky.
func TestFanOut_SingleItemDirect(t *testing.T) {
	caller := goid()
	var workGoid int
	err := FanOut(context.Background(), []int{42}, 4,
		func(_ context.Context, _ int, _ int) error {
			workGoid = goid()
			return nil
		})
	if err != nil {
		t.Fatalf("got err %v, want nil", err)
	}
	if workGoid != caller {
		t.Errorf("single-item path spawned a goroutine: caller=%d work=%d",
			caller, workGoid)
	}
}

// TestFanOut_SlotStableResults verifies that results land in the
// caller-chosen slot regardless of completion order. Each work
// invocation sleeps a different amount so completion order
// reverses input order, but slot indices must stay stable.
func TestFanOut_SlotStableResults(t *testing.T) {
	const n = 10
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}
	results := make([]int, n)

	err := FanOut(context.Background(), items, 4,
		func(_ context.Context, i int, item int) error {
			// Reverse completion order: item 0 sleeps longest.
			time.Sleep(time.Duration(n-item) * time.Millisecond)
			results[i] = item * 10
			return nil
		})
	if err != nil {
		t.Fatalf("got err %v, want nil", err)
	}
	for i := range n {
		if results[i] != i*10 {
			t.Errorf("results[%d] = %d, want %d", i, results[i], i*10)
		}
	}
}

// TestFanOut_FirstRealErrorWins verifies that when one work fails
// and siblings get cancelled, the returned error is the real
// failure, not the fallout context.Canceled. The error fires from
// item 0 so the worker pool definitely claims it (workers pull
// via an atomic counter — items beyond the pool size only run
// after earlier items complete).
func TestFanOut_FirstRealErrorWins(t *testing.T) {
	sentinel := errors.New("boom")
	items := make([]int, 20)

	err := FanOut(context.Background(), items, 4,
		func(ctx context.Context, i int, _ int) error {
			if i == 0 {
				return sentinel
			}
			// Block until cancelled so item 0's error wins the race.
			// Worker that picks item 0 fires sentinel, the rest see
			// cancelled ctx (either before claiming or via this select).
			<-ctx.Done()
			return ctx.Err()
		})
	if !errors.Is(err, sentinel) {
		t.Errorf("got err %v, want %v", err, sentinel)
	}
}

// TestFanOut_ParentCancelSurfaces verifies that a parent-ctx cancel
// (no work returning a real error) surfaces as the parent ctx err
// rather than nil.
func TestFanOut_ParentCancelSurfaces(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled before FanOut runs

	items := make([]int, 8)
	err := FanOut(ctx, items, 4,
		func(ctx context.Context, _ int, _ int) error {
			<-ctx.Done()
			return ctx.Err()
		})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("got err %v, want context.Canceled", err)
	}
}

// TestFanOut_BoundedGoroutineCount verifies the worker pool caps
// goroutines at min(concurrency, len(items)) regardless of N. With
// 1000 items and concurrency=4, peak goroutines must stay near
// baseline+4 even though items are large in count.
//
// Workers block on a barrier so they accumulate before any can
// exit — peak measurement is meaningful.
func TestFanOut_BoundedGoroutineCount(t *testing.T) {
	const items = 1000
	const concurrency = 4

	in := make([]int, items)
	var inFlight atomic.Int32
	var peak atomic.Int32
	var releaseOnce sync.Once
	release := make(chan struct{})

	// Once we've seen `concurrency` workers in flight (proves the
	// pool is full), unblock them so they finish the items.
	checkAndRelease := func() {
		cur := inFlight.Add(1)
		// Track peak.
		for {
			old := peak.Load()
			if cur <= old || peak.CompareAndSwap(old, cur) {
				break
			}
		}
		if cur >= int32(concurrency) {
			releaseOnce.Do(func() { close(release) })
		}
		<-release
		inFlight.Add(-1)
	}

	err := FanOut(context.Background(), in, concurrency,
		func(_ context.Context, _ int, _ int) error {
			checkAndRelease()
			return nil
		})
	if err != nil {
		t.Fatalf("got err %v, want nil", err)
	}
	// Peak must equal concurrency exactly: the barrier guarantees
	// all workers accumulate before any can exit (peak ≥ N), and
	// the pool spawns exactly N workers (peak ≤ N).
	if peak.Load() != int32(concurrency) {
		t.Errorf("peak in-flight workers = %d, want %d",
			peak.Load(), concurrency)
	}
}

// TestFanOut_AllItemsProcessed verifies every item is claimed
// exactly once when no errors fire — guards the atomic counter
// from off-by-one or double-claim bugs.
func TestFanOut_AllItemsProcessed(t *testing.T) {
	const n = 500
	items := make([]int, n)
	var seen [n]atomic.Int32

	err := FanOut(context.Background(), items, 8,
		func(_ context.Context, i int, _ int) error {
			seen[i].Add(1)
			return nil
		})
	if err != nil {
		t.Fatalf("got err %v, want nil", err)
	}
	for i := range n {
		if v := seen[i].Load(); v != 1 {
			t.Errorf("item %d seen %d times, want 1", i, v)
		}
	}
}

// TestFanOut_ConcurrencyZeroOrNegative pins the floor: a
// non-positive concurrency clamps to 1 worker rather than
// deadlocking on a 0-sized pool.
func TestFanOut_ConcurrencyZeroOrNegative(t *testing.T) {
	for _, c := range []int{0, -1, -100} {
		err := FanOut(context.Background(), []int{1, 2, 3}, c,
			func(_ context.Context, _ int, _ int) error {
				return nil
			})
		if err != nil {
			t.Errorf("concurrency=%d: got err %v, want nil", c, err)
		}
	}
}
