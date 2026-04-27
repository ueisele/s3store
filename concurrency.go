package s3store

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// fanOut runs work in parallel across items[0..len(items)) using a
// worker pool of at most min(concurrency, len(items)) goroutines.
// Each work invocation gets a per-call ctx that's cancelled when
// any sibling errors or the caller cancels, so blocked S3 calls
// bail promptly. Results land in the slot the caller chose by
// capturing i in the closure — slot indices are stable so callers
// can write into preallocated result slots without coordination.
//
// Workers share a single atomic counter to claim items, so each
// (i, item) pair is processed by exactly one worker and slot i is
// only written by that worker.
//
// Bounded goroutine count: unlike the older one-goroutine-per-item
// shape, this caps spawned goroutines at the worker count even when
// len(items) is large. Matters for nested fan-out (e.g. write
// partitions × projection markers per partition) where the old shape
// could spawn N×K goroutines that mostly parked on the per-target
// MaxInflightRequests semaphore inside Target.put.
//
// Error semantics: the first real (non-cancellation) error wins
// and cancels the rest. context.Canceled errors from siblings
// after that cancel are filtered out so callers see the
// root-cause failure, not the fallout. If no real error fired but
// the parent ctx is done, the parent ctx error is returned so a
// caller-triggered cancel surfaces as an error instead of an
// empty-success.
//
// Fast path: len(items) == 1 calls work directly without spawning
// a goroutine, avoiding scheduler overhead for the sugar-wrapper
// single-item case.
//
// Self-instrumentation: when m is non-nil, fanOut records exactly
// one observation pair (s3store.fanout.items, s3store.fanout.workers)
// per call, with the actual worker count it computed. Call sites
// don't recompute the formula — fanOut owns the truth so internal
// logic changes can't drift apart from the metric.
//
// Used as the single fan-out primitive across the package —
// partition writes, parallel data-file downloads, multi-pattern
// LISTs, BackfillProjection, etc. all funnel through here so the
// "first error wins, parent cancel surfaces" semantics are
// implemented once.
func fanOut[I any](
	ctx context.Context,
	items []I,
	concurrency int,
	m *metrics,
	work func(ctx context.Context, i int, item I) error,
) error {
	if len(items) == 0 {
		// No-work path: don't record a fan-out observation. The
		// histograms only hold real dispatches, not "we were called
		// with nothing." recordFanout's own items==0 guard would
		// skip this anyway — short-circuiting here keeps the intent
		// explicit at the call site.
		return nil
	}
	if len(items) == 1 {
		m.recordFanout(ctx, 1, 1)
		return work(ctx, 0, items[0])
	}

	workers := max(1, min(concurrency, len(items)))
	m.recordFanout(ctx, len(items), workers)

	parentCtx := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errs := make([]error, len(items))
	var next atomic.Int64
	var wg sync.WaitGroup

	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for {
				// Honour cancellation between items so a sibling's
				// error short-circuits the remaining work claims.
				// In-flight work() calls see the cancelled ctx
				// propagated into their AWS SDK calls.
				if ctx.Err() != nil {
					return
				}
				i := int(next.Add(1)) - 1
				if i >= len(items) {
					return
				}
				if err := work(ctx, i, items[i]); err != nil {
					errs[i] = err
					cancel()
					return
				}
			}
		}()
	}
	wg.Wait()

	for _, err := range errs {
		if err == nil || errors.Is(err, context.Canceled) {
			continue
		}
		return err
	}
	return parentCtx.Err()
}

// fanOutMapReduce runs mapFn across items in parallel via fanOut,
// then folds the per-item slices through reduceFn. The map step
// reuses every guarantee of fanOut (worker pool, first-error wins,
// cancellation propagation, fan-out metric); the reduce step runs
// once on the main goroutine after all workers finish, so reduceFn
// does not need to be safe for concurrent use.
//
// Per-item results are stored in a preallocated [][]O slot so
// workers never share state. On any map error the reduce step is
// skipped and the zero value of R is returned alongside the error.
//
// Empty input returns reduceFn(nil) — reducers that need a different
// empty-case answer should check for it themselves.
func fanOutMapReduce[I, O, R any](
	ctx context.Context,
	items []I,
	concurrency int,
	m *metrics,
	mapFn func(ctx context.Context, item I) ([]O, error),
	reduceFn func([][]O) R,
) (R, error) {
	results := make([][]O, len(items))
	err := fanOut(ctx, items, concurrency, m,
		func(ctx context.Context, i int, item I) error {
			r, err := mapFn(ctx, item)
			if err != nil {
				return err
			}
			results[i] = r
			return nil
		})
	if err != nil {
		var zero R
		return zero, err
	}
	return reduceFn(results), nil
}
