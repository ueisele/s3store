package core

import (
	"context"
	"errors"
	"sync"
)

// FanOut runs work in parallel across items[0..len(items)) with
// up to concurrency goroutines in flight. Each work invocation
// gets a per-call ctx that's cancelled when any sibling errors
// or the caller cancels, so blocked S3 calls bail promptly.
// Results land in the slot the caller chose by capturing i in
// the closure — slot indices are stable so callers can write
// into preallocated result slots without coordination.
//
// Error semantics: the first real (non-cancellation) error wins
// and cancels the rest. context.Canceled errors from siblings
// after that cancel are filtered out so callers see the
// root-cause failure, not the fallout. If every slot bailed
// with Canceled, the parent ctx error is returned so a
// caller-triggered cancel surfaces as an error instead of an
// empty-success.
//
// Fast path: len(items) == 1 calls work directly without spawning
// a goroutine, avoiding scheduler overhead for the sugar-wrapper
// single-item case.
//
// Used as the single fan-out primitive across the library —
// partition writes, parallel data-file downloads, multi-pattern
// LISTs, BackfillIndex, etc. all funnel through here so the
// "first error wins, parent cancel surfaces" semantics are
// implemented once.
func FanOut[I any](
	ctx context.Context,
	items []I,
	concurrency int,
	work func(ctx context.Context, i int, item I) error,
) error {
	if len(items) == 0 {
		return nil
	}
	if len(items) == 1 {
		return work(ctx, 0, items[0])
	}

	parentCtx := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sem := make(chan struct{}, concurrency)
	errs := make([]error, len(items))
	var wg sync.WaitGroup

	for i, it := range items {
		wg.Add(1)
		go func(i int, it I) {
			defer wg.Done()
			// Acquire inside the goroutine so a parent-ctx cancel
			// or sibling failure unblocks waiters promptly instead
			// of letting the main loop dispatch every item upfront.
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				errs[i] = ctx.Err()
				return
			}
			defer func() { <-sem }()

			if err := work(ctx, i, it); err != nil {
				errs[i] = err
				cancel()
			}
		}(i, it)
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
