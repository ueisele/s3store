package core

import (
	"context"
	"errors"
	"sync"
	"time"
)

// KeyMeta pairs an S3 object key with an insertedAt timestamp
// — the file's write time as best the caller can derive it.
// Threaded through the read path so downstream consumers can
// stamp versionedRecord.insertedAt without re-consulting S3.
//
// Source of InsertedAt depends on the call site:
//   - listMatchingParquet (data files): S3 LastModified from
//     the LIST response.
//   - PollRecords: the ref filename's dataTsMicros (writer's
//     wall clock at write-start), which is more accurate than
//     the ref's LastModified.
//
// Both are monotonic, both serve as DefaultVersionOf fallback
// inputs and as the sort-fallback tiebreaker when no
// InsertedAtField column is configured.
type KeyMeta struct {
	Key        string
	InsertedAt time.Time
}

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

// RunPlansConcurrent runs listOne across every plan via FanOut
// and returns the deduplicated union of results in first-seen
// order. keyOf extracts the dedup key so callers can use richer
// result types (e.g. KeyMeta) while still deduping on the
// underlying S3 key.
//
// Single source of truth for the multi-pattern LIST fan-out used
// by s3parquet (ReadMany, LookupMany, BackfillIndexMany) and
// s3sql (ReadMany, QueryMany).
func RunPlansConcurrent[P any, R any](
	ctx context.Context,
	plans []P,
	concurrency int,
	listOne func(ctx context.Context, p P) ([]R, error),
	keyOf func(R) string,
) ([]R, error) {
	results := make([][]R, len(plans))
	err := FanOut(ctx, plans, concurrency,
		func(ctx context.Context, i int, plan P) error {
			r, err := listOne(ctx, plan)
			if err != nil {
				return err
			}
			results[i] = r
			return nil
		})
	if err != nil {
		return nil, err
	}
	return UnionKeys(results, keyOf), nil
}

// UnionKeys flattens a set of per-plan LIST results into a
// single deduplicated slice, preserving first-seen order so the
// downstream GET pipeline is deterministic. keyOf extracts the
// dedup key from each result (use the identity function when R
// is itself a string).
func UnionKeys[R any](perPlan [][]R, keyOf func(R) string) []R {
	total := 0
	for _, r := range perPlan {
		total += len(r)
	}
	if total == 0 {
		return nil
	}
	seen := make(map[string]struct{}, total)
	out := make([]R, 0, total)
	for _, r := range perPlan {
		for _, v := range r {
			k := keyOf(v)
			if _, ok := seen[k]; ok {
				continue
			}
			seen[k] = struct{}{}
			out = append(out, v)
		}
	}
	return out
}

// DedupePatterns removes literal-duplicate entries from a
// patterns slice, preserving first-seen order. Cheap pre-step
// for the *Many functions so accidental duplicates (e.g. from a
// generated list with possible repeats) don't cause duplicate
// LIST round-trips. Doesn't catch semantic overlap — patterns
// like "*" and "2026-*" both list overlapping files but aren't
// string-equal; that case is still handled by UnionKeys at the
// key level, just at the cost of one extra LIST.
func DedupePatterns(patterns []string) []string {
	if len(patterns) <= 1 {
		return patterns
	}
	seen := make(map[string]struct{}, len(patterns))
	out := make([]string, 0, len(patterns))
	for _, p := range patterns {
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	return out
}
