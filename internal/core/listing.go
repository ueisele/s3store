package core

import (
	"context"
	"errors"
	"sync"
)

// RunPlansConcurrent runs listOne across every plan with a
// bounded concurrency cap and returns the deduplicated union of
// keys in first-seen order. Single source of truth for the
// multi-pattern LIST fan-out used by s3parquet (ReadMany,
// LookupMany, BackfillIndexMany) and s3sql (ReadMany, QueryMany).
//
// Fast path: len(plans) == 1 calls listOne directly, avoiding
// goroutine / channel overhead for the sugar-wrapper single-
// pattern case.
//
// Error semantics: first real error wins and cancels the rest.
// context.Canceled entries written by siblings after the cancel
// are filtered out so callers see the root cause, not the
// fallout.
func RunPlansConcurrent[P any](
	ctx context.Context,
	plans []P,
	concurrency int,
	listOne func(ctx context.Context, p P) ([]string, error),
) ([]string, error) {
	if len(plans) == 1 {
		return listOne(ctx, plans[0])
	}

	results := make([][]string, len(plans))
	errs := make([]error, len(plans))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for i, plan := range plans {
		wg.Add(1)
		go func(i int, plan P) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				errs[i] = ctx.Err()
				return
			}
			defer func() { <-sem }()

			keys, err := listOne(ctx, plan)
			if err != nil {
				errs[i] = err
				cancel()
				return
			}
			results[i] = keys
		}(i, plan)
	}
	wg.Wait()

	for _, e := range errs {
		if e == nil || errors.Is(e, context.Canceled) {
			continue
		}
		return nil, e
	}
	return UnionKeys(results), nil
}

// UnionKeys flattens a set of per-plan LIST results into a
// single deduplicated slice, preserving first-seen order so the
// downstream GET pipeline is deterministic.
func UnionKeys(perPlan [][]string) []string {
	total := 0
	for _, keys := range perPlan {
		total += len(keys)
	}
	if total == 0 {
		return nil
	}
	seen := make(map[string]struct{}, total)
	out := make([]string, 0, total)
	for _, keys := range perPlan {
		for _, k := range keys {
			if _, ok := seen[k]; ok {
				continue
			}
			seen[k] = struct{}{}
			out = append(out, k)
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
