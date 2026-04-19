package s3parquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/parquet-go/parquet-go"
	"github.com/ueisele/s3store/internal/core"
)

// pollDownloadConcurrency caps the number of parquet files
// downloaded + decoded in parallel by Read and PollRecords.
//
// 8 is a sweet spot across three constraints:
//
//   - Network: ~30–100 ms S3 GET latency per object, so 1 → 8
//     in parallel is a big throughput win. 8 → 32 is diminishing
//     returns for small/medium parquet files.
//   - CPU: parquet decode is CPU-bound. On an 8-core host, 8
//     concurrent decoders roughly saturate cores; more just
//     causes contention.
//   - AWS SDK transport: the default HTTP client's
//     MaxConnsPerHost is around 10, so 8 avoids connection
//     churn without needing a custom transport.
//
// Fixed rather than configurable in v1. If profiling shows
// this is the bottleneck (e.g. many small files on a large-
// core host, or a constrained environment that needs a lower
// cap), promote to a Config knob — ~5 lines, non-breaking.
const pollDownloadConcurrency = 8

// versionedRecord carries a decoded record together with the
// write time of the parquet file it came from. The library
// passes insertedAt to Config.VersionOf, which can use it as
// a fallback or ignore it in favor of a domain-level version.
type versionedRecord[T any] struct {
	rec        T
	insertedAt time.Time
}

// Read returns all records whose data files match the given
// key pattern, optionally deduplicated to latest-per-entity
// when EntityKeyOf is configured.
//
// Accepts the same glob grammar as s3sql.Read: whole-segment
// "*" and a single trailing "*" inside a value.
//
// Memory: all matching records are buffered before dedup/return.
// For unbounded reads, use PollRecords to stream incrementally.
//
// Single-pattern sugar over ReadMany — use ReadMany directly
// when the caller has an arbitrary set of partition tuples
// (e.g. a non-Cartesian "(period=A, customer=X), (period=B,
// customer=Y)" selection) that can't be expressed as one
// pattern.
func (s *Reader[T]) Read(
	ctx context.Context, keyPattern string, opts ...QueryOption,
) ([]T, error) {
	return s.ReadMany(ctx, []string{keyPattern}, opts...)
}

// ReadMany runs Read across every pattern in patterns and
// returns the concatenated result, with dedup applied globally
// when EntityKeyOf is configured (an entity that appears under
// two patterns is kept as the latest version across the union,
// not per-pattern).
//
// Each pattern uses the grammar described on Read. Pass more
// than one when the target set is NOT a Cartesian product of
// per-segment values — e.g. the tuples (period=A, customer=X)
// and (period=B, customer=Y) but not the off-diagonal pairs.
// For a Cartesian "all N × M" shape, pre-expand the list in
// caller code or use a single pattern with whole-segment "*".
//
// LIST calls fan out with the same concurrency cap as GETs
// (pollDownloadConcurrency), literal-duplicate patterns are
// dropped up front, and duplicate keys that arise when
// patterns semantically overlap are collapsed before the GET
// phase so every parquet file is fetched and decoded at most
// once. Passing an empty slice is a no-op: (nil, nil).
//
// Errors: the first malformed pattern fails the whole call,
// surfaced with the offending index so the caller can locate
// it. Any sub-LIST error fails fast and cancels the rest.
func (s *Reader[T]) ReadMany(
	ctx context.Context, patterns []string, opts ...QueryOption,
) ([]T, error) {
	var o core.QueryOpts
	o.Apply(opts...)

	patterns = dedupePatterns(patterns)
	if len(patterns) == 0 {
		return nil, nil
	}

	plans := make([]*readPlan, len(patterns))
	for i, p := range patterns {
		plan, err := buildReadPlan(p, s.dataPath, s.cfg.Target.PartitionKeyParts)
		if err != nil {
			return nil, fmt.Errorf(
				"s3parquet: ReadMany pattern %d %q: %w", i, p, err)
		}
		plans[i] = plan
	}

	keys, err := s.listAllMatchingParquet(ctx, plans)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, nil
	}

	versioned, err := s.downloadAndDecodeAll(ctx, keys)
	if err != nil {
		return nil, err
	}

	if o.IncludeHistory || !s.cfg.dedupEnabled() {
		return stripVersions(versioned), nil
	}
	return dedupLatest(versioned, s.cfg.EntityKeyOf, s.cfg.VersionOf), nil
}

// listMatchingParquet lists every parquet object under the
// plan's ListPrefix and returns the subset whose Hive key
// matches the plan's predicate. S3 LIST handles pagination; the
// predicate runs in memory per key.
func (s *Reader[T]) listMatchingParquet(
	ctx context.Context, plan *readPlan,
) ([]string, error) {
	paginator := s.cfg.Target.list(plan.ListPrefix)

	var keys []string
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf(
				"s3parquet: list data files: %w", err)
		}
		for _, obj := range page.Contents {
			objKey := aws.ToString(obj.Key)
			if !strings.HasSuffix(objKey, ".parquet") {
				continue
			}
			hiveKey, ok := hiveKeyOfDataFile(objKey, s.dataPath)
			if !ok {
				continue
			}
			if plan.Match(hiveKey) {
				keys = append(keys, objKey)
			}
		}
	}
	return keys, nil
}

// listAllMatchingParquet runs listMatchingParquet across every
// plan with bounded concurrency and returns the unioned set of
// keys, deduplicated (overlapping plans can list the same
// parquet file, e.g. "period=*" and "period=2026-03" both cover
// March data). Fast-path: len(plans) == 1 falls through to the
// single-plan implementation with no goroutine overhead.
func (s *Reader[T]) listAllMatchingParquet(
	ctx context.Context, plans []*readPlan,
) ([]string, error) {
	return runPlansConcurrent(ctx, plans, s.listMatchingParquet)
}

// runPlansConcurrent runs listOne across every plan with the
// shared concurrency cap (pollDownloadConcurrency) and returns
// the deduplicated union of keys in first-seen order. Used by
// Reader.ReadMany, Index.LookupMany, and BackfillIndexMany as
// the single source of truth for the fan-out pattern.
//
// Fast path: len(plans) == 1 calls listOne directly, avoiding
// goroutine / channel overhead for the sugar-wrapper single-
// pattern case.
//
// Error semantics: first real error wins and cancels the rest.
// context.Canceled entries written by siblings after cancel are
// filtered out so callers see the root cause, not the fallout.
func runPlansConcurrent[P any](
	ctx context.Context,
	plans []P,
	listOne func(ctx context.Context, p P) ([]string, error),
) ([]string, error) {
	if len(plans) == 1 {
		return listOne(ctx, plans[0])
	}

	results := make([][]string, len(plans))
	errs := make([]error, len(plans))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sem := make(chan struct{}, pollDownloadConcurrency)
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
	return unionKeys(results), nil
}

// dedupePatterns removes literal-duplicate entries from a
// patterns slice, preserving first-seen order. Cheap pre-step
// for the *Many functions so accidental duplicates (e.g. from
// a generated list with possible repeats) don't cause duplicate
// LIST round-trips. Doesn't catch semantic overlap — patterns
// like "*" and "2026-*" both list overlapping files but aren't
// string-equal; that case is still handled by unionKeys at the
// key level, just at the cost of one extra LIST.
func dedupePatterns(patterns []string) []string {
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

// unionKeys flattens a set of per-plan LIST results into a
// single deduplicated slice, preserving first-seen order so the
// downstream GET pipeline is deterministic.
func unionKeys(perPlan [][]string) []string {
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

// downloadAndDecodeAll fans out a bounded set of parallel
// downloads, decodes each parquet file into []T, and returns
// the concatenated result wrapped with each source file's
// insertedAt. Preserves the input key order so callers who
// don't dedup observe a deterministic stream.
func (s *Reader[T]) downloadAndDecodeAll(
	ctx context.Context, keys []string,
) ([]versionedRecord[T], error) {
	if len(keys) == 0 {
		return nil, nil
	}

	results := make([][]versionedRecord[T], len(keys))
	errs := make([]error, len(keys))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sem := make(chan struct{}, pollDownloadConcurrency)
	var wg sync.WaitGroup
	for i, key := range keys {
		wg.Add(1)
		go func(i int, key string) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				errs[i] = ctx.Err()
				return
			}
			defer func() { <-sem }()

			tsMicros, _, err := core.ParseDataFileName(path.Base(key))
			if err != nil {
				errs[i] = fmt.Errorf(
					"s3parquet: parse data filename %s: %w", key, err)
				cancel()
				return
			}
			insertedAt := time.UnixMicro(tsMicros)

			data, err := s.cfg.Target.get(ctx, key)
			if err != nil {
				// A dangling ref or a LIST-to-GET race produces a
				// NoSuchKey here. Skip-and-notify instead of
				// failing the whole batch, so one bad ref doesn't
				// poison every subsequent Poll/Read. Any other
				// error (throttle, network, auth) is still fatal —
				// we don't want to silently drop real records.
				if _, ok := errors.AsType[*s3types.NoSuchKey](err); ok {
					if s.cfg.OnMissingData != nil {
						s.cfg.OnMissingData(key)
					}
					return
				}
				errs[i] = fmt.Errorf(
					"s3parquet: get %s: %w", key, err)
				cancel()
				return
			}
			recs, err := decodeParquet[T](data)
			if err != nil {
				errs[i] = fmt.Errorf(
					"s3parquet: decode %s: %w", key, err)
				cancel()
				return
			}
			// Populate Config.InsertedAtField if set. Happens on
			// every Read/PollRecords call — zero reflect cost
			// when insertedAtFieldIndex is nil.
			if s.insertedAtFieldIndex != nil {
				tsVal := reflect.ValueOf(insertedAt)
				for j := range recs {
					rv := reflect.ValueOf(&recs[j]).Elem()
					rv.FieldByIndex(s.insertedAtFieldIndex).
						Set(tsVal)
				}
			}
			versioned := make([]versionedRecord[T], len(recs))
			for j, r := range recs {
				versioned[j] = versionedRecord[T]{
					rec:        r,
					insertedAt: insertedAt,
				}
			}
			results[i] = versioned
		}(i, key)
	}
	wg.Wait()

	// First-error wins: we cancel on the first failure so later
	// goroutines see ctx.Err(); return the earliest "real"
	// error rather than a cancellation.
	for _, e := range errs {
		if e == nil || e == context.Canceled {
			continue
		}
		return nil, e
	}

	// Count for preallocation.
	total := 0
	for _, r := range results {
		total += len(r)
	}
	out := make([]versionedRecord[T], 0, total)
	for _, r := range results {
		out = append(out, r...)
	}
	return out, nil
}

// decodeParquet reads all rows of a parquet file into []T. T
// must be parquet-go-friendly (field-tagged, primitive-backed).
func decodeParquet[T any](data []byte) ([]T, error) {
	reader := parquet.NewGenericReader[T](bytes.NewReader(data))
	defer func() { _ = reader.Close() }()

	total := reader.NumRows()
	if total == 0 {
		return nil, nil
	}

	out := make([]T, total)
	n, err := reader.Read(out)
	if err != nil && !errors.Is(err, io.EOF) {
		// parquet-go returns io.EOF at the end of the file;
		// treat that as a clean termination, not an error.
		return nil, err
	}
	return out[:n], nil
}

// stripVersions drops the per-record insertedAt metadata when
// the caller didn't ask for dedup. Works on the already-decoded
// slice so we don't pay an extra allocation per record.
func stripVersions[T any](in []versionedRecord[T]) []T {
	if len(in) == 0 {
		return nil
	}
	out := make([]T, len(in))
	for i, v := range in {
		out[i] = v.rec
	}
	return out
}

// dedupLatest keeps the record with the maximum version per
// entity, in the order each entity was first seen. Stable under
// equal versions: earlier occurrences win ties, so callers who
// rely on first-write semantics aren't surprised by later
// duplicates.
//
// versionOf is invoked with each record and the insertedAt of
// the source file, so a caller can fall back to file time when
// the record has no domain-level version.
func dedupLatest[T any](
	records []versionedRecord[T],
	entityKey func(T) string,
	versionOf func(record T, insertedAt time.Time) int64,
) []T {
	if len(records) == 0 {
		return nil
	}
	type slot struct {
		index   int
		version int64
	}
	seen := make(map[string]slot, len(records))
	order := make([]string, 0, len(records))
	for i, vr := range records {
		k := entityKey(vr.rec)
		v := versionOf(vr.rec, vr.insertedAt)
		cur, ok := seen[k]
		if !ok {
			seen[k] = slot{index: i, version: v}
			order = append(order, k)
			continue
		}
		if v > cur.version {
			seen[k] = slot{index: i, version: v}
		}
	}
	out := make([]T, len(order))
	for i, k := range order {
		out[i] = records[seen[k].index].rec
	}
	return out
}
