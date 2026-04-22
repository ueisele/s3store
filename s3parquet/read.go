package s3parquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"maps"
	"path"
	"reflect"
	"slices"
	"sort"
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
// insertedAt is sourced from the writer-populated
// InsertedAtField column when the reader has InsertedAtField
// configured, else from the S3 object's LastModified. fileName
// is the base name of the source parquet key — used as a
// deterministic tiebreaker in the record-level sort.
type versionedRecord[T any] struct {
	rec        T
	insertedAt time.Time
	fileName   string
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

	sort.SliceStable(versioned, func(i, j int) bool {
		return s.sortCmp(versioned[i], versioned[j]) < 0
	})

	if !s.cfg.dedupEnabled() {
		return stripVersions(versioned), nil
	}
	if o.IncludeHistory {
		return stripVersions(dedupReplicas(versioned,
			s.cfg.EntityKeyOf, s.cfg.VersionOf)), nil
	}
	return dedupLatest(versioned, s.cfg.EntityKeyOf, s.cfg.VersionOf), nil
}

// ReadIter returns an iter.Seq2[T, error] yielding records one
// at a time instead of buffering them like Read does. Use when
// the result set is large enough that Read's O(records) memory
// becomes a problem.
//
// Dedup behaviour:
//   - Default (EntityKeyOf set, no WithHistory): per-partition
//     dedup. Files within each partition download in parallel
//     (up to pollDownloadConcurrency) into one buffered batch,
//     dedupLatest picks one record per entity, the batch is
//     yielded in lex/insertion order (first-seen wins on ties),
//     then dropped. Memory: O(one partition's pre-dedup
//     records). Differs from Read's global dedup — correct
//     only when the partition key strictly determines every
//     component of EntityKeyOf (no entity spans partitions).
//     For layouts that don't satisfy this invariant, use Read
//     instead.
//   - EntityKeyOf nil OR WithHistory(): no dedup. Files
//     downloaded one partition at a time; records yielded in
//     lex/insertion order. Memory: O(one partition's records).
//     Order guarantee lets WithHistory callers observe per-
//     entity version order within a partition.
//
// Cleanup: breaking out of the for-range loop or panicking
// inside the consumer cancels in-flight downloads via the
// iterator's deferred cancel — no manual Close required.
//
// Partition order: lex across partitions, lex/insertion order
// within each partition on both paths.
//
// Prefetch: WithReadAheadPartitions(n) runs a background
// producer that downloads up to n partitions ahead of the yield
// position. Default 0 = strict-serial. Useful when the consumer
// does non-trivial per-record work — hides the next partition's
// S3 round trips behind the current partition's yield loop at
// O((n+1) partitions) peak memory.
func (s *Reader[T]) ReadIter(
	ctx context.Context, keyPattern string, opts ...QueryOption,
) iter.Seq2[T, error] {
	return s.ReadManyIter(ctx, []string{keyPattern}, opts...)
}

// ReadManyIter is the multi-pattern sibling of ReadIter. Same
// per-partition dedup contract; passing a non-Cartesian set of
// tuples (period=A, customer=X) and (period=B, customer=Y)
// works the same way it does on ReadMany. Empty pattern slice
// yields nothing.
func (s *Reader[T]) ReadManyIter(
	ctx context.Context, patterns []string, opts ...QueryOption,
) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var o core.QueryOpts
		o.Apply(opts...)

		patterns = dedupePatterns(patterns)
		if len(patterns) == 0 {
			return
		}

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		plans := make([]*readPlan, len(patterns))
		for i, p := range patterns {
			plan, err := buildReadPlan(p, s.dataPath, s.cfg.Target.PartitionKeyParts)
			if err != nil {
				yield(*new(T), fmt.Errorf(
					"s3parquet: ReadManyIter pattern %d %q: %w",
					i, p, err))
				return
			}
			plans[i] = plan
		}

		keys, err := s.listAllMatchingParquet(ctx, plans)
		if err != nil {
			yield(*new(T), err)
			return
		}
		if len(keys) == 0 {
			return
		}

		readAhead := o.ReadAheadPartitions
		if readAhead < 0 {
			readAhead = 0
		}
		s.streamByPartition(ctx, keys, o.IncludeHistory, readAhead, yield)
	}
}

// partitionBatch is one partition's download result, passed from
// the background producer to the yield loop over the pipeline
// channel in the readAhead>0 path.
type partitionBatch[T any] struct {
	recs []versionedRecord[T]
	err  error
}

// streamByPartition processes keys one Hive partition at a time
// in lex order. Two execution modes:
//
//   - readAhead == 0 (default): strict-serial. Download the
//     current partition's files, yield every record, move on.
//     Memory: O(one partition's records).
//   - readAhead > 0: pipelined. A background producer downloads
//     partitions sequentially and pushes each decoded batch onto
//     a buffered channel (size readAhead); the yield loop pulls
//     one batch at a time. Consumer work on partition N overlaps
//     with downloads for N+1 … N+readAhead, hiding S3 round-trip
//     latency when the consumer does real work per record.
//     Memory: up to readAhead + 2 partitions worth of records
//     (one being yielded, readAhead buffered in the channel, one
//     in the producer's hand while its send is blocked).
//
// Dedup/no-dedup handling is identical across both modes and
// lives in emitPartition.
func (s *Reader[T]) streamByPartition(
	ctx context.Context, keys []core.KeyMeta,
	includeHistory bool, readAhead int,
	yield func(T, error) bool,
) {
	byPartition := s.groupKeysByPartition(keys)
	partitions := slices.Sorted(maps.Keys(byPartition))

	if readAhead <= 0 {
		for _, p := range partitions {
			files := byPartition[p]
			sortKeyMetasByKey(files)
			recs, err := s.downloadAndDecodeAll(ctx, files)
			if err != nil {
				yield(*new(T), err)
				return
			}
			if !s.emitPartition(recs, includeHistory, yield) {
				return
			}
		}
		return
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	pipeline := make(chan partitionBatch[T], readAhead)
	go func() {
		defer close(pipeline)
		for _, p := range partitions {
			files := byPartition[p]
			sortKeyMetasByKey(files)
			recs, err := s.downloadAndDecodeAll(ctx, files)
			select {
			case pipeline <- partitionBatch[T]{recs: recs, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	for b := range pipeline {
		if b.err != nil {
			yield(*new(T), b.err)
			return
		}
		if !s.emitPartition(b.recs, includeHistory, yield) {
			return
		}
	}
}

// sortKeyMetasByKey orders a partition's files by their S3 key
// for deterministic download order. Emission order is then
// decided by emitPartition's record-content sort, so this only
// affects the internal download pipeline's determinism.
func sortKeyMetasByKey(files []core.KeyMeta) {
	sort.Slice(files, func(i, j int) bool {
		return files[i].Key < files[j].Key
	})
}

// emitPartition yields one partition's records to the consumer.
// When EntityKeyOf is configured, applies either latest-per-
// entity dedup (default) or replica dedup (WithHistory) — the
// latter-only branch keeps distinct versions while collapsing
// byte-identical replicas. Returns false when the consumer asked
// to stop (yield returned false), so the outer loop can break
// cleanly.
//
// Records are sorted by the reader's resolved sortCmp before
// dedup / emission so callers observe a deterministic order
// regardless of LIST response shape or goroutine scheduling.
func (s *Reader[T]) emitPartition(
	recs []versionedRecord[T], includeHistory bool,
	yield func(T, error) bool,
) bool {
	if len(recs) == 0 {
		return true
	}
	sort.SliceStable(recs, func(i, j int) bool {
		return s.sortCmp(recs[i], recs[j]) < 0
	})
	if !s.cfg.dedupEnabled() {
		for _, vr := range recs {
			if !yield(vr.rec, nil) {
				return false
			}
		}
		return true
	}
	if includeHistory {
		for _, vr := range dedupReplicas(recs,
			s.cfg.EntityKeyOf, s.cfg.VersionOf) {
			if !yield(vr.rec, nil) {
				return false
			}
		}
		return true
	}
	for _, rec := range dedupLatest(recs,
		s.cfg.EntityKeyOf, s.cfg.VersionOf) {
		if !yield(rec, nil) {
			return false
		}
	}
	return true
}

// listMatchingParquet lists every parquet object under the
// plan's ListPrefix and returns the subset whose Hive key
// matches the plan's predicate, carrying each object's
// LastModified so the read path can stamp InsertedAtField from
// S3 metadata rather than re-parsing the filename. S3 LIST
// handles pagination; the predicate runs in memory per key.
func (s *Reader[T]) listMatchingParquet(
	ctx context.Context, plan *readPlan,
) ([]core.KeyMeta, error) {
	paginator := s.cfg.Target.list(plan.ListPrefix)

	var out []core.KeyMeta
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
				out = append(out, core.KeyMeta{
					Key:        objKey,
					InsertedAt: aws.ToTime(obj.LastModified),
				})
			}
		}
	}
	return out, nil
}

// listAllMatchingParquet runs listMatchingParquet across every
// plan with bounded concurrency and returns the unioned set of
// KeyMetas, deduplicated on key (overlapping plans can list the
// same parquet file, e.g. "period=*" and "period=2026-03" both
// cover March data). Fast-path: len(plans) == 1 falls through
// to the single-plan implementation with no goroutine overhead.
func (s *Reader[T]) listAllMatchingParquet(
	ctx context.Context, plans []*readPlan,
) ([]core.KeyMeta, error) {
	return runPlansConcurrent(ctx, plans,
		s.listMatchingParquet, keyMetaKey)
}

// keyMetaKey is the keyOf function for KeyMeta fan-outs.
func keyMetaKey(k core.KeyMeta) string { return k.Key }

// runPlansConcurrent forwards to core.RunPlansConcurrent,
// pinning the per-package concurrency cap. Kept as a package-
// local name so the existing call sites don't have to pass
// pollDownloadConcurrency every time.
func runPlansConcurrent[P any, R any](
	ctx context.Context,
	plans []P,
	listOne func(ctx context.Context, p P) ([]R, error),
	keyOf func(R) string,
) ([]R, error) {
	return core.RunPlansConcurrent(
		ctx, plans, pollDownloadConcurrency, listOne, keyOf)
}

// identityKey is the keyOf function for []string fan-outs — the
// element is itself the dedup key. Used by the index/backfill
// callers that haven't been migrated to []core.KeyMeta.
func identityKey(s string) string { return s }

// dedupePatterns forwards to core.DedupePatterns.
var dedupePatterns = core.DedupePatterns

// downloadAndDecodeAll fans out a bounded set of parallel
// downloads, decodes each parquet file into []T, and returns
// the concatenated result wrapped with each source file's
// insertedAt (sourced from LastModified). The input is sorted
// by S3 key for deterministic download order; user-visible
// emission order is set later by the caller's sort.
func (s *Reader[T]) downloadAndDecodeAll(
	ctx context.Context, keys []core.KeyMeta,
) ([]versionedRecord[T], error) {
	if len(keys) == 0 {
		return nil, nil
	}

	sortKeyMetasByKey(keys)

	results := make([][]versionedRecord[T], len(keys))
	errs := make([]error, len(keys))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sem := make(chan struct{}, pollDownloadConcurrency)
	var wg sync.WaitGroup
	for i, km := range keys {
		wg.Add(1)
		go func(i int, km core.KeyMeta) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				errs[i] = ctx.Err()
				return
			}
			defer func() { <-sem }()

			recs, err := s.downloadAndDecodeOne(ctx, km)
			if err != nil {
				errs[i] = err
				cancel()
				return
			}
			results[i] = recs
		}(i, km)
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

// downloadAndDecodeOne is the per-file body shared by
// downloadAndDecodeAll and the iter streaming paths. Pulls one
// parquet object from S3, decodes it, and wraps each record with
// its insertedAt plus the source filename for sort tiebreaking.
//
// insertedAt source, in priority order:
//
//  1. When InsertedAtField is configured, read the writer-
//     populated column off each decoded record — exact, same
//     value across every read path.
//  2. Otherwise fall back to the S3 object's LastModified from
//     the LIST response. DefaultVersionOf's "newer wins" dedup
//     still works on this fallback (different by the LIST-vs-
//     HEAD precision delta, but monotonic over time).
//
// Returns (nil, nil) when the object is missing — a dangling ref
// or a LIST-to-GET race. The OnMissingData hook is invoked so
// the caller can log/count without failing the read.
func (s *Reader[T]) downloadAndDecodeOne(
	ctx context.Context, km core.KeyMeta,
) ([]versionedRecord[T], error) {
	key := km.Key
	fileName := path.Base(key)

	data, err := s.cfg.Target.get(ctx, key)
	if err != nil {
		if _, ok := errors.AsType[*s3types.NoSuchKey](err); ok {
			if s.cfg.OnMissingData != nil {
				s.cfg.OnMissingData(key)
			}
			return nil, nil
		}
		return nil, fmt.Errorf("s3parquet: get %s: %w", key, err)
	}
	recs, err := decodeParquet[T](data)
	if err != nil {
		return nil, fmt.Errorf("s3parquet: decode %s: %w", key, err)
	}
	versioned := make([]versionedRecord[T], len(recs))
	for j, r := range recs {
		ia := km.InsertedAt
		if s.insertedAtFieldIndex != nil {
			ia = reflect.ValueOf(&recs[j]).Elem().
				FieldByIndex(s.insertedAtFieldIndex).
				Interface().(time.Time)
		}
		versioned[j] = versionedRecord[T]{
			rec:        r,
			insertedAt: ia,
			fileName:   fileName,
		}
	}
	return versioned, nil
}

// groupKeysByPartition splits a flat list of data-file KeyMetas
// into one slice per Hive partition (the path between dataPath
// and the filename). Emission order is decided by the record-
// level sort in emitPartition — groupKeysByPartition itself no
// longer implies chronological-within-partition output.
func (s *Reader[T]) groupKeysByPartition(
	keys []core.KeyMeta,
) map[string][]core.KeyMeta {
	out := make(map[string][]core.KeyMeta)
	for _, k := range keys {
		hk, ok := hiveKeyOfDataFile(k.Key, s.dataPath)
		if !ok {
			// Defensively skip keys that don't parse. List paths
			// already filtered to .parquet, so reaching here means
			// a layout corruption — drop, don't error.
			continue
		}
		out[hk] = append(out[hk], k)
	}
	return out
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

// dedupReplicas collapses records that share (entity, version)
// down to one, keeping the first-seen occurrence. Two records
// with identical (entity, version) describe the same logical
// write (a retry, zombie, or cross-node race); emitting both as
// "distinct versions" would mislead callers.
//
// Only called on the WithHistory path. Without WithHistory,
// dedupLatest already collapses replicas as a side effect of
// its max-version-per-entity reduction (first-seen wins on ties,
// and replicas tie on version).
//
// Runs after the reader's sort, so "first-seen" is deterministic:
// input order is (entity, version) ascending, tiebroken by the
// lex-first source filename via sort stability.
func dedupReplicas[T any](
	records []versionedRecord[T],
	entityKey func(T) string,
	versionOf func(record T, insertedAt time.Time) int64,
) []versionedRecord[T] {
	if len(records) == 0 {
		return nil
	}
	type key struct {
		entity  string
		version int64
	}
	seen := make(map[key]struct{}, len(records))
	out := make([]versionedRecord[T], 0, len(records))
	for _, vr := range records {
		k := key{
			entity:  entityKey(vr.rec),
			version: versionOf(vr.rec, vr.insertedAt),
		}
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, vr)
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
