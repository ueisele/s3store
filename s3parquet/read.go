package s3parquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"maps"
	"slices"
	"sort"

	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/parquet-go/parquet-go"
	"github.com/ueisele/s3store/internal/core"
)

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

	patterns = core.DedupePatterns(patterns)
	if len(patterns) == 0 {
		return nil, nil
	}

	plans, err := core.BuildReadPlans(patterns, s.dataPath, s.cfg.Target.PartitionKeyParts())
	if err != nil {
		return nil, fmt.Errorf("s3parquet: ReadMany %w", err)
	}

	keys, err := s.cfg.Target.ListDataFilesMany(ctx, plans, s.cfg.ConsistencyControl)
	if err != nil {
		return nil, err
	}
	keys, err = core.ApplyIdempotentReadOpts(keys, s.dataPath, &o)
	if err != nil {
		return nil, fmt.Errorf("s3parquet: %w", err)
	}
	if len(keys) == 0 {
		return nil, nil
	}

	records, err := s.downloadAndDecodeAll(ctx, keys)
	if err != nil {
		return nil, err
	}

	out := make([]T, 0, len(records))
	for r := range s.sortAndIterate(records, o.IncludeHistory) {
		out = append(out, r)
	}
	return out, nil
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

		patterns = core.DedupePatterns(patterns)
		if len(patterns) == 0 {
			return
		}

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		plans, err := core.BuildReadPlans(patterns, s.dataPath, s.cfg.Target.PartitionKeyParts())
		if err != nil {
			yield(*new(T), fmt.Errorf("s3parquet: ReadManyIter %w", err))
			return
		}

		keys, err := s.cfg.Target.ListDataFilesMany(ctx, plans, s.cfg.ConsistencyControl)
		if err != nil {
			yield(*new(T), err)
			return
		}
		keys, err = core.ApplyIdempotentReadOpts(keys, s.dataPath, &o)
		if err != nil {
			yield(*new(T), fmt.Errorf("s3parquet: %w", err))
			return
		}
		if len(keys) == 0 {
			return
		}

		s.streamEager(ctx, keys, &o, yield)
	}
}

// partitionBatch is one partition's download result, passed from
// the background producer to the yield loop over the pipeline
// channel in the readAhead>0 path.
type partitionBatch[T any] struct {
	recs []T
	err  error
}

// streamByPartition processes keys one Hive partition at a time
// in lex order, using downloadOne to fetch each partition's files.
// Two execution modes:
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
//     Memory: up to readAhead + 2 partitions worth of records.
//
// Dedup/no-dedup handling is identical across both modes and
// lives in emitPartition. The downloadOne function lets the
// row-group-filtered read path (ReadIterWhere) reuse this same
// orchestrator with a different per-partition fetcher.
func (s *Reader[T]) streamByPartition(
	ctx context.Context, keys []core.KeyMeta,
	includeHistory bool, readAhead int,
	downloadOne func(ctx context.Context, files []core.KeyMeta) ([]T, error),
	yield func(T, error) bool,
) {
	byPartition := s.groupKeysByPartition(keys)
	partitions := slices.Sorted(maps.Keys(byPartition))

	if readAhead <= 0 {
		for _, p := range partitions {
			files := byPartition[p]
			sortKeyMetasByKey(files)
			recs, err := downloadOne(ctx, files)
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
			recs, err := downloadOne(ctx, files)
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

// sortAndIterate sorts records in place by the reader's resolved
// sortCmp (when dedup is enabled) and yields them through the
// dedup cascade as an iter.Seq[T]:
//
//   - dedup disabled (sortCmp == nil): yields records in input
//     order — no sort, no allocation.
//   - WithHistory + dedup enabled: dedupReplicasSeq collapses
//     records that share (entity, version) to one (first-seen
//     wins on ties), preserving distinct versions.
//   - default: dedupLatestSeq emits the LAST record of each
//     entity group (sort places max-version last per entity);
//     replicas collapse as a side effect since ties on version
//     keep the first-seen via stable sort.
//
// Used by ReadMany / PollRecords (sync, collect into a pre-sized
// []T) and emitPartition (streaming, yield directly). Returning
// iter.Seq[T] lets the streaming caller skip the per-partition
// []T materialization that the old slice-returning helper
// allocated.
func (s *Reader[T]) sortAndIterate(
	records []T, includeHistory bool,
) iter.Seq[T] {
	if s.sortCmp == nil {
		return slices.Values(records)
	}
	sort.SliceStable(records, func(i, j int) bool {
		return s.sortCmp(records[i], records[j]) < 0
	})
	if includeHistory {
		return dedupReplicasSeq(records,
			s.cfg.EntityKeyOf, s.cfg.VersionOf)
	}
	return dedupLatestSeq(records, s.cfg.EntityKeyOf)
}

// emitPartition yields one partition's records to the consumer
// through sortAndIterate, so the iter paths observe the same
// order / replica-collapse / latest-per-entity semantics as the
// materialised Read paths. Returns false when the consumer asked
// to stop (yield returned false), so the outer loop can break
// cleanly.
func (s *Reader[T]) emitPartition(
	recs []T, includeHistory bool,
	yield func(T, error) bool,
) bool {
	for r := range s.sortAndIterate(recs, includeHistory) {
		if !yield(r, nil) {
			return false
		}
	}
	return true
}

// identityKey is the keyOf function for []string fan-outs — the
// element is itself the dedup key. Used by the index/backfill
// callers that haven't been migrated to []core.KeyMeta.
func identityKey(s string) string { return s }

// downloadAndDecodeAll fans out a bounded set of parallel
// downloads, decodes each parquet file into []T, and returns
// the concatenated result. The input is sorted by S3 key for
// deterministic download order; user-visible emission order is
// set later by the caller's sortAndIterate.
func (s *Reader[T]) downloadAndDecodeAll(
	ctx context.Context, keys []core.KeyMeta,
) ([]T, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	sortKeyMetasByKey(keys)

	results := make([][]T, len(keys))
	if err := core.FanOut(ctx, keys,
		s.cfg.Target.EffectiveMaxInflightRequests(),
		func(ctx context.Context, i int, km core.KeyMeta) error {
			recs, err := s.downloadAndDecodeOne(ctx, km)
			if err != nil {
				return err
			}
			results[i] = recs
			return nil
		}); err != nil {
		return nil, err
	}

	// Count for preallocation.
	total := 0
	for _, r := range results {
		total += len(r)
	}
	out := make([]T, 0, total)
	for _, r := range results {
		out = append(out, r...)
	}
	return out, nil
}

// downloadAndDecodeOne is the per-file body shared by
// downloadAndDecodeAll and the iter streaming paths. Pulls one
// parquet object from S3 and decodes it into []T.
//
// Returns (nil, nil) when the object is missing — a dangling ref
// or a LIST-to-GET race. The OnMissingData hook is invoked so
// the caller can log/count without failing the read.
func (s *Reader[T]) downloadAndDecodeOne(
	ctx context.Context, km core.KeyMeta,
) ([]T, error) {
	key := km.Key

	data, err := s.cfg.Target.get(
		ctx, key, s.cfg.ConsistencyControl)
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
	return recs, nil
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
		hk, ok := core.HiveKeyOfDataFile(k.Key, s.dataPath)
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

// dedupReplicasSeq collapses records that share (entity, version)
// to one, keeping the first occurrence per group. Input MUST be
// pre-sorted by (entity, version) ascending so equal pairs land
// adjacent and the one-pass walk works with O(1) state. Only
// called on the WithHistory path; without WithHistory,
// dedupLatestSeq absorbs replicas as a side effect of picking
// the per-entity tail.
//
// "First occurrence" is deterministic because the upstream
// sort.SliceStable preserves input order for equal-key elements,
// and the upstream input is in file lex order.
func dedupReplicasSeq[T any](
	records []T,
	entityKey func(T) string,
	versionOf func(T) int64,
) iter.Seq[T] {
	return func(yield func(T) bool) {
		if len(records) == 0 {
			return
		}
		var prevEntity string
		var prevVersion int64
		first := true
		for _, r := range records {
			e := entityKey(r)
			v := versionOf(r)
			if !first && e == prevEntity && v == prevVersion {
				continue
			}
			if !yield(r) {
				return
			}
			prevEntity = e
			prevVersion = v
			first = false
		}
	}
}

// dedupLatestSeq keeps the record with the maximum version per
// entity, emitting one record per entity in entity-sort order
// (ascending). Input MUST be pre-sorted by (entity, version)
// ascending — the LAST record in each contiguous entity group is
// then the latest version for that entity, so a single pass with
// O(1) state suffices: hold the current "pending" record, emit
// it on entity transition, emit the final pending after the loop.
//
// versionOf isn't called inside the walk — sort already placed
// records in version order. Kept on the signature for API
// symmetry with dedupReplicasSeq and to document the
// pre-condition.
func dedupLatestSeq[T any](
	records []T, entityKey func(T) string,
) iter.Seq[T] {
	return func(yield func(T) bool) {
		if len(records) == 0 {
			return
		}
		var prevEntity string
		var pending T
		first := true
		for _, r := range records {
			e := entityKey(r)
			if !first && e != prevEntity {
				if !yield(pending) {
					return
				}
			}
			pending = r
			prevEntity = e
			first = false
		}
		yield(pending)
	}
}
