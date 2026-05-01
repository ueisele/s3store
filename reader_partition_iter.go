package s3store

import (
	"context"
	"fmt"
	"iter"
	"time"
)

// HivePartition is the canonical (Key, Rows) value type for
// per-partition reads and writes. Key is the partition-key
// string ("period=X/customer=Y") matching what WriteWithKey
// takes as input.
//
// Read-side: ReadPartitionIter / ReadPartitionRangeIter yield
// one HivePartition per Hive partition with Rows already
// sort+dedup'd (per-partition latest-per-entity by default; opt
// out with WithHistory for replica-only collapse). Yield order
// is lex-ascending by Key. Within Rows, order depends on dedup
// configuration: when EntityKeyOf is set, records are in
// (entity, version) ascending order under both dedup paths (the
// WithHistory replica path still sorts by the same comparator).
// When EntityKeyOf is nil (no dedup configured), records emit
// in decode order: file lex order, then parquet row order
// within each file.
//
// Write-side: GroupByPartition returns []HivePartition[T]
// lex-ordered by Key, with per-partition Rows in input
// (insertion) order. No sort or dedup runs on the write side —
// the writer doesn't have an EntityKeyOf to compare on.
//
// Both sides share the deterministic-emission contract: same
// input produces byte-identical HivePartition slices across
// calls. See "Deterministic emission order across read and
// write paths" in CLAUDE.md.
type HivePartition[T any] struct {
	Key  string
	Rows []T
}

// ReadPartitionIter is the partition-grouped variant of ReadIter:
// instead of yielding records one at a time, it yields one
// HivePartition[T] per Hive partition with all of that partition's
// records collected as Rows.
//
// Use when the consumer needs to process a partition's records as
// a batch — joins, group-bys, per-partition aggregates — without
// re-grouping a flat record stream by hand.
//
// Dedup, byte-budget, read-ahead, and missing-data semantics are
// identical to ReadIter:
//   - Per-partition dedup: correct only when the partition key
//     strictly determines every component of EntityKeyOf so no
//     entity ever spans partitions. Use WithHistory to opt out
//     (replica-only collapse).
//   - Memory bounded by WithReadAheadPartitions /
//     WithReadAheadBytes.
//   - NoSuchKey on a data file is strict (mirrors ReadIter): the
//     iter surfaces the wrapped error so the caller's retry can
//     resolve the LIST-to-GET race.
//
// Breaking out of the for-range loop cancels in-flight downloads.
// Empty patterns slice yields nothing; a malformed pattern
// surfaces as the iter's first error.
func (s *Reader[T]) ReadPartitionIter(
	ctx context.Context, keyPatterns []string, opts ...ReadOption,
) iter.Seq2[HivePartition[T], error] {
	return func(yield func(HivePartition[T], error) bool) {
		scope := s.cfg.Target.metrics.methodScope(ctx, methodReadPartitionIter)
		var iterErr error
		defer scope.end(&iterErr)
		var o readOpts
		o.apply(opts...)

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		keys, err := resolvePatterns(
			ctx, s.cfg.Target, keyPatterns, methodReadPartitionIter)
		if err != nil {
			iterErr = fmt.Errorf("ReadPartitionIter: %w", err)
			yield(HivePartition[T]{}, iterErr)
			return
		}
		if len(keys) == 0 {
			return
		}

		s.downloadAndDecodeIter(ctx, keys, &o, scope, false,
			partitionEmit(yield, &iterErr))
	}
}

// ReadPartitionRangeIter is the partition-grouped variant of
// ReadRangeIter: streams every record written in the
// [since, until) time window as one HivePartition[T] per Hive
// partition.
//
// Same time-window resolution as ReadRangeIter: zero time.Time on
// either bound means unbounded — since=zero starts at the stream
// head, until=zero walks to the live tip (now - SettleWindow,
// captured at call entry so the upper bound stays stable under
// concurrent writes).
//
// Same per-partition dedup as ReadRangeIter (default
// latest-per-entity per partition; pass WithHistory to opt out
// for replica-only). Memory bounded by WithReadAheadPartitions /
// WithReadAheadBytes.
//
// Tolerant of NoSuchKey on data files (mirrors ReadRangeIter):
// missing files are skipped with a slog.Warn + missing-data
// counter so the consumer can keep advancing — operator-driven
// prunes leave refs pointing at deleted files and a strict failure
// here can't be resolved by a caller retry.
//
// Does NOT expose per-batch offsets; consumer aborts cannot
// safely resume. Use PollRecords + manual partition grouping when
// you need to checkpoint between batches.
func (s *Reader[T]) ReadPartitionRangeIter(
	ctx context.Context,
	since, until time.Time,
	opts ...ReadOption,
) iter.Seq2[HivePartition[T], error] {
	// Resolve time bounds outside the closure so the live-tip
	// snapshot freezes at call entry, not at first iteration —
	// see resolveRangeBounds.
	sinceOffset, untilOffset := s.resolveRangeBounds(since, until)

	return func(yield func(HivePartition[T], error) bool) {
		scope := s.cfg.Target.metrics.methodScope(ctx, methodReadPartitionRangeIter)
		var iterErr error
		defer scope.end(&iterErr)
		var o readOpts
		o.apply(opts...)

		keys, err := s.walkRangeKeys(ctx, sinceOffset, untilOffset)
		if err != nil {
			iterErr = err
			yield(HivePartition[T]{}, err)
			return
		}
		if len(keys) == 0 {
			return
		}

		s.downloadAndDecodeIter(ctx, keys, &o, scope, true,
			partitionEmit(yield, &iterErr))
	}
}

// ReadPartitionEntriesIter is the partition-grouped variant of
// ReadEntriesIter: streams records from a pre-resolved
// []StreamEntry as one HivePartition[T] per Hive partition.
//
// Use when the consumer already has the entries (typically from
// PollRange) AND wants per-partition batches for joins,
// group-bys, or zip-by-partition workflows across multiple
// Stores.
//
// Same per-partition dedup, byte-budget, read-ahead, tolerant-
// NoSuchKey semantics as ReadPartitionRangeIter. Same upfront
// cross-store guard as ReadEntriesIter — every entry's DataPath
// must belong to this Reader's prefix or the iter yields a
// wrapped error before any S3 traffic.
//
// Empty entries slice yields nothing without error. Partitions
// emit in lex order of HivePartition.Key.
func (s *Reader[T]) ReadPartitionEntriesIter(
	ctx context.Context, entries []StreamEntry, opts ...ReadOption,
) iter.Seq2[HivePartition[T], error] {
	return func(yield func(HivePartition[T], error) bool) {
		scope := s.cfg.Target.metrics.methodScope(ctx, methodReadPartitionEntriesIter)
		var iterErr error
		defer scope.end(&iterErr)
		var o readOpts
		o.apply(opts...)

		if err := s.validateEntriesBelongHere(entries); err != nil {
			iterErr = err
			yield(HivePartition[T]{}, err)
			return
		}
		if len(entries) == 0 {
			return
		}

		keys := entriesToKeys(entries)
		s.downloadAndDecodeIter(ctx, keys, &o, scope, true,
			partitionEmit(yield, &iterErr))
	}
}

// partitionEmit returns the per-batch emit callback that yields
// one HivePartition[T] per partition. Used by ReadPartitionIter /
// ReadPartitionRangeIter / ReadPartitionEntriesIter — paths that
// surface records grouped by Hive partition.
//
// On a hard pipeline error: sets *iterErr, yields a zero
// HivePartition with the error, returns (0, false) so the emit
// loop terminates and scope.end picks up iterErr for outcome
// classification.
//
// On success: yields one HivePartition{Key: partKey, Rows: recs}
// with the already-dedup'd records. Returns (len(recs), true) on
// consumer accept, (0, false) on consumer break — the partition
// is either fully accepted or fully dropped.
//
// Free function (not a method) because the closure needs no
// Reader state — partKey and recs come from the pipeline and
// HivePartition is a plain value type.
func partitionEmit[T any](
	yield func(HivePartition[T], error) bool, iterErr *error,
) func(string, []T, error) (int64, bool) {
	return func(partKey string, recs []T, err error) (int64, bool) {
		if err != nil {
			*iterErr = err
			yield(HivePartition[T]{}, err)
			return 0, false
		}
		if !yield(HivePartition[T]{Key: partKey, Rows: recs}, nil) {
			return 0, false
		}
		return int64(len(recs)), true
	}
}
