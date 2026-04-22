package s3parquet

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
)

// WriteRowGroupsBy is the auto-key sibling of
// WriteWithKeyRowGroupsBy: groups records by PartitionKeyOf (same
// as Write), and within each partition produces one row group
// per distinct flushKeyOf value. Use when a batch spans multiple
// partitions and you want each partition's file to be pruneable
// by the flush column.
//
// Shares Write's contract: returns one WriteResult per partition
// in sorted-key order, fan-out bounded by PartitionWriteConcurrency,
// first real failure wins with partial results surfaced, empty
// records → (nil, nil), PartitionKeyOf required, caller-cancel
// reported as an error even when no real failure occurred.
//
// flushKeyOf is applied inside each partition after grouping, so
// records that share a partition key are sorted by flushKeyOf
// and flushed to a new row group on every flushKeyOf change.
// Records in different partitions can share flushKeyOf values
// without interference — each partition's file is independent.
//
// flushKeyOf must be non-nil. See WriteWithKeyRowGroupsBy for the
// call-cost note — the closure is called O(N log N) during sort
// plus ~2N during encode, per partition.
func (s *Writer[T]) WriteRowGroupsBy(
	ctx context.Context, records []T,
	flushKeyOf func(T) string,
) ([]WriteResult, error) {
	if len(records) == 0 {
		return nil, nil
	}
	if s.cfg.PartitionKeyOf == nil {
		return nil, fmt.Errorf(
			"s3parquet: PartitionKeyOf is required for " +
				"WriteRowGroupsBy; use WriteWithKeyRowGroupsBy " +
				"for explicit keys")
	}
	if flushKeyOf == nil {
		return nil, fmt.Errorf(
			"s3parquet: WriteRowGroupsBy: flushKeyOf is required")
	}
	return s.writeGroupedFanOut(ctx, records,
		func(ctx context.Context, key string, recs []T) (*WriteResult, error) {
			return s.WriteWithKeyRowGroupsBy(ctx, key, recs, flushKeyOf)
		})
}

// WriteWithKeyRowGroupsBy writes records to a single parquet file at
// key, producing one row group per distinct flushKeyOf value.
// Records are sorted by flushKeyOf first, so the resulting file
// has tight per-row-group min/max statistics on whichever column
// flushKeyOf reads from — ReadIterWhere + a predicate on that
// column can then prune to the exact row group.
//
// Use when partitions are wider than the natural entity unit —
// e.g. charge_period_start={day} holding all customers for a
// day, with one row group per customer. The read-side predicate
// column name must be the parquet tag of the field flushKeyOf
// reads from; otherwise the min/max stats won't line up with the
// predicate.
//
// Semantics mirror WriteWithKey: empty records → (nil, nil);
// markers and the ref are emitted identically; cleanup on
// ref-PUT failure uses the same lost-ack + orphan-delete path.
// Stable sort, so within-flushKey order is deterministic across
// retries (matters for VersionOf tie-breaking on Read dedup).
//
// flushKeyOf must be non-nil; a nil closure is rejected as a
// caller bug rather than silently falling back to a single row
// group. It is called O(N log N) times during the sort and twice
// per record during encode (to detect run boundaries), so keep
// it a cheap struct-field read — allocating helpers like
// fmt.Sprintf inside will dominate the write path for large
// batches.
func (s *Writer[T]) WriteWithKeyRowGroupsBy(
	ctx context.Context, key string, records []T,
	flushKeyOf func(T) string,
) (*WriteResult, error) {
	if len(records) == 0 {
		return nil, nil
	}
	if flushKeyOf == nil {
		return nil, fmt.Errorf(
			"s3parquet: WriteWithKeyRowGroupsBy: flushKeyOf is required")
	}
	if err := s.validateKey(key); err != nil {
		return nil, err
	}

	sorted := slices.Clone(records)
	sort.SliceStable(sorted, func(i, j int) bool {
		return flushKeyOf(sorted[i]) < flushKeyOf(sorted[j])
	})

	// Capture writeStartTime here (before encode) so the same
	// value propagates into the InsertedAtField column, the data
	// filename's tsMicros, and the x-amz-meta-created-at header —
	// matching the plain-write path semantics.
	writeStartTime := time.Now()
	if s.insertedAtFieldIndex != nil {
		populateInsertedAt(sorted, s.insertedAtFieldIndex, writeStartTime)
	}

	parquetBytes, err := encodeParquetWithFlush(
		sorted, s.compressionCodec, flushKeyOf)
	if err != nil {
		return nil, fmt.Errorf(
			"s3parquet: parquet encode: %w", err)
	}

	// Pass the sorted slice to writeEncodedPayload so index
	// markers (if any) enumerate the actually-stored records in
	// the same order. collectIndexMarkerPaths dedupes by path, so
	// ordering doesn't change the marker set, but we keep the
	// invariant that records passed here match the file contents.
	return s.writeEncodedPayload(ctx, key, sorted, parquetBytes, writeStartTime)
}

// encodeParquetWithFlush writes records into a parquet byte
// stream, flushing a new row group on every flushKeyOf change.
// Callers must have sorted records by flushKeyOf first so equal
// values appear in contiguous runs; otherwise a record whose
// flushKey differs from the previous one will still start a new
// row group, but runs for the same flushKey may be split across
// multiple row groups (wasteful but still correct).
//
// Writes one batch per run rather than one record at a time so
// parquet-go's per-call overhead is amortised. Close() emits the
// final row group, so no trailing Flush is needed.
func encodeParquetWithFlush[T any](
	records []T, codec compress.Codec, flushKeyOf func(T) string,
) ([]byte, error) {
	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[T](
		&buf, parquet.Compression(codec))

	i := 0
	for i < len(records) {
		j := i + 1
		cur := flushKeyOf(records[i])
		for j < len(records) && flushKeyOf(records[j]) == cur {
			j++
		}
		if _, err := writer.Write(records[i:j]); err != nil {
			return nil, err
		}
		if j < len(records) {
			if err := writer.Flush(); err != nil {
				return nil, err
			}
		}
		i = j
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
