package s3store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/parquet-go/parquet-go"
)

// Read returns all records whose data files match any of the
// given key patterns. Patterns use the grammar described in
// validateKeyPattern; pass multiple when the target set isn't
// a Cartesian product (e.g. (period=A, customer=X) and
// (period=B, customer=Y) but not the off-diagonal pairs).
//
// When EntityKeyOf and VersionOf are configured, the result is
// deduplicated per Hive partition to the latest version per
// entity (pass WithHistory to opt out). Correctness requires
// EntityKeyOf to be fully determined by the partition key so no
// entity ever spans partitions — same precondition as ReadIter.
// Overlapping patterns are safe — each parquet file is fetched
// and decoded at most once.
//
// Records emit in partition-lex order with per-partition
// (entity, version) order within each. All records are buffered
// before return — for unbounded reads, use ReadIter or
// ReadPartitionIter instead. Empty patterns slice returns
// (nil, nil); a malformed pattern fails with the offending
// index.
//
// On NoSuchKey: Read fails (LIST-to-GET race is rare enough
// that surfacing it as an error is more honest than silently
// skipping, and the caller's retry resolves it).
func (s *Reader[T]) Read(
	ctx context.Context, keyPatterns []string, opts ...ReadOption,
) (out []T, err error) {
	scope := s.cfg.Target.metrics.methodScope(ctx, methodRead)
	defer scope.end(&err)
	var o readOpts
	o.apply(opts...)

	keys, err := resolvePatterns(
		ctx, s.cfg.Target, keyPatterns, methodRead)
	if err != nil {
		return nil, fmt.Errorf("Read: %w", err)
	}
	if len(keys) == 0 {
		return nil, nil
	}

	var batchErr error
	emit := func(_ string, recs []T, e error) (int64, bool) {
		if e != nil {
			batchErr = e
			return 0, false
		}
		out = append(out, recs...)
		return int64(len(recs)), true
	}
	s.downloadAndDecodeIter(ctx, keys, &o, scope, emit)
	if batchErr != nil {
		return nil, batchErr
	}
	return out, nil
}

// identityKey is the keyOf function for []string fan-outs — the
// element is itself the dedup key. Used by projection/backfill
// callers that union per-pattern lookup results.
func identityKey(s string) string { return s }

// methodTolerantOfMissingData reports whether a method should
// skip-and-warn on NoSuchKey rather than fail. Tolerant: paths
// where a single missing data file shouldn't poison the whole
// operation and a caller retry can't easily resolve it (refs and
// projection markers persist beyond the data file).
//
//   - PollRecords / ReadRangeIter / ReadPartitionRangeIter walk
//     the ref stream; an operator-driven prune can leave a ref
//     pointing at nothing and the consumer must keep advancing.
//   - ReadEntriesIter / ReadPartitionEntriesIter take pre-resolved
//     StreamEntry slices; an operator prune between resolution
//     and read can race the same way, with no caller retry able
//     to resolve it (the entry set is fixed).
//   - BackfillProjection is a long-running operator job; failing on
//     one race-deleted file would force a full restart.
//
// Strict: paths where a NoSuchKey is genuinely a LIST-to-GET
// race, narrow in practice, and a caller retry resolves it (the
// next LIST won't include the deleted file).
//
//   - Read / ReadIter / ReadPartitionIter are user-facing
//     single-shot snapshot reads; loud failure is more honest
//     than silent skip.
func methodTolerantOfMissingData(m methodKind) bool {
	switch m {
	case methodPollRecords, methodReadRangeIter,
		methodReadPartitionRangeIter, methodReadEntriesIter,
		methodReadPartitionEntriesIter, methodBackfill:
		return true
	default:
		return false
	}
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
