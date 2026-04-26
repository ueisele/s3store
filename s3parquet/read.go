package s3parquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"slices"
	"sort"

	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/parquet-go/parquet-go"
	"github.com/ueisele/s3store/internal/core"
)

// Read returns all records whose data files match any of the
// given key patterns. Patterns use the grammar described in
// core.ValidateKeyPattern; pass multiple when the target set isn't
// a Cartesian product (e.g. (period=A, customer=X) and
// (period=B, customer=Y) but not the off-diagonal pairs).
//
// When EntityKeyOf and VersionOf are configured, the result is
// deduplicated globally to the latest version per entity across
// the union (pass WithHistory to opt out). Overlapping patterns
// are safe — each parquet file is fetched and decoded at most once.
//
// All records are buffered before return — for unbounded reads,
// use ReadIter instead. Empty patterns slice returns (nil, nil);
// a malformed pattern fails with the offending index.
func (s *Reader[T]) Read(
	ctx context.Context, keyPatterns []string, opts ...QueryOption,
) ([]T, error) {
	var o core.QueryOpts
	o.Apply(opts...)

	keys, err := ResolvePatterns(
		ctx, s.cfg.Target, core.DedupePatterns(keyPatterns), &o)
	if err != nil {
		return nil, fmt.Errorf("s3parquet: Read %w", err)
	}
	if len(keys) == 0 {
		return nil, nil
	}

	records, err := s.downloadAndDecodeAll(ctx, keys)
	if err != nil {
		return nil, err
	}
	return s.sortAndCollect(records, o.IncludeHistory), nil
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
//     records that share (entity, version) to one. Replicas are
//     by definition equivalent (same logical write) so the
//     specific record yielded is implementation-defined.
//   - default: dedupLatestSeq emits the highest-version record
//     per entity. Sort places that record LAST in each entity
//     group; the walker's `pending` advances on every iteration
//     so when multiple records share the max version (replicas of
//     the latest write) the LAST one wins — which is the
//     lex-later filename, matching s3sql's
//     `ORDER BY filename DESC`.
//
// Used by Read / PollRecords (sync, collect into a pre-sized
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

// sortAndCollect is the sync-caller wrapper around sortAndIterate
// that materialises the result into a []T. When dedup is disabled
// (sortCmp == nil) the input slice is returned as-is — no copy,
// no allocation — since sortAndIterate would just yield each
// record back unchanged. When dedup is enabled the result is
// collected into a slice pre-sized to len(records) (an upper
// bound; the actual size after dedup is ≤ len(records)).
//
// Used by Read and PollRecords. Streaming callers
// (emitPartition) still iterate sortAndIterate directly.
func (s *Reader[T]) sortAndCollect(
	records []T, includeHistory bool,
) []T {
	if s.sortCmp == nil {
		return records
	}
	out := make([]T, 0, len(records))
	for r := range s.sortAndIterate(records, includeHistory) {
		out = append(out, r)
	}
	return out
}

// identityKey is the keyOf function for []string fan-outs — the
// element is itself the dedup key. Used by index/backfill
// callers that union per-pattern lookup results.
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
	return recs, nil
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
// to one. Input MUST be pre-sorted by (entity, version) ascending
// so equal pairs land adjacent and the one-pass walk works with
// O(1) state. Only called on the WithHistory path; without
// WithHistory, dedupLatestSeq does its own per-entity reduction.
//
// Which physical record is yielded for a (entity, version) tied
// group is implementation-defined: under the WithHistory contract
// replicas describe the same logical write (a retry, zombie, or
// cross-node race) and are equivalent, so any pick is correct.
// This implementation happens to yield the first one and skip
// the rest — cheaper than tracking a `pending` and emitting on
// transition — but callers must not rely on that.
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

// dedupLatestSeq keeps the highest-version record per entity,
// emitting one record per entity in entity-sort order (ascending).
// Input MUST be pre-sorted by (entity, version) ascending — the
// LAST record in each contiguous entity group is then the latest
// version for that entity, so a single pass with O(1) state
// suffices: hold the current "pending" record, emit it on entity
// transition, emit the final pending after the loop.
//
// Tie-break on equal max version: `pending = r` advances on every
// iteration, so when multiple records share the highest version
// the LAST one wins. With stable sort + lex-ordered input
// (preparePartitions sorts files by S3 key) this means the
// lex-later filename wins — same rule as s3sql's
// `ORDER BY filename DESC`.
//
// versionOf isn't called inside the walk — sort already placed
// records in version order. Not on the signature; the
// pre-condition is documented above.
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
