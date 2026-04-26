package s3parquet

import (
	"iter"
	"slices"
	"sort"
)

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
