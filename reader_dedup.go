package s3store

import (
	"sort"
)

// sortAndDedup sorts records in-place by the reader's resolved
// sortCmp (when dedup is enabled) and returns the slice with
// duplicates compacted out. Three modes:
//
//   - dedup disabled (sortCmp == nil): returns records unchanged
//     (input order, no copy).
//   - WithHistory + dedup enabled: dedupReplicas collapses records
//     that share (entity, version) to one. Replicas are by
//     definition equivalent (same logical write) so the specific
//     record retained is implementation-defined.
//   - default: dedupLatest emits the highest-version record per
//     entity. Sort places that record LAST in each entity group;
//     the walker advances `pending` on every iteration so when
//     multiple records share the max version (replicas of the
//     latest write) the LAST one wins — i.e. the lex-later
//     filename within a partition.
//
// In-place compaction: the returned slice shares the input's
// backing array (returned as records[:n], n <= len(records)).
// Callers may treat the input slice as consumed.
func (s *Reader[T]) sortAndDedup(
	records []T, includeHistory bool,
) []T {
	if s.sortCmp == nil {
		return records
	}
	sort.SliceStable(records, func(i, j int) bool {
		return s.sortCmp(records[i], records[j]) < 0
	})
	if includeHistory {
		return dedupReplicas(records,
			s.cfg.EntityKeyOf, s.cfg.VersionOf)
	}
	return dedupLatest(records, s.cfg.EntityKeyOf)
}

// dedupReplicas collapses records that share (entity, version) to
// one in-place. Input MUST be pre-sorted by (entity, version)
// ascending so equal pairs land adjacent and the one-pass walk
// works with O(1) state.
//
// Which physical record survives a (entity, version) tied group
// is implementation-defined: under the WithHistory contract
// replicas describe the same logical write (a retry, zombie, or
// cross-node race) and are equivalent, so any pick is correct.
// This implementation keeps the FIRST record of each tied group
// and skips the rest.
//
// Returns records[:n] — same backing array, length truncated to
// the survivor count.
func dedupReplicas[T any](
	records []T,
	entityKey func(T) string,
	versionOf func(T) int64,
) []T {
	if len(records) == 0 {
		return records
	}
	n := 0
	var prevEntity string
	var prevVersion int64
	first := true
	for _, r := range records {
		e := entityKey(r)
		v := versionOf(r)
		if !first && e == prevEntity && v == prevVersion {
			continue
		}
		records[n] = r
		n++
		prevEntity = e
		prevVersion = v
		first = false
	}
	return records[:n]
}

// dedupLatest keeps the highest-version record per entity in-place,
// emitting one record per entity in entity-sort order (ascending).
// Input MUST be pre-sorted by (entity, version) ascending — the
// LAST record in each contiguous entity group is then the latest
// version for that entity, so a single pass with O(1) state
// suffices: hold the current "pending" record, write it on entity
// transition, write the final pending after the loop.
//
// Tie-break on equal max version: `pending = r` advances on every
// iteration, so when multiple records share the highest version
// the LAST one wins. With stable sort + lex-ordered input
// (preparePartitions sorts files by S3 key) this means the
// lex-later filename wins.
//
// Returns records[:n] — same backing array, length truncated to
// the survivor count. Safety: n <= i throughout (each entity
// group contributes one survivor, written before reading the next
// group's first record), so the in-place writes never clobber an
// unread input slot.
func dedupLatest[T any](
	records []T, entityKey func(T) string,
) []T {
	if len(records) == 0 {
		return records
	}
	n := 0
	var prevEntity string
	var pending T
	first := true
	for _, r := range records {
		e := entityKey(r)
		if !first && e != prevEntity {
			records[n] = pending
			n++
		}
		pending = r
		prevEntity = e
		first = false
	}
	records[n] = pending
	n++
	return records[:n]
}
