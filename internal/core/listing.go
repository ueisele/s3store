package core

import "time"

// KeyMeta pairs an S3 object key with an insertedAt timestamp
// — the file's write time as best the caller can derive it —
// and the compressed object size from the LIST response.
// Threaded through the read path so downstream consumers can
// stamp versionedRecord.insertedAt without re-consulting S3,
// and so byte-budget read-ahead can decide whether to pre-fetch
// without an extra HEAD.
//
// Source of InsertedAt depends on the call site:
//   - ListDataFiles: S3 LastModified from the LIST response.
//   - PollRecords: the ref filename's dataTsMicros (writer's
//     wall clock at write-start), which is more accurate than
//     the ref's LastModified.
//
// Both are monotonic, both serve as DefaultVersionOf fallback
// inputs and as the sort-fallback tiebreaker when no
// InsertedAtField column is configured.
//
// Size is the compressed object size from the LIST response
// (S3's Contents.Size). Zero when the call site doesn't have
// access to a LIST result (e.g. PollRecords assembles KeyMetas
// from ref filenames). Used by ReadIter / ReadManyIter as the
// download-stage memory estimate; the uncompressed size used by
// the byte-budget gate is read from each parquet file's footer
// after download.
type KeyMeta struct {
	Key        string
	InsertedAt time.Time
	Size       int64
}

// UnionKeys flattens a set of per-plan LIST results into a
// single deduplicated slice, preserving first-seen order so the
// downstream GET pipeline is deterministic. keyOf extracts the
// dedup key from each result (use the identity function when R
// is itself a string).
func UnionKeys[R any](perPlan [][]R, keyOf func(R) string) []R {
	total := 0
	for _, r := range perPlan {
		total += len(r)
	}
	if total == 0 {
		return nil
	}
	seen := make(map[string]struct{}, total)
	out := make([]R, 0, total)
	for _, r := range perPlan {
		for _, v := range r {
			k := keyOf(v)
			if _, ok := seen[k]; ok {
				continue
			}
			seen[k] = struct{}{}
			out = append(out, v)
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
