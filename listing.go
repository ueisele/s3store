package s3store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

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
// Both are monotonic; consumed by the no-dedup sort cascade
// (sort by insertedAt asc) when EntityKeyOf isn't configured.
//
// Size is the compressed object size from the LIST response
// (S3's Contents.Size). Zero when the call site doesn't have
// access to a LIST result (e.g. PollRecords assembles KeyMetas
// from ref filenames). Used by ReadIter as the download-stage
// memory estimate; the uncompressed size used by the byte-budget
// gate is read from each parquet file's footer after download.
type KeyMeta struct {
	Key        string
	InsertedAt time.Time
	Size       int64
}

// unionKeys flattens a set of per-plan LIST results into a
// single deduplicated slice, preserving first-seen order so the
// downstream GET pipeline is deterministic. keyOf extracts the
// dedup key from each result (use the identity function when R
// is itself a string).
func unionKeys[R any](elements [][]R, keyOf func(R) string) []R {
	total := 0
	for _, r := range elements {
		total += len(r)
	}
	if total == 0 {
		return nil
	}
	seen := make(map[string]struct{}, total)
	out := make([]R, 0, total)
	for _, r := range elements {
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

// dedupePatterns removes literal-duplicate entries from a
// patterns slice, preserving first-seen order. Cheap pre-step
// for the multi-pattern read paths so accidental duplicates
// (e.g. from a generated list with possible repeats) don't cause
// duplicate LIST round-trips. Doesn't catch semantic overlap —
// patterns like "*" and "2026-*" both list overlapping files but
// aren't string-equal; that case is still handled by unionKeys
// at the key level, just at the cost of one extra LIST.
//
// Implemented as a thin wrapper around unionKeys (single batch +
// identity key) so the dedup loop lives in one place.
func dedupePatterns(patterns []string) []string {
	if len(patterns) <= 1 {
		return patterns
	}
	return unionKeys([][]string{patterns}, func(s string) string { return s })
}

// ResolvePatterns chains the standard pattern→keys pipeline:
// dedupe → buildReadPlans → listDataFiles →
// applyIdempotentReadOpts. Returns the deduplicated KeyMetas
// matching any pattern, with IdempotentRead filtering applied
// when opts.IdempotentReadToken is set. Empty patterns slice or
// no matches → (nil, nil).
//
// Used by every snapshot-style read entry point (Read / ReadIter /
// ReadRangeIter) so the chain lives in exactly one place. Pattern
// dedupe runs here rather than at every call site so accidental
// duplicates don't cause duplicate LIST round-trips, regardless
// of caller.
//
// The partition LIST inherits the target's ConsistencyControl
// from t — every t.listEach call does — so callers don't need
// to thread the level explicitly.
func ResolvePatterns(
	ctx context.Context, t S3Target, patterns []string,
	opts *QueryOpts,
) ([]KeyMeta, error) {
	patterns = dedupePatterns(patterns)
	if len(patterns) == 0 {
		return nil, nil
	}
	dataPath := dataPath(t.Prefix())
	plans, err := buildReadPlans(patterns, dataPath, t.PartitionKeyParts())
	if err != nil {
		return nil, err
	}
	keys, err := listDataFiles(ctx, t, plans)
	if err != nil {
		return nil, err
	}
	return applyIdempotentReadOpts(keys, dataPath, opts)
}

// listDataFiles returns the deduplicated union of parquet objects
// under each plan's ListPrefix whose Hive key matches the plan's
// predicate. Each KeyMeta carries S3 LastModified so downstream
// filters (WithIdempotentRead, sort fallbacks) can reason about
// write time without a second round-trip. Overlapping plans (e.g.
// "period=*" and "period=2026-03-*") are deduped on key so
// downstream GET / scan work is not duplicated.
//
// Plans run with bounded concurrency capped at MaxInflightRequests;
// each page-fetch additionally acquires the target's semaphore (via
// listPage) so all callers compete for the same budget. The Hive-
// key range is computed from Prefix() — the data-file prefix —
// so files outside the partition tree are silently skipped.
func listDataFiles(
	ctx context.Context, t S3Target,
	plans []*readPlan,
) ([]KeyMeta, error) {
	dataPath := dataPath(t.Prefix())
	return fanOutMapReduce(ctx, plans,
		t.EffectiveMaxInflightRequests(),
		t.metrics,
		func(ctx context.Context, plan *readPlan) ([]KeyMeta, error) {
			var out []KeyMeta
			err := t.listEach(ctx, plan.listPrefix, "", 0,
				func(obj s3types.Object) (bool, error) {
					objKey := aws.ToString(obj.Key)
					if !strings.HasSuffix(objKey, ".parquet") {
						return true, nil
					}
					hiveKey, ok := hiveKeyOfDataFile(objKey, dataPath)
					if !ok {
						return true, nil
					}
					if plan.match(hiveKey) {
						out = append(out, KeyMeta{
							Key:        objKey,
							InsertedAt: aws.ToTime(obj.LastModified),
							Size:       aws.ToInt64(obj.Size),
						})
					}
					return true, nil
				})
			if err != nil {
				return nil, fmt.Errorf("list data files: %w", err)
			}
			return out, nil
		},
		func(per [][]KeyMeta) []KeyMeta {
			return unionKeys(per, func(k KeyMeta) string { return k.Key })
		})
}
