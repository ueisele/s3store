package s3parquet

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// ResolvePatterns chains the standard pattern→keys pipeline:
// dedupe → buildReadPlans → listDataFiles →
// applyIdempotentReadOpts. Returns the deduplicated KeyMetas
// matching any pattern, with IdempotentRead filtering applied
// when opts.IdempotentReadToken is set. Empty patterns slice or
// no matches → (nil, nil).
//
// Used by every snapshot-style read entry point (Read / ReadIter /
// ReadRangeIter on the parquet side, Query on the SQL side) so
// the chain lives in exactly one place. Pattern dedupe runs here
// rather than at every call site so accidental duplicates don't
// cause duplicate LIST round-trips, regardless of caller.
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
	dataPath := DataPath(t.Prefix())
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
	dataPath := DataPath(t.Prefix())
	return fanOutMapReduce(ctx, plans,
		t.EffectiveMaxInflightRequests(),
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
