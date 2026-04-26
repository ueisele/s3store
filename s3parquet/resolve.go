package s3parquet

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/ueisele/s3store/internal/core"
)

// ResolvePatterns chains the standard pattern→keys pipeline:
// BuildReadPlans → listDataFiles → ApplyIdempotentReadOpts.
// Returns the deduplicated KeyMetas matching any pattern, with
// IdempotentRead filtering applied when opts.IdempotentReadToken
// is set. Empty patterns slice or no matches → (nil, nil).
//
// Used by every snapshot-style read entry point (Read / ReadIter /
// PollRecordsIter on the parquet side, Query on the SQL side) so
// the three-step chain lives in exactly one place. Lives outside
// S3Target because it composes higher-level concepts (read plans,
// idempotency tokens, QueryOpts) that the low-level S3 handle
// shouldn't carry.
func ResolvePatterns(
	ctx context.Context, t S3Target, patterns []string,
	opts *core.QueryOpts, consistency ConsistencyLevel,
) ([]core.KeyMeta, error) {
	if len(patterns) == 0 {
		return nil, nil
	}
	dataPath := core.DataPath(t.Prefix())
	plans, err := core.BuildReadPlans(patterns, dataPath, t.PartitionKeyParts())
	if err != nil {
		return nil, err
	}
	keys, err := listDataFiles(ctx, t, plans, consistency)
	if err != nil {
		return nil, err
	}
	return core.ApplyIdempotentReadOpts(keys, dataPath, opts)
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
	plans []*core.ReadPlan, consistency ConsistencyLevel,
) ([]core.KeyMeta, error) {
	dataPath := core.DataPath(t.Prefix())
	return core.FanOutMapReduce(ctx, plans,
		t.EffectiveMaxInflightRequests(),
		func(ctx context.Context, plan *core.ReadPlan) ([]core.KeyMeta, error) {
			var out []core.KeyMeta
			err := t.listEach(ctx, plan.ListPrefix, "", 0, consistency,
				func(obj s3types.Object) (bool, error) {
					objKey := aws.ToString(obj.Key)
					if !strings.HasSuffix(objKey, ".parquet") {
						return true, nil
					}
					hiveKey, ok := core.HiveKeyOfDataFile(objKey, dataPath)
					if !ok {
						return true, nil
					}
					if plan.Match(hiveKey) {
						out = append(out, core.KeyMeta{
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
		func(per [][]core.KeyMeta) []core.KeyMeta {
			return core.UnionKeys(per, func(k core.KeyMeta) string { return k.Key })
		})
}
