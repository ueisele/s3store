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
//   - listDataFiles (snapshot reads): S3 LastModified from the
//     LIST response.
//   - PollRecords / ReadRangeIter: the ref filename's refMicroTs
//     (writer's wall clock captured immediately before the ref
//     PUT), which is the same value the writer encodes into the
//     ref key.
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
// dedupe → buildReadPlans → listDataFiles → gateByCommit. Returns
// the deduplicated KeyMetas matching any pattern, gated against
// `<token>.commit` markers so uncommitted parquets are invisible.
// Empty patterns slice or no matches → (nil, nil).
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
//
// method is the caller's methodKind used as the metric label for
// commit-gate HEADs (s3store.read.commit_head{method=...}).
func ResolvePatterns(
	ctx context.Context, t S3Target, patterns []string,
	method methodKind,
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
	keys, commits, err := listDataFiles(ctx, t, plans)
	if err != nil {
		return nil, err
	}
	return gateByCommit(ctx, t, dataPath, keys, commits, method)
}

// listDataFiles returns the deduplicated union of parquet objects
// under each plan's ListPrefix whose Hive key matches the plan's
// predicate, plus the set of `<token>.commit` markers visible in
// the same LIST iteration. Each parquet KeyMeta carries S3
// LastModified so downstream sort fallbacks can reason about
// write time without a second round-trip. Overlapping plans (e.g.
// "period=*" and "period=2026-03-*") are deduped on key so
// downstream GET / scan work is not duplicated.
//
// commits is keyed by `<partition>:<token>` and records every
// `<token>.commit` whose containing partition matched its plan's
// predicate. Returned alongside the parquet KeyMetas because both
// kinds of object live under the same partition prefix — capturing
// them in one LIST iteration is one round-trip cheaper than a
// separate scan.
//
// Plans run with bounded concurrency capped at MaxInflightRequests;
// each page-fetch additionally acquires the target's semaphore (via
// listPage) so all callers compete for the same budget. The Hive-
// key range is computed from Prefix() — the data-file prefix —
// so files outside the partition tree are silently skipped.
func listDataFiles(
	ctx context.Context, t S3Target,
	plans []*readPlan,
) (parquets []KeyMeta, commits map[string]struct{}, err error) {
	dataPath := dataPath(t.Prefix())

	type planResult struct {
		Parquets []KeyMeta
		Commits  []string // composite "partition:token"
	}

	merged, err := fanOutMapReduce(ctx, plans,
		t.EffectiveMaxInflightRequests(),
		t.metrics,
		func(ctx context.Context, plan *readPlan) ([]planResult, error) {
			var pr planResult
			err := t.listEach(ctx, plan.listPrefix, "", 0,
				func(obj s3types.Object) (bool, error) {
					objKey := aws.ToString(obj.Key)
					hiveKey, ok := hiveKeyOfPartitionFile(objKey, dataPath)
					if !ok {
						return true, nil
					}
					if !plan.match(hiveKey) {
						return true, nil
					}
					base := objKey[strings.LastIndex(objKey, "/")+1:]
					if strings.HasSuffix(objKey, ".parquet") {
						if _, _, ok := dataFileTokenAndID(base); !ok {
							// Parquet under partition prefix but not
							// shaped like a data file (externally
							// written / stale layout) — skip.
							return true, nil
						}
						pr.Parquets = append(pr.Parquets, KeyMeta{
							Key:        objKey,
							InsertedAt: aws.ToTime(obj.LastModified),
							Size:       aws.ToInt64(obj.Size),
						})
						return true, nil
					}
					if strings.HasSuffix(objKey, commitMarkerSuffix) {
						token, ok := commitTokenFromBasename(base)
						if !ok {
							return true, nil
						}
						pr.Commits = append(pr.Commits, hiveKey+":"+token)
					}
					return true, nil
				})
			if err != nil {
				return nil, fmt.Errorf("list data files: %w", err)
			}
			return []planResult{pr}, nil
		},
		func(per [][]planResult) []planResult {
			var flat []planResult
			for _, slice := range per {
				flat = append(flat, slice...)
			}
			return flat
		})
	if err != nil {
		return nil, nil, err
	}

	// Union per-plan results: dedupe parquet keys (overlapping
	// plans), and merge commit sets.
	commits = make(map[string]struct{})
	totalParquets := 0
	for _, pr := range merged {
		totalParquets += len(pr.Parquets)
		for _, c := range pr.Commits {
			commits[c] = struct{}{}
		}
	}
	if totalParquets == 0 {
		return nil, commits, nil
	}
	seen := make(map[string]struct{}, totalParquets)
	parquets = make([]KeyMeta, 0, totalParquets)
	for _, pr := range merged {
		for _, k := range pr.Parquets {
			if _, dup := seen[k.Key]; dup {
				continue
			}
			seen[k.Key] = struct{}{}
			parquets = append(parquets, k)
		}
	}
	return parquets, commits, nil
}
