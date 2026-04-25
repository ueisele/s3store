package core

import (
	"context"
	"fmt"
	"strings"
)

// ReadPlan describes how to execute a key-pattern read against
// S3: a prefix to LIST under, and a per-key predicate that picks
// out only the matching Hive paths. The predicate accepts the
// Hive key portion of the S3 object key (everything after
// dataPath + "/" and before the trailing /<shortID>.parquet).
//
// Shared between s3parquet (list-and-filter in Go) and s3sql
// (multi-pattern reads that pre-LIST exact file URIs to hand to
// DuckDB). Lives in core because it's pure grammar-plus-string
// logic, no S3 calls, no DuckDB.
type ReadPlan struct {
	ListPrefix string
	Match      func(hiveKey string) bool
}

// BuildReadPlans validates each pattern against the shared
// grammar and returns one ReadPlan per input. On the first
// validation failure returns the index, the offending pattern,
// and the wrapped error: `pattern N "p": <err>`. Callers wrap the
// result with their own package + method prefix so the surfaced
// chain reads naturally (e.g. `s3parquet: ReadMany pattern N "p":
// ...`).
//
// Pure Go — no S3 calls; suitable for unit-testing.
func BuildReadPlans(
	patterns []string, dataPath string, partitionKeyParts []string,
) ([]*ReadPlan, error) {
	plans := make([]*ReadPlan, len(patterns))
	for i, p := range patterns {
		plan, err := BuildReadPlan(p, dataPath, partitionKeyParts)
		if err != nil {
			return nil, fmt.Errorf("pattern %d %q: %w", i, p, err)
		}
		plans[i] = plan
	}
	return plans, nil
}

// BuildReadPlan validates a key pattern against the shared
// grammar and returns an execution plan. The dataPath is the
// prefix under which data files live (as returned by DataPath);
// the plan's ListPrefix is rooted at dataPath and extends as
// far as the longest literal the pattern allows, including the
// common prefix of a leading range/prefix segment.
//
// Pure Go — no S3 calls; suitable for unit-testing.
func BuildReadPlan(
	pattern string, dataPath string, partitionKeyParts []string,
) (*ReadPlan, error) {
	segs, err := ParseKeyPattern(pattern, partitionKeyParts)
	if err != nil {
		return nil, err
	}

	// Match-all shortcut.
	if segs == nil {
		return &ReadPlan{
			ListPrefix: dataPath + "/",
			Match:      func(string) bool { return true },
		}, nil
	}

	return &ReadPlan{
		ListPrefix: listPrefixForSegments(dataPath, segs),
		Match: func(hiveKey string) bool {
			return matchHiveKey(hiveKey, segs)
		},
	}, nil
}

// listPrefixForSegments extends dataPath with every leading
// SegExact segment (full directory narrowing) and — if the first
// non-exact segment is SegPrefix or SegRange — appends whatever
// literal prefix that segment carries, even though it doesn't end
// on a partition-directory boundary. SegWildAll contributes
// nothing.
func listPrefixForSegments(
	dataPath string, segs []Segment,
) string {
	var exactDirs []string
	var partial string

	for _, seg := range segs {
		if seg.Kind == SegExact {
			exactDirs = append(exactDirs, seg.KeyPart+"="+seg.Value)
			continue
		}
		switch seg.Kind {
		case SegPrefix:
			partial = seg.KeyPart + "=" + seg.Value
		case SegRange:
			cp := CommonPrefix(seg.Value, seg.ToValue)
			if cp != "" {
				partial = seg.KeyPart + "=" + cp
			}
		}
		break
	}

	base := dataPath + "/"
	if len(exactDirs) > 0 {
		base += strings.Join(exactDirs, "/") + "/"
	}
	return base + partial
}

// matchHiveKey returns true when every segment of hiveKey matches
// the corresponding parsed pattern segment.
func matchHiveKey(hiveKey string, segs []Segment) bool {
	keySegs := strings.Split(hiveKey, "/")
	if len(keySegs) != len(segs) {
		return false
	}
	for i, seg := range segs {
		if seg.Kind == SegWildAll {
			continue
		}
		kseg := keySegs[i]
		prefix := seg.KeyPart + "="
		if !strings.HasPrefix(kseg, prefix) {
			return false
		}
		kval := kseg[len(prefix):]
		switch seg.Kind {
		case SegExact:
			if kval != seg.Value {
				return false
			}
		case SegPrefix:
			if !strings.HasPrefix(kval, seg.Value) {
				return false
			}
		case SegRange:
			if seg.Value != "" && kval < seg.Value {
				return false
			}
			if seg.ToValue != "" && kval >= seg.ToValue {
				return false
			}
		}
	}
	return true
}

// HiveKeyOfDataFile returns the Hive-key portion of a data-file
// key (the part between dataPath + "/" and the final
// "/<shortID>.parquet"). Returns the key and true if the input
// is shaped like a data file under dataPath; false otherwise.
func HiveKeyOfDataFile(s3Key, dataPath string) (string, bool) {
	prefix := dataPath + "/"
	if !strings.HasPrefix(s3Key, prefix) {
		return "", false
	}
	rest := s3Key[len(prefix):]
	slash := strings.LastIndex(rest, "/")
	if slash < 0 {
		return "", false
	}
	tail := rest[slash+1:]
	if !strings.HasSuffix(tail, ".parquet") {
		return "", false
	}
	return rest[:slash], true
}

// RunPlansConcurrent runs listOne across every plan via FanOut
// and returns the deduplicated union of results in first-seen
// order. keyOf extracts the dedup key so callers can use richer
// result types (e.g. KeyMeta) while still deduping on the
// underlying S3 key.
//
// Single source of truth for the multi-pattern LIST fan-out used
// by s3parquet (ReadMany, LookupMany, BackfillIndexMany) and
// s3sql (ReadMany, QueryMany).
func RunPlansConcurrent[P any, R any](
	ctx context.Context,
	plans []P,
	concurrency int,
	listOne func(ctx context.Context, p P) ([]R, error),
	keyOf func(R) string,
) ([]R, error) {
	results := make([][]R, len(plans))
	err := FanOut(ctx, plans, concurrency,
		func(ctx context.Context, i int, plan P) error {
			r, err := listOne(ctx, plan)
			if err != nil {
				return err
			}
			results[i] = r
			return nil
		})
	if err != nil {
		return nil, err
	}
	return UnionKeys(results, keyOf), nil
}
