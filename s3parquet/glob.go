package s3parquet

import (
	"strings"

	"github.com/ueisele/s3store/internal/core"
)

// readPlan describes how to execute a key-pattern read against
// S3: a prefix to LIST under, and a per-key predicate that picks
// out only the matching Hive paths. The predicate accepts the
// Hive key portion of the S3 object key (everything after
// dataPath + "/" and before the trailing /<shortID>.parquet).
type readPlan struct {
	ListPrefix string
	Match      func(hiveKey string) bool
}

// buildReadPlan validates a key pattern against the shared
// grammar and returns an execution plan. The dataPath is the
// prefix under which data files live (as returned by
// core.DataPath); the plan's ListPrefix is rooted at dataPath
// and extends as far as the longest literal the pattern allows,
// including the common prefix of a leading range/prefix segment.
//
// Pure Go — no S3 calls; suitable for unit-testing.
func buildReadPlan(
	pattern string, dataPath string, partitionKeyParts []string,
) (*readPlan, error) {
	segs, err := core.ParseKeyPattern(pattern, partitionKeyParts)
	if err != nil {
		return nil, err
	}

	// Match-all shortcut.
	if segs == nil {
		return &readPlan{
			ListPrefix: dataPath + "/",
			Match:      func(string) bool { return true },
		}, nil
	}

	return &readPlan{
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
	dataPath string, segs []core.Segment,
) string {
	var exactDirs []string
	var partial string

	for _, seg := range segs {
		if seg.Kind == core.SegExact {
			exactDirs = append(exactDirs, seg.KeyPart+"="+seg.Value)
			continue
		}
		switch seg.Kind {
		case core.SegPrefix:
			partial = seg.KeyPart + "=" + seg.Value
		case core.SegRange:
			cp := core.CommonPrefix(seg.Value, seg.ToValue)
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
func matchHiveKey(hiveKey string, segs []core.Segment) bool {
	keySegs := strings.Split(hiveKey, "/")
	if len(keySegs) != len(segs) {
		return false
	}
	for i, seg := range segs {
		if seg.Kind == core.SegWildAll {
			continue
		}
		kseg := keySegs[i]
		prefix := seg.KeyPart + "="
		if !strings.HasPrefix(kseg, prefix) {
			return false
		}
		kval := kseg[len(prefix):]
		switch seg.Kind {
		case core.SegExact:
			if kval != seg.Value {
				return false
			}
		case core.SegPrefix:
			if !strings.HasPrefix(kval, seg.Value) {
				return false
			}
		case core.SegRange:
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

// hiveKeyOfDataFile returns the Hive-key portion of a data-file
// key (the part between dataPath + "/" and the final
// "/<shortID>.parquet"). Returns the key and true if the input
// is shaped like a data file under dataPath; false otherwise.
func hiveKeyOfDataFile(s3Key, dataPath string) (string, bool) {
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
