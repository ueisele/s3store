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
// and extends as far as the longest literal the pattern allows.
//
// Pure Go — no S3 calls; suitable for unit-testing.
func buildReadPlan(
	pattern string, dataPath string, partitionKeyParts []string,
) (*readPlan, error) {
	if err := core.ValidateKeyPattern(pattern, partitionKeyParts); err != nil {
		return nil, err
	}

	// Match-all shortcut.
	if pattern == "" || pattern == "*" {
		return &readPlan{
			ListPrefix: dataPath + "/",
			Match:      func(string) bool { return true },
		}, nil
	}

	segments := strings.Split(pattern, "/")

	// Walk segments from the front as long as each is fully
	// literal (no "*", no whole-segment wildcard). That prefix
	// becomes the S3 LIST prefix; anything beyond it is an
	// in-memory check.
	literalEnd := 0
	for i, seg := range segments {
		part := partitionKeyParts[i]
		if seg == "*" {
			break
		}
		value := seg[len(part)+1:] // strip "keyPart="
		if strings.Contains(value, "*") {
			break
		}
		literalEnd = i + 1
	}

	var listPrefix string
	if literalEnd == 0 {
		listPrefix = dataPath + "/"
	} else {
		listPrefix = dataPath + "/" +
			strings.Join(segments[:literalEnd], "/") + "/"
	}

	// Match always runs against the full Hive key: we trust the
	// LIST prefix to narrow the S3 call but the predicate stays
	// independently correct, so a caller who passes in unrelated
	// keys still gets the right answer.
	return &readPlan{
		ListPrefix: listPrefix,
		Match: func(hiveKey string) bool {
			return matchHiveKey(hiveKey, segments, partitionKeyParts)
		},
	}, nil
}

// matchHiveKey returns true when every segment of hiveKey
// matches the corresponding pattern segment. Pattern segments
// can be either "*" (match anything in that slot) or
// "part=value" where value is a literal or a literal followed
// by a single trailing "*".
func matchHiveKey(
	hiveKey string, patternSegs []string, partitionKeyParts []string,
) bool {
	keySegs := strings.Split(hiveKey, "/")
	if len(keySegs) != len(patternSegs) {
		return false
	}
	for i, pseg := range patternSegs {
		if pseg == "*" {
			continue
		}
		kseg := keySegs[i]
		part := partitionKeyParts[i]
		prefix := part + "="
		if !strings.HasPrefix(kseg, prefix) {
			return false
		}
		kval := kseg[len(prefix):]
		pval := pseg[len(prefix):]
		if strings.HasSuffix(pval, "*") {
			if !strings.HasPrefix(kval, pval[:len(pval)-1]) {
				return false
			}
		} else if kval != pval {
			return false
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
