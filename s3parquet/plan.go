package s3parquet

import (
	"fmt"
	"strings"
)

// segmentKind identifies how a key-pattern segment matches values.
type segmentKind int

const (
	// segWildAll matches any value for this segment ("*").
	segWildAll segmentKind = iota
	// segExact matches one literal value ("keyPart=abc").
	segExact
	// segPrefix matches any value starting with a literal prefix
	// ("keyPart=abc*"). Value holds the prefix without '*'.
	segPrefix
	// segRange matches any value v with From <= v < To (lex).
	// Either end may be empty for an unbounded side.
	segRange
)

// segment is a parsed key-pattern segment.
type segment struct {
	kind    segmentKind
	keyPart string // corresponding PartitionKeyParts entry
	value   string // segExact: literal; segPrefix: prefix; segRange: From
	toValue string // segRange only: To
}

// parseKeyPattern validates and parses a key pattern into a
// []segment of the same length as partitionKeyParts. Match-all
// patterns ("" or "*") return (nil, nil); callers should treat a
// nil slice as "match everything" and skip any per-segment work.
func parseKeyPattern(
	pattern string, partitionKeyParts []string,
) ([]segment, error) {
	if err := validateKeyPattern(pattern, partitionKeyParts); err != nil {
		return nil, err
	}
	if pattern == "" || pattern == "*" {
		return nil, nil
	}
	raw := strings.Split(pattern, "/")
	out := make([]segment, len(raw))
	for i, seg := range raw {
		part := partitionKeyParts[i]
		if seg == "*" {
			out[i] = segment{kind: segWildAll, keyPart: part}
			continue
		}
		// "<part>=" prefix guaranteed present by validateKeyPattern.
		value := seg[len(part)+1:]
		if strings.Contains(value, "..") {
			from, to, _ := strings.Cut(value, "..")
			out[i] = segment{
				kind: segRange, keyPart: part,
				value: from, toValue: to,
			}
			continue
		}
		if strings.HasSuffix(value, "*") {
			out[i] = segment{
				kind: segPrefix, keyPart: part,
				value: value[:len(value)-1],
			}
			continue
		}
		out[i] = segment{kind: segExact, keyPart: part, value: value}
	}
	return out, nil
}

// commonPrefix returns the longest shared prefix of a and b.
// Returns "" if either argument is empty — used by range segments
// where an empty endpoint means unbounded (no narrowing possible).
func commonPrefix(a, b string) string {
	if a == "" || b == "" {
		return ""
	}
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return a[:i]
		}
	}
	return a[:n]
}

// readPlan describes how to execute a key-pattern read against
// S3: a prefix to LIST under, and a per-key predicate that picks
// out only the matching Hive paths. The predicate accepts the
// Hive key portion of the S3 object key (everything after
// dataPath + "/" and before the trailing /<shortID>.parquet).
type readPlan struct {
	listPrefix string
	match      func(hiveKey string) bool
}

// buildReadPlans validates each pattern against the shared
// grammar and returns one readPlan per input. The dataPath is
// the prefix under which data files live (as returned by
// DataPath); each plan's listPrefix is rooted at dataPath
// and extends as far as the longest literal the pattern allows,
// including the common prefix of a leading range/prefix segment.
//
// On the first validation failure returns the index, the
// offending pattern, and the wrapped error: `pattern N "p":
// <err>`. Callers wrap the result with their own method prefix
// so the surfaced chain reads naturally (e.g. `s3parquet: Read
// pattern N "p": ...`).
//
// Pure Go — no S3 calls; suitable for unit-testing.
func buildReadPlans(
	keyPatterns []string, dataPath string, partitionKeyParts []string,
) ([]*readPlan, error) {
	plans := make([]*readPlan, len(keyPatterns))
	for i, p := range keyPatterns {
		segs, err := parseKeyPattern(p, partitionKeyParts)
		if err != nil {
			return nil, fmt.Errorf("pattern %d %q: %w", i, p, err)
		}

		// Match-all shortcut.
		if segs == nil {
			plans[i] = &readPlan{
				listPrefix: dataPath + "/",
				match:      func(string) bool { return true },
			}
			continue
		}

		plans[i] = &readPlan{
			listPrefix: listPrefixForSegments(dataPath, segs),
			match: func(hiveKey string) bool {
				return matchHiveKey(hiveKey, segs)
			},
		}
	}
	return plans, nil
}

// listPrefixForSegments extends dataPath with every leading
// segExact segment (full directory narrowing) and — if the first
// non-exact segment is segPrefix or segRange — appends whatever
// literal prefix that segment carries, even though it doesn't end
// on a partition-directory boundary. segWildAll contributes
// nothing.
func listPrefixForSegments(
	dataPath string, segs []segment,
) string {
	var exactDirs []string
	var partial string

	for _, seg := range segs {
		if seg.kind == segExact {
			exactDirs = append(exactDirs, seg.keyPart+"="+seg.value)
			continue
		}
		switch seg.kind {
		case segPrefix:
			partial = seg.keyPart + "=" + seg.value
		case segRange:
			cp := commonPrefix(seg.value, seg.toValue)
			if cp != "" {
				partial = seg.keyPart + "=" + cp
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
func matchHiveKey(hiveKey string, segs []segment) bool {
	keySegs := strings.Split(hiveKey, "/")
	if len(keySegs) != len(segs) {
		return false
	}
	for i, seg := range segs {
		if seg.kind == segWildAll {
			continue
		}
		kseg := keySegs[i]
		prefix := seg.keyPart + "="
		if !strings.HasPrefix(kseg, prefix) {
			return false
		}
		kval := kseg[len(prefix):]
		switch seg.kind {
		case segExact:
			if kval != seg.value {
				return false
			}
		case segPrefix:
			if !strings.HasPrefix(kval, seg.value) {
				return false
			}
		case segRange:
			if seg.value != "" && kval < seg.value {
				return false
			}
			if seg.toValue != "" && kval >= seg.toValue {
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
