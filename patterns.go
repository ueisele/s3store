package s3store

import (
	"fmt"
	"strings"
)

// Key-pattern grammar.
//
// Patterns describe which partitions a read covers. The grammar
// is shared by every snapshot read; see validateKeyPattern below
// for the full spec. Validation, parsing, and per-key matching
// all live in this file so the rules read end-to-end without
// hopping files.

// validatePartitionKeyParts rejects PartitionKeyParts entries that
// would break the Hive layout or the key parser: empty strings,
// names containing '=' (the k-v separator) or '/' (the segment
// separator), and duplicate names.
func validatePartitionKeyParts(parts []string) error {
	if len(parts) == 0 {
		return fmt.Errorf("s3store: PartitionKeyParts is required")
	}
	seen := make(map[string]bool, len(parts))
	for i, p := range parts {
		if p == "" {
			return fmt.Errorf(
				"s3store: PartitionKeyParts[%d] is empty", i)
		}
		if strings.ContainsAny(p, "=/") {
			return fmt.Errorf(
				"s3store: PartitionKeyParts[%d] %q must not contain "+
					"'=' or '/'", i, p)
		}
		if seen[p] {
			return fmt.Errorf(
				"s3store: PartitionKeyParts[%d] %q is duplicated", i, p)
		}
		seen[p] = true
	}
	return nil
}

// validateHivePartitionValue rejects values that can't be safely
// embedded in a "col=value" hive-path segment:
//
//   - empty (would produce "col=" which carries no information),
//   - contains "/" (segment separator — would split the key),
//   - contains ".." (reserved by the key-pattern range grammar
//     as the FROM..TO separator; a value containing ".." would
//     be unaddressable on read).
//
// "=" inside the value is allowed because segments split on the
// first "=" only, matching the data-partition convention.
func validateHivePartitionValue(value string) error {
	if value == "" {
		return fmt.Errorf("hive partition value is empty")
	}
	if strings.Contains(value, "/") {
		return fmt.Errorf(
			"hive partition value %q contains '/'", value)
	}
	if strings.Contains(value, "..") {
		return fmt.Errorf(
			"hive partition value %q contains '..' "+
				"(reserved by key-pattern range grammar)", value)
	}
	return nil
}

// validateKeyPattern enforces the shared glob grammar across
// every read path:
//
//   - pattern == "" or pattern == "*"  → match everything
//   - otherwise, pattern has exactly len(partitionKeyParts) segments
//     separated by '/'
//   - each segment is either:
//     "*"             — whole-segment wildcard, or
//     "<keyPart>=V"   — where V is either a literal, a literal
//     ending in a single trailing '*', or a
//     range "FROM..TO" (half-open, lex order)
//
// Range form:
//   - "FROM..TO" matches any value v with FROM <= v < TO (lex)
//   - either side may be empty for an unbounded end
//   - endpoints are plain literals: no '*', no '..'
//   - "..TO" / "FROM.." are allowed; ".." alone is not (use '*')
//
// Rejected:
//   - leading or middle '*' inside a value ("*-17", "2026-*-17")
//   - multiple '*' in one value
//   - char classes "[abc]", alternation "{a,b}", '?'
//   - more than one '..' in a range value ("a..b..c")
func validateKeyPattern(pattern string, partitionKeyParts []string) error {
	if pattern == "" || pattern == "*" {
		return nil
	}
	segments := strings.Split(pattern, "/")
	if len(segments) != len(partitionKeyParts) {
		return fmt.Errorf(
			"s3store: key pattern %q has %d segments, "+
				"expected %d (%v)",
			pattern, len(segments), len(partitionKeyParts), partitionKeyParts)
	}
	for i, seg := range segments {
		part := partitionKeyParts[i]
		if seg == "*" {
			continue
		}
		prefix := part + "="
		if !strings.HasPrefix(seg, prefix) {
			return fmt.Errorf(
				"s3store: key pattern %q segment %d is %q, "+
					"expected %q=... or %q",
				pattern, i, seg, part, "*")
		}
		value := seg[len(prefix):]
		if err := validatePatternValue(value); err != nil {
			return fmt.Errorf(
				"s3store: key pattern %q segment %d: %w",
				pattern, i, err)
		}
	}
	return nil
}

func validatePatternValue(value string) error {
	if strings.ContainsAny(value, "/?[]{}") {
		return fmt.Errorf(
			"value %q contains disallowed character "+
				"(allowed: literal, trailing '*', or range "+
				"'FROM..TO')", value)
	}
	if strings.Contains(value, "..") {
		return validateRangeValue(value)
	}
	n := strings.Count(value, "*")
	if n == 0 {
		return nil
	}
	if n > 1 {
		return fmt.Errorf(
			"value %q contains multiple '*' "+
				"(only a single trailing '*' is supported)",
			value)
	}
	if !strings.HasSuffix(value, "*") {
		return fmt.Errorf(
			"value %q contains non-trailing '*' "+
				"(only a single trailing '*' is supported)",
			value)
	}
	return nil
}

// validateRangeValue enforces the "FROM..TO" grammar. '/', '?',
// '[]', '{}' are already rejected by the caller.
func validateRangeValue(value string) error {
	parts := strings.Split(value, "..")
	if len(parts) != 2 {
		return fmt.Errorf(
			"value %q contains multiple '..' "+
				"(a range has exactly one 'FROM..TO')", value)
	}
	from, to := parts[0], parts[1]
	if from == "" && to == "" {
		return fmt.Errorf(
			"value %q is an empty range "+
				"(use '*' to match everything)", value)
	}
	for _, end := range [2]string{from, to} {
		if strings.Contains(end, "*") {
			return fmt.Errorf(
				"value %q range endpoint %q contains '*' "+
					"(endpoints must be plain literals)",
				value, end)
		}
	}
	if from != "" && to != "" && from > to {
		return fmt.Errorf(
			"value %q range is reversed: %q > %q",
			value, from, to)
	}
	return nil
}

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
// the prefix under which data files live; each plan's listPrefix
// is rooted at dataPath and extends as far as the longest literal
// the pattern allows, including the common prefix of a leading
// range/prefix segment.
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
