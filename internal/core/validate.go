package core

import (
	"fmt"
	"strings"
)

// ValidatePartitionKeyParts rejects PartitionKeyParts entries that would break
// the Hive layout or the key parser: empty strings, names
// containing '=' (the k-v separator) or '/' (the segment
// separator), and duplicate names.
func ValidatePartitionKeyParts(parts []string) error {
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

// ValidateKeyPattern enforces the shared glob grammar across
// every read path:
//
//   - pattern == "" or pattern == "*"  → match everything
//   - otherwise, pattern has exactly len(partitionKeyParts) segments
//     separated by '/'
//   - each segment is either:
//     "*"            — whole-segment wildcard, or
//     "<keyPart>=V"  — where V is either a literal (no
//     wildcards) or a literal ending in
//     a single trailing '*'
//
// Rejected:
//   - leading or middle '*' inside a value ("*-17", "2026-*-17")
//   - multiple '*' in one value
//   - char classes "[abc]", alternation "{a,b}", '?'
//
// Narrower than DuckDB's native glob dialect on purpose, so the
// pure-Go and SQL read paths accept exactly the same patterns.
func ValidateKeyPattern(pattern string, partitionKeyParts []string) error {
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
				"(allowed: literal or trailing '*')", value)
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
