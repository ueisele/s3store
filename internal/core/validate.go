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
