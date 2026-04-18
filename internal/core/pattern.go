package core

import "strings"

// SegmentKind identifies how a key-pattern segment matches values.
type SegmentKind int

const (
	// SegWildAll matches any value for this segment ("*").
	SegWildAll SegmentKind = iota
	// SegExact matches one literal value ("keyPart=abc").
	SegExact
	// SegPrefix matches any value starting with a literal prefix
	// ("keyPart=abc*"). Value holds the prefix without '*'.
	SegPrefix
	// SegRange matches any value v with From <= v < To (lex).
	// Either end may be empty for an unbounded side.
	SegRange
)

// Segment is a parsed key-pattern segment, shared between the
// s3parquet matcher and the s3sql SQL emitter so both paths agree
// on what each segment means.
type Segment struct {
	Kind    SegmentKind
	KeyPart string // corresponding PartitionKeyParts entry
	Value   string // SegExact: literal; SegPrefix: prefix; SegRange: From
	ToValue string // SegRange only: To
}

// ParseKeyPattern validates and parses a key pattern into a
// []Segment of the same length as partitionKeyParts. Match-all
// patterns ("" or "*") return (nil, nil); callers should treat a
// nil slice as "match everything" and skip any per-segment work.
func ParseKeyPattern(
	pattern string, partitionKeyParts []string,
) ([]Segment, error) {
	if err := ValidateKeyPattern(pattern, partitionKeyParts); err != nil {
		return nil, err
	}
	if pattern == "" || pattern == "*" {
		return nil, nil
	}
	raw := strings.Split(pattern, "/")
	out := make([]Segment, len(raw))
	for i, seg := range raw {
		part := partitionKeyParts[i]
		if seg == "*" {
			out[i] = Segment{Kind: SegWildAll, KeyPart: part}
			continue
		}
		// "<part>=" prefix guaranteed present by ValidateKeyPattern.
		value := seg[len(part)+1:]
		if strings.Contains(value, "..") {
			from, to, _ := strings.Cut(value, "..")
			out[i] = Segment{
				Kind: SegRange, KeyPart: part,
				Value: from, ToValue: to,
			}
			continue
		}
		if strings.HasSuffix(value, "*") {
			out[i] = Segment{
				Kind: SegPrefix, KeyPart: part,
				Value: value[:len(value)-1],
			}
			continue
		}
		out[i] = Segment{Kind: SegExact, KeyPart: part, Value: value}
	}
	return out, nil
}

// CommonPrefix returns the longest shared prefix of a and b.
// Returns "" if either argument is empty — used by range segments
// where an empty endpoint means unbounded (no narrowing possible).
func CommonPrefix(a, b string) string {
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
