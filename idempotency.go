package s3store

import (
	"fmt"
	"path"
	"strings"
	"time"
)

// validateIdempotencyToken rejects token values that can't be
// safely embedded in a data-file path or a ref filename. Run at
// WithIdempotencyToken-application time so typos surface
// immediately at the call site, not buried inside the write
// path's PUT error.
//
// Rules:
//   - non-empty
//   - no "/" (would split the S3 key into unintended segments)
//   - no ";" (the ref filename uses ';' as a header/hive
//     separator; a token containing ';' would split the ref
//     filename at the wrong position)
//   - no ".." (collides with the key-pattern grammar's range
//     separator; tokens with ".." would be unaddressable on read)
//   - no whitespace, no control characters — printable ASCII
//     subset 0x21..0x7E
//   - <= 200 characters so the resulting data path stays well
//     under S3's 1024-byte key limit even with long Hive keys
func validateIdempotencyToken(token string) error {
	if token == "" {
		return fmt.Errorf(
			"s3store: IdempotencyToken must not be empty")
	}
	if len(token) > 200 {
		return fmt.Errorf(
			"s3store: IdempotencyToken must be <= 200 characters "+
				"(got %d)", len(token))
	}
	if strings.Contains(token, "/") {
		return fmt.Errorf(
			"s3store: IdempotencyToken %q must not contain '/'",
			token)
	}
	if strings.Contains(token, ";") {
		return fmt.Errorf(
			"s3store: IdempotencyToken %q must not contain "+
				"';' (reserved as the ref-filename header/hive "+
				"separator)", token)
	}
	if strings.Contains(token, "..") {
		return fmt.Errorf(
			"s3store: IdempotencyToken %q must not contain "+
				"'..' (reserved by the key-pattern grammar)",
			token)
	}
	for i := 0; i < len(token); i++ {
		c := token[i]
		if c < 0x21 || c > 0x7E {
			return fmt.Errorf(
				"s3store: IdempotencyToken %q contains a "+
					"non-printable-ASCII byte at index %d "+
					"(want 0x21..0x7E)", token, i)
		}
	}
	return nil
}

// applyIdempotentReadOpts is the QueryOpts-aware wrapper around
// applyIdempotentRead. When opts.IdempotentReadToken is empty
// the input is returned unchanged. When set, the token is
// validated via validateIdempotencyToken (returning a
// "WithIdempotentRead: <err>" wrapped error so callers add only
// their entry-point prefix), then the keys are filtered per
// partition.
//
// Single source of truth for the validate-and-apply combo so
// every reader entry point gets the same behaviour.
func applyIdempotentReadOpts(
	keys []KeyMeta, dataPath string, opts *QueryOpts,
) ([]KeyMeta, error) {
	if opts.IdempotentReadToken == "" {
		return keys, nil
	}
	if err := validateIdempotencyToken(opts.IdempotentReadToken); err != nil {
		return nil, fmt.Errorf("WithIdempotentRead: %w", err)
	}
	return applyIdempotentRead(keys, dataPath, opts.IdempotentReadToken), nil
}

// applyIdempotentRead filters a flat LIST of data-file KeyMetas
// so the result reflects state as of the first write of the given
// idempotency token (see WithIdempotentRead for the motivating
// use-case).
//
// The token is a raw idempotency token as passed to
// WithIdempotencyToken on the write side. Every successful
// attempt of the token lands at a fresh data file whose basename
// has shape "{token}-{attemptID}.parquet" where attemptID is a
// UUIDv7 (32 lowercase hex chars) — so the matcher here has to
// recognize that shape rather than a single canonical filename.
//
// Two filters apply per partition:
//
//   - Self-exclusion: files whose basename matches the token's
//     per-attempt shape are dropped.
//   - Later-write exclusion: among files matching the token, the
//     minimum LastModified becomes barrier[partition]. For every
//     other file in the same partition, files whose LastModified
//     is >= barrier[partition] are dropped.
//
// Partitions where the token does not appear are unfiltered — on
// the first attempt the full current state is returned.
//
// Caller validates the token via validateIdempotencyToken before
// reaching here (the reader code does this explicitly). Keys that
// don't parse as data files under dataPath pass through unfiltered
// so the helper doesn't silently drop garbage that higher layers
// are meant to notice.
func applyIdempotentRead(
	keys []KeyMeta, dataPath, token string,
) []KeyMeta {
	if token == "" || len(keys) == 0 {
		return keys
	}

	// Pass 1: compute min(LastModified) per partition across token
	// matches.
	barrier := make(map[string]time.Time)
	for _, k := range keys {
		if !dataFileBasenameMatchesToken(path.Base(k.Key), token) {
			continue
		}
		hk, ok := hiveKeyOfDataFile(k.Key, dataPath)
		if !ok {
			continue
		}
		cur, seen := barrier[hk]
		if !seen || k.InsertedAt.Before(cur) {
			barrier[hk] = k.InsertedAt
		}
	}

	if len(barrier) == 0 {
		return keys
	}

	// Pass 2: apply self-exclusion + later-write exclusion per
	// partition. Non-data-file keys and keys in unbarriered
	// partitions flow through untouched.
	out := make([]KeyMeta, 0, len(keys))
	for _, k := range keys {
		hk, ok := hiveKeyOfDataFile(k.Key, dataPath)
		if !ok {
			out = append(out, k)
			continue
		}
		b, has := barrier[hk]
		if !has {
			out = append(out, k)
			continue
		}
		if dataFileBasenameMatchesToken(path.Base(k.Key), token) {
			continue
		}
		if !k.InsertedAt.Before(b) {
			continue
		}
		out = append(out, k)
	}
	return out
}

// dataFileBasenameMatchesToken reports whether base (a parquet
// data-file basename) has the per-attempt shape
// "{token}-{attemptID}.parquet" where attemptID is a UUIDv7
// rendered as 32 lowercase hex characters (canonical form with
// internal dashes stripped — see paths.go). The trailing
// fixed-width 32-hex anchor lets a token containing dashes match
// unambiguously: only the last 32 hex chars before ".parquet"
// form the attempt-id, anything before (after stripping the
// separating dash) is the token verbatim.
func dataFileBasenameMatchesToken(base, token string) bool {
	const wantSuffixLen = 1 + attemptIDHexLen + len(".parquet") // -uuidv7hex.parquet
	if len(base) != len(token)+wantSuffixLen {
		return false
	}
	if !strings.HasPrefix(base, token+"-") {
		return false
	}
	if !strings.HasSuffix(base, ".parquet") {
		return false
	}
	rest := base[len(token)+1 : len(base)-len(".parquet")]
	return len(rest) == attemptIDHexLen && isLowerHex(rest)
}
