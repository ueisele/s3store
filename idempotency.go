package s3store

import (
	"fmt"
	"strings"
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
