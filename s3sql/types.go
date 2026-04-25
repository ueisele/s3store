package s3sql

import (
	"github.com/ueisele/s3store/internal/core"
)

// QueryOption configures read-path behavior.
type QueryOption = core.QueryOption

// WithHistory disables latest-per-entity deduplication on Query
// and QueryMany. When EntityKeyColumns is empty, dedup is already
// a no-op regardless of this option.
func WithHistory() QueryOption {
	return core.WithHistory()
}

// WithIdempotentRead makes Query / QueryMany retry-safe: the
// result reflects state as of the first write of the given
// idempotency token. Pair with WithIdempotencyToken on the write
// side so one token drives both sides. See core.WithIdempotentRead
// for the full contract.
func WithIdempotentRead(token string) QueryOption {
	return core.WithIdempotentRead(token)
}
