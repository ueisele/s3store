package s3parquet

import (
	"errors"
	"time"

	"github.com/ueisele/s3store/internal/core"
)

// ErrRefStreamDisabled is returned by Poll / PollRecords /
// PollRecordsIter when the Target has DisableRefStream set. The
// dataset was written without ref files, so there is no stream
// to tail. OffsetAt stays usable (pure timestamp encoding).
//
// The umbrella s3store package re-exports this so errors.Is
// matches whether the caller imported s3store or s3parquet.
var ErrRefStreamDisabled = errors.New(
	"ref stream disabled on this Store; " +
		"Poll/PollRecords/PollRecordsIter unavailable")

// Offset represents a position in the stream.
type Offset = core.Offset

// OffsetUnbounded is the "no bound on this side" sentinel for
// Poll / PollRecords / PollRecordsIter. As `since` it means
// stream head; as `until` it means live tip. See core.OffsetUnbounded.
const OffsetUnbounded = core.OffsetUnbounded

// StreamEntry is a lightweight ref returned by Poll.
type StreamEntry = core.StreamEntry

// WriteResult contains metadata about a completed write.
type WriteResult = core.WriteResult

// WriteOption configures write-path behavior (today:
// WithIdempotencyToken). Accepted by Write / WriteWithKey as a
// variadic tail.
type WriteOption = core.WriteOption

// WithIdempotencyToken marks a write as a retry-safe logical unit.
// See core.WithIdempotencyToken for the full contract — token
// replaces the default {tsMicros}-{shortID} id in the data
// filename so retries produce deterministic paths; maxRetryAge
// bounds the scoped LIST used to dedup refs on the retry path.
func WithIdempotencyToken(
	token string, maxRetryAge time.Duration,
) WriteOption {
	return core.WithIdempotencyToken(token, maxRetryAge)
}

// QueryOption configures read-path behavior.
type QueryOption = core.QueryOption

// WithHistory disables latest-per-entity deduplication on Read
// and PollRecords. When EntityKeyOf is nil, dedup is already a
// no-op regardless of this option.
func WithHistory() QueryOption {
	return core.WithHistory()
}

// WithUntilOffset bounds Poll / PollRecords from above: only
// entries with offset < until are returned (half-open range).
// Pair with Store.OffsetAt to read records in a time window.
func WithUntilOffset(until Offset) QueryOption {
	return core.WithUntilOffset(until)
}

// WithReadAheadPartitions tells ReadIter / PollRecordsIter to
// prefetch n partitions ahead of the yield position. Default is 1
// (one partition lookahead so decode of N+1 overlaps yield of N).
// Values < 1 are floored to 1. See core.WithReadAheadPartitions
// for the full contract.
func WithReadAheadPartitions(n int) QueryOption {
	return core.WithReadAheadPartitions(n)
}

// WithReadAheadBytes caps the cumulative uncompressed parquet
// bytes that may sit decoded in the ReadIter / PollRecordsIter
// pipeline ahead of the current yield position. Composes with
// WithReadAheadPartitions — whichever cap binds first holds the
// producer back. See core.WithReadAheadBytes for the full
// contract.
func WithReadAheadBytes(n int64) QueryOption {
	return core.WithReadAheadBytes(n)
}

// WithIdempotentRead makes snapshot reads retry-safe: the result
// reflects state as of the first write of the given idempotency
// token. Applies to Read / ReadIter / PollRecordsIter. Ignored
// on PollRecords — the offset cursor already provides retry-safety
// on that path. Pair with WithIdempotencyToken on the write side
// so one token drives both sides of the retry. See
// core.WithIdempotentRead for the full contract.
func WithIdempotentRead(token string) QueryOption {
	return core.WithIdempotentRead(token)
}
