package s3parquet

import (
	"time"

	"github.com/ueisele/s3store/internal/core"
	"github.com/ueisele/s3store/internal/refstream"
)

// ErrRefStreamDisabled is returned by Poll / PollRecords /
// PollRecordsAll when the Target has DisableRefStream set. The
// dataset was written without ref files, so there is no stream
// to tail. OffsetAt stays usable (pure timestamp encoding).
//
// Aliased to the shared sentinel in internal/refstream so
// errors.Is matches across s3parquet, s3sql, and the umbrella.
var ErrRefStreamDisabled = refstream.ErrDisabled

// Offset represents a position in the stream.
type Offset = core.Offset

// StreamEntry is a lightweight ref returned by Poll.
type StreamEntry = core.StreamEntry

// WriteResult contains metadata about a completed write.
type WriteResult = core.WriteResult

// WriteOption configures write-path behavior (today:
// WithIdempotencyToken). Accepted by Write / WriteWithKey /
// WriteRowGroupsBy / WriteWithKeyRowGroupsBy as a variadic tail.
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

// WithReadAheadPartitions tells ReadIter / ReadManyIter to
// prefetch n partitions ahead of the yield position. Default
// (n == 0) is strict-serial. See core.WithReadAheadPartitions
// for the full contract.
func WithReadAheadPartitions(n int) QueryOption {
	return core.WithReadAheadPartitions(n)
}

// WithIdempotentRead makes Read / ReadIter / ReadMany /
// ReadManyIter / ReadIterWhere / ReadManyIterWhere / PollRecords
// retry-safe: the result reflects state as of the first write of
// the given idempotency token. Pair with WithIdempotencyToken on
// the write side so one token drives both sides of the retry.
// See core.WithIdempotentRead for the full contract.
func WithIdempotentRead(token string) QueryOption {
	return core.WithIdempotentRead(token)
}
