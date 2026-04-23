package s3parquet

import (
	"errors"
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

// ErrRefSettleBudgetExceeded is returned from the write path when
// the initial ref PUT blew SettleWindow AND the internal recovery
// PUT (a second attempt with a fresh refTsMicros) also missed its
// budget. The stale refTsMicros sits past the settle cutoff a
// concurrent Poll would compute, so consumers who advanced past
// it can't observe the write — and the library gave up trying to
// publish a fresher one on its own.
//
// Normal budget-blown path (no sentinel): the library detects the
// first PUT's elapsed > SettleWindow, runs the recovery PUT with
// a new refTsMicros, and best-effort deletes the stale ref. The
// write returns success with the fresh RefPath / Offset. A rare
// duplicate lands when a consumer polled during the "stale ref
// landed → delete propagated" gap; reader-layer (entity, version)
// dedup absorbs it.
//
// Sentinel path: both the initial PUT and the recovery PUT missed
// budget — typically indicates persistent backend slowness or a
// mis-sized SettleWindow. Callers should treat this as a failed
// write and retry; with WithIdempotencyToken the retry is
// deterministic (same data path, scoped-LIST dedup on the ref
// side). On retry, findExistingRef's freshness filter rejects the
// now-stale ref as a dedup target so the retry emits a fresh ref
// rather than silently matching the stale one.
//
// Real PUT failures where HEAD also reports the ref absent (PUT
// didn't land, caller ctx cancelled, etc.) surface as a wrapped
// put-ref error — not this sentinel — because the ref is known-
// absent and the caller can retry without the freshness concern.
var ErrRefSettleBudgetExceeded = errors.New(
	"s3parquet: ref PUT exceeded SettleWindow on initial and " +
		"recovery attempts; retry (with WithIdempotencyToken for " +
		"deterministic recovery)")

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
