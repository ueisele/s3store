package core

// QueryOption configures read-path behavior. Shared across
// every read API (Read, Query, QueryRow, PollRecords) so there's
// one option type and one mental model for every read.
type QueryOption func(*QueryOpts)

// QueryOpts is the resolved set of read-path options after
// applying a chain of QueryOption values. Exported so each
// sub-package can build its own option handling without a
// second layer of indirection.
type QueryOpts struct {
	IncludeHistory bool
	// Until, when non-empty, is an exclusive upper bound on
	// stream offsets returned by Poll / PollRecords: entries
	// whose offset is >= Until are skipped, giving a half-open
	// [since, Until) range. Matches Kafka's offset semantics.
	Until Offset
	// ReadAheadPartitions controls how many partitions ahead of
	// the current yield position a streaming read (ReadIter /
	// ReadManyIter) may download. Zero keeps the current strict-
	// serial behavior (download one, yield all, advance, repeat);
	// higher values overlap the next partition's download with the
	// current partition's yield loop, at O(N+1 partitions) memory.
	// Ignored by read paths that don't partition work this way
	// (s3sql, which streams through DuckDB).
	ReadAheadPartitions int
	// ReadAheadBytes caps the cumulative uncompressed parquet
	// bytes that may sit decoded in the streaming read pipeline
	// ahead of the current yield position. Zero disables the cap
	// (only ReadAheadPartitions binds). The value is checked
	// against the sum of each buffered partition's
	// total_uncompressed_size as reported by the parquet footer
	// — exact, not a heuristic. Decoded Go memory typically runs
	// 1–2× the uncompressed size depending on data shape (string
	// headers, slice/map pointer overhead).
	ReadAheadBytes int64
	// IdempotentReadToken, when set, filters the LIST result so
	// the Read returns state as of the first write of the given
	// idempotency token — the caller's own prior attempts are
	// dropped, and every other file with LastModified at or after
	// the barrier is dropped too (per partition). Enables retry-
	// safe read-modify-write: the second attempt reads the same
	// state the first attempt saw, computes the same diff, writes
	// the same bytes. Validated via ValidateIdempotencyToken at
	// read time — shares the grammar with WithIdempotencyToken so
	// the token a caller stores for their write also drives the
	// matching read.
	IdempotentReadToken string
}

// Apply runs every option against the receiver.
func (o *QueryOpts) Apply(opts ...QueryOption) {
	for _, opt := range opts {
		opt(o)
	}
}

// WithHistory disables latest-per-key deduplication on any read
// path. Without it, reads are deduped by the package-specific
// dedup rule; with it, every version of every record is
// returned.
//
// When no dedup rule is configured (VersionColumn empty in
// s3sql, or VersionOf nil in s3parquet), dedup is a no-op
// regardless of this option.
func WithHistory() QueryOption {
	return func(o *QueryOpts) {
		o.IncludeHistory = true
	}
}

// WithUntilOffset bounds Poll / PollRecords from above: only
// entries with offset < until are returned (half-open range).
// Pair with OffsetAt to read records in a time window.
// Zero-value offset disables the bound.
func WithUntilOffset(until Offset) QueryOption {
	return func(o *QueryOpts) {
		o.Until = until
	}
}

// WithReadAheadPartitions tells ReadIter / ReadManyIter /
// PollRecordsIter to prefetch n partitions ahead of the current
// yield position. Default is 1 — minimum useful lookahead so
// decode of partition N+1 overlaps yield of partition N. Pass a
// larger value for more aggressive prefetch on consumers that do
// non-trivial per-record work; combine with WithReadAheadBytes
// to bound stacking of skewed-size partitions. Values < 1 are
// floored to 1 (strict-serial decode is no longer offered as a
// public mode — the byte cap handles bounded-memory pipelines).
//
// Each partition is downloaded + decoded into a single buffered
// batch; a background decoder keeps the pipeline topped up to n
// ahead while the main goroutine yields records from the current
// batch. Memory: O((n+1) partitions) — current + n prefetched.
//
// No-op on read paths that don't stream partition-by-partition
// (s3sql).
func WithReadAheadPartitions(n int) QueryOption {
	return func(o *QueryOpts) {
		o.ReadAheadPartitions = n
	}
}

// WithReadAheadBytes caps the cumulative uncompressed parquet
// bytes that may sit decoded in the ReadIter / ReadManyIter /
// PollRecordsIter pipeline ahead of the current yield position.
// Zero (default) disables the cap; only WithReadAheadPartitions
// binds.
//
// Composes with WithReadAheadPartitions — both are evaluated and
// whichever cap binds first holds the producer back. Useful when
// partition sizes are skewed: a tiny WithReadAheadPartitions(1)
// is too conservative for many small partitions but a larger
// value risks OOM on a few large ones; a byte cap auto-tunes
// across both.
//
// The byte total is read from each parquet file's footer
// (total_uncompressed_size summed across row groups), so the
// cap is exact, not a heuristic. Decoded Go memory typically
// runs 1–2× the uncompressed size depending on data shape.
//
// Per-partition guarantee: if a single partition's uncompressed
// size exceeds the cap, that one partition still decodes (the
// cap can't be enforced below the partition granularity without
// row-group-level streaming). The cap only prevents *additional*
// partitions from joining the buffer.
//
// No-op on read paths that don't stream partition-by-partition
// (s3sql).
func WithReadAheadBytes(n int64) QueryOption {
	return func(o *QueryOpts) {
		o.ReadAheadBytes = n
	}
}

// WithIdempotentRead makes a snapshot read retry-safe: the result
// reflects state as of the first write of the given idempotency
// token. Pairs with WithIdempotencyToken on the write side so a
// read-modify-write cycle is deterministic across retries — the
// second attempt's Read sees the same state the first attempt
// saw, computes the same diff, and writes the same bytes. One
// token, both sides.
//
// Applies to snapshot-style reads: Read / ReadIter / ReadMany /
// ReadManyIter / PollRecordsIter / Query / QueryMany. NOT applied
// on PollRecords (cursor-based, CDC-style) — the offset cursor
// already provides retry-safety on that path, and the by-
// LastModified barrier doesn't compose cleanly with offset-window
// semantics.
//
// Two filters apply at LIST time, per partition:
//
//   - Self-exclusion: files whose basename equals "{token}.parquet"
//     (the caller's own prior attempts) are dropped.
//   - Later-write exclusion: among files matching the token, the
//     writer records barrier[partition] = min(LastModified). For
//     every other file in that partition, files with LastModified
//     >= barrier[partition] are dropped.
//
// On the first attempt (no matching files yet) no barrier applies
// — the Read returns the current state. Partitions where the
// token does not appear are also unfiltered.
//
// Correctness relies on the caller's single-writer-per-partition
// invariant: between the first attempt's read-start and its first
// write, no other data lands in the partition, so min(LastModified
// of own files) is a sufficient barrier. If the invariant is
// violated, self-exclusion still catches own attempts but the
// retry may include concurrent writers' data the first attempt
// didn't see; record-layer dedup (EntityKeyOf + VersionOf)
// absorbs the overlap.
//
// On the s3sql side, setting WithIdempotentRead forces the Go-
// side LIST path (the single-pattern DuckDB-glob fast path is
// skipped) so the same filter applies consistently.
//
// token must pass ValidateIdempotencyToken — same grammar as
// WithIdempotencyToken so one stored token drives both sides.
// Invalid tokens surface at LIST time (not option-application
// time, since QueryOption has no error return).
func WithIdempotentRead(token string) QueryOption {
	return func(o *QueryOpts) {
		o.IdempotentReadToken = token
	}
}
