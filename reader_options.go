package s3store

// QueryOption configures read-path behavior. Shared across
// snapshot reads (Read / ReadIter / ReadRangeIter); Poll and
// PollRecords have their own PollOption type — the option
// spaces don't overlap.
type QueryOption func(*QueryOpts)

// QueryOpts is the resolved set of read-path options after
// applying a chain of QueryOption values.
type QueryOpts struct {
	IncludeHistory bool
	// ReadAheadPartitions controls how many partitions ahead of
	// the current yield position a streaming read (ReadIter) may
	// buffer in the decoder→yield channel. nil (the default
	// when the option is not supplied) means "1" — minimum
	// useful lookahead so decode of partition N+1 overlaps yield
	// of partition N. *p == 0 is the explicit-no-buffer mode:
	// unbuffered handoff, decoder still works on N+1 concurrent
	// with yield emitting N but never holds two partitions in
	// memory at once. *p > 0 buffers up to p partitions in the
	// channel, at O(p+1 partitions) memory.
	//
	// Pointer-typed so the zero value of WithReadAheadPartitions
	// (an explicit 0) stays distinguishable from "option not
	// supplied" (which falls back to the default of 1).
	ReadAheadPartitions *int
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
	// the same bytes. Validated via validateIdempotencyToken at
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
// path. Without it, reads are deduped by EntityKeyOf + VersionOf
// (latest per entity per partition); with it, every version of
// every record is returned.
//
// When no dedup rule is configured (EntityKeyOf or VersionOf nil),
// dedup is a no-op regardless of this option.
func WithHistory() QueryOption {
	return func(o *QueryOpts) {
		o.IncludeHistory = true
	}
}

// WithReadAheadPartitions tells ReadIter / ReadRangeIter how
// many partitions to buffer ahead of the current yield position.
// Default (option not supplied) is 1 — minimum useful lookahead
// so decode of partition N+1 overlaps yield of partition N.
// Pass a larger value for more aggressive prefetch on consumers
// that do non-trivial per-record work; combine with
// WithReadAheadBytes to bound stacking of skewed-size partitions.
//
// n=0 is the explicit-no-buffer mode: unbuffered handoff between
// decoder and yield loop. The decoder still works on partition
// N+1 concurrent with yield emitting N (the handoff just blocks
// the decoder briefly), but never two decoded partitions sit in
// memory at once. Useful when records are large and the
// consumer's per-record work is fast — the byte cap is then the
// only memory regulator.
//
// Negative values are floored to 0.
//
// Each buffered partition holds its decoded records in memory
// until the yield loop consumes them. Memory: O((n+1) partitions)
// — current + n prefetched.
func WithReadAheadPartitions(n int) QueryOption {
	if n < 0 {
		n = 0
	}
	return func(o *QueryOpts) {
		o.ReadAheadPartitions = &n
	}
}

// WithReadAheadBytes caps the cumulative uncompressed parquet
// bytes that may sit decoded in the ReadIter / ReadRangeIter
// pipeline ahead of the current yield position.
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
// Applies to snapshot-style reads: Read / ReadIter /
// ReadRangeIter. NOT applied on PollRecords (cursor-based,
// CDC-style) — the offset cursor already provides retry-safety
// on that path, and the by-LastModified barrier doesn't compose
// cleanly with offset-window semantics.
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
// token must pass validateIdempotencyToken — same grammar as
// WithIdempotencyToken so one stored token drives both sides.
// Invalid tokens surface at LIST time (not option-application
// time, since QueryOption has no error return).
func WithIdempotentRead(token string) QueryOption {
	return func(o *QueryOpts) {
		o.IdempotentReadToken = token
	}
}

// PollOption configures Poll / PollRecords. Separate from
// QueryOption (which serves the snapshot read paths) so each
// option type only carries knobs its read path actually
// honours — no "ignored on this path" footguns.
type PollOption func(*pollOpts)

// pollOpts is the resolved set of Poll / PollRecords options.
type pollOpts struct {
	// until, when non-empty, is an exclusive upper bound on
	// stream offsets returned: entries whose offset is >= until
	// are skipped, giving a half-open [since, until) range.
	// Matches Kafka's offset semantics.
	until Offset
}

// WithUntilOffset bounds Poll / PollRecords from above: only
// entries with offset < until are returned (half-open range).
// Pair with Reader.OffsetAt to read records in a time window.
// Zero-value offset disables the bound.
func WithUntilOffset(until Offset) PollOption {
	return func(o *pollOpts) {
		o.until = until
	}
}
