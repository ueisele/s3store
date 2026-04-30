package s3store

// ReadOption configures read-path behavior. Shared across
// snapshot reads (Read / ReadIter / ReadRangeIter); Poll and
// PollRecords have their own PollOption type — the option
// spaces don't overlap.
type ReadOption func(*readOpts)

// readOpts is the resolved set of read-path options after
// applying a chain of ReadOption values. Unexported because
// callers should not construct it directly — every field is set
// through a With* constructor.
type readOpts struct {
	includeHistory bool
	// readAheadPartitions controls how many partitions ahead of
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
	readAheadPartitions *int
	// readAheadBytes caps the cumulative uncompressed parquet
	// bytes that may sit decoded in the streaming read pipeline
	// ahead of the current yield position. Zero disables the cap
	// (only readAheadPartitions binds). The value is checked
	// against the sum of each buffered partition's
	// total_uncompressed_size as reported by the parquet footer
	// — exact, not a heuristic. Decoded Go memory typically runs
	// 1–2× the uncompressed size depending on data shape (string
	// headers, slice/map pointer overhead).
	readAheadBytes int64
}

// apply runs every option against the receiver.
func (o *readOpts) apply(opts ...ReadOption) {
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
func WithHistory() ReadOption {
	return func(o *readOpts) {
		o.includeHistory = true
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
func WithReadAheadPartitions(n int) ReadOption {
	if n < 0 {
		n = 0
	}
	return func(o *readOpts) {
		o.readAheadPartitions = &n
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
func WithReadAheadBytes(n int64) ReadOption {
	return func(o *readOpts) {
		o.readAheadBytes = n
	}
}

// PollOption configures Poll / PollRecords. Separate from
// ReadOption (which serves the snapshot read paths) so each
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
