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

// WithReadAheadPartitions tells ReadIter / ReadManyIter to
// prefetch n partitions ahead of the current yield position. Each
// partition is downloaded + decoded into a single buffered batch;
// a background producer keeps the pipeline topped up to n ahead
// while the main goroutine yields records from the current batch.
//
// Default (n == 0) is strict-serial: one partition downloaded,
// fully yielded, then the next. n >= 1 hides the consumer's
// per-partition work time behind the next partition's download,
// at O(n+1 partitions) memory (current + n prefetched). Good
// default for workloads with many small partitions where S3
// round-trip latency dominates wall time.
//
// Negative values are clamped to 0. No-op on read paths that
// don't stream partition-by-partition (s3sql).
func WithReadAheadPartitions(n int) QueryOption {
	return func(o *QueryOpts) {
		o.ReadAheadPartitions = n
	}
}
