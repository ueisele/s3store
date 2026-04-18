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
