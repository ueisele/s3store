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
