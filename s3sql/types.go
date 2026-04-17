package s3sql

import "github.com/ueisele/s3store/internal/core"

// Offset represents a position in the stream.
type Offset = core.Offset

// StreamEntry is a lightweight ref returned by Poll.
type StreamEntry = core.StreamEntry

// WriteResult contains metadata about a completed write. Not
// produced by s3sql itself (s3sql is read-only), but re-exported
// so callers who mix s3sql with s3parquet don't need a separate
// import to name the type.
type WriteResult = core.WriteResult

// QueryOption configures read-path behavior.
type QueryOption = core.QueryOption

// WithHistory disables latest-per-key deduplication on Read,
// Query, QueryRow, and PollRecords. When VersionColumn is
// empty, dedup is already a no-op regardless of this option.
func WithHistory() QueryOption {
	return core.WithHistory()
}
