package s3sql

import (
	"github.com/ueisele/s3store/internal/core"
	"github.com/ueisele/s3store/internal/refstream"
)

// ErrRefStreamDisabled is returned by Poll / PollRecords /
// PollRecordsAll when Config.DisableRefStream is set. OffsetAt
// is still usable (pure timestamp encoding, no S3 dependency).
//
// Aliased to the shared sentinel in internal/refstream so
// errors.Is matches across s3parquet, s3sql, and the umbrella.
var ErrRefStreamDisabled = refstream.ErrDisabled

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

// WithUntilOffset bounds Poll / PollRecords from above: only
// entries with offset < until are returned (half-open range).
// Pair with Store.OffsetAt to read records in a time window.
func WithUntilOffset(until Offset) QueryOption {
	return core.WithUntilOffset(until)
}

// WithReadAheadPartitions is accepted for API symmetry with
// s3parquet and the umbrella, but is a no-op on the s3sql read
// paths — DuckDB streams rows across the full file union natively
// and does not process one Hive partition at a time.
func WithReadAheadPartitions(n int) QueryOption {
	return core.WithReadAheadPartitions(n)
}
