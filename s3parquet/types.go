package s3parquet

import (
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
