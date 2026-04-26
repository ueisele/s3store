package s3parquet

import "errors"

// ErrRefStreamDisabled is returned by Poll / PollRecords /
// ReadRangeIter when the Target has DisableRefStream set. The
// dataset was written without ref files, so there is no stream
// to tail. OffsetAt stays usable (pure timestamp encoding).
//
// The umbrella s3store package re-exports this so errors.Is
// matches whether the caller imported s3store or s3parquet.
var ErrRefStreamDisabled = errors.New(
	"ref stream disabled on this Store; " +
		"Poll/PollRecords/ReadRangeIter unavailable")

// All read/write/poll option types and constructors live next
// to their consumers:
//   - Offset, StreamEntry, PollOption, WithUntilOffset → poll.go
//   - WriteResult → write.go
//   - WriteOption, WriteOpts, WithIdempotencyToken → writeopt.go
//   - QueryOption, QueryOpts, WithHistory, WithReadAheadPartitions,
//     WithReadAheadBytes, WithIdempotentRead → queryopt.go
//   - KeyMeta → listing.go
//   - DataPath → datafile.go
//   - ParquetField, ParquetFields → parquetfield.go
