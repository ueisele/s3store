// Package core contains pure primitives shared by s3store's
// sub-packages: types, path builders, ref-key codecs, and
// validation. No behavior lives here — just strings, types, and
// validation predicates that every variant of read/write/query
// must agree on.
//
// Nothing in this package uses cgo. It's safe to import from any
// cgo-free package (s3parquet) and from the cgo-requiring one
// (s3sql) alike.
package core

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// RefSeparator splits the fixed-width "{ts}-{id}" header from
// the PathEscape'd Hive key in a ref filename. PathEscape always
// escapes ';' (as %3B), so this character cannot appear in the
// encoded key — the split on it is unambiguous even when the
// original key contains arbitrary bytes.
const RefSeparator = ";"

// Offset represents a position in the stream.
type Offset string

// OffsetUnbounded is the "no bound on this side" sentinel used by
// Poll / PollRecords. The interpretation depends on which
// parameter it's passed to:
//
//   - As `since`: start at the stream head (earliest available).
//   - As the upper bound (passed via WithUntilOffset, or implied
//     when no bound is given): walk to the live tip
//     (now - SettleWindow) as of the call. To keep up with new
//     writes, call again from the last offset.
//
// Equivalent to Offset("") — use the named constant at call sites
// for self-documenting intent. ReadRangeIter takes time.Time bounds
// instead and uses time.Time{} as its own unbounded sentinel.
const OffsetUnbounded Offset = ""

// StreamEntry is a lightweight ref.
//
//   - Offset and RefPath carry the same underlying S3 key string.
//     Offset is typed for cursor-advancing in Poll-style APIs;
//     RefPath exposes the same value as an explicit S3 object
//     path for callers that want to GET the ref directly. Mirrors
//     the Offset / RefPath split on WriteResult.
//   - Key is the Hive-style partition key ("period=X/customer=Y")
//     that the writer originally passed to WriteWithKey — useful
//     for consumers to route records by partition without parsing
//     DataPath.
//   - DataPath is the S3 object key of the data file this ref
//     points at; GET it to fetch the parquet payload.
//   - InsertedAt is the writer's wall-clock capture at write-start,
//     decoded from the dataTsMicros embedded in the ref filename.
//     Identical to what the writer stamped into the InsertedAtField
//     parquet column, so consumers see the write time without
//     reading the data file.
type StreamEntry struct {
	Offset     Offset
	Key        string
	DataPath   string
	RefPath    string
	InsertedAt time.Time
}

// WriteResult contains metadata about a completed write.
// InsertedAt is the writer's wall-clock capture at write-start
// — the same value that populates the configured InsertedAtField
// column, the x-amz-meta-created-at header, and the dataTsMicros
// component of the ref filename. Exposed on the result so callers
// can log / persist it (e.g., into an outbox table) without
// parsing the data path or issuing a HEAD.
type WriteResult struct {
	Offset     Offset
	DataPath   string
	RefPath    string
	InsertedAt time.Time
}

// BaseConfig holds the fields every sub-package's Config needs.
// Sub-packages flatten these fields rather than embedding this
// struct, so user-construction syntax stays clean. It exists
// mainly as documentation of the shared contract.
type BaseConfig struct {
	Bucket            string
	Prefix            string
	PartitionKeyParts []string
	S3Client          *s3.Client
}
