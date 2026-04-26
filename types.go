package s3store

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ueisele/s3store/internal/core"
	"github.com/ueisele/s3store/s3parquet"
)

// CompressionCodec mirrors s3parquet.CompressionCodec so umbrella
// users don't have to import the sub-package for the constants.
type CompressionCodec = s3parquet.CompressionCodec

// Compression codec constants re-exported from s3parquet so
// umbrella callers stay in one package.
const (
	CompressionSnappy       = s3parquet.CompressionSnappy
	CompressionZstd         = s3parquet.CompressionZstd
	CompressionGzip         = s3parquet.CompressionGzip
	CompressionUncompressed = s3parquet.CompressionUncompressed
)

// Config defines how the umbrella Store is set up. T is the
// record type returned by Read and PollRecords.
//
// This struct flattens the fields every sub-package
// (s3parquet, s3sql) needs into one place so users who want the
// full feature set construct a single config. Users who need
// only write, only read, or only SQL should import
// s3store/s3parquet or s3store/s3sql directly.
//
// T must be a struct whose exported fields carry parquet struct
// tags (e.g. `parquet:"customer"`). The parquet writer + reader
// use those tags directly; the SQL reader uses the column names
// referenced by VersionColumn / EntityKeyColumns to drive the
// dedup CTE.
type Config[T any] struct {
	// S3 bucket name.
	Bucket string

	// Prefix under which data files are stored.
	Prefix string

	// PartitionKeyParts defines the Hive-partition key segments in order.
	PartitionKeyParts []string

	// S3Client is the AWS S3 client to use. DuckDB's httpfs
	// settings (endpoint, region, URL style, use_ssl) are
	// auto-derived from this client's Options() at New() time.
	S3Client *s3.Client

	// PartitionKeyOf extracts the Hive-partition key from a
	// record. Required for Write(); used to group records.
	PartitionKeyOf func(T) string

	// EntityKeyOf identifies the unique entity for a record on
	// the parquet read path (Read / PollRecords / ReadIter):
	// records with the same EntityKeyOf value collapse to the
	// latest version per VersionOf within a partition. Leave nil
	// to disable dedup. Mirrors s3parquet's EntityKeyOf.
	EntityKeyOf func(T) string

	// VersionOf returns the comparable version stamp for a record
	// on the parquet read path. Required when EntityKeyOf is set,
	// otherwise ignored. To use the writer-stamped insertedAt as
	// the version, configure InsertedAtField on the writer side
	// and reference the same field here:
	//
	//	VersionOf: func(r T) int64 { return r.InsertedAt.UnixMicro() }
	VersionOf func(T) int64

	// VersionColumn is the column name that orders versions of
	// the same entity on the SQL read path: the record with the
	// greatest VersionColumn value per entity wins. Required
	// when EntityKeyColumns is set; otherwise ignored.
	VersionColumn string

	// EntityKeyColumns are the SQL-side columns that identify a
	// unique entity for latest-per-entity dedup. Leave empty
	// to disable dedup entirely. Mirrors EntityKeyOf on the
	// parquet read path; both must be set together for callers
	// who exercise both read paths.
	EntityKeyColumns []string

	// TableAlias is the name used in SQL queries for the
	// wrapper CTE. Required.
	TableAlias string

	// SettleWindow is how far behind the stream tip Poll and
	// PollRecords read. Default: 5s.
	SettleWindow time.Duration

	// ExtraInitSQL runs after the auto-derived S3 settings at
	// DuckDB init. Use for CREATE SECRET, credential overrides,
	// or additional extension loads.
	ExtraInitSQL []string

	// Compression selects the parquet compression codec used on
	// Write. Zero value defaults to snappy — the ecosystem
	// default. See s3parquet.CompressionCodec for the accepted
	// values.
	Compression s3parquet.CompressionCodec

	// InsertedAtField names a time.Time field on T that the
	// writer populates with its wall-clock time.Now() at write
	// time; Read and PollRecords surface the same value back as
	// a normal parquet column on T. Forwarded to WriterConfig
	// only — the reader has no special handling, the column
	// just shows up on T like any other tagged field. The field
	// must carry a non-empty, non-"-" parquet tag (e.g.
	// `parquet:"inserted_at"`).
	InsertedAtField string

	// DisableRefStream opts the dataset out of writing stream ref
	// files under <Prefix>/_stream/refs/. Saves one S3 PUT per
	// distinct partition key touched by a Write. Read / Query /
	// ReadMany / QueryMany are unaffected; Poll / PollRecords /
	// PollRecordsIter return ErrRefStreamDisabled. OffsetAt still
	// works (pure timestamp encoding).
	DisableRefStream bool

	// MaxInflightRequests caps S3 requests in flight per library
	// call. Zero → default (32). Forwarded to the shared
	// s3parquet.S3Target so Writer and Reader share one cap. See
	// s3parquet.S3Target.MaxInflightRequests for the full contract
	// and the http.Transport.MaxConnsPerHost interaction.
	MaxInflightRequests int

	// DisableCleanup disables best-effort orphan cleanup on the
	// write path's failure branches. Forwarded to
	// s3parquet.WriterConfig; see that field for the full contract.
	DisableCleanup bool

	// ConsistencyControl sets the Consistency-Control HTTP header
	// on correctness-critical S3 operations. Empty value sends no
	// header (AWS S3 / MinIO default). On NetApp StorageGRID, set
	// to one of the stronger levels for Phase 3's idempotency
	// guarantees. Forwarded to all three sub-handles so they
	// cannot drift.
	ConsistencyControl s3parquet.ConsistencyLevel
}

// ErrRefStreamDisabled is returned by Poll / PollRecords /
// PollRecordsIter when Config.DisableRefStream is set. Aliased
// to the shared sentinel so errors.Is matches regardless of
// which sub-handle produced the error.
var ErrRefStreamDisabled = s3parquet.ErrRefStreamDisabled

// Re-export core types so callers of the umbrella never need
// to import internal/core directly.

// Offset represents a position in the stream.
type Offset = core.Offset

// OffsetUnbounded is the "no bound on this side" sentinel for
// Poll / PollRecords / PollRecordsIter. As `since` it means
// stream head; as `until` it means live tip. See core.OffsetUnbounded.
const OffsetUnbounded = core.OffsetUnbounded

// StreamEntry is a lightweight ref.
type StreamEntry = core.StreamEntry

// WriteResult contains metadata about a completed write.
type WriteResult = core.WriteResult

// WriteOption configures write-path behavior. See
// s3parquet.WriteOption for the full contract — this umbrella
// alias exists so callers don't have to import s3parquet just to
// pass WithIdempotencyToken through.
type WriteOption = core.WriteOption

// WithIdempotencyToken marks a write as a retry-safe logical unit.
// See core.WithIdempotencyToken for the full contract (token
// replaces the library-default id in the data filename; retries
// trigger overwrite-prevention so the parquet body is not re-
// uploaded; maxRetryAge bounds the scoped LIST used to dedup
// the ref emission).
func WithIdempotencyToken(
	token string, maxRetryAge time.Duration,
) WriteOption {
	return core.WithIdempotencyToken(token, maxRetryAge)
}

// QueryOption configures read-path behavior.
type QueryOption = core.QueryOption

// WithHistory disables latest-per-key deduplication on any
// read path. When neither EntityKeyOf (parquet path) nor
// EntityKeyColumns (SQL path) is configured, dedup is a no-op
// regardless of this option.
func WithHistory() QueryOption {
	return core.WithHistory()
}

// WithUntilOffset bounds Poll / PollRecords from above: only
// entries with offset < until are returned (half-open range).
// Pair with OffsetAt to read records in a time window.
func WithUntilOffset(until Offset) QueryOption {
	return core.WithUntilOffset(until)
}

// WithReadAheadPartitions tells partition-streaming readers
// (ReadIter / ReadManyIter / PollRecordsIter) to prefetch n
// partitions ahead of the yield position. Default is 1 (one
// partition lookahead so decode of N+1 overlaps yield of N).
// Values < 1 are floored to 1.
func WithReadAheadPartitions(n int) QueryOption {
	return core.WithReadAheadPartitions(n)
}

// WithReadAheadBytes caps the cumulative uncompressed parquet
// bytes that may sit decoded ahead of the yield position on
// ReadIter / ReadManyIter / PollRecordsIter. Composes with
// WithReadAheadPartitions
// — whichever cap binds first holds the producer back. Useful
// for skewed partition sizes. See core.WithReadAheadBytes for
// the full contract.
func WithReadAheadBytes(n int64) QueryOption {
	return core.WithReadAheadBytes(n)
}

// WithIdempotentRead makes snapshot reads retry-safe: the result
// reflects state as of the first write of the given idempotency
// token — the caller's own prior attempts are excluded, and every
// other file with LastModified at or after the barrier is excluded
// too (per partition). Applies to snapshot-style reads (Read /
// ReadIter / ReadMany / ReadManyIter / PollRecordsIter / Query /
// QueryMany). Ignored on PollRecords — the offset cursor already
// provides retry-safety on that path. Pair with
// WithIdempotencyToken on the write side; one token drives both
// halves of a retry-safe read-modify-write. See
// core.WithIdempotentRead for the full contract.
func WithIdempotentRead(token string) QueryOption {
	return core.WithIdempotentRead(token)
}
