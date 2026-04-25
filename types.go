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
// tags (e.g. `parquet:"customer"`). s3store uses those tags to
// drive both the parquet writer (via s3parquet) and the SQL
// reader's reflection-based row binder (via s3sql) — one schema
// declaration covers both sides.
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

	// VersionColumn is the column name that orders versions of
	// the same entity on the SQL read path: the record with the
	// greatest VersionColumn value per entity wins. Required
	// when EntityKeyColumns is set; otherwise ignored.
	VersionColumn string

	// EntityKeyColumns are the SQL-side columns that identify a
	// unique entity for latest-per-entity dedup. Leave empty
	// to disable dedup entirely. Mirrors s3parquet's
	// EntityKeyOf — explicit opt-in, no default.
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
	// default. Forwarded to the s3parquet sub-store. See
	// s3parquet.CompressionCodec for the accepted values.
	Compression s3parquet.CompressionCodec

	// InsertedAtField names a time.Time field on T that the
	// writer populates with its wall-clock time.Now() at write
	// time; Read and PollRecords surface the same value back.
	// The field must carry a non-empty, non-"-" parquet tag (e.g.
	// `parquet:"inserted_at"`) — the value is a real parquet
	// column, not library-managed metadata. Forwarded to both
	// sub-stores so the umbrella's read paths return the
	// identical value across s3parquet and s3sql.
	InsertedAtField string

	// DisableRefStream opts the dataset out of writing stream ref
	// files under <Prefix>/_stream/refs/. Saves one S3 PUT per
	// distinct partition key touched by a Write. Read / Query /
	// QueryRow / ReadMany / QueryMany / QueryRowMany are
	// unaffected; Poll / PollRecords / PollRecordsAll return
	// ErrRefStreamDisabled. OffsetAt still works (pure timestamp
	// encoding). Forwarded to both sub-stores.
	DisableRefStream bool

	// PartitionWriteConcurrency caps how many partitions a single
	// Write fans out in parallel. Zero → default (8). See
	// s3parquet.WriterConfig.PartitionWriteConcurrency for tuning
	// guidance; forwarded there verbatim.
	PartitionWriteConcurrency int

	// DisableCleanup disables best-effort orphan cleanup on the
	// write path's failure branches. Forwarded to
	// s3parquet.WriterConfig; see that field for the full contract.
	DisableCleanup bool

	// ConsistencyControl sets the Consistency-Control HTTP header
	// on correctness-critical S3 operations. Empty value sends no
	// header (AWS S3 / MinIO default). On NetApp StorageGRID, set
	// to one of the stronger levels for Phase 3's idempotency
	// guarantees. Forwarded to both s3parquet.WriterConfig and
	// s3parquet.ReaderConfig so the two halves cannot drift.
	ConsistencyControl s3parquet.ConsistencyLevel
}

// ErrRefStreamDisabled is returned by Poll / PollRecords /
// PollRecordsAll when Config.DisableRefStream is set. Aliased
// to the shared sentinel so errors.Is matches regardless of
// which sub-store produced the error.
var ErrRefStreamDisabled = s3parquet.ErrRefStreamDisabled

// Re-export core types so callers of the umbrella never need
// to import internal/core directly.

// Offset represents a position in the stream.
type Offset = core.Offset

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
// read path. When VersionColumn is empty, dedup is a no-op
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
// (currently s3parquet.Reader.ReadIter / ReadManyIter) to
// prefetch n partitions ahead of the yield position. Default
// is 0 (strict-serial). Accepted on the umbrella for API
// symmetry; the umbrella's ReadIter forwards to s3sql which
// ignores the option.
func WithReadAheadPartitions(n int) QueryOption {
	return core.WithReadAheadPartitions(n)
}

// WithIdempotentRead makes reads retry-safe: the result reflects
// state as of the first write of the given idempotency token —
// the caller's own prior attempts are excluded, and every other
// file with LastModified at or after the barrier is excluded too
// (per partition). Pair with WithIdempotencyToken on the write
// side; one token drives both halves of a retry-safe read-modify-
// write. See core.WithIdempotentRead for the full contract.
func WithIdempotentRead(token string) QueryOption {
	return core.WithIdempotentRead(token)
}
