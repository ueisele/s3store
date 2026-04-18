package s3store

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ueisele/s3store/internal/core"
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

	// BloomFilterColumns lists parquet column names (top-level)
	// that Write should emit per-row-group split-block bloom
	// filters for. Only the SQL read path (DuckDB via s3sql)
	// consults these filters; the umbrella's Read / PollRecords
	// go through s3sql so they benefit. Forwarded to the
	// s3parquet sub-store unchanged. See s3parquet.Config for
	// full details.
	BloomFilterColumns []string

	// InsertedAtField names a time.Time field on T that Read and
	// PollRecords populate with the source parquet file's write
	// timestamp on decode. The field must be tagged `parquet:"-"`
	// (validated at New()) so it stays off the parquet schema.
	// Forwarded to both sub-stores so the umbrella's read paths
	// — whether routed via s3parquet.PollRecords or s3sql.Read /
	// PollRecords — populate the field consistently.
	InsertedAtField string
}

// Re-export core types so callers of the umbrella never need
// to import internal/core directly.

// Offset represents a position in the stream.
type Offset = core.Offset

// StreamEntry is a lightweight ref.
type StreamEntry = core.StreamEntry

// WriteResult contains metadata about a completed write.
type WriteResult = core.WriteResult

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
