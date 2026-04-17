package s3store

import (
	"database/sql"
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

	// ScanFunc maps a sql.Rows row to a record. Used by Read
	// and PollRecords (which go through DuckDB) to return
	// typed []T.
	ScanFunc func(*sql.Rows) (T, error)

	// VersionColumn is the column name used for deduplication
	// in Read / PollRecords / Query. Leave empty to disable
	// dedup on the SQL path.
	VersionColumn string

	// DeduplicateBy defines the columns that identify a unique
	// record for SQL-side dedup. Defaults to PartitionKeyParts.
	DeduplicateBy []string

	// TableAlias is the name used in SQL queries for the
	// wrapper CTE. Required.
	TableAlias string

	// SettleWindow is how far behind the stream tip Poll and
	// PollRecords read. Default: 5s.
	SettleWindow time.Duration

	// ColumnDefaults maps column names to SQL default
	// expressions for files that predate the column.
	ColumnDefaults map[string]string

	// ColumnAliases maps a new column name to a chain of old
	// names it should absorb, in priority order. See s3sql's
	// package docs for the full semantics.
	ColumnAliases map[string][]string

	// ExtraInitSQL runs after the auto-derived S3 settings at
	// DuckDB init. Use for CREATE SECRET, credential overrides,
	// or additional extension loads.
	ExtraInitSQL []string
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
