package s3store

import (
	"database/sql"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	_ "github.com/marcboeker/go-duckdb"
)

// Config defines how a Store is set up. T is the record type.
type Config[T any] struct {
	// S3 bucket name.
	Bucket string

	// Prefix under which data files are stored.
	Prefix string

	// KeyParts defines the Hive-partition key segments in order.
	KeyParts []string

	// KeyFunc extracts the Hive-partition key from a record.
	// Used by Write() to group records.
	KeyFunc func(T) string

	// ScanFunc maps a sql.Rows row to a record.
	// Used by Read() and PollRecords() to return typed []T.
	// Column order must match SELECT * with ColumnAliases and
	// ColumnDefaults applied.
	ScanFunc func(*sql.Rows) (T, error)

	// VersionColumn is the column name used for deduplication.
	// Leave empty to disable.
	VersionColumn string

	// DeduplicateBy defines the columns that identify a unique
	// record. If empty, partitions by all KeyParts.
	DeduplicateBy []string

	// TableAlias is the name used in SQL queries.
	TableAlias string

	// SettleWindow is how far behind the stream tip the consumer
	// reads. Default: 5s.
	SettleWindow time.Duration

	// ColumnDefaults maps column names to default SQL expressions
	// for files that predate the column.
	ColumnDefaults map[string]string

	// ColumnAliases maps new column names to a chain of old names.
	// Generates COALESCE(new, old1, old2, ...).
	ColumnAliases map[string][]string

	// S3Client is the AWS S3 client to use.
	S3Client *s3.Client

	// S3Endpoint overrides the S3 endpoint.
	S3Endpoint string
}

func (c Config[T]) settleWindow() time.Duration {
	if c.SettleWindow > 0 {
		return c.SettleWindow
	}
	return 5 * time.Second
}

// Offset represents a position in the stream.
type Offset string

// StreamEntry is a lightweight ref.
type StreamEntry struct {
	Offset   Offset
	Key      string
	DataPath string
}

// QueryOption configures query behavior.
type QueryOption func(*queryOpts)

type queryOpts struct {
	includeHistory bool
}

// WithHistory disables deduplication.
func WithHistory() QueryOption {
	return func(o *queryOpts) {
		o.includeHistory = true
	}
}

// PollOption configures PollRecords behavior.
type PollOption func(*pollOpts)

type pollOpts struct {
	compacted bool
}

// WithCompaction makes PollRecords apply latest-per-key
// deduplication within each batch — Kafka compacted-topic
// semantics. Without it (the default), PollRecords is a pure
// stream that returns every record in every referenced file.
//
// Requires Config.VersionColumn to be set; PollRecords errors
// otherwise, since there is no ordering to dedupe on.
//
// Note: s3store is append-only, so compacted mode is
// upsert-only — there is no tombstone or key-delete mechanism.
func WithCompaction() PollOption {
	return func(o *pollOpts) {
		o.compacted = true
	}
}

// WriteResult contains metadata about a completed write.
type WriteResult struct {
	Offset   Offset
	DataPath string
	RefPath  string
}

// duckDBSettingsSQL returns the non-extension session settings
// applied after the httpfs extension is loaded.
func duckDBSettingsSQL(endpoint string) []string {
	stmts := []string{
		"SET s3_url_style='path'",
		// Cache parquet footers so the introspection LIMIT 0
		// and the main query don't double-read metadata.
		"SET enable_object_cache=true",
	}
	if endpoint != "" {
		stmts = append(stmts,
			"SET s3_endpoint='"+endpoint+"'")
	}
	return stmts
}

// ensureHTTPFS tries LOAD first and falls back to INSTALL only
// when LOAD fails. This avoids a network roundtrip on every
// New() once the extension is cached, works in air-gapped
// environments with pre-installed extensions, and prevents
// INSTALL from silently upgrading an already-present version.
func ensureHTTPFS(db *sql.DB) error {
	if _, err := db.Exec("LOAD httpfs"); err == nil {
		return nil
	}
	if _, err := db.Exec("INSTALL httpfs"); err != nil {
		return err
	}
	_, err := db.Exec("LOAD httpfs")
	return err
}

func openDuckDB(endpoint string) (*sql.DB, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, err
	}
	if err := ensureHTTPFS(db); err != nil {
		db.Close()
		return nil, err
	}
	for _, stmt := range duckDBSettingsSQL(endpoint) {
		if _, err := db.Exec(stmt); err != nil {
			db.Close()
			return nil, err
		}
	}
	return db, nil
}
