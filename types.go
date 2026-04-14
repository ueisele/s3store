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
	//
	// The column order ScanFunc must read matches this:
	//
	//   - base columns from the scanned files, in their
	//     file-schema order (union_by_name),
	//   - minus any old-name columns EXCLUDEd by an alias,
	//   - with ColumnAliases / ColumnDefaults on existing
	//     columns REPLACEd in their original position,
	//   - with alias targets whose new name isn't in any file
	//     yet, and defaults for columns that don't exist at
	//     all, appended at the end (in sorted-key order).
	//
	// When no schema-evolution transforms are configured, the
	// order is just the file-schema order.
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

	// ColumnAliases maps a new column name to a chain of old
	// names it should absorb, in priority order. Behavior:
	//
	//   - If the new column exists in at least one scanned
	//     file, the library emits
	//     SELECT * REPLACE (COALESCE(new, old1, old2, ...) AS new)
	//     so new files' values take precedence with old files
	//     falling back to the old names.
	//   - If the new column doesn't exist in any scanned file
	//     yet, the library emits the COALESCE as a new
	//     appended column (useful for preemptive rename
	//     configuration before any file has the new name).
	//   - Old-name columns named in the chain are EXCLUDEd
	//     from the star expansion in both cases, so they
	//     don't linger next to the aliased column.
	//   - If neither the new name nor any old name exists in
	//     any scanned file, a typed NULL is appended under
	//     the new name so ScanFunc always finds the column.
	//
	// The same old-name may appear in multiple aliases;
	// duplicates in the resulting EXCLUDE list are removed.
	ColumnAliases map[string][]string

	// S3Client is the AWS S3 client to use.
	S3Client *s3.Client

	// S3Endpoint overrides the S3 endpoint passed to DuckDB's
	// httpfs extension (format: "host:port", no scheme).
	S3Endpoint string

	// ExtraInitSQL are additional DuckDB statements executed
	// once, after the default init (httpfs, settings, endpoint).
	// Useful for injecting S3 credentials, disabling SSL for
	// MinIO / localstack, setting a region, or loading other
	// DuckDB extensions. Statements run in order.
	ExtraInitSQL []string
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

// QueryOption configures read-path behavior. Shared across
// Query, QueryRow, and PollRecords so there's one option and
// one mental model for every read API.
type QueryOption func(*queryOpts)

type queryOpts struct {
	includeHistory bool
}

// WithHistory disables latest-per-key deduplication on any
// read path (Query, QueryRow, PollRecords). Without it, reads
// are deduped by VersionColumn + DeduplicateBy (defaulting to
// KeyParts); with it, every version of every record is
// returned.
//
// When VersionColumn is empty, dedup is a no-op regardless of
// this option — there's no ordering to dedup on.
func WithHistory() QueryOption {
	return func(o *queryOpts) {
		o.includeHistory = true
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

func openDuckDB(endpoint string, extra []string) (*sql.DB, error) {
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
	for _, stmt := range extra {
		if _, err := db.Exec(stmt); err != nil {
			db.Close()
			return nil, err
		}
	}
	return db, nil
}
