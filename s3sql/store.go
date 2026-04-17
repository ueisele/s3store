package s3sql

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/ueisele/s3store/internal/core"
)

// Config defines how an s3sql Store is set up. T is the record
// type returned by Read and PollRecords; arbitrary SQL queries
// via Query / QueryRow return *sql.Rows / *sql.Row directly.
type Config[T any] struct {
	// Bucket is the S3 bucket name.
	Bucket string

	// Prefix under which data files are stored.
	Prefix string

	// KeyParts defines the Hive-partition key segments in order.
	KeyParts []string

	// S3Client is the AWS S3 client used for ref listing (Poll).
	// DuckDB's httpfs extension uses its own S3 client for actual
	// parquet reads; its settings are auto-derived from this
	// client's Options() (endpoint, region, URL style,
	// use_ssl). Override via ExtraInitSQL if needed.
	S3Client *s3.Client

	// ScanFunc maps a sql.Rows row to a record.
	ScanFunc func(*sql.Rows) (T, error)

	// TableAlias is the name used in SQL queries for the CTE
	// that wraps the base parquet scan. Required.
	TableAlias string

	// SettleWindow is how far behind the stream tip Poll and
	// PollRecords read. Default: 5s.
	SettleWindow time.Duration

	// VersionColumn is the column name used for deduplication.
	// Leave empty to disable dedup (pure stream semantics).
	VersionColumn string

	// DeduplicateBy defines the columns that identify a unique
	// record for dedup purposes. Defaults to KeyParts.
	DeduplicateBy []string

	// ColumnDefaults maps column names to SQL default expressions
	// for files that predate the column.
	ColumnDefaults map[string]string

	// ColumnAliases maps a new column name to a chain of old
	// names it should absorb, in priority order. See package
	// docs / README for the full semantics.
	ColumnAliases map[string][]string

	// ExtraInitSQL runs after the auto-derived S3 settings and
	// the object-cache pragma, in order. Use for CREATE SECRET,
	// credential overrides, or other extension loads.
	ExtraInitSQL []string
}

func (c Config[T]) settleWindow() time.Duration {
	if c.SettleWindow > 0 {
		return c.SettleWindow
	}
	return 5 * time.Second
}

func (c Config[T]) dedupColumns() []string {
	if len(c.DeduplicateBy) > 0 {
		return c.DeduplicateBy
	}
	return c.KeyParts
}

// Store is the cgo / DuckDB entry point to an s3store.
type Store[T any] struct {
	cfg      Config[T]
	s3       *s3.Client
	db       *sql.DB
	dataPath string
	refPath  string
}

// New constructs a Store, opens a DuckDB connection, loads
// httpfs, and applies auto-derived + user-supplied settings.
func New[T any](cfg Config[T]) (*Store[T], error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("s3sql: Bucket is required")
	}
	if cfg.Prefix == "" {
		return nil, fmt.Errorf("s3sql: Prefix is required")
	}
	if cfg.TableAlias == "" {
		return nil, fmt.Errorf("s3sql: TableAlias is required")
	}
	if cfg.S3Client == nil {
		return nil, fmt.Errorf("s3sql: S3Client is required")
	}
	if err := core.ValidateKeyParts(cfg.KeyParts); err != nil {
		return nil, err
	}

	db, err := openDuckDB(cfg.S3Client, cfg.ExtraInitSQL)
	if err != nil {
		return nil, fmt.Errorf(
			"s3sql: failed to open DuckDB: %w", err)
	}

	return &Store[T]{
		cfg:      cfg,
		s3:       cfg.S3Client,
		db:       db,
		dataPath: core.DataPath(cfg.Prefix),
		refPath:  core.RefPath(cfg.Prefix),
	}, nil
}

// Close releases the DuckDB connection.
func (s *Store[T]) Close() error {
	return s.db.Close()
}

// s3URI returns the s3:// URI for a key in the store's bucket.
func (s *Store[T]) s3URI(key string) string {
	return fmt.Sprintf("s3://%s/%s", s.cfg.Bucket, key)
}

// sqlQuote returns a DuckDB single-quoted string literal with
// embedded apostrophes doubled. Used everywhere a user-derived
// string is embedded into SQL.
func sqlQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// buildParquetURI compiles a user-supplied key pattern into an
// S3 glob URI passed directly to DuckDB's httpfs / read_parquet.
// The pattern is validated against the shared grammar; DuckDB
// handles expansion of the allowed wildcards natively.
func (s *Store[T]) buildParquetURI(
	keyPattern string,
) (string, error) {
	if err := core.ValidateKeyPattern(
		keyPattern, s.cfg.KeyParts,
	); err != nil {
		return "", err
	}

	if keyPattern == "" || keyPattern == "*" {
		return s.s3URI(s.dataPath + "/**/*.parquet"), nil
	}

	segments := strings.Split(keyPattern, "/")
	for i, seg := range segments {
		if seg == "*" {
			segments[i] = s.cfg.KeyParts[i] + "=*"
		}
	}
	return s.s3URI(
		s.dataPath + "/" + strings.Join(segments, "/") +
			"/*.parquet"), nil
}
