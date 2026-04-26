package s3sql

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/ueisele/s3store/s3parquet"
)

// ReaderConfig defines how an s3sql Reader is set up. Query
// returns *sql.Rows directly; the caller binds rows themselves
// (or via the s3sql.ScanAll helper).
//
// The S3-wiring bundle (Bucket, Prefix, S3Client,
// PartitionKeyParts) is carried through a shared
// s3parquet.S3Target so a Writer and a Reader built against the
// same dataset cannot drift on those fields.
type ReaderConfig struct {
	// Target carries the dataset's S3 wiring and partitioning
	// metadata. Required; same S3Target the writing
	// s3parquet.Writer was built with.
	Target s3parquet.S3Target

	// TableAlias is the name used in SQL queries for the CTE
	// that wraps the base parquet scan. Required.
	TableAlias string

	// VersionColumn is the column name that orders versions of
	// the same entity: the record with the greatest VersionColumn
	// value per entity wins. Required when EntityKeyColumns is
	// set; otherwise ignored.
	VersionColumn string

	// EntityKeyColumns are the columns that identify a unique
	// entity for latest-per-entity deduplication. Leave empty
	// to disable dedup entirely (pure stream semantics).
	//
	// Mirrors s3parquet's EntityKeyOf: explicit opt-in. There's
	// no default — partition layout and entity identity are
	// different axes, and defaulting one to the other silently
	// produces wrong results when a partition holds multiple
	// entities.
	//
	// When dedup is enabled the dedup CTE references DuckDB's
	// read_parquet(filename=true) helper, which injects a column
	// literally named "filename". A dataset whose parquet schema
	// already maps a column called "filename" will produce a
	// duplicate-column error from read_parquet at query time —
	// rename the column on the writer side.
	EntityKeyColumns []string

	// ExtraInitSQL runs after the auto-derived S3 settings and
	// the object-cache pragma, in order. Use for CREATE SECRET,
	// credential overrides, or other extension loads.
	ExtraInitSQL []string
}

// dedupEnabled reports whether the reader should emit a dedup
// CTE. Gated on EntityKeyColumns being non-empty; NewReader
// guarantees VersionColumn is also set when this is true.
func (c ReaderConfig) dedupEnabled() bool {
	return len(c.EntityKeyColumns) > 0
}

// Reader is the cgo / DuckDB SQL entry point to an s3store
// dataset. Read-only and SQL-only: typed iteration / streaming
// lives on s3parquet.Reader.
type Reader struct {
	cfg      ReaderConfig
	db       *sql.DB
	dataPath string
}

// NewReader constructs a Reader, opens a DuckDB connection,
// loads httpfs, and applies auto-derived + user-supplied
// settings. The Target is validated as a partitioned-data target
// (Bucket / Prefix / S3Client / PartitionKeyParts required).
func NewReader(cfg ReaderConfig) (*Reader, error) {
	if err := cfg.Target.Validate(); err != nil {
		return nil, fmt.Errorf("s3sql: %w", err)
	}
	if cfg.TableAlias == "" {
		return nil, fmt.Errorf("s3sql: TableAlias is required")
	}
	// EntityKeyColumns + VersionColumn must be set together or
	// not at all. Either alone is a misconfiguration that would
	// silently produce wrong dedup (without VersionColumn,
	// QUALIFY has nothing to ORDER BY) or a dead config field
	// (without EntityKeyColumns, VersionColumn is never used).
	if len(cfg.EntityKeyColumns) > 0 && cfg.VersionColumn == "" {
		return nil, fmt.Errorf(
			"s3sql: VersionColumn is required when " +
				"EntityKeyColumns is set")
	}
	if cfg.VersionColumn != "" && len(cfg.EntityKeyColumns) == 0 {
		return nil, fmt.Errorf(
			"s3sql: EntityKeyColumns is required when " +
				"VersionColumn is set")
	}

	db, err := openDuckDB(cfg.Target.S3Client(), cfg.ExtraInitSQL)
	if err != nil {
		return nil, fmt.Errorf(
			"s3sql: failed to open DuckDB: %w", err)
	}

	return &Reader{
		cfg:      cfg,
		db:       db,
		dataPath: s3parquet.DataPath(cfg.Target.Prefix()),
	}, nil
}

// Target returns the S3Target this Reader is bound to.
func (s *Reader) Target() s3parquet.S3Target {
	return s.cfg.Target
}

// Close releases the DuckDB connection.
func (s *Reader) Close() error {
	return s.db.Close()
}

// s3URI returns the s3:// URI for a key in the reader's bucket.
func (s *Reader) s3URI(key string) string {
	return fmt.Sprintf("s3://%s/%s", s.cfg.Target.Bucket(), key)
}

// sqlQuote returns a DuckDB single-quoted string literal with
// embedded apostrophes doubled. Used everywhere a user-derived
// string is embedded into SQL.
func sqlQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// QueryOption configures read-path behavior.
type QueryOption = s3parquet.QueryOption

// WithHistory disables latest-per-entity deduplication on Query.
// When EntityKeyColumns is empty, dedup is already a no-op
// regardless of this option.
func WithHistory() QueryOption {
	return s3parquet.WithHistory()
}

// WithIdempotentRead makes Query retry-safe: the result reflects
// state as of the first write of the given idempotency token.
// Pair with WithIdempotencyToken on the write side so one token
// drives both sides. See s3parquet.WithIdempotentRead for the full
// contract.
func WithIdempotentRead(token string) QueryOption {
	return s3parquet.WithIdempotentRead(token)
}
