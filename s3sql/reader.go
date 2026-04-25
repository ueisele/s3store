package s3sql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/ueisele/s3store/internal/core"
	"github.com/ueisele/s3store/s3parquet"
)

// ReaderConfig defines how an s3sql Reader is set up. T is the
// record type whose parquet tags name the columns the dedup CTE
// references; arbitrary SQL queries via Query / QueryMany return
// *sql.Rows directly and the caller binds rows themselves.
//
// The S3-wiring bundle (Bucket, Prefix, S3Client,
// PartitionKeyParts) is carried through a shared
// s3parquet.S3Target so a Writer and a Reader built against the
// same dataset cannot drift on those fields.
type ReaderConfig[T any] struct {
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
	EntityKeyColumns []string

	// ExtraInitSQL runs after the auto-derived S3 settings and
	// the object-cache pragma, in order. Use for CREATE SECRET,
	// credential overrides, or other extension loads.
	ExtraInitSQL []string

	// ConsistencyControl sets the Consistency-Control HTTP header
	// on the Go-side LIST that resolves the file set for Query /
	// QueryMany. Together with the writer's matching setting,
	// this gives read-after-write file discovery on
	// strong-consistent backends like StorageGRID.
	//
	// The DuckDB-issued GET that fetches each parquet body cannot
	// carry the header — DuckDB's httpfs has no per-request hook
	// for s3:// URLs. In practice this is fine: data files are
	// write-once and StorageGRID's read-after-new-write covers the
	// first read of any new key.
	ConsistencyControl s3parquet.ConsistencyLevel
}

// dedupEnabled reports whether the reader should emit a dedup
// CTE. Gated on EntityKeyColumns being non-empty; NewReader
// guarantees VersionColumn is also set when this is true.
func (c ReaderConfig[T]) dedupEnabled() bool {
	return len(c.EntityKeyColumns) > 0
}

// Reader is the cgo / DuckDB SQL entry point to an s3store
// dataset. Read-only and SQL-only: typed iteration / streaming
// lives on s3parquet.Reader.
type Reader[T any] struct {
	cfg      ReaderConfig[T]
	db       *sql.DB
	dataPath string
}

// NewReader constructs a Reader, opens a DuckDB connection,
// loads httpfs, and applies auto-derived + user-supplied
// settings. The Target is validated as a partitioned-data target
// (Bucket / Prefix / S3Client / PartitionKeyParts required).
func NewReader[T any](cfg ReaderConfig[T]) (*Reader[T], error) {
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

	// The dedup CTE's filename DESC tie-breaker relies on DuckDB's
	// read_parquet(filename=true) helper, which injects a column
	// literally named "filename". A T that already maps a parquet
	// column of that name would either lose its binding (the CTE
	// would EXCLUDE it) or produce a duplicate-schema error inside
	// read_parquet. Catch the Go-side case at NewReader.
	if cfg.dedupEnabled() {
		if err := checkFilenameCollision(reflect.TypeFor[T]()); err != nil {
			return nil, err
		}
	}

	db, err := openDuckDB(cfg.Target.S3Client(), cfg.ExtraInitSQL)
	if err != nil {
		return nil, fmt.Errorf(
			"s3sql: failed to open DuckDB: %w", err)
	}

	return &Reader[T]{
		cfg:      cfg,
		db:       db,
		dataPath: core.DataPath(cfg.Target.Prefix()),
	}, nil
}

// checkFilenameCollision rejects T types whose parquet tags claim
// the literal column name "filename" — that name is reserved for
// DuckDB's read_parquet(filename=true) helper used by the dedup
// CTE.
func checkFilenameCollision(rt reflect.Type) error {
	for rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	if rt.Kind() != reflect.Struct {
		return nil
	}
	for i := range rt.NumField() {
		f := rt.Field(i)
		tag, ok := f.Tag.Lookup("parquet")
		if !ok {
			continue
		}
		name, _, _ := strings.Cut(tag, ",")
		if name == "filename" {
			return fmt.Errorf(
				"s3sql: T has a `parquet:\"filename\"` field, " +
					"which collides with DuckDB's " +
					"read_parquet(filename=true) used for " +
					"the dedup tie-breaker; rename the field")
		}
	}
	return nil
}

// Target returns the S3Target this Reader is bound to.
func (s *Reader[T]) Target() s3parquet.S3Target {
	return s.cfg.Target
}

// Close releases the DuckDB connection.
func (s *Reader[T]) Close() error {
	return s.db.Close()
}

// s3URI returns the s3:// URI for a key in the reader's bucket.
func (s *Reader[T]) s3URI(key string) string {
	return fmt.Sprintf("s3://%s/%s", s.cfg.Target.Bucket(), key)
}

// sqlQuote returns a DuckDB single-quoted string literal with
// embedded apostrophes doubled. Used everywhere a user-derived
// string is embedded into SQL.
func sqlQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
