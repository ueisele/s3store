package s3sql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/ueisele/s3store/internal/core"
)

// Config defines how an s3sql Store is set up. T is the record
// type returned by Read and PollRecords; arbitrary SQL queries
// via Query / QueryRow return *sql.Rows / *sql.Row directly.
//
// T must be a struct whose exported fields carry parquet struct
// tags (e.g. `parquet:"customer"`). s3sql builds a reflection-
// based row binder once at New() from those tags; the binder
// drives both the SELECT column list and the per-row Scan into
// typed records. Columns absent from the parquet file land as
// the field's Go zero value; user types implementing
// sql.Scanner are supported.
type Config[T any] struct {
	// Bucket is the S3 bucket name.
	Bucket string

	// Prefix under which data files are stored.
	Prefix string

	// PartitionKeyParts defines the Hive-partition key segments in order.
	PartitionKeyParts []string

	// S3Client is the AWS S3 client used for ref listing (Poll).
	// DuckDB's httpfs extension uses its own S3 client for actual
	// parquet reads; its settings are auto-derived from this
	// client's Options() (endpoint, region, URL style,
	// use_ssl). Override via ExtraInitSQL if needed.
	S3Client *s3.Client

	// TableAlias is the name used in SQL queries for the CTE
	// that wraps the base parquet scan. Required.
	TableAlias string

	// SettleWindow is how far behind the stream tip Poll and
	// PollRecords read. Default: 5s.
	SettleWindow time.Duration

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
}

func (c Config[T]) settleWindow() time.Duration {
	if c.SettleWindow > 0 {
		return c.SettleWindow
	}
	return 5 * time.Second
}

// dedupEnabled reports whether the store should emit a dedup
// CTE for reads. Gated on EntityKeyColumns being non-empty;
// New() guarantees VersionColumn is also set when this is true.
func (c Config[T]) dedupEnabled() bool {
	return len(c.EntityKeyColumns) > 0
}

// Store is the cgo / DuckDB entry point to an s3store.
type Store[T any] struct {
	cfg      Config[T]
	s3       *s3.Client
	db       *sql.DB
	dataPath string
	refPath  string
	binder   *binder
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
	if err := core.ValidatePartitionKeyParts(cfg.PartitionKeyParts); err != nil {
		return nil, err
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

	var zero T
	b, err := buildBinder(reflect.TypeOf(zero))
	if err != nil {
		return nil, fmt.Errorf("s3sql: %w", err)
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
		binder:   b,
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
//
// Range segments (FROM..TO) become a common-prefix glob here;
// the exact bounds are enforced by a WHERE clause built in
// buildRangeWhere, applied over the hive partition column.
func (s *Store[T]) buildParquetURI(
	keyPattern string,
) (string, error) {
	segs, err := core.ParseKeyPattern(
		keyPattern, s.cfg.PartitionKeyParts)
	if err != nil {
		return "", err
	}
	if segs == nil {
		return s.s3URI(s.dataPath + "/**/*.parquet"), nil
	}

	globSegs := make([]string, len(segs))
	for i, seg := range segs {
		switch seg.Kind {
		case core.SegWildAll:
			globSegs[i] = seg.KeyPart + "=*"
		case core.SegExact:
			globSegs[i] = seg.KeyPart + "=" + seg.Value
		case core.SegPrefix:
			globSegs[i] = seg.KeyPart + "=" + seg.Value + "*"
		case core.SegRange:
			cp := core.CommonPrefix(seg.Value, seg.ToValue)
			if cp == "" {
				globSegs[i] = seg.KeyPart + "=*"
			} else {
				globSegs[i] = seg.KeyPart + "=" + cp + "*"
			}
		}
	}
	return s.s3URI(
		s.dataPath + "/" + strings.Join(globSegs, "/") +
			"/*.parquet"), nil
}

// buildRangeWhere emits a WHERE-clause body (without the leading
// "WHERE") enforcing the bounds of every range segment in the
// pattern against its hive partition column. Returns "" when the
// pattern has no range segments — the scan is then unfiltered.
//
// DuckDB's hive_partitioning=true exposes each PartitionKeyParts
// entry as a column, and pushes string comparisons on those
// columns down to file-selection time, so this is effectively
// partition pruning at the SQL layer.
func (s *Store[T]) buildRangeWhere(
	keyPattern string,
) (string, error) {
	segs, err := core.ParseKeyPattern(
		keyPattern, s.cfg.PartitionKeyParts)
	if err != nil {
		return "", err
	}
	var preds []string
	for _, seg := range segs {
		if seg.Kind != core.SegRange {
			continue
		}
		col := sqlIdentifier(seg.KeyPart)
		if seg.Value != "" {
			preds = append(preds,
				col+" >= "+sqlQuote(seg.Value))
		}
		if seg.ToValue != "" {
			preds = append(preds,
				col+" < "+sqlQuote(seg.ToValue))
		}
	}
	return strings.Join(preds, " AND "), nil
}

// sqlIdentifier returns a DuckDB double-quoted identifier with
// embedded double-quotes doubled. Partition-key names are only
// validated for '=' and '/', so double-quoting defends against
// arbitrary characters (including SQL keywords) in the name.
func sqlIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
