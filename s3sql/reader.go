package s3sql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/ueisele/s3store/internal/core"
	"github.com/ueisele/s3store/s3parquet"
)

// ReaderConfig defines how an s3sql Reader is set up. T is the
// record type returned by Read and PollRecords; arbitrary SQL
// queries via Query / QueryRow return *sql.Rows / *sql.Row
// directly.
//
// T must be a struct whose exported fields carry parquet struct
// tags (e.g. `parquet:"customer"`). s3sql builds a reflection-
// based row binder once at NewReader() from those tags; the
// binder drives both the SELECT column list and the per-row Scan
// into typed records. Columns absent from the parquet file land
// as the field's Go zero value; user types implementing
// sql.Scanner are supported.
//
// The S3-wiring bundle (Bucket, Prefix, S3Client,
// PartitionKeyParts, SettleWindow, DisableRefStream) is carried
// through a shared s3parquet.S3Target so a Writer and a Reader
// built against the same dataset cannot drift on those fields.
// A service that writes and reads in the same process can build
// the Target once and pass the same value to both
// s3parquet.WriterConfig.Target and ReaderConfig.Target.
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

	// InsertedAtField names a time.Time field on T populated by
	// the writer (s3parquet.WriterConfig.InsertedAtField) as a real
	// parquet column. The field must carry a non-empty, non-"-"
	// parquet tag (e.g. `parquet:"inserted_at"`) — DuckDB decodes
	// the column natively via SELECT *, so no filename-routing
	// plumbing is involved on this read path.
	//
	// Empty disables the feature. Paired with the writer's
	// InsertedAtField: the value round-trips through the parquet
	// file, identical at every read path.
	InsertedAtField string

	// ConsistencyControl sets the Consistency-Control HTTP header
	// on the S3 operations this Reader controls — today that's
	// LIST of the ref stream. Data-file reads go through DuckDB's
	// opaque HTTP client and are unaffected: on weakly-consistent
	// backends (StorageGRID read-after-new-write), use the
	// s3parquet.Reader paths (Read / ReadIter / ReadIterWhere) if
	// strong-consistent GETs of parquet data are required. See
	// s3parquet.WriterConfig.ConsistencyControl for the full
	// contract.
	ConsistencyControl s3parquet.ConsistencyLevel
}

// dedupEnabled reports whether the reader should emit a dedup
// CTE for reads. Gated on EntityKeyColumns being non-empty;
// NewReader guarantees VersionColumn is also set when this is true.
func (c ReaderConfig[T]) dedupEnabled() bool {
	return len(c.EntityKeyColumns) > 0
}

// Reader is the cgo / DuckDB entry point to an s3store dataset.
// Read-only: the write path lives in s3parquet.Writer.
type Reader[T any] struct {
	cfg      ReaderConfig[T]
	db       *sql.DB
	dataPath string
	refPath  string
	binder   *binder

	// insertedAtFieldIndex is the reflect struct-field path for
	// Config.InsertedAtField, resolved once at NewReader(). nil
	// when unset — scanAll short-circuits the filename-column
	// lookup in that case, so there's no hot-path cost.
	insertedAtFieldIndex []int
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

	b, err := buildBinder(reflect.TypeFor[T]())
	if err != nil {
		return nil, fmt.Errorf("s3sql: %w", err)
	}

	insertedAtIdx, err := validateInsertedAtField[T](cfg.InsertedAtField)
	if err != nil {
		return nil, err
	}

	// The dedup CTE's filename DESC tie-breaker relies on DuckDB's
	// read_parquet(filename=true) helper, which injects a column
	// literally named "filename". A T that already maps a parquet
	// column of that name would either lose its binding (the CTE
	// would EXCLUDE it) or produce a duplicate-schema error inside
	// read_parquet. Catch the Go-side case at NewReader — the
	// on-disk-parquet-has-"filename" case is still DuckDB's to
	// reject at query time since we can't see the file schemas
	// here.
	if _, ok := b.byName["filename"]; ok && cfg.dedupEnabled() {
		return nil, fmt.Errorf(
			"s3sql: T has a `parquet:\"filename\"` field, " +
				"which collides with DuckDB's " +
				"read_parquet(filename=true) used for " +
				"the dedup tie-breaker; rename the field")
	}

	db, err := openDuckDB(cfg.Target.S3Client, cfg.ExtraInitSQL)
	if err != nil {
		return nil, fmt.Errorf(
			"s3sql: failed to open DuckDB: %w", err)
	}

	return &Reader[T]{
		cfg:                  cfg,
		db:                   db,
		dataPath:             core.DataPath(cfg.Target.Prefix),
		refPath:              core.RefPath(cfg.Target.Prefix),
		binder:               b,
		insertedAtFieldIndex: insertedAtIdx,
	}, nil
}

// validateInsertedAtField resolves Config.InsertedAtField to a
// struct-field index on T. Mirrors s3parquet.validateInsertedAtField:
// the field must exist, be time.Time, and carry a non-empty,
// non-"-" parquet tag so DuckDB's SELECT * sees a real column
// (the writer populates it as part of the parquet schema).
func validateInsertedAtField[T any](name string) ([]int, error) {
	if name == "" {
		return nil, nil
	}
	rt := reflect.TypeFor[T]()
	if rt.Kind() != reflect.Struct {
		return nil, fmt.Errorf(
			"s3sql: InsertedAtField requires T to be a struct, got %s",
			rt)
	}
	f, ok := rt.FieldByName(name)
	if !ok {
		return nil, fmt.Errorf(
			"s3sql: InsertedAtField %q: no such field on %s",
			name, rt)
	}
	if f.Type != reflect.TypeFor[time.Time]() {
		return nil, fmt.Errorf(
			"s3sql: InsertedAtField %q: must be time.Time, got %s",
			name, f.Type)
	}
	tag := f.Tag.Get("parquet")
	name0, _, _ := strings.Cut(tag, ",")
	if name0 == "" || name0 == "-" {
		return nil, fmt.Errorf(
			"s3sql: InsertedAtField %q: must carry a non-empty, "+
				"non-\"-\" parquet tag so DuckDB can decode the "+
				"writer-populated column (got %q)", name, tag)
	}
	return f.Index, nil
}

// Target returns the S3Target this Reader is bound to. Useful
// for tooling that constructs other read-only handles (indexes,
// backfill) against the same dataset without reaching back into
// the Config.
func (s *Reader[T]) Target() s3parquet.S3Target {
	return s.cfg.Target
}

// Close releases the DuckDB connection.
func (s *Reader[T]) Close() error {
	return s.db.Close()
}

// s3URI returns the s3:// URI for a key in the reader's bucket.
func (s *Reader[T]) s3URI(key string) string {
	return fmt.Sprintf("s3://%s/%s", s.cfg.Target.Bucket, key)
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
func (s *Reader[T]) buildParquetURI(
	keyPattern string,
) (string, error) {
	segs, err := core.ParseKeyPattern(
		keyPattern, s.cfg.Target.PartitionKeyParts)
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
func (s *Reader[T]) buildRangeWhere(
	keyPattern string,
) (string, error) {
	segs, err := core.ParseKeyPattern(
		keyPattern, s.cfg.Target.PartitionKeyParts)
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
