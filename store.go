package s3store

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// refSeparator splits the fixed-width "{ts}-{id}" header from
// the PathEscape'd Hive key in a ref filename. PathEscape always
// escapes ';' (as %3B), so this character cannot appear in the
// encoded key — the split on it is unambiguous even when the
// original key contains arbitrary bytes.
const refSeparator = ";"

// Store provides append-only storage on S3 with Parquet data
// files, a change stream, and embedded DuckDB for all reads.
type Store[T any] struct {
	cfg      Config[T]
	s3       *s3.Client
	db       *sql.DB
	dataPath string
	refPath  string
}

func New[T any](cfg Config[T]) (*Store[T], error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("s3store: Bucket is required")
	}
	if cfg.Prefix == "" {
		return nil, fmt.Errorf("s3store: Prefix is required")
	}
	if cfg.TableAlias == "" {
		return nil, fmt.Errorf(
			"s3store: TableAlias is required")
	}
	if cfg.S3Client == nil {
		return nil, fmt.Errorf(
			"s3store: S3Client is required")
	}
	if len(cfg.KeyParts) == 0 {
		return nil, fmt.Errorf(
			"s3store: KeyParts is required")
	}
	if err := validateKeyParts(cfg.KeyParts); err != nil {
		return nil, err
	}

	db, err := openDuckDB(cfg.S3Endpoint, cfg.ExtraInitSQL)
	if err != nil {
		return nil, fmt.Errorf(
			"s3store: failed to open DuckDB: %w", err)
	}

	return &Store[T]{
		cfg:      cfg,
		s3:       cfg.S3Client,
		db:       db,
		dataPath: cfg.Prefix + "/data",
		refPath:  cfg.Prefix + "/_stream/refs",
	}, nil
}

func (s *Store[T]) Close() error {
	return s.db.Close()
}

// validateKeyParts rejects KeyParts entries that would break
// the Hive layout or the key parser: empty strings, names
// containing '=' (the k-v separator) or '/' (the segment
// separator), and duplicate names. Called once from New().
func validateKeyParts(parts []string) error {
	seen := make(map[string]bool, len(parts))
	for i, p := range parts {
		if p == "" {
			return fmt.Errorf(
				"s3store: KeyParts[%d] is empty", i)
		}
		if strings.ContainsAny(p, "=/") {
			return fmt.Errorf(
				"s3store: KeyParts[%d] %q must not contain "+
					"'=' or '/'", i, p)
		}
		if seen[p] {
			return fmt.Errorf(
				"s3store: KeyParts[%d] %q is duplicated", i, p)
		}
		seen[p] = true
	}
	return nil
}

func (s *Store[T]) s3URI(key string) string {
	return fmt.Sprintf("s3://%s/%s", s.cfg.Bucket, key)
}

// sqlQuote returns a DuckDB single-quoted string literal. Any
// embedded apostrophe is doubled, which is the SQL standard
// escape for a single quote inside a string literal. The
// returned value includes the surrounding quotes and can be
// concatenated directly into SQL.
//
// Used everywhere we embed a user-derived value (parquet URI,
// error message) into a SQL string, so partition values that
// contain an apostrophe (e.g. customer=o'brien) can't break
// the query.
func sqlQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// buildParquetURI compiles a user-supplied key pattern into an
// S3 glob that matches exactly the intended set of parquet
// files under Config.Prefix/data.
//
// The pattern must have exactly len(KeyParts) "/"-delimited
// segments, each either
//
//   - "{KeyParts[i]}=<value-or-duckdb-glob>", or
//   - "*" (shorthand; rewritten to "{KeyParts[i]}=*")
//
// As a convenience, the literal "*" alone matches every file
// under the data prefix at any depth.
//
// Truncated patterns (fewer segments than KeyParts) and
// mislabelled segments (part name doesn't match the configured
// position) are rejected rather than silently returning zero
// rows.
func (s *Store[T]) buildParquetURI(
	keyPattern string,
) (string, error) {
	if keyPattern == "" || keyPattern == "*" {
		return s.s3URI(s.dataPath + "/**/*.parquet"), nil
	}

	segments := strings.Split(keyPattern, "/")
	if len(segments) != len(s.cfg.KeyParts) {
		return "", fmt.Errorf(
			"s3store: key pattern %q has %d segments, "+
				"expected %d (%v)",
			keyPattern, len(segments),
			len(s.cfg.KeyParts), s.cfg.KeyParts)
	}

	for i, seg := range segments {
		part := s.cfg.KeyParts[i]
		if seg == "*" {
			segments[i] = part + "=*"
			continue
		}
		if !strings.HasPrefix(seg, part+"=") {
			return "", fmt.Errorf(
				"s3store: key pattern %q segment %d is %q, "+
					"expected %q=... or %q",
				keyPattern, i, seg, part, "*")
		}
	}

	return s.s3URI(
		s.dataPath + "/" + strings.Join(segments, "/") +
			"/*.parquet"), nil
}

func (s *Store[T]) dedupColumns() []string {
	if len(s.cfg.DeduplicateBy) > 0 {
		return s.cfg.DeduplicateBy
	}
	return s.cfg.KeyParts
}

func (s *Store[T]) encodeRefKey(
	tsMicros int64, shortID string, key string,
) string {
	return fmt.Sprintf("%s/%d-%s%s%s.ref",
		s.refPath, tsMicros, shortID,
		refSeparator, url.PathEscape(key))
}

func (s *Store[T]) parseRefKey(refKey string) (
	key string, shortID string, err error,
) {
	name := refKey
	if idx := strings.LastIndex(name, "/"); idx >= 0 {
		name = name[idx+1:]
	}
	name = strings.TrimSuffix(name, ".ref")

	parts := strings.SplitN(name, refSeparator, 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf(
			"s3store: invalid ref key: %s", refKey)
	}

	tsAndID := parts[0]
	dashIdx := strings.Index(tsAndID, "-")
	if dashIdx < 0 {
		return "", "", fmt.Errorf(
			"s3store: invalid ref key: %s", refKey)
	}
	shortID = tsAndID[dashIdx+1:]

	key, err = url.PathUnescape(parts[1])
	if err != nil {
		return "", "", fmt.Errorf(
			"s3store: invalid ref key %q: %w", refKey, err)
	}
	return key, shortID, nil
}

func (s *Store[T]) buildDataPath(
	key string, shortID string,
) string {
	return fmt.Sprintf("%s/%s/%s.parquet",
		s.dataPath, key, shortID)
}

// introspectColumns returns the set of columns the given base
// scan expression produces. Used by schema-evolution transforms
// to decide whether a ColumnAlias/ColumnDefault target already
// exists in _raw (use REPLACE) or has to be materialized as a
// new column (append to the star expansion).
//
// The query uses LIMIT 0 so no data pages are read — DuckDB
// only touches parquet footers for the union-by-name schema,
// which the main query is about to do anyway.
func (s *Store[T]) introspectColumns(
	ctx context.Context, scanExpr string,
) (map[string]bool, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT * FROM ("+scanExpr+") LIMIT 0")
	if err != nil {
		return nil, fmt.Errorf(
			"s3store: introspect columns: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf(
			"s3store: introspect columns: %w", err)
	}

	set := make(map[string]bool, len(cols))
	for _, c := range cols {
		set[c] = true
	}
	return set, nil
}

// scanAll reads all rows from a DuckDB result set into typed
// records via ScanFunc.
func (s *Store[T]) scanAll(rows *sql.Rows) ([]T, error) {
	if s.cfg.ScanFunc == nil {
		return nil, fmt.Errorf(
			"s3store: ScanFunc is required")
	}
	var records []T
	for rows.Next() {
		r, err := s.cfg.ScanFunc(rows)
		if err != nil {
			return nil, fmt.Errorf(
				"s3store: scan row: %w", err)
		}
		records = append(records, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"s3store: iterate rows: %w", err)
	}
	return records, nil
}
