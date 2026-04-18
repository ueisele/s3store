package s3sql

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

// scanExprForPattern returns the base read_parquet scan for a
// Hive-glob pattern. Shared by Query, QueryRow, Read.
//
// Range segments (FROM..TO) in the pattern become a common-prefix
// S3 glob plus a WHERE clause on the corresponding hive partition
// column, so DuckDB can prune at file-selection time.
//
// The URI is SQL-quoted via sqlQuote so partition values that
// contain an apostrophe don't break the query at plan time.
//
// withFilename adds filename=true to read_parquet so the source
// object key is exposed as a `filename` column. Used only when
// the dedup CTE needs a deterministic tie-breaker (see
// wrapScanExpr); callers compute the flag as
// !includeHistory && dedupEnabled().
func (s *Store[T]) scanExprForPattern(
	key string, withFilename bool,
) (string, error) {
	parquetURI, err := s.buildParquetURI(key)
	if err != nil {
		return "", err
	}
	where, err := s.buildRangeWhere(key)
	if err != nil {
		return "", err
	}
	scan := fmt.Sprintf(
		"SELECT * FROM read_parquet(%s, "+
			"hive_partitioning=true, hive_types_autocast=false, "+
			"union_by_name=true%s)",
		sqlQuote(parquetURI), filenameOpt(withFilename))
	if where != "" {
		scan += " WHERE " + where
	}
	return scan, nil
}

// filenameOpt returns the trailing ", filename=true" fragment if
// withFilename is set; empty string otherwise. Pulled out so the
// two read_parquet builders (scanExprForPattern, PollRecords)
// share the exact option spelling.
func filenameOpt(withFilename bool) string {
	if withFilename {
		return ", filename=true"
	}
	return ""
}

// errorRow returns a *sql.Row that will fail on Scan with the
// given error. The delivery mechanism is a DuckDB `error()`
// call that raises at execution time, so the error surfaces
// through the standard database/sql Row API without requiring
// a signature change on QueryRow.
func (s *Store[T]) errorRow(
	ctx context.Context, err error,
) *sql.Row {
	return s.db.QueryRowContext(ctx,
		"SELECT error("+sqlQuote(err.Error())+")")
}

// wrapScanExpr wraps a base scan expression with an optional
// dedup CTE and the user's SQL query. Shared by Query, QueryRow,
// Read, PollRecords.
//
// When dedup applies, the CTE resolves version ties with
// filename DESC (lexicographically later S3 key wins) so two
// writes at the same VersionColumn value produce a deterministic
// winner across re-runs. That's only possible when the scan
// exposes a `filename` column, which happens when callers pass
// scanExpr built with withFilename=true — same condition this
// function uses to decide whether to dedup, so the wiring is
// symmetric. The helper EXCLUDEs the filename column so it
// doesn't leak into user SQL.
func (s *Store[T]) wrapScanExpr(
	scanExpr string,
	userSQL string,
	includeHistory bool,
) string {
	var sb strings.Builder
	sb.WriteString("WITH ")
	if !includeHistory && s.cfg.dedupEnabled() {
		dedupCols := strings.Join(s.cfg.EntityKeyColumns, ", ")
		fmt.Fprintf(&sb,
			"%s AS (\n"+
				"  SELECT * EXCLUDE (filename) FROM (\n"+
				"    %s\n"+
				"    QUALIFY ROW_NUMBER() OVER "+
				"(PARTITION BY %s ORDER BY %s DESC, filename DESC"+
				") = 1\n"+
				"  )\n"+
				")\n",
			s.cfg.TableAlias, scanExpr,
			dedupCols, s.cfg.VersionColumn)
	} else {
		fmt.Fprintf(&sb,
			"%s AS (\n  %s\n)\n",
			s.cfg.TableAlias, scanExpr)
	}
	sb.WriteString(userSQL)
	return sb.String()
}

// scanAll reads every row from a DuckDB result set into a []T
// using the pre-built binder. Column order is taken from the
// result set (rows.Columns()) so struct field order in T is
// independent of the parquet file's column order.
func (s *Store[T]) scanAll(rows *sql.Rows) ([]T, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf(
			"s3sql: get columns: %w", err)
	}

	// Pre-resolve per-column binders once; nil means the column
	// isn't mapped to a T field and gets a discard destination.
	fbs := make([]*fieldBinder, len(cols))
	for i, c := range cols {
		fbs[i] = s.binder.byName[c]
	}

	var records []T
	for rows.Next() {
		dests := make([]any, len(cols))
		for i, fb := range fbs {
			if fb == nil {
				dests[i] = new(any)
				continue
			}
			dests[i] = fb.makeDest()
		}
		if err := rows.Scan(dests...); err != nil {
			return nil, fmt.Errorf(
				"s3sql: scan row: %w", err)
		}

		var rec T
		rv := reflect.ValueOf(&rec).Elem()
		for i, fb := range fbs {
			if fb == nil {
				continue
			}
			fb.assign(rv.FieldByIndex(fb.fieldIndex), dests[i])
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"s3sql: iterate rows: %w", err)
	}
	return records, nil
}
