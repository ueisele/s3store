package s3sql

import (
	"context"
	"database/sql"
	"fmt"
	"path"
	"reflect"
	"strings"
	"time"

	"github.com/ueisele/s3store/internal/core"
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
// object key is exposed as a `filename` column. Two consumers
// use it — the dedup CTE's tie-breaker on equal VersionColumn
// values, and the InsertedAtField populate path that parses
// tsMicros out of the filename on scan. Callers typically
// compute the flag via needsFilename().
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

// scanExprForURIs is the multi-pattern sibling of
// scanExprForPattern: given a pre-computed, deduplicated list of
// exact file URIs (from listAllMatchingURIs), it emits the same
// read_parquet(...) shape but with the URI list in place of a
// glob and no range-WHERE clause (row-level filtering already
// happened in Go via plan.Match).
//
// Used by the multi-pattern path of ReadMany / QueryMany /
// QueryRowMany. The single-pattern path still goes through
// scanExprForPattern so DuckDB keeps its plan-time partition
// pruning and the glob-expansion round-trip.
func (s *Store[T]) scanExprForURIs(
	uris []string, withFilename bool,
) string {
	quoted := make([]string, len(uris))
	for i, u := range uris {
		quoted[i] = sqlQuote(u)
	}
	list := "[" + strings.Join(quoted, ", ") + "]"
	return fmt.Sprintf(
		"SELECT * FROM read_parquet(%s, "+
			"hive_partitioning=true, hive_types_autocast=false, "+
			"union_by_name=true%s)",
		list, filenameOpt(withFilename))
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

// needsFilename reports whether read_parquet must be invoked with
// filename=true for this query. Two consumers need the column:
// the dedup CTE (tie-breaker on equal VersionColumn values) and
// the InsertedAtField populate path (source tsMicros). The
// scan-expr builders and wrapScanExpr both consult this so the
// scan / CTE / post-scan binder agree on whether filename is
// present.
func (s *Store[T]) needsFilename(includeHistory bool) bool {
	return (!includeHistory && s.cfg.dedupEnabled()) ||
		s.cfg.InsertedAtField != ""
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
// Two knobs interact here, set by the caller through scanExpr
// and the Config:
//
//   - Dedup CTE (applied when !includeHistory && dedupEnabled).
//     Uses filename DESC as the tie-breaker on equal
//     VersionColumn so a retry with the same domain timestamp
//     has a deterministic winner.
//   - InsertedAtField populate path. When configured, the CTE
//     keeps the `filename` column in its output so scanAll can
//     parse tsMicros from it and populate the user's struct
//     field. Without it, the dedup-only case EXCLUDEs `filename`
//     so nothing leaks into user SQL.
//
// Either knob requires scanExpr to have been built with
// filename=true; the caller computes that flag from the same
// condition this function uses, keeping the two sides in sync.
func (s *Store[T]) wrapScanExpr(
	scanExpr string,
	userSQL string,
	includeHistory bool,
) string {
	dedup := !includeHistory && s.cfg.dedupEnabled()
	keepFilename := s.cfg.InsertedAtField != ""

	var sb strings.Builder
	sb.WriteString("WITH ")
	switch {
	case dedup && keepFilename:
		dedupCols := strings.Join(s.cfg.EntityKeyColumns, ", ")
		fmt.Fprintf(&sb,
			"%s AS (\n"+
				"  %s\n"+
				"  QUALIFY ROW_NUMBER() OVER "+
				"(PARTITION BY %s ORDER BY %s DESC, filename DESC"+
				") = 1\n"+
				")\n",
			s.cfg.TableAlias, scanExpr,
			dedupCols, s.cfg.VersionColumn)
	case dedup && !keepFilename:
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
	default:
		// Either no dedup at all, or dedup-off with
		// InsertedAtField set. Either way the CTE is just the
		// scan; filename (if present) flows through.
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
//
// When Config.InsertedAtField is set AND the result set carries
// a `filename` column (DuckDB's read_parquet(filename=true)
// helper), scanAll routes filename into a string destination,
// parses the source file's tsMicros out of its last path
// segment, and writes the resulting time.Time into the user's
// InsertedAtField. Any other unmapped column (including filename
// when InsertedAtField is unset) still gets a discard dest.
func (s *Store[T]) scanAll(rows *sql.Rows) ([]T, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf(
			"s3sql: get columns: %w", err)
	}

	// Pre-resolve per-column binders once; nil means the column
	// isn't mapped to a T field and gets a discard destination.
	fbs := make([]*fieldBinder, len(cols))
	filenameCol := -1
	for i, c := range cols {
		if c == "filename" && s.insertedAtFieldIndex != nil {
			filenameCol = i
			continue
		}
		fbs[i] = s.binder.byName[c]
	}

	var records []T
	for rows.Next() {
		dests := make([]any, len(cols))
		var filenameDest string
		for i, fb := range fbs {
			if i == filenameCol {
				dests[i] = &filenameDest
				continue
			}
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
		if filenameCol >= 0 {
			tsMicros, _, err := core.ParseDataFileName(
				path.Base(filenameDest))
			if err != nil {
				return nil, fmt.Errorf(
					"s3sql: parse filename %q for "+
						"InsertedAtField: %w",
					filenameDest, err)
			}
			rv.FieldByIndex(s.insertedAtFieldIndex).Set(
				reflect.ValueOf(time.UnixMicro(tsMicros)))
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"s3sql: iterate rows: %w", err)
	}
	return records, nil
}
