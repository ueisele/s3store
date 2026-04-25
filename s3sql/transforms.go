package s3sql

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

// scanExprForURIs builds the read_parquet scan over a pre-resolved,
// deduplicated list of exact file URIs (from listAllMatchingURIs).
// All read paths funnel through here — the Go-side LIST owns
// pattern expansion and range-bound enforcement, so DuckDB sees
// only an explicit URI list without partition-pruning hints.
//
// withFilename adds filename=true to read_parquet so the source
// object key is exposed as a `filename` column — used by the
// dedup CTE as a tie-breaker on equal VersionColumn values.
// Callers typically compute the flag via needsFilename().
func (s *Reader[T]) scanExprForURIs(
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
// two read_parquet builders (scanExprForURIs, PollRecords) share
// the exact option spelling.
func filenameOpt(withFilename bool) string {
	if withFilename {
		return ", filename=true"
	}
	return ""
}

// needsFilename reports whether read_parquet must be invoked with
// filename=true for this query. The dedup CTE uses `filename` as
// a tie-breaker — both for the latest-per-entity path (tie-break
// on equal VersionColumn) and the history-with-replica-dedup path
// (tie-break within one (entity, version) group so the same row
// wins on every re-read). Needed whenever dedup is enabled,
// regardless of WithHistory. The scan-expr builders and
// wrapScanExpr both consult this so the scan and the CTE agree on
// whether filename is present.
func (s *Reader[T]) needsFilename() bool {
	return s.cfg.dedupEnabled()
}

// errorRow returns a *sql.Row that will fail on Scan with the
// given error. The delivery mechanism is a DuckDB `error()`
// call that raises at execution time, so the error surfaces
// through the standard database/sql Row API without requiring
// a signature change on QueryRow.
func (s *Reader[T]) errorRow(
	ctx context.Context, err error,
) *sql.Row {
	return s.db.QueryRowContext(ctx,
		"SELECT error("+sqlQuote(err.Error())+")")
}

// wrapScanExpr wraps a base scan expression with an optional
// dedup CTE and the user's SQL query. Shared by Query, QueryRow,
// Read, PollRecords.
//
// Three branches, driven by (dedupEnabled, includeHistory):
//
//   - dedupEnabled && !includeHistory: latest-per-entity CTE
//     picks the row with the highest VersionColumn per entity,
//     tie-broken by filename DESC. This path implicitly collapses
//     replicas (same entity, same version) because the tie-break
//     picks exactly one row per (entity, version, filename) group.
//   - dedupEnabled && includeHistory: replica-dedup-only CTE
//     partitions on (entity, version) and picks one row per group
//     by filename DESC. Distinct versions of each entity flow
//     through; byte-identical replicas from retries / zombies
//     collapse to one.
//   - !dedupEnabled: CTE is just the scan; replica collapse
//     requires EntityKeyColumns + VersionColumn and is skipped
//     when neither is configured.
//
// All dedup branches EXCLUDE the filename helper column so the
// helper doesn't leak into user SQL.
//
// Dedup requires scanExpr to have been built with filename=true;
// the caller computes that flag via needsFilename, keeping the
// two sides in sync.
func (s *Reader[T]) wrapScanExpr(
	scanExpr string,
	userSQL string,
	includeHistory bool,
) string {
	var sb strings.Builder
	sb.WriteString("WITH ")
	switch {
	case !s.cfg.dedupEnabled():
		fmt.Fprintf(&sb,
			"%s AS (\n  %s\n)\n",
			s.cfg.TableAlias, scanExpr)
	case includeHistory:
		entityCols := strings.Join(s.cfg.EntityKeyColumns, ", ")
		fmt.Fprintf(&sb,
			"%s AS (\n"+
				"  SELECT * EXCLUDE (filename) FROM (\n"+
				"    %s\n"+
				"    QUALIFY ROW_NUMBER() OVER "+
				"(PARTITION BY %s, %s ORDER BY filename DESC"+
				") = 1\n"+
				"  )\n"+
				")\n",
			s.cfg.TableAlias, scanExpr,
			entityCols, s.cfg.VersionColumn)
	default:
		entityCols := strings.Join(s.cfg.EntityKeyColumns, ", ")
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
			entityCols, s.cfg.VersionColumn)
	}
	sb.WriteString(userSQL)
	return sb.String()
}

// rowBinder captures the per-result-set state (column ordering,
// per-column binders) so scanAll and the iter readers share the
// per-row decode loop without recomputing rows.Columns() and the
// binder lookup on every row.
type rowBinder[T any] struct {
	cols []string
	fbs  []*fieldBinder
}

// newRowBinder resolves rows.Columns() once and returns the
// per-result-set binder. Mirrors scanAll's old setup block;
// kept here so the iter path doesn't duplicate it.
func (s *Reader[T]) newRowBinder(rows *sql.Rows) (*rowBinder[T], error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("s3sql: get columns: %w", err)
	}
	fbs := make([]*fieldBinder, len(cols))
	for i, c := range cols {
		fbs[i] = s.binder.byName[c]
	}
	return &rowBinder[T]{
		cols: cols,
		fbs:  fbs,
	}, nil
}

// bindNext consumes one row from rows (caller must have already
// confirmed rows.Next() returned true) and writes the decoded
// record into *out. Reuses the pre-resolved binders; allocates
// only the per-row dests slice.
func (b *rowBinder[T]) bindNext(rows *sql.Rows, out *T) error {
	dests := make([]any, len(b.cols))
	for i, fb := range b.fbs {
		if fb == nil {
			dests[i] = new(any)
			continue
		}
		dests[i] = fb.makeDest()
	}
	if err := rows.Scan(dests...); err != nil {
		return fmt.Errorf("s3sql: scan row: %w", err)
	}
	rv := reflect.ValueOf(out).Elem()
	for i, fb := range b.fbs {
		if fb == nil {
			continue
		}
		fb.assign(rv.FieldByIndex(fb.fieldIndex), dests[i])
	}
	return nil
}

// scanAll reads every row from a DuckDB result set into a []T
// using the pre-built binder. Column order is taken from the
// result set (rows.Columns()) so struct field order in T is
// independent of the parquet file's column order. Any unmapped
// column (including read_parquet's `filename` helper when it
// leaks through a non-dedup path) still gets a discard dest.
func (s *Reader[T]) scanAll(rows *sql.Rows) ([]T, error) {
	rb, err := s.newRowBinder(rows)
	if err != nil {
		return nil, err
	}
	var records []T
	for rows.Next() {
		var rec T
		if err := rb.bindNext(rows, &rec); err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("s3sql: iterate rows: %w", err)
	}
	return records, nil
}
