package s3sql

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// scanExprForPattern returns the base read_parquet scan for a
// Hive-glob pattern. Shared by Query, QueryRow, Read.
//
// The URI is SQL-quoted via sqlQuote so partition values that
// contain an apostrophe don't break the query at plan time.
func (s *Store[T]) scanExprForPattern(
	key string,
) (string, error) {
	parquetURI, err := s.buildParquetURI(key)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"SELECT * FROM read_parquet(%s, "+
			"hive_partitioning=true, union_by_name=true)",
		sqlQuote(parquetURI)), nil
}

// buildWrappedQuery does the work shared by every read path:
// introspect the base scan's columns (only when schema-evolution
// transforms are configured) and produce the final wrapped SQL.
func (s *Store[T]) buildWrappedQuery(
	ctx context.Context,
	scanExpr string,
	userSQL string,
	includeHistory bool,
) (string, error) {
	var existingCols map[string]bool
	if len(s.cfg.ColumnAliases) > 0 ||
		len(s.cfg.ColumnDefaults) > 0 {
		cols, err := s.introspectColumns(ctx, scanExpr)
		if err != nil {
			return "", err
		}
		existingCols = cols
	}
	return s.wrapScanExpr(
		scanExpr, userSQL, existingCols, includeHistory), nil
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

// wrapScanExpr wraps a base scan expression with column
// transforms, deduplication CTE, and the user's SQL query.
// Shared by Query, QueryRow, Read, PollRecords.
func (s *Store[T]) wrapScanExpr(
	scanExpr string,
	userSQL string,
	existingCols map[string]bool,
	includeHistory bool,
) string {
	replaces, additions, excludes := s.buildColumnTransforms(existingCols)
	hasTransforms := len(replaces) > 0 || len(additions) > 0 || len(excludes) > 0

	var rawCTE string
	if hasTransforms {
		rawCTE = fmt.Sprintf("_raw AS (%s),\n", scanExpr)

		starExpr := "*"
		if len(excludes) > 0 {
			starExpr += fmt.Sprintf(" EXCLUDE (%s)",
				strings.Join(excludes, ", "))
		}
		if len(replaces) > 0 {
			starExpr += fmt.Sprintf(" REPLACE (%s)",
				strings.Join(replaces, ", "))
		}
		selectList := starExpr
		if len(additions) > 0 {
			selectList = starExpr + ", " +
				strings.Join(additions, ", ")
		}

		scanExpr = fmt.Sprintf(
			"SELECT %s FROM _raw", selectList)
	}

	var sb strings.Builder
	sb.WriteString("WITH ")

	if rawCTE != "" {
		sb.WriteString(rawCTE)
	}

	if !includeHistory && s.cfg.VersionColumn != "" {
		dedupCols := strings.Join(s.cfg.dedupColumns(), ", ")
		fmt.Fprintf(&sb,
			"%s AS (\n  %s\n  QUALIFY ROW_NUMBER() OVER "+
				"(PARTITION BY %s ORDER BY %s DESC"+
				") = 1\n)\n",
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

// buildColumnTransforms splits ColumnAliases and ColumnDefaults
// into three slices: replaces (SELECT * REPLACE), additions
// (new columns appended after *), and excludes (old columns
// consumed by an alias, removed from *).
//
// Map keys are iterated in sorted order so generated SQL is
// deterministic run to run.
func (s *Store[T]) buildColumnTransforms(
	existingCols map[string]bool,
) (replaces []string, additions []string, excludes []string) {
	for _, newName := range slices.Sorted(
		maps.Keys(s.cfg.ColumnAliases),
	) {
		oldNames := s.cfg.ColumnAliases[newName]
		newExists := existingCols[newName]

		var args []string
		if newExists {
			args = append(args, newName)
		}
		for _, old := range oldNames {
			if existingCols[old] {
				args = append(args, old)
				// Consumed by the alias — drop from output.
				excludes = append(excludes, old)
			}
		}

		var expr string
		switch {
		case len(args) == 0:
			// No source columns exist — emit a typed NULL so
			// the column is present in the output schema and
			// downstream ScanFunc still finds it.
			expr = fmt.Sprintf("NULL AS %s", newName)
		default:
			expr = fmt.Sprintf("COALESCE(%s) AS %s",
				strings.Join(args, ", "), newName)
		}

		if newExists {
			replaces = append(replaces, expr)
		} else {
			additions = append(additions, expr)
		}
	}

	for _, col := range slices.Sorted(
		maps.Keys(s.cfg.ColumnDefaults),
	) {
		if _, isAlias := s.cfg.ColumnAliases[col]; isAlias {
			continue
		}
		defaultVal := s.cfg.ColumnDefaults[col]

		if !existingCols[col] {
			additions = append(additions, fmt.Sprintf(
				"%s AS %s", defaultVal, col))
			continue
		}
		replaces = append(replaces, fmt.Sprintf(
			"COALESCE(%s, %s) AS %s",
			col, defaultVal, col))
	}

	// Dedupe excludes: two or more aliases can reference the
	// same old column ({amount: [value], cost: [value]}), which
	// would otherwise emit SELECT * EXCLUDE (value, value) and
	// fail at plan time. Preserves first-seen order.
	if len(excludes) > 1 {
		seen := make(map[string]bool, len(excludes))
		deduped := make([]string, 0, len(excludes))
		for _, col := range excludes {
			if !seen[col] {
				seen[col] = true
				deduped = append(deduped, col)
			}
		}
		excludes = deduped
	}

	return replaces, additions, excludes
}

// introspectColumns returns the set of columns the given base
// scan expression produces. Driven by schema-evolution logic
// (only runs when ColumnAliases or ColumnDefaults are
// configured). Uses LIMIT 0 so only parquet footers are read.
func (s *Store[T]) introspectColumns(
	ctx context.Context, scanExpr string,
) (map[string]bool, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT * FROM ("+scanExpr+") LIMIT 0")
	if err != nil {
		return nil, fmt.Errorf(
			"s3sql: introspect columns: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf(
			"s3sql: introspect columns: %w", err)
	}

	set := make(map[string]bool, len(cols))
	for _, c := range cols {
		set[c] = true
	}
	return set, nil
}

// scanAll reads all rows from a DuckDB result set into typed
// records via the configured ScanFunc.
func (s *Store[T]) scanAll(rows *sql.Rows) ([]T, error) {
	if s.cfg.ScanFunc == nil {
		return nil, fmt.Errorf(
			"s3sql: ScanFunc is required")
	}
	var records []T
	for rows.Next() {
		r, err := s.cfg.ScanFunc(rows)
		if err != nil {
			return nil, fmt.Errorf(
				"s3sql: scan row: %w", err)
		}
		records = append(records, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"s3sql: iterate rows: %w", err)
	}
	return records, nil
}
