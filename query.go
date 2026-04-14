package s3store

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// Query executes a SQL query scoped to files matching the given
// key pattern. Supports arbitrary globs via DuckDB.
// Deduplicated by default. Use WithHistory() for all versions.
func (s *Store[T]) Query(
	ctx context.Context,
	key string,
	sqlQuery string,
	opts ...QueryOption,
) (*sql.Rows, error) {
	o := &queryOpts{}
	for _, opt := range opts {
		opt(o)
	}

	scanExpr, err := s.scanExprForPattern(key)
	if err != nil {
		return nil, err
	}
	wrapped, err := s.buildWrappedQuery(
		ctx, scanExpr, sqlQuery, o.includeHistory)
	if err != nil {
		return nil, err
	}
	return s.db.QueryContext(ctx, wrapped)
}

// QueryRow executes a query returning at most one row.
// Any construction-time error (pattern validation, column
// introspection failure) is surfaced through the returned
// *sql.Row when Scan is called, matching database/sql
// conventions.
func (s *Store[T]) QueryRow(
	ctx context.Context,
	key string,
	sqlQuery string,
	opts ...QueryOption,
) *sql.Row {
	o := &queryOpts{}
	for _, opt := range opts {
		opt(o)
	}

	scanExpr, err := s.scanExprForPattern(key)
	if err != nil {
		return s.errorRow(ctx, err)
	}
	wrapped, err := s.buildWrappedQuery(
		ctx, scanExpr, sqlQuery, o.includeHistory)
	if err != nil {
		return s.errorRow(ctx, err)
	}
	return s.db.QueryRowContext(ctx, wrapped)
}

// scanExprForPattern returns the base read_parquet scan for a
// Hive-glob pattern. Shared by Query, QueryRow, and Read.
func (s *Store[T]) scanExprForPattern(
	key string,
) (string, error) {
	parquetURI, err := s.buildParquetURI(key)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"SELECT * FROM read_parquet('%s', "+
			"hive_partitioning=true, union_by_name=true)",
		parquetURI), nil
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
	msg := strings.ReplaceAll(err.Error(), "'", "''")
	return s.db.QueryRowContext(ctx,
		"SELECT error('"+msg+"')")
}

// wrapScanExpr wraps a base scan expression with column
// transforms, deduplication CTE, and the user's SQL query.
// Shared by Query, QueryRow, Read, and PollRecords.
//
// existingCols is the set of columns the base scanExpr actually
// produces (or nil when no schema-evolution transforms are
// configured and introspection was skipped). It drives the
// split between REPLACE clauses (for columns already in _raw)
// and appended SELECT expressions (for columns that don't exist
// yet and have to be materialized from olds, a default literal,
// or NULL).
func (s *Store[T]) wrapScanExpr(
	scanExpr string,
	userSQL string,
	existingCols map[string]bool,
	includeHistory bool,
) string {
	replaces, additions := s.buildColumnTransforms(existingCols)
	hasTransforms := len(replaces) > 0 || len(additions) > 0

	var rawCTE string
	if hasTransforms {
		rawCTE = fmt.Sprintf("_raw AS (%s),\n", scanExpr)

		starExpr := "*"
		if len(replaces) > 0 {
			starExpr = fmt.Sprintf("* REPLACE (%s)",
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
		dedupCols := strings.Join(s.dedupColumns(), ", ")
		sb.WriteString(fmt.Sprintf(
			"%s AS (\n  %s\n  QUALIFY ROW_NUMBER() OVER "+
				"(PARTITION BY %s ORDER BY %s DESC"+
				") = 1\n)\n",
			s.cfg.TableAlias, scanExpr,
			dedupCols, s.cfg.VersionColumn))
	} else {
		sb.WriteString(fmt.Sprintf(
			"%s AS (\n  %s\n)\n",
			s.cfg.TableAlias, scanExpr))
	}

	sb.WriteString(userSQL)
	return sb.String()
}

// buildColumnTransforms splits ColumnAliases and ColumnDefaults
// into two slices:
//
//   - replaces: expressions for columns that already exist in
//     the base scan; emitted inside SELECT * REPLACE (...)
//   - additions: expressions for columns that do not exist in
//     the base scan yet; appended after the star expansion
//
// existingCols is the set of columns present in _raw. When nil
// (no transforms are configured), both returned slices are nil.
//
// Map keys are iterated in sorted order so generated SQL is
// deterministic run to run — important for plan-cache hit
// rates, diffable logs, and snapshot-style tests.
func (s *Store[T]) buildColumnTransforms(
	existingCols map[string]bool,
) (replaces []string, additions []string) {
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
			}
		}

		var expr string
		switch {
		case len(args) == 0:
			// No source columns exist — emit a typed NULL so
			// the column is present in the output schema and
			// downstream ScanFunc still finds it. Consumers
			// with non-nullable receivers will error visibly.
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
			// Column never appeared in any file — materialize
			// it as a constant-valued column so the output
			// schema always has it.
			additions = append(additions, fmt.Sprintf(
				"%s AS %s", defaultVal, col))
			continue
		}
		replaces = append(replaces, fmt.Sprintf(
			"COALESCE(%s, %s) AS %s",
			col, defaultVal, col))
	}

	return replaces, additions
}
