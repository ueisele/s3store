package s3sql

import (
	"context"
	"database/sql"

	"github.com/ueisele/s3store/internal/core"
)

// Query executes a SQL query scoped to files matching the given
// key pattern. Glob grammar follows core.ValidateKeyPattern.
// Deduplicated by VersionColumn+DeduplicateBy by default; pass
// WithHistory() for all versions.
func (s *Store[T]) Query(
	ctx context.Context,
	keyPattern string,
	sqlQuery string,
	opts ...core.QueryOption,
) (*sql.Rows, error) {
	var o core.QueryOpts
	o.Apply(opts...)

	scanExpr, err := s.scanExprForPattern(keyPattern)
	if err != nil {
		return nil, err
	}
	wrapped, err := s.buildWrappedQuery(
		ctx, scanExpr, sqlQuery, o.IncludeHistory)
	if err != nil {
		return nil, err
	}
	return s.db.QueryContext(ctx, wrapped)
}

// QueryRow executes a query returning at most one row. Any
// construction-time error (pattern validation, column
// introspection failure) is surfaced through the returned
// *sql.Row when Scan is called, matching database/sql
// conventions.
func (s *Store[T]) QueryRow(
	ctx context.Context,
	keyPattern string,
	sqlQuery string,
	opts ...core.QueryOption,
) *sql.Row {
	var o core.QueryOpts
	o.Apply(opts...)

	scanExpr, err := s.scanExprForPattern(keyPattern)
	if err != nil {
		return s.errorRow(ctx, err)
	}
	wrapped, err := s.buildWrappedQuery(
		ctx, scanExpr, sqlQuery, o.IncludeHistory)
	if err != nil {
		return s.errorRow(ctx, err)
	}
	return s.db.QueryRowContext(ctx, wrapped)
}
