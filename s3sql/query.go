package s3sql

import (
	"context"
	"database/sql"

	"github.com/ueisele/s3store/internal/core"
)

// Query executes a SQL query scoped to files matching the given
// key pattern. Glob grammar follows core.ValidateKeyPattern.
// Deduplicated by VersionColumn + EntityKeyColumns when both
// are configured; pass WithHistory() to opt out.
func (s *Store[T]) Query(
	ctx context.Context,
	keyPattern string,
	sqlQuery string,
	opts ...QueryOption,
) (*sql.Rows, error) {
	var o core.QueryOpts
	o.Apply(opts...)

	scanExpr, err := s.scanExprForPattern(keyPattern)
	if err != nil {
		return nil, err
	}
	return s.db.QueryContext(ctx,
		s.wrapScanExpr(scanExpr, sqlQuery, o.IncludeHistory))
}

// QueryRow executes a query returning at most one row. Any
// construction-time error (pattern validation) is surfaced
// through the returned *sql.Row when Scan is called, matching
// database/sql conventions.
func (s *Store[T]) QueryRow(
	ctx context.Context,
	keyPattern string,
	sqlQuery string,
	opts ...QueryOption,
) *sql.Row {
	var o core.QueryOpts
	o.Apply(opts...)

	scanExpr, err := s.scanExprForPattern(keyPattern)
	if err != nil {
		return s.errorRow(ctx, err)
	}
	return s.db.QueryRowContext(ctx,
		s.wrapScanExpr(scanExpr, sqlQuery, o.IncludeHistory))
}
