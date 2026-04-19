package s3sql

import (
	"context"
	"database/sql"
	"strings"
)

// noFilesErrFragment is the DuckDB error text for a glob pattern
// that matches zero files:
//
//	IO Error: No files found that match the pattern "..."
//
// Used by the Read / ReadMany / QueryMany / QueryRowMany
// convenience wrappers to normalize "zero matches" to an empty
// result instead of an error. The raw Query / QueryRow pair
// propagates the DuckDB error unchanged for callers that opt
// into database/sql semantics directly.
const noFilesErrFragment = "No files found that match the pattern"

// isNoFilesMatchedError reports whether err is DuckDB's error
// for a glob pattern that matched zero files. Matched by text
// fragment (duckdb-go wraps the native error without a stable
// sentinel); the fragment is specific enough that a false
// positive on another error is effectively impossible.
func isNoFilesMatchedError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), noFilesErrFragment)
}

// emptyRows returns a *sql.Rows that yields zero rows. Used by
// QueryMany when Go-side LIST proves no files match any pattern
// (or when the single-pattern delegate's DuckDB call errors
// with "No files found"). Callers using the standard
// for-rows.Next loop see a clean empty iteration; callers that
// inspect rows.Columns() see a single synthetic NULL column —
// document this if anyone ever leans on the column list.
func (s *Reader[T]) emptyRows(ctx context.Context) (*sql.Rows, error) {
	return s.db.QueryContext(ctx, "SELECT NULL WHERE 1=0")
}

// emptyRow returns a *sql.Row whose Scan returns sql.ErrNoRows,
// matching database/sql's standard "no rows" contract.
func (s *Reader[T]) emptyRow(ctx context.Context) *sql.Row {
	return s.db.QueryRowContext(ctx, "SELECT NULL WHERE 1=0")
}
