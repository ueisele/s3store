package s3sql

import (
	"context"
	"database/sql"
	"fmt"
)

// noFilesErrFragment matches the wording DuckDB uses for a glob
// that resolves to zero files:
//
//	IO Error: No files found that match the pattern "..."
//
// Query synthesizes errors carrying this fragment when the
// Go-side LIST returns zero matches, so callers that branch on
// the message fragment keep working regardless of whether the
// LIST was performed by DuckDB (legacy fast path) or by us
// (current behaviour).
const noFilesErrFragment = "No files found that match the pattern"

// noFilesMatchedErr synthesizes an error carrying noFilesErrFragment
// for the zero-match path, so callers that already pattern-match
// the DuckDB-style "No files found" string keep working.
func noFilesMatchedErr(pattern string) error {
	return fmt.Errorf(
		"IO Error: %s %q", noFilesErrFragment, pattern)
}

// emptyRows returns a *sql.Rows that yields zero rows. Used by
// QueryMany when Go-side LIST proves no files match any pattern.
// Callers using the standard for-rows.Next loop see a clean empty
// iteration; callers that inspect rows.Columns() see a single
// synthetic NULL column.
func (s *Reader[T]) emptyRows(ctx context.Context) (*sql.Rows, error) {
	return s.db.QueryContext(ctx, "SELECT NULL WHERE 1=0")
}
