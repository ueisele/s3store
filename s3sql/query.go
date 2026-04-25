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
//
// Does not normalize "zero files match" to an empty result —
// returns an isNoFilesMatchedError-compatible error so callers
// can branch on it. Use QueryMany if you want empty treated as
// a successful zero-row iteration.
//
// Execution model: a Go-side S3 LIST resolves the matching file
// set, the (optional) WithIdempotentRead barrier filters it, and
// the deduplicated URI list is handed to read_parquet([…]). The
// LIST carries ConsistencyControl, so reads are read-after-write
// on strong-consistent backends. See QueryMany for the full
// rationale on why DuckDB's glob-expansion fast path is not used
// (httpfs cannot carry per-request consistency headers).
func (s *Reader[T]) Query(
	ctx context.Context,
	keyPattern string,
	sqlQuery string,
	opts ...QueryOption,
) (*sql.Rows, error) {
	var o core.QueryOpts
	o.Apply(opts...)

	uris, err := s.listAllMatchingURIs(
		ctx, []string{keyPattern}, &o, "Query")
	if err != nil {
		return nil, err
	}
	if len(uris) == 0 {
		return nil, noFilesMatchedErr(keyPattern)
	}
	scanExpr := s.scanExprForURIs(uris, s.needsFilename())
	return s.db.QueryContext(ctx,
		s.wrapScanExpr(scanExpr, sqlQuery, o.IncludeHistory))
}

// QueryMany runs a single SQL query over the deduplicated union
// of files matching every pattern. Unlike calling Query N
// times, this runs ONE DuckDB query so aggregations, joins, and
// ORDER BY apply across the full set.
//
// Execution model: per-pattern Go-side S3 LIST with bounded
// concurrency, deduplicated union of exact file URIs, then one
// read_parquet([…]) scan in DuckDB. The Go-side LIST carries
// ConsistencyControl on every page so the file set linearizes
// with concurrent writes on strong-consistent backends.
//
// Empty patterns slice → empty *sql.Rows. Zero file matches →
// empty *sql.Rows. The synthetic empty cursor carries a single
// NULL column; callers iterating via the standard
// for-rows.Next-rows.Scan loop see a clean empty iteration.
func (s *Reader[T]) QueryMany(
	ctx context.Context,
	patterns []string,
	sqlQuery string,
	opts ...QueryOption,
) (*sql.Rows, error) {
	patterns = core.DedupePatterns(patterns)
	if len(patterns) == 0 {
		return s.emptyRows(ctx)
	}

	var o core.QueryOpts
	o.Apply(opts...)

	uris, err := s.listAllMatchingURIs(ctx, patterns, &o, "QueryMany")
	if err != nil {
		return nil, err
	}
	if len(uris) == 0 {
		return s.emptyRows(ctx)
	}

	scanExpr := s.scanExprForURIs(
		uris, s.needsFilename())
	return s.db.QueryContext(ctx,
		s.wrapScanExpr(scanExpr, sqlQuery, o.IncludeHistory))
}
