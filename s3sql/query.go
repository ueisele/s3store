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
// DuckDB's "No files found" error propagates. Use QueryMany (or
// check isNoFilesMatchedError in caller code) if you want empty
// treated as a successful zero-row iteration.
//
// Execution model:
//
//   - WithIdempotentRead unset (default): glob URI handed to
//     DuckDB; plan-time partition pruning and glob expansion
//     happen server-side. No Go-side S3 LIST.
//   - WithIdempotentRead set: Go-side S3 LIST runs first, the
//     per-partition barrier filter is applied, and the
//     deduplicated URI list is passed to read_parquet([...]). A
//     barrier that filters out every match surfaces an
//     isNoFilesMatchedError-compatible error so callers checking
//     that helper keep working.
func (s *Reader[T]) Query(
	ctx context.Context,
	keyPattern string,
	sqlQuery string,
	opts ...QueryOption,
) (*sql.Rows, error) {
	var o core.QueryOpts
	o.Apply(opts...)

	if o.IdempotentReadToken != "" {
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

	scanExpr, err := s.scanExprForPattern(
		keyPattern, s.needsFilename())
	if err != nil {
		return nil, err
	}
	return s.db.QueryContext(ctx,
		s.wrapScanExpr(scanExpr, sqlQuery, o.IncludeHistory))
}

// QueryMany runs a single SQL query over the deduplicated union
// of files matching every pattern. Unlike calling Query N
// times, this runs ONE DuckDB query so aggregations, joins, and
// ORDER BY apply across the full set.
//
// Execution model:
//
//   - Single-pattern fast path (len(patterns) == 1 after
//     literal dedup, WithIdempotentRead unset): delegates to
//     Query — DuckDB handles glob expansion and partition
//     pruning server-side. "No files found" is normalized to an
//     empty *sql.Rows.
//   - Multi-pattern path (or any pattern count with
//     WithIdempotentRead set): per-pattern S3 LIST in Go with a
//     bounded concurrency cap, deduplicated union of exact file
//     URIs → one read_parquet([...]) scan. DuckDB plans once
//     over the full file set. The barrier filter (if set)
//     applies to the KeyMeta list before URI emission.
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

	// Same barrier-forces-LIST rule as ReadMany (see that method).
	if len(patterns) == 1 && o.IdempotentReadToken == "" {
		rows, err := s.Query(ctx, patterns[0], sqlQuery, opts...)
		if err != nil {
			if isNoFilesMatchedError(err) {
				return s.emptyRows(ctx)
			}
			return nil, err
		}
		return rows, nil
	}

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

// QueryRow executes a query returning at most one row. Any
// construction-time error (pattern validation) is surfaced
// through the returned *sql.Row when Scan is called, matching
// database/sql conventions.
//
// Does not normalize "zero files match" — DuckDB's error
// propagates via Scan. Use QueryRowMany if you want the
// standard sql.ErrNoRows contract on empty matches.
//
// Execution model mirrors Query: without WithIdempotentRead the
// glob URI goes to DuckDB; with it, a Go-side S3 LIST drives the
// filter and an isNoFilesMatchedError-compatible error is
// surfaced via Scan if the barrier leaves zero matches.
func (s *Reader[T]) QueryRow(
	ctx context.Context,
	keyPattern string,
	sqlQuery string,
	opts ...QueryOption,
) *sql.Row {
	var o core.QueryOpts
	o.Apply(opts...)

	if o.IdempotentReadToken != "" {
		uris, err := s.listAllMatchingURIs(
			ctx, []string{keyPattern}, &o, "QueryRow")
		if err != nil {
			return s.errorRow(ctx, err)
		}
		if len(uris) == 0 {
			return s.errorRow(ctx, noFilesMatchedErr(keyPattern))
		}
		scanExpr := s.scanExprForURIs(uris, s.needsFilename())
		return s.db.QueryRowContext(ctx,
			s.wrapScanExpr(scanExpr, sqlQuery, o.IncludeHistory))
	}

	scanExpr, err := s.scanExprForPattern(
		keyPattern, s.needsFilename())
	if err != nil {
		return s.errorRow(ctx, err)
	}
	return s.db.QueryRowContext(ctx,
		s.wrapScanExpr(scanExpr, sqlQuery, o.IncludeHistory))
}

// QueryRowMany is QueryMany's single-row sibling. Empty
// patterns / zero file matches produce a *sql.Row whose Scan
// returns sql.ErrNoRows — standard database/sql semantics.
//
// Unlike QueryMany, this always pre-LISTs in Go (even for a
// single pattern) rather than using DuckDB's glob fast path.
// *sql.Row is a lazy handle — the underlying query runs when
// Scan is called and surfaces errors there, so there's no way
// to synchronously detect DuckDB's "No files found" error
// before returning to the caller. Pre-LIST sidesteps that by
// knowing the match set up front; the extra S3 LIST is the
// same one DuckDB would issue internally, so net latency is
// unchanged.
func (s *Reader[T]) QueryRowMany(
	ctx context.Context,
	patterns []string,
	sqlQuery string,
	opts ...QueryOption,
) *sql.Row {
	patterns = core.DedupePatterns(patterns)
	if len(patterns) == 0 {
		return s.emptyRow(ctx)
	}

	var o core.QueryOpts
	o.Apply(opts...)

	uris, err := s.listAllMatchingURIs(ctx, patterns, &o, "QueryRowMany")
	if err != nil {
		return s.errorRow(ctx, err)
	}
	if len(uris) == 0 {
		return s.emptyRow(ctx)
	}

	scanExpr := s.scanExprForURIs(
		uris, s.needsFilename())
	return s.db.QueryRowContext(ctx,
		s.wrapScanExpr(scanExpr, sqlQuery, o.IncludeHistory))
}
