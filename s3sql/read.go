package s3sql

import (
	"context"

	"github.com/ueisele/s3store/internal/core"
)

// Read returns the latest version of all records matching the
// key pattern. Uses DuckDB with union_by_name for schema
// evolution and QUALIFY for deduplication.
//
// Accepts the shared glob grammar (see core.ValidateKeyPattern).
// When VersionColumn is empty, dedup is a no-op — every record
// in every matching file is returned in DuckDB order.
//
// Empty match is NOT an error: a pattern that matches no files
// returns (nil, nil). Callers who want the raw
// database/sql-style behaviour (DuckDB's "No files found"
// error) can call Query directly and handle the error
// themselves.
//
// Single-pattern sugar over ReadMany — use ReadMany directly
// when the caller has an arbitrary set of partition tuples
// (e.g. a non-Cartesian "(period=A, customer=X), (period=B,
// customer=Y)" selection) that can't be expressed as one
// pattern.
func (s *Store[T]) Read(
	ctx context.Context,
	keyPattern string,
	opts ...QueryOption,
) ([]T, error) {
	return s.ReadMany(ctx, []string{keyPattern}, opts...)
}

// ReadMany runs Read across every pattern in patterns and
// returns the concatenated result, with dedup applied globally
// when EntityKeyColumns + VersionColumn are configured (an
// entity that appears under two patterns is kept as the latest
// version across the union, not per-pattern).
//
// Each pattern uses the grammar described on Read. Pass more
// than one when the target set is NOT a Cartesian product of
// per-segment values — e.g. the tuples (period=A, customer=X)
// and (period=B, customer=Y) but not the off-diagonal pairs.
// For a Cartesian "all N × M" shape, use a single pattern with
// whole-segment "*".
//
// Execution model:
//
//   - Single-pattern fast path (len(patterns) == 1 after
//     literal dedup): hands the glob URI directly to DuckDB so
//     plan-time partition pruning and glob expansion happen
//     server-side. No Go-side S3 LIST. "No files found" from
//     DuckDB is translated to an empty result.
//   - Multi-pattern path: per-pattern S3 LIST in Go with a
//     bounded concurrency cap, deduplicated union of exact file
//     URIs → one read_parquet([uri1, ..., uriN]) call. DuckDB
//     plans once over the full set, enabling cross-pattern
//     aggregations / GROUP BY / joins the single-pattern path
//     can't give you.
//
// Empty slice → (nil, nil). Zero file matches → (nil, nil).
// First malformed pattern fails with its index.
func (s *Store[T]) ReadMany(
	ctx context.Context,
	patterns []string,
	opts ...QueryOption,
) ([]T, error) {
	patterns = core.DedupePatterns(patterns)
	switch len(patterns) {
	case 0:
		return nil, nil
	case 1:
		// Fast path: let DuckDB expand the glob so plan-time
		// partition pruning stays in effect. "No files found"
		// is normalized to an empty result — the multi-pattern
		// branch below returns (nil, nil) on zero matches, so
		// this branch should too.
		rows, err := s.Query(ctx, patterns[0],
			"SELECT * FROM "+s.cfg.TableAlias, opts...)
		if err != nil {
			if isNoFilesMatchedError(err) {
				return nil, nil
			}
			return nil, err
		}
		defer rows.Close()
		return s.scanAll(rows)
	}

	var o core.QueryOpts
	o.Apply(opts...)

	uris, err := s.listAllMatchingURIs(ctx, patterns, "ReadMany")
	if err != nil {
		return nil, err
	}
	if len(uris) == 0 {
		return nil, nil
	}

	scanExpr := s.scanExprForURIs(uris, s.needsFilename(o.IncludeHistory))
	rows, err := s.db.QueryContext(ctx,
		s.wrapScanExpr(scanExpr,
			"SELECT * FROM "+s.cfg.TableAlias, o.IncludeHistory))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return s.scanAll(rows)
}
