package s3sql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

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
func (s *Reader) Query(
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
func (s *Reader) QueryMany(
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
func (s *Reader) scanExprForURIs(
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
// withFilename is set; empty string otherwise.
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
// regardless of WithHistory.
func (s *Reader) needsFilename() bool {
	return s.cfg.dedupEnabled()
}

// wrapScanExpr wraps a base scan expression with an optional
// dedup CTE and the user's SQL query. Shared by Query, QueryMany.
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
func (s *Reader) wrapScanExpr(
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
func (s *Reader) emptyRows(ctx context.Context) (*sql.Rows, error) {
	return s.db.QueryContext(ctx, "SELECT NULL WHERE 1=0")
}
