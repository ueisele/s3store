package s3sql

import (
	"fmt"
	"strings"
)

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
func (s *Reader[T]) scanExprForURIs(
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
func (s *Reader[T]) needsFilename() bool {
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
func (s *Reader[T]) wrapScanExpr(
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
