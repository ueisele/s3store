package s3store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// BackfillStats reports the work BackfillProjection did: how many
// parquet objects it scanned, how many records it decoded, and
// how many marker PUTs it issued. Markers is per-object, not
// globally deduplicated — a marker path produced by N parquet
// files is counted N times (reflects S3 request cost, not
// unique marker count). Useful for progress logging in a
// migration job.
type BackfillStats struct {
	DataObjects int
	Records     int
	Markers     int
}

// BackfillProjection scans existing parquet data and writes
// projection markers for every record already present. The normal
// path is to wire projections via WriterConfig.Projections /
// Config.Projections before the first Write; BackfillProjection is
// the relief valve for records written before the projection
// existed.
//
// keyPatterns use the grammar from validateKeyPattern,
// evaluated against target.PartitionKeyParts() (NOT the
// projection's Columns) — backfill walks parquet data files,
// which are keyed by partition. "*" covers everything; shard
// across partitions to parallelize a migration. Overlapping
// patterns are deduplicated, so each parquet file is scanned at
// most once.
//
// until is an exclusive upper bound on data-file LastModified.
// Typical use: until = deployTime_of_live_writer, so backfill
// covers historical gaps while the live writer covers everything
// from deploy onward. Pass time.Time{} (the zero value) to cover
// every file currently present (redundant with the live writer
// but harmless — PUT is idempotent).
//
// On a data-file GET returning NoSuchKey (LIST-to-GET race,
// operator-driven prune, or lifecycle deletion), the file is
// skipped: a slog.Warn at level WARN names the path, the
// s3store.read.missing_data counter is incremented with
// method=backfill, and the backfill continues. Failing the whole
// backfill on one missing file would force a full restart of a
// long-running operator job.
//
// Safe to run concurrently with a live writer (S3 PUT is
// idempotent) and safe to retry after a crash. Empty patterns
// slice is a no-op: (BackfillStats{}, nil). First malformed
// pattern fails with its index.
func BackfillProjection[T any](
	ctx context.Context,
	target S3Target,
	def ProjectionDef[T],
	keyPatterns []string,
	until time.Time,
) (stats BackfillStats, err error) {
	scope := target.metrics.methodScope(ctx, methodBackfill)
	defer func() {
		scope.addRecords(int64(stats.Records))
		scope.addFiles(int64(stats.DataObjects))
		scope.end(&err)
	}()

	keyPatterns = dedupePatterns(keyPatterns)
	if len(keyPatterns) == 0 {
		return stats, nil
	}

	// Full Target check — BackfillProjection LISTs partitioned data
	// files (plan.Match consults PartitionKeyParts), so
	// validateLookup's reduced subset isn't enough.
	if err := target.Validate(); err != nil {
		return stats, err
	}
	if err := validateProjectionDefShape(def.Name, def.Columns); err != nil {
		return stats, err
	}
	of, err := resolveOf(def)
	if err != nil {
		return stats, err
	}

	projectionPath := projectionBasePath(target.Prefix(), def.Name)

	dataPath := dataPath(target.Prefix())
	plans, err := buildReadPlans(keyPatterns, dataPath, target.PartitionKeyParts())
	if err != nil {
		return stats, fmt.Errorf(
			"s3store: BackfillProjection %w", err)
	}

	keys, err := listDataFiles(ctx, target, plans)
	if err != nil {
		return stats, err
	}
	// Apply the until cutoff in Go: the LIST already gave us
	// LastModified per object via KeyMeta.InsertedAt, so we
	// don't need a dedicated until-aware list helper. A zero
	// until disables the filter.
	if !until.IsZero() {
		filtered := keys[:0]
		for _, k := range keys {
			if k.InsertedAt.Before(until) {
				filtered = append(filtered, k)
			}
		}
		keys = filtered
	}
	if len(keys) == 0 {
		return stats, nil
	}
	stats.DataObjects = len(keys)

	var recordsTotal, markersTotal atomic.Int64
	err = fanOut(ctx, keys,
		target.EffectiveMaxInflightRequests(),
		target.metrics,
		func(ctx context.Context, _ int, km KeyMeta) error {
			key := km.Key
			paths, nRecs, err := backfillMarkersForObject(
				ctx, target, def.Name, def.Columns, of, projectionPath, key)
			if err != nil {
				// Operator-driven deletion (LIST-to-GET race,
				// lifecycle, manual prune): a data file listed a
				// moment ago is gone now. Skip and continue — one
				// missing file shouldn't fail the whole backfill.
				// Other GET errors remain fatal.
				if _, ok := errors.AsType[*s3types.NoSuchKey](err); ok {
					slog.Warn("s3store: data file missing, skipping",
						"path", key, "method", string(methodBackfill))
					scope.recordMissingData()
					return nil
				}
				return err
			}
			recordsTotal.Add(int64(nRecs))

			// Serial marker PUTs within the object — the per-target
			// MaxInflightRequests semaphore on target.put already
			// caps net in-flight, so per-object fan-out would just
			// queue at the semaphore.
			for _, p := range paths {
				if err := target.put(
					ctx, p, nil, "application/octet-stream",
				); err != nil {
					return fmt.Errorf(
						"s3store: backfill projection %q: put marker: %w",
						def.Name, err)
				}
			}
			markersTotal.Add(int64(len(paths)))
			return nil
		})

	stats.Records = int(recordsTotal.Load())
	stats.Markers = int(markersTotal.Load())
	return stats, err
}

// backfillMarkersForObject decodes one parquet data object and
// returns the deduplicated marker paths its records produce under
// the resolved Of, plus the record count (for stats). Pulled out
// of BackfillProjection's main loop so the dedup map doesn't leak
// across objects — each file stands on its own, keeping memory
// bounded by the largest file rather than the full backfill set.
func backfillMarkersForObject[T any](
	ctx context.Context,
	target S3Target,
	name string,
	columns []string,
	of func(T) ([]string, error),
	projectionPath string,
	key string,
) ([]string, int, error) {
	data, err := target.get(ctx, key)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"s3store: backfill get %s: %w", key, err)
	}
	recs, err := decodeParquet[T](data)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"s3store: backfill decode %s: %w", key, err)
	}

	seen := make(map[string]struct{})
	for _, rec := range recs {
		values, err := of(rec)
		if err != nil {
			return nil, 0, fmt.Errorf(
				"s3store: backfill projection %q on %s: %w",
				name, key, err)
		}
		if values == nil {
			continue
		}
		p, err := markerPathFromValues(name, projectionPath, columns, values)
		if err != nil {
			return nil, 0, fmt.Errorf(
				"s3store: backfill projection %q on %s: %w",
				name, key, err)
		}
		seen[p] = struct{}{}
	}
	if len(seen) == 0 {
		return nil, len(recs), nil
	}
	paths := make([]string, 0, len(seen))
	for p := range seen {
		paths = append(paths, p)
	}
	return paths, len(recs), nil
}
