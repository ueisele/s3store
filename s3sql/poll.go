package s3sql

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ueisele/s3store/internal/core"
	"github.com/ueisele/s3store/internal/refstream"
)

// Poll returns up to maxEntries stream entries (refs only) after
// the given offset, up to now - SettleWindow. Pure S3 LIST; no
// GETs, no DuckDB.
//
// Accepts WithUntilOffset to bound the walk from above: entries
// with offset >= until are skipped and the paginator breaks
// early.
//
// The paginator + cutoff logic lives in internal/refstream — it's
// byte-identical with s3parquet.Poll and shares this one
// implementation.
func (s *Store[T]) Poll(
	ctx context.Context,
	since Offset,
	maxEntries int32,
	opts ...QueryOption,
) ([]StreamEntry, Offset, error) {
	var o core.QueryOpts
	o.Apply(opts...)

	entries, offset, err := refstream.Poll(ctx, s.s3,
		refstream.PollOpts{
			Bucket:       s.cfg.Bucket,
			RefPath:      s.refPath,
			DataPath:     s.dataPath,
			Since:        since,
			MaxEntries:   maxEntries,
			Until:        o.Until,
			SettleWindow: s.cfg.settleWindow(),
		})
	if err != nil {
		return nil, since, fmt.Errorf("s3sql: %w", err)
	}
	return entries, offset, nil
}

// PollRecords returns a flat slice of typed records from the
// files referenced by up to maxEntries refs after the offset.
// Runs through DuckDB so dedup and schema-evolution transforms
// are consistent with Read.
//
// Dedup applies by default (latest-per-key by VersionColumn);
// pass WithHistory() to disable.
func (s *Store[T]) PollRecords(
	ctx context.Context,
	since Offset,
	maxEntries int32,
	opts ...QueryOption,
) ([]T, Offset, error) {
	var o core.QueryOpts
	o.Apply(opts...)

	entries, newOffset, err := s.Poll(ctx, since, maxEntries, opts...)
	if err != nil {
		return nil, since, err
	}
	if len(entries) == 0 {
		return nil, since, nil
	}

	uris := make([]string, len(entries))
	for i, e := range entries {
		uris[i] = sqlQuote(s.s3URI(e.DataPath))
	}

	// Match scanExprForPattern's read_parquet options: expose
	// hive partition columns as VARCHAR (hive_types_autocast=false
	// prevents DuckDB from re-typing ISO dates as DATE/TIMESTAMP
	// mid-read, which would diverge from strict lex comparison on
	// the Go side). Keeps Read and PollRecords seeing the same
	// shape regardless of which code path surfaced the files.
	scanExpr := fmt.Sprintf(
		"SELECT * FROM read_parquet([%s], "+
			"hive_partitioning=true, hive_types_autocast=false, "+
			"union_by_name=true)",
		strings.Join(uris, ", "))

	query := s.wrapScanExpr(scanExpr,
		"SELECT * FROM "+s.cfg.TableAlias, o.IncludeHistory)

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, since,
			fmt.Errorf("s3sql: poll query: %w", err)
	}
	defer rows.Close()

	records, err := s.scanAll(rows)
	if err != nil {
		return nil, since, err
	}

	return records, newOffset, nil
}

// PollRecordsAll reads every record in [since, until) in one
// call. Internally loops PollRecords until the window is drained,
// so memory scales with window size rather than stream length
// and any S3 / DuckDB error surfaces from the batch where it
// happened.
//
// Pass Offset("") for since to start at the stream head;
// pass Offset("") for until to read to the settle-window
// cutoff (= live tip). Combine with OffsetAt for time windows.
//
// Dedup semantics match PollRecords: per-batch. If you need
// window-global latest-per-key, pass WithHistory and dedup
// client-side.
func (s *Store[T]) PollRecordsAll(
	ctx context.Context,
	since, until Offset,
	opts ...QueryOption,
) ([]T, error) {
	opts = append(opts, WithUntilOffset(until))
	return refstream.PollAll(ctx, since,
		func(ctx context.Context, since Offset, max int32,
		) ([]T, Offset, error) {
			return s.PollRecords(ctx, since, max, opts...)
		})
}

// OffsetAt returns the stream offset corresponding to wall-clock
// time t: any ref written at or after t sorts >= the returned
// offset, any ref written before t sorts <. Pure computation —
// no S3 call. Pair with WithUntilOffset (or PollRecordsAll's
// until parameter) to read records within a half-open
// [since, until) time window.
//
//	start := store.OffsetAt(time.Date(y, m, d,   0,0,0,0, loc))
//	end   := store.OffsetAt(time.Date(y, m, d+1, 0,0,0,0, loc))
//	records, _ := store.PollRecordsAll(ctx, start, end)
func (s *Store[T]) OffsetAt(t time.Time) Offset {
	return refstream.OffsetAt(s.refPath, t)
}
