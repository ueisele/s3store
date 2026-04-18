package s3sql

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ueisele/s3store/internal/core"
)

// s3ListMaxKeys is the per-request page-size cap enforced by S3.
const s3ListMaxKeys int32 = 1000

// Poll returns up to maxEntries stream entries (refs only)
// after the given offset, up to now - SettleWindow. Pure S3
// LIST; no GETs, no DuckDB.
//
// Accepts WithUntilOffset to bound the walk from above: entries
// with offset >= until are skipped and the paginator breaks
// early.
func (s *Store[T]) Poll(
	ctx context.Context,
	since core.Offset,
	maxEntries int32,
	opts ...core.QueryOption,
) ([]core.StreamEntry, core.Offset, error) {
	if maxEntries <= 0 {
		return nil, since, fmt.Errorf(
			"s3sql: maxEntries must be > 0")
	}

	var o core.QueryOpts
	o.Apply(opts...)

	cutoffPrefix := core.RefCutoff(
		s.refPath, time.Now(), s.cfg.settleWindow())

	pageSize := maxEntries
	if pageSize > s3ListMaxKeys {
		pageSize = s3ListMaxKeys
	}

	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(s.cfg.Bucket),
		Prefix:  aws.String(s.refPath + "/"),
		MaxKeys: aws.Int32(pageSize),
	}
	if since != "" {
		input.StartAfter = aws.String(string(since))
	}

	var entries []core.StreamEntry
	var lastKey string

	paginator := s3.NewListObjectsV2Paginator(s.s3, input)
outer:
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, since,
				fmt.Errorf("s3sql: list refs: %w", err)
		}
		for _, obj := range page.Contents {
			if int32(len(entries)) >= maxEntries {
				break outer
			}
			objKey := aws.ToString(obj.Key)
			if objKey > cutoffPrefix {
				break outer
			}
			if o.Until != "" && objKey >= string(o.Until) {
				break outer
			}
			key, tsMicros, shortID, err := core.ParseRefKey(objKey)
			if err != nil {
				return nil, since, err
			}
			entries = append(entries, core.StreamEntry{
				Offset: core.Offset(objKey),
				Key:    key,
				DataPath: core.BuildDataFilePath(
					s.dataPath, key, tsMicros, shortID),
			})
			lastKey = objKey
		}
	}

	if lastKey != "" {
		return entries, core.Offset(lastKey), nil
	}
	return nil, since, nil
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
	since core.Offset,
	maxEntries int32,
	opts ...core.QueryOption,
) ([]T, core.Offset, error) {
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

// pollRecordsAllBatch is the per-iteration batch size used by
// PollRecordsAll. Tuned for S3 LIST page size (1000).
const pollRecordsAllBatch int32 = 1000

// PollRecordsAll reads every record in [since, until) in one
// call. Internally loops PollRecords with a fixed batch size
// until the window is drained, so memory scales with window
// size rather than stream length, and any S3 / DuckDB error
// surfaces from the batch where it happened.
//
// Pass core.Offset("") for since to start at the stream head;
// pass core.Offset("") for until to read to the settle-window
// cutoff (= live tip). Combine with OffsetAt for time windows.
//
// Dedup semantics match PollRecords: per-batch. If you need
// window-global latest-per-key, pass WithHistory and dedup
// client-side.
func (s *Store[T]) PollRecordsAll(
	ctx context.Context,
	since, until core.Offset,
	opts ...core.QueryOption,
) ([]T, error) {
	opts = append(opts, core.WithUntilOffset(until))

	var all []T
	for {
		batch, next, err := s.PollRecords(
			ctx, since, pollRecordsAllBatch, opts...)
		if err != nil {
			return nil, err
		}
		if len(batch) == 0 {
			return all, nil
		}
		all = append(all, batch...)
		since = next
	}
}

// OffsetAt returns the stream offset corresponding to wall-
// clock time t: any ref written at or after t sorts >= the
// returned offset, any ref written before t sorts < it.
//
// Pure computation — no S3 call. Internally compares in UTC
// microseconds (offsets are encoded from time.UnixMicro). Pair
// with WithUntilOffset (or PollRecordsAll's until parameter)
// to read records within a half-open [since, until) time
// window — to cover a full day, until is the start of the
// *next* day:
//
//	start := store.OffsetAt(time.Date(y, m, d,   0,0,0,0, loc))
//	end   := store.OffsetAt(time.Date(y, m, d+1, 0,0,0,0, loc))
//	records, _ := store.PollRecordsAll(ctx, start, end)
func (s *Store[T]) OffsetAt(t time.Time) core.Offset {
	return core.Offset(core.RefCutoff(s.refPath, t, 0))
}
