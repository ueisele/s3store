package s3parquet

import (
	"context"
	"fmt"
	"time"

	"github.com/ueisele/s3store/internal/core"
	"github.com/ueisele/s3store/internal/refstream"
)

// Poll returns up to maxEntries stream entries (refs only) after
// the given offset, up to now - SettleWindow. Issues one or more
// S3 LIST calls (page size capped at 1000) and no GETs.
//
// Accepts WithUntilOffset to bound the walk from above: entries
// with offset >= until are skipped and the paginator breaks
// early so long streams don't have to be scanned past the window
// of interest.
//
// ConsistencyControl on the Reader's config is forwarded as the
// Consistency-Control HTTP header on every paginator LIST, so on
// StorageGRID strong-global / strong-site the LIST linearizes with
// the writer's ref PUT (no silent miss when a newly-written ref
// propagates slower than SettleWindow).
//
// The paginator + cutoff logic lives in internal/refstream — it's
// byte-identical with s3sql.Poll and shares this one
// implementation.
func (s *Reader[T]) Poll(
	ctx context.Context,
	since Offset,
	maxEntries int32,
	opts ...QueryOption,
) ([]StreamEntry, Offset, error) {
	if s.cfg.Target.DisableRefStream {
		return nil, since, ErrRefStreamDisabled
	}

	var o core.QueryOpts
	o.Apply(opts...)

	entries, offset, err := refstream.Poll(ctx, s.cfg.Target.S3Client,
		refstream.PollOpts{
			Bucket:             s.cfg.Target.Bucket,
			RefPath:            s.refPath,
			DataPath:           s.dataPath,
			Since:              since,
			MaxEntries:         maxEntries,
			Until:              o.Until,
			SettleWindow:       s.cfg.Target.EffectiveSettleWindow(),
			ConsistencyControl: string(s.cfg.ConsistencyControl),
		})
	if err != nil {
		return nil, since, fmt.Errorf("s3parquet: %w", err)
	}
	return entries, offset, nil
}

// PollRecords returns a flat slice of typed records from the
// files referenced by up to maxEntries refs after the offset.
// Downloads run in parallel (limit pollDownloadConcurrency).
//
// By default applies latest-per-entity dedup within the batch
// (consistent with Read). Pass WithHistory() to disable dedup
// and get every record in ref order.
//
// When dedup is disabled (no EntityKeyOf, or WithHistory()), the
// returned records follow ref order (= timestamp order) and then
// parquet-file row order within each ref.
func (s *Reader[T]) PollRecords(
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

	// Carry each entry's InsertedAt (= dataTsMicros from the ref
	// filename) on the KeyMeta. Used as the fallback when the
	// reader has no InsertedAtField configured — same value the
	// writer captured, so dedup / sort matches the column path.
	keys := make([]core.KeyMeta, len(entries))
	for i, e := range entries {
		keys[i] = core.KeyMeta{
			Key:        e.DataPath,
			InsertedAt: e.InsertedAt,
		}
	}

	keys, err = s.applyIdempotentRead(keys, &o)
	if err != nil {
		return nil, since, err
	}
	if len(keys) == 0 {
		return nil, newOffset, nil
	}

	versioned, err := s.downloadAndDecodeAll(ctx, keys)
	if err != nil {
		return nil, since, err
	}

	return s.sortAndDedup(versioned, o.IncludeHistory), newOffset, nil
}

// PollRecordsAll reads every record in [since, until) in one
// call. Internally loops PollRecords until the window is drained,
// so memory scales with window size rather than stream length
// and any S3 / decode error surfaces from the batch where it
// happened.
//
// Pass Offset("") for since to start at the stream head;
// pass Offset("") for until to read to the settle-window
// cutoff (= live tip). Combine with OffsetAt for time windows.
//
// Dedup semantics match PollRecords: per-batch. If you need
// window-global latest-per-entity, pass WithHistory and dedup
// client-side.
func (s *Reader[T]) PollRecordsAll(
	ctx context.Context,
	since, until Offset,
	opts ...QueryOption,
) ([]T, error) {
	// Append Until last so it wins over any WithUntilOffset the
	// caller snuck in via opts — the `until` parameter is the
	// method's contract.
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
// until parameter) to read records within a time window. To
// cover a full day, until is the start of the *next* day:
//
//	start := store.OffsetAt(time.Date(y, m, d,   0,0,0,0, loc))
//	end   := store.OffsetAt(time.Date(y, m, d+1, 0,0,0,0, loc))
//	records, _ := store.PollRecordsAll(ctx, start, end)
func (s *Reader[T]) OffsetAt(t time.Time) Offset {
	return refstream.OffsetAt(s.refPath, t)
}
