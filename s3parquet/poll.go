package s3parquet

import (
	"context"
	"fmt"
	"iter"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/ueisele/s3store/internal/core"
)

// s3ListMaxKeys is the per-request page-size cap enforced by S3.
const s3ListMaxKeys int32 = 1000

// pollIterBatch returns the inner batch size used by
// PollRecordsIter: 2 * EffectiveMaxInflightRequests, capped at
// the S3 LIST page size. The 2× headroom lets the pipeline
// producer stage the next batch while the consumer drains the
// previous one without ballooning the in-memory record count;
// the cap on s3ListMaxKeys stays inside the LIST page-size limit.
//
// At MaxInflightRequests=32 (default) this is 64 — far below the
// previous static 1000, which materialized up to 1000 decoded
// files at once.
func pollIterBatch(t S3Target) int32 {
	return min(int32(2*t.EffectiveMaxInflightRequests()), s3ListMaxKeys)
}

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
func (s *Reader[T]) Poll(
	ctx context.Context,
	since Offset,
	maxEntries int32,
	opts ...QueryOption,
) ([]StreamEntry, Offset, error) {
	if s.cfg.Target.DisableRefStream() {
		return nil, since, ErrRefStreamDisabled
	}
	if maxEntries <= 0 {
		return nil, since, fmt.Errorf(
			"s3parquet: maxEntries must be > 0")
	}

	var o core.QueryOpts
	o.Apply(opts...)

	cutoffPrefix := core.RefCutoff(s.refPath, time.Now(),
		s.cfg.Target.EffectiveSettleWindow())

	var entries []StreamEntry
	var lastKey string

	err := s.cfg.Target.listEach(ctx,
		s.refPath+"/", string(since),
		min(maxEntries, s3ListMaxKeys),
		s.cfg.ConsistencyControl,
		func(obj s3types.Object) (bool, error) {
			if int32(len(entries)) >= maxEntries {
				return false, nil
			}
			objKey := aws.ToString(obj.Key)
			if objKey > cutoffPrefix {
				return false, nil
			}
			if o.Until != "" && objKey >= string(o.Until) {
				return false, nil
			}
			key, _, id, dataTsMicros, err := core.ParseRefKey(objKey)
			if err != nil {
				return false, fmt.Errorf("parse ref: %w", err)
			}
			entries = append(entries, StreamEntry{
				Offset:     Offset(objKey),
				Key:        key,
				DataPath:   core.BuildDataFilePath(s.dataPath, key, id),
				RefPath:    objKey,
				InsertedAt: time.UnixMicro(dataTsMicros),
			})
			lastKey = objKey
			return true, nil
		})
	if err != nil {
		return nil, since, fmt.Errorf("s3parquet: list refs: %w", err)
	}

	if lastKey != "" {
		return entries, Offset(lastKey), nil
	}
	return nil, since, nil
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

	keys, err = core.ApplyIdempotentReadOpts(keys, s.dataPath, &o)
	if err != nil {
		return nil, since, fmt.Errorf("s3parquet: %w", err)
	}
	if len(keys) == 0 {
		return nil, newOffset, nil
	}

	records, err := s.downloadAndDecodeAll(ctx, keys)
	if err != nil {
		return nil, since, err
	}
	return s.sortAndCollect(records, o.IncludeHistory), newOffset, nil
}

// PollRecordsIter streams every record in [since, until) as an
// iter.Seq2[T, error]. A background producer goroutine drives
// PollRecords with batch size = 2 × MaxInflightRequests (capped
// at the LIST page size) and pushes results onto a buffered
// channel; the consumer pulls one batch at a time and iterates
// records out. Steady state pipelines: producer fetches batch
// N+1 while consumer drains batch N, hiding the LIST + download
// latency.
//
// Memory peak: 2 batches' worth of decoded records (one staged
// in the channel, one being iterated). With the default
// MaxInflightRequests=32 that's ~128 files' worth of records,
// orders of magnitude below the older 1000-files-per-batch
// shape that risked OOM on wide windows.
//
// A consumer that breaks out of the range loop cancels the
// producer's context via defer; the in-flight LIST/download
// bails on ctx, the producer exits, the channel closes.
//
// Pass Offset("") for since to start at the stream head;
// pass Offset("") for until to read to the settle-window
// cutoff (= live tip). Combine with OffsetAt for time windows.
//
// Dedup semantics match PollRecords: per-batch. If you need
// window-global latest-per-entity, pass WithHistory and dedup
// client-side.
//
// Errors are yielded as (zero-value, err); the loop must check
// err before using the record. The iter terminates after an
// error.
func (s *Reader[T]) PollRecordsIter(
	ctx context.Context,
	since, until Offset,
	opts ...QueryOption,
) iter.Seq2[T, error] {
	// Append Until last so it wins over any WithUntilOffset the
	// caller snuck in via opts — the `until` parameter is the
	// method's contract.
	opts = append(opts, WithUntilOffset(until))
	batch := pollIterBatch(s.cfg.Target)

	return func(yield func(T, error) bool) {
		// Cancel propagates to producer when consumer breaks.
		pollCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		type batchResult struct {
			recs []T
			next Offset
			err  error
		}
		// Buffer=1: producer can stage one batch ahead. Larger
		// would multiply memory; 0 would serialize (no pipeline).
		ch := make(chan batchResult, 1)

		go func() {
			defer close(ch)
			cur := since
			for {
				recs, next, err := s.PollRecords(
					pollCtx, cur, batch, opts...)
				select {
				case ch <- batchResult{recs: recs, next: next, err: err}:
				case <-pollCtx.Done():
					return
				}
				if err != nil || len(recs) == 0 {
					return
				}
				cur = next
			}
		}()

		for b := range ch {
			if b.err != nil {
				yield(*new(T), b.err)
				return
			}
			for _, r := range b.recs {
				if !yield(r, nil) {
					return
				}
			}
		}
	}
}

// OffsetAt returns the stream offset corresponding to wall-clock
// time t: any ref written at or after t sorts >= the returned
// offset, any ref written before t sorts <. Pure computation —
// no S3 call. Pair with WithUntilOffset (or PollRecordsIter's
// until parameter) to read records within a time window. To
// cover a full day, until is the start of the *next* day:
//
//	start := store.OffsetAt(time.Date(y, m, d,   0,0,0,0, loc))
//	end   := store.OffsetAt(time.Date(y, m, d+1, 0,0,0,0, loc))
//	for r, err := range store.PollRecordsIter(ctx, start, end) { ... }
func (s *Reader[T]) OffsetAt(t time.Time) Offset {
	return Offset(core.RefCutoff(s.refPath, t, 0))
}
