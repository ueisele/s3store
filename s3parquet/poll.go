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
// iter.Seq2[T, error] via the same streamEager pipeline that
// backs ReadIter. The ref stream is walked upfront (LIST only,
// no body fetches) into a flat []KeyMeta, then handed to
// streamEager which downloads + decodes + yields with byte-budget
// streaming, cross-file pipelining, and per-partition dedup.
//
// Memory: streamEager bounds in-flight body memory via
// WithReadAheadPartitions (partition-count cap, default 1 = one
// partition lookahead) and WithReadAheadBytes (uncompressed-byte
// cap, default 0 = uncapped). Both apply here just as on
// ReadIter. KeyMeta slice is small (~100 bytes per ref), so even
// windows of 100k+ refs stay well under MB of metadata before
// streaming begins.
//
// Latency note: walking refs upfront means LIST completes before
// the first record yields. For typical windows (last hour, last
// day) this is sub-100ms. For huge backfill windows it can be
// seconds — chunk via since/until to stream incrementally.
//
// Consumer break cancels ctx via defer in streamEager, stopping
// in-flight downloads cleanly.
//
// Pass OffsetUnbounded for since to start at the stream head;
// pass OffsetUnbounded for until to walk to the settle-window
// cutoff (= live tip as of the call). Single-pass: terminates
// when the walk catches up to the cutoff, does NOT tail. Each
// internal Poll computes its own now-based cutoff, so writes
// landing during the walk may still be picked up; writes landing
// after termination are not. To keep up with new writes, call
// PollRecordsIter (or PollRecords) again with the last seen
// offset. Combine with OffsetAt for time windows.
//
// Dedup semantics match ReadIter: per-partition (refs are
// grouped into partitions by Hive key inside streamEager). If
// you need window-global latest-per-entity, pass WithHistory and
// dedup client-side.
//
// Resumption: PollRecordsIter does NOT expose the per-batch
// offset, so consumer aborts (ctx cancel, range break) cannot be
// safely resumed. Use PollRecords (Kafka-style batched API) when
// you need to checkpoint offsets between batches.
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

	return func(yield func(T, error) bool) {
		var o core.QueryOpts
		o.Apply(opts...)

		// Walk the ref stream into a flat KeyMeta slice. LIST-only
		// (no parquet bodies fetched), so this phase is cheap. Use
		// the LIST page max as the per-Poll cap to minimize round
		// trips — the slice growth here is bounded metadata, not
		// decoded record memory.
		var keys []core.KeyMeta
		cur := since
		for {
			entries, next, err := s.Poll(ctx, cur, s3ListMaxKeys, opts...)
			if err != nil {
				yield(*new(T), err)
				return
			}
			if len(entries) == 0 {
				break
			}
			for _, e := range entries {
				keys = append(keys, core.KeyMeta{
					Key:        e.DataPath,
					InsertedAt: e.InsertedAt,
				})
			}
			cur = next
		}
		if len(keys) == 0 {
			return
		}

		keys, err := core.ApplyIdempotentReadOpts(keys, s.dataPath, &o)
		if err != nil {
			yield(*new(T), fmt.Errorf("s3parquet: %w", err))
			return
		}
		if len(keys) == 0 {
			return
		}

		s.streamEager(ctx, keys, &o, yield)
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
