package s3parquet

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"
	"github.com/ueisele/s3store/internal/core"
)

// s3ListMaxKeys is the per-request page-size cap enforced by S3.
const s3ListMaxKeys int32 = 1000

// pollAllBatch is the inner batch size used by PollRecordsAll.
// Tuned for S3 LIST page size so the inner paginator does one
// LIST per iteration at steady state.
const pollAllBatch int32 = 1000

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

	pageSize := min(maxEntries, s3ListMaxKeys)

	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(s.cfg.Target.Bucket()),
		Prefix:  aws.String(s.refPath + "/"),
		MaxKeys: aws.Int32(pageSize),
	}
	if since != "" {
		input.StartAfter = aws.String(string(since))
	}

	paginator := s3.NewListObjectsV2Paginator(
		s.cfg.Target.S3Client(), input)

	// Consistency-Control header surfaces here (and only here) for
	// the ref-stream LIST. Empty value sends no header — correct on
	// AWS S3 / MinIO; required on StorageGRID strong-global to
	// linearize the LIST with concurrent ref PUTs.
	var apiOpts []func(*middleware.Stack) error
	if s.cfg.ConsistencyControl != "" {
		apiOpts = []func(*middleware.Stack) error{
			core.AddHeaderMiddleware(
				"Consistency-Control",
				string(s.cfg.ConsistencyControl)),
		}
	}

	var entries []StreamEntry
	var lastKey string

outer:
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx, func(o *s3.Options) {
			o.APIOptions = append(o.APIOptions, apiOpts...)
		})
		if err != nil {
			return nil, since,
				fmt.Errorf("s3parquet: list refs: %w", err)
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
			key, _, id, dataTsMicros, err := core.ParseRefKey(objKey)
			if err != nil {
				return nil, since,
					fmt.Errorf("s3parquet: parse ref: %w", err)
			}
			entries = append(entries, StreamEntry{
				Offset:     Offset(objKey),
				Key:        key,
				DataPath:   core.BuildDataFilePath(s.dataPath, key, id),
				RefPath:    objKey,
				InsertedAt: time.UnixMicro(dataTsMicros),
			})
			lastKey = objKey
		}
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

	var all []T
	for {
		batch, next, err := s.PollRecords(ctx, since, pollAllBatch, opts...)
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
	return Offset(core.RefCutoff(s.refPath, t, 0))
}
