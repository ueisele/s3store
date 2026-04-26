package s3parquet

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/ueisele/s3store/internal/core"
)

// s3ListMaxKeys is the per-request page-size cap enforced by S3.
const s3ListMaxKeys int32 = 1000

// Poll returns up to maxEntries stream entries (refs only) after
// the given offset, capped at now - SettleWindow to avoid races
// with in-flight writes. One or more S3 LIST calls, no GETs.
// Returns (entries, nextOffset, error); checkpoint nextOffset and
// pass it as `since` on the next call.
//
// Pass WithUntilOffset to bound the walk from above so long
// streams aren't scanned past the window of interest.
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

// PollRecords returns typed records from the files referenced by
// up to maxEntries refs after the offset, plus the next offset
// for checkpointing. Cursor-based, CDC-style: caller resumes from
// the returned offset on the next call.
//
// Replica-dedup only: records sharing (entity, version) collapse
// to one (rare retries / zombies); distinct versions of the same
// entity all flow through. Latest-per-entity dedup is NOT offered
// — meaningless on a cursor since the next batch may carry a newer
// version of the same entity. For latest-per-entity, use Read or
// ReadRangeIter (snapshot-style).
//
// WithIdempotentRead is accepted but ignored: the offset cursor
// already provides retry-safety on this path.
//
// Records follow ref order (= timestamp order), then parquet-file
// row order within each ref.
func (s *Reader[T]) PollRecords(
	ctx context.Context,
	since Offset,
	maxEntries int32,
	opts ...QueryOption,
) ([]T, Offset, error) {
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

	records, err := s.downloadAndDecodeAll(ctx, keys)
	if err != nil {
		return nil, since, err
	}
	// includeHistory=true: replica-dedup only, no version collapse.
	// See method docstring for why latest-per-entity isn't offered.
	return s.sortAndCollect(records, true), newOffset, nil
}

// OffsetAt returns the stream offset corresponding to wall-clock
// time t: any ref written at or after t sorts >= the returned
// offset, any ref written before t sorts <. Pure computation —
// no S3 call. Pair with WithUntilOffset on Poll / PollRecords to
// read records within a time window — or use ReadRangeIter, which
// takes time.Time bounds directly.
func (s *Reader[T]) OffsetAt(t time.Time) Offset {
	return Offset(core.RefCutoff(s.refPath, t, 0))
}
