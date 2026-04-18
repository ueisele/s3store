package s3parquet

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ueisele/s3store/internal/core"
)

// s3ListMaxKeys is the per-request page-size cap enforced by S3.
const s3ListMaxKeys int32 = 1000

// Poll returns up to maxEntries stream entries (refs only)
// after the given offset, up to now - SettleWindow. Issues one
// or more S3 LIST calls (page size capped at 1000) and no GETs.
//
// Accepts WithUntilOffset to bound the walk from above: entries
// with offset >= until are skipped and the paginator breaks
// early so long streams don't have to be scanned past the
// window of interest.
func (s *Store[T]) Poll(
	ctx context.Context,
	since core.Offset,
	maxEntries int32,
	opts ...core.QueryOption,
) ([]core.StreamEntry, core.Offset, error) {
	if maxEntries <= 0 {
		return nil, since, fmt.Errorf(
			"s3parquet: maxEntries must be > 0")
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

	// Lazy allocation: grow via append instead of pre-sizing
	// to maxEntries. Avoids wasted capacity for callers that
	// ask for a large cap but typically receive far fewer
	// entries (and append's doubling is cheap at the sizes
	// Poll actually returns).
	var entries []core.StreamEntry
	var lastKey string

	paginator := s3.NewListObjectsV2Paginator(s.s3, input)
outer:
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
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
// Downloads run in parallel (limit pollDownloadConcurrency).
//
// By default applies latest-per-entity dedup within the batch
// (consistent with Read). Pass WithHistory() to disable dedup
// and get every record in ref order.
//
// When dedup is disabled (no EntityKeyOf, or WithHistory()),
// the returned records follow ref order (= timestamp order)
// and then parquet-file row order within each ref.
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

	keys := make([]string, len(entries))
	for i, e := range entries {
		keys[i] = e.DataPath
	}

	versioned, err := s.downloadAndDecodeAll(ctx, keys)
	if err != nil {
		return nil, since, err
	}

	if o.IncludeHistory || !s.cfg.dedupEnabled() {
		return stripVersions(versioned), newOffset, nil
	}
	return dedupLatest(versioned, s.cfg.EntityKeyOf, s.cfg.VersionOf),
		newOffset, nil
}

// OffsetAt returns the stream offset corresponding to wall-
// clock time t: any ref written at or after t sorts >= the
// returned offset, any ref written before t sorts < it.
//
// Pure computation — no S3 call. Pair with WithUntilOffset to
// read records within a time window:
//
//	start := store.OffsetAt(from)
//	end   := store.OffsetAt(to)
//	records, _, _ := store.PollRecords(ctx, start, 100,
//	    s3parquet.WithUntilOffset(end))
func (s *Store[T]) OffsetAt(t time.Time) core.Offset {
	return core.Offset(core.RefCutoff(s.refPath, t, 0))
}
