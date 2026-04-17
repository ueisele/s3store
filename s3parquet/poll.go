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

// Poll returns up to maxEntries stream entries (refs only) after
// the given offset, up to now - SettleWindow. Issues one or more
// S3 LIST calls (page size capped at 1000) and no GETs.
func (s *Store[T]) Poll(
	ctx context.Context,
	since core.Offset,
	maxEntries int32,
) ([]core.StreamEntry, core.Offset, error) {
	if maxEntries <= 0 {
		return nil, since, fmt.Errorf(
			"s3parquet: maxEntries must be > 0")
	}

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
			key, shortID, err := core.ParseRefKey(objKey)
			if err != nil {
				return nil, since, err
			}
			entries = append(entries, core.StreamEntry{
				Offset:   core.Offset(objKey),
				Key:      key,
				DataPath: core.BuildDataFilePath(s.dataPath, key, shortID),
			})
			lastKey = objKey
		}
	}

	if lastKey != "" {
		return entries, core.Offset(lastKey), nil
	}
	return nil, since, nil
}
