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

// Poll returns up to maxEntries stream entries (refs only) after
// the given offset, up to now - SettleWindow. Pure S3 LIST; no
// GETs, no DuckDB.
func (s *Store[T]) Poll(
	ctx context.Context,
	since core.Offset,
	maxEntries int32,
) ([]core.StreamEntry, core.Offset, error) {
	if maxEntries <= 0 {
		return nil, since, fmt.Errorf(
			"s3sql: maxEntries must be > 0")
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

	entries, newOffset, err := s.Poll(ctx, since, maxEntries)
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

	scanExpr := fmt.Sprintf(
		"SELECT * FROM read_parquet([%s], "+
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
