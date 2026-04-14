package s3store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// s3ListMaxKeys is the per-request page size cap enforced by S3.
const s3ListMaxKeys int32 = 1000

// Poll returns up to maxEntries stream entries (refs only) after
// the given offset, up to now - settle_window. Issues one or more
// S3 LIST calls (page size capped at 1000) and no GETs.
func (s *Store[T]) Poll(
	ctx context.Context,
	since Offset,
	maxEntries int32,
) ([]StreamEntry, Offset, error) {
	if maxEntries <= 0 {
		return nil, since, fmt.Errorf(
			"s3store: maxEntries must be > 0")
	}

	cutoff := time.Now().Add(-s.cfg.settleWindow())
	cutoffPrefix := fmt.Sprintf("%s/%d",
		s.refPath, cutoff.UnixMicro())

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

	entries := make([]StreamEntry, 0, maxEntries)
	var lastKey string

	paginator := s3.NewListObjectsV2Paginator(s.s3, input)
outer:
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, since,
				fmt.Errorf("s3store: list refs: %w", err)
		}
		for _, obj := range page.Contents {
			if int32(len(entries)) >= maxEntries {
				break outer
			}
			objKey := aws.ToString(obj.Key)
			if objKey > cutoffPrefix {
				break outer
			}
			key, shortID, err := s.parseRefKey(objKey)
			if err != nil {
				return nil, since, err
			}
			entries = append(entries, StreamEntry{
				Offset:   Offset(objKey),
				Key:      key,
				DataPath: s.buildDataPath(key, shortID),
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
//
// By default PollRecords is a pure stream: every record in
// every referenced file is returned, in file and row order,
// with schema evolution (ColumnAliases, ColumnDefaults,
// union_by_name) applied.
//
// Pass WithCompaction() to get Kafka compacted-topic semantics
// instead: within each batch, only the latest version
// (by VersionColumn) of each key is returned. Across batches,
// newer writes supersede older ones in the consumer's view, so
// a consumer that applies each record as an upsert to a local
// store converges on the latest-per-key view. WithCompaction
// requires Config.VersionColumn to be set.
//
// For point-in-time deduplicated snapshots use Read. For full
// SQL access including WithHistory use Query.
func (s *Store[T]) PollRecords(
	ctx context.Context,
	since Offset,
	maxEntries int32,
	opts ...PollOption,
) ([]T, Offset, error) {
	o := &pollOpts{}
	for _, opt := range opts {
		opt(o)
	}
	if o.compacted && s.cfg.VersionColumn == "" {
		return nil, since, fmt.Errorf(
			"s3store: WithCompaction requires " +
				"Config.VersionColumn to be set")
	}

	entries, newOffset, err :=
		s.Poll(ctx, since, maxEntries)
	if err != nil {
		return nil, since, err
	}
	if len(entries) == 0 {
		return nil, since, nil
	}

	uris := make([]string, len(entries))
	for i, e := range entries {
		uris[i] = "'" + s.s3URI(e.DataPath) + "'"
	}

	scanExpr := fmt.Sprintf(
		"SELECT * FROM read_parquet([%s], "+
			"union_by_name=true)",
		strings.Join(uris, ", "))

	query, err := s.buildWrappedQuery(ctx, scanExpr,
		"SELECT * FROM "+s.cfg.TableAlias, !o.compacted)
	if err != nil {
		return nil, since, err
	}

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, since,
			fmt.Errorf("s3store: poll query: %w", err)
	}
	defer rows.Close()

	records, err := s.scanAll(rows)
	if err != nil {
		return nil, since, err
	}

	return records, newOffset, nil
}
