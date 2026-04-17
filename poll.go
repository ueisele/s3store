package s3store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ueisele/s3store/internal/core"
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
	var entries []StreamEntry
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
// By default — consistent with Query, QueryRow, and Read —
// PollRecords applies latest-per-key deduplication within each
// batch by VersionColumn. Across batches, newer writes
// supersede older ones in the consumer's view, so a consumer
// that applies each record as an upsert to a local store
// converges on a latest-per-key view. This is Kafka
// compacted-topic semantics and the primary use case for
// materialized-view consumers (the billing example in the
// README).
//
// Pass WithHistory() to disable dedup and get a pure stream:
// every record in every referenced file, in file and row
// order. Use this for audit logs or when you need to observe
// superseded versions.
//
// When VersionColumn is empty, dedup is a no-op regardless of
// WithHistory — there's no ordering to dedup on, so the result
// is always the stream.
//
// Note: s3store is append-only, so the default compacted mode
// is upsert-only — there is no tombstone or key-delete
// mechanism.
func (s *Store[T]) PollRecords(
	ctx context.Context,
	since Offset,
	maxEntries int32,
	opts ...QueryOption,
) ([]T, Offset, error) {
	o := &queryOpts{}
	for _, opt := range opts {
		opt(o)
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
		uris[i] = sqlQuote(s.s3URI(e.DataPath))
	}

	scanExpr := fmt.Sprintf(
		"SELECT * FROM read_parquet([%s], "+
			"union_by_name=true)",
		strings.Join(uris, ", "))

	query, err := s.buildWrappedQuery(ctx, scanExpr,
		"SELECT * FROM "+s.cfg.TableAlias, o.IncludeHistory)
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
