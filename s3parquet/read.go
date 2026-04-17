package s3parquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"
	"github.com/ueisele/s3store/internal/core"
)

// pollDownloadConcurrency caps the number of parquet files
// PollRecords downloads in parallel. Fixed rather than
// configurable in v1 — revisit only if profiling shows a real
// need.
const pollDownloadConcurrency = 8

// Read returns all records whose data files match the given
// key pattern, optionally deduplicated to latest-per-entity
// when EntityKeyOf and VersionOf are configured.
//
// Accepts the same glob grammar as s3sql.Read: whole-segment
// "*" and a single trailing "*" inside a value.
//
// Memory: all matching records are buffered before dedup/return.
// For unbounded reads, use PollRecords to stream incrementally.
func (s *Store[T]) Read(
	ctx context.Context, keyPattern string, opts ...core.QueryOption,
) ([]T, error) {
	var o core.QueryOpts
	o.Apply(opts...)

	plan, err := buildReadPlan(keyPattern, s.dataPath, s.cfg.KeyParts)
	if err != nil {
		return nil, err
	}

	keys, err := s.listMatchingParquet(ctx, plan)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, nil
	}

	records, err := s.downloadAndDecodeAll(ctx, keys)
	if err != nil {
		return nil, err
	}

	if o.IncludeHistory || !s.cfg.dedupEnabled() {
		return records, nil
	}
	return dedupLatest(records, s.cfg.EntityKeyOf, s.cfg.VersionOf), nil
}

// PollRecords returns a flat slice of typed records from the
// files referenced by up to maxEntries refs after the offset.
// Downloads run in parallel (limit pollDownloadConcurrency).
//
// By default applies latest-per-entity dedup within the batch
// (consistent with Read). Pass WithHistory() to disable dedup
// and get every record in ref order.
//
// When dedup is disabled (either no EntityKeyOf/VersionOf, or
// WithHistory()), the returned records follow ref order
// (= timestamp order) and then parquet-file row order within
// each ref.
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

	keys := make([]string, len(entries))
	for i, e := range entries {
		keys[i] = e.DataPath
	}

	records, err := s.downloadAndDecodeAll(ctx, keys)
	if err != nil {
		return nil, since, err
	}

	if o.IncludeHistory || !s.cfg.dedupEnabled() {
		return records, newOffset, nil
	}
	return dedupLatest(records, s.cfg.EntityKeyOf, s.cfg.VersionOf),
		newOffset, nil
}

// listMatchingParquet lists every parquet object under the
// plan's ListPrefix and returns the subset whose Hive key
// matches the plan's predicate. S3 LIST handles pagination; the
// predicate runs in memory per key.
func (s *Store[T]) listMatchingParquet(
	ctx context.Context, plan *readPlan,
) ([]string, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.cfg.Bucket),
		Prefix: aws.String(plan.ListPrefix),
	}
	paginator := s3.NewListObjectsV2Paginator(s.s3, input)

	var keys []string
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf(
				"s3parquet: list data files: %w", err)
		}
		for _, obj := range page.Contents {
			objKey := aws.ToString(obj.Key)
			if !strings.HasSuffix(objKey, ".parquet") {
				continue
			}
			hiveKey, ok := hiveKeyOfDataFile(objKey, s.dataPath)
			if !ok {
				continue
			}
			if plan.Match(hiveKey) {
				keys = append(keys, objKey)
			}
		}
	}
	return keys, nil
}

// downloadAndDecodeAll fans out a bounded set of parallel
// downloads, decodes each parquet file into []T, and returns
// the concatenated result. Preserves the input key order so
// callers who don't dedup observe a deterministic stream.
func (s *Store[T]) downloadAndDecodeAll(
	ctx context.Context, keys []string,
) ([]T, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	results := make([][]T, len(keys))
	errs := make([]error, len(keys))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sem := make(chan struct{}, pollDownloadConcurrency)
	var wg sync.WaitGroup
	for i, key := range keys {
		wg.Add(1)
		go func(i int, key string) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				errs[i] = ctx.Err()
				return
			}
			defer func() { <-sem }()

			data, err := s.getObjectBytes(ctx, key)
			if err != nil {
				errs[i] = fmt.Errorf(
					"s3parquet: get %s: %w", key, err)
				cancel()
				return
			}
			recs, err := decodeParquet[T](data)
			if err != nil {
				errs[i] = fmt.Errorf(
					"s3parquet: decode %s: %w", key, err)
				cancel()
				return
			}
			results[i] = recs
		}(i, key)
	}
	wg.Wait()

	// First-error wins: we cancel on the first failure so later
	// goroutines see ctx.Err(); return the earliest "real"
	// error rather than a cancellation.
	for _, e := range errs {
		if e == nil || e == context.Canceled {
			continue
		}
		return nil, e
	}

	// Count for preallocation.
	total := 0
	for _, r := range results {
		total += len(r)
	}
	out := make([]T, 0, total)
	for _, r := range results {
		out = append(out, r...)
	}
	return out, nil
}

// decodeParquet reads all rows of a parquet file into []T. T
// must be parquet-go-friendly (field-tagged, primitive-backed).
func decodeParquet[T any](data []byte) ([]T, error) {
	reader := parquet.NewGenericReader[T](bytes.NewReader(data))
	defer reader.Close()

	total := reader.NumRows()
	if total == 0 {
		return nil, nil
	}

	out := make([]T, total)
	n, err := reader.Read(out)
	if err != nil && !errors.Is(err, io.EOF) {
		// parquet-go returns io.EOF at the end of the file;
		// treat that as a clean termination, not an error.
		return nil, err
	}
	return out[:n], nil
}

// dedupLatest keeps the record with the maximum version per
// entity, in the order each entity was first seen. Stable under
// equal versions: earlier occurrences win ties, so callers who
// rely on first-write semantics aren't surprised by later
// duplicates.
func dedupLatest[T any](
	records []T,
	entityKey func(T) string,
	version func(T) int64,
) []T {
	if len(records) == 0 {
		return records
	}
	type slot struct {
		index   int
		version int64
	}
	seen := make(map[string]slot, len(records))
	order := make([]string, 0, len(records))
	for i, r := range records {
		k := entityKey(r)
		v := version(r)
		cur, ok := seen[k]
		if !ok {
			seen[k] = slot{index: i, version: v}
			order = append(order, k)
			continue
		}
		if v > cur.version {
			seen[k] = slot{index: i, version: v}
		}
	}
	out := make([]T, len(order))
	for i, k := range order {
		out[i] = records[seen[k].index]
	}
	return out
}
