package s3sql

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ueisele/s3store/internal/core"
)

// listFanOutConcurrency caps parallel LIST calls in the *Many
// multi-pattern path. Matches s3parquet's pollDownloadConcurrency
// — well below S3's 5500 LIST/s-per-prefix limit and the AWS SDK
// default MaxConnsPerHost.
const listFanOutConcurrency = 8

// listMatchingParquet LISTs every parquet object under the
// plan's ListPrefix and returns the subset whose Hive key
// matches the plan's predicate. Mirrors s3parquet's helper of
// the same name so both read paths see identical file-selection
// semantics.
//
// Used by the multi-pattern fast path on *Many methods: each
// pattern becomes one plan, each plan gets one LIST, and the
// resulting file URIs go into a single read_parquet([...]) call
// so DuckDB plans once over the full set.
func (s *Reader[T]) listMatchingParquet(
	ctx context.Context, plan *core.ReadPlan,
) ([]string, error) {
	paginator := s3.NewListObjectsV2Paginator(s.cfg.Target.S3Client,
		&s3.ListObjectsV2Input{
			Bucket: aws.String(s.cfg.Target.Bucket),
			Prefix: aws.String(plan.ListPrefix),
		})

	var keys []string
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf(
				"s3sql: list data files: %w", err)
		}
		for _, obj := range page.Contents {
			objKey := aws.ToString(obj.Key)
			if !strings.HasSuffix(objKey, ".parquet") {
				continue
			}
			hiveKey, ok := core.HiveKeyOfDataFile(objKey, s.dataPath)
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

// listAllMatchingURIs runs listMatchingParquet across every
// pattern with bounded concurrency and returns the deduplicated
// union of file URIs (s3://bucket/key form, ready to pass into
// DuckDB's read_parquet). Each pattern becomes one ReadPlan; a
// file matched by two patterns is returned exactly once.
//
// method identifies the caller (e.g. "ReadMany", "QueryMany")
// so pattern-validation errors surface with the right entry
// point in the message. Callers should already have dropped
// literal-duplicate patterns via core.DedupePatterns — this
// function doesn't repeat that work.
func (s *Reader[T]) listAllMatchingURIs(
	ctx context.Context, patterns []string, method string,
) ([]string, error) {
	plans := make([]*core.ReadPlan, len(patterns))
	for i, p := range patterns {
		plan, err := core.BuildReadPlan(
			p, s.dataPath, s.cfg.Target.PartitionKeyParts)
		if err != nil {
			return nil, fmt.Errorf(
				"s3sql: %s pattern %d %q: %w",
				method, i, p, err)
		}
		plans[i] = plan
	}

	keys, err := core.RunPlansConcurrent(ctx, plans,
		listFanOutConcurrency, s.listMatchingParquet)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, nil
	}

	uris := make([]string, len(keys))
	for i, k := range keys {
		uris[i] = s.s3URI(k)
	}
	return uris, nil
}
