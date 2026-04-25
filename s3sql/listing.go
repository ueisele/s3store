package s3sql

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"
	"github.com/ueisele/s3store/internal/core"
)

// listMatchingParquet LISTs every parquet object under the
// plan's ListPrefix and returns the subset whose Hive key
// matches the plan's predicate, carrying each object's
// LastModified so downstream filters (WithIdempotentRead) can
// reason about write time without a second S3 round-trip.
// Mirrors s3parquet's helper of the same name so both read paths
// see identical file-selection semantics.
//
// Used by the multi-pattern fast path on *Many methods: each
// pattern becomes one plan, each plan gets one LIST, and the
// resulting file URIs go into a single read_parquet([...]) call
// so DuckDB plans once over the full set.
//
// Carries ReaderConfig.ConsistencyControl on the LIST so the file
// set linearizes with Writer-side PUTs on strong-global /
// strong-site StorageGRID. Empty → no header → bucket default
// (correct on AWS S3 / MinIO).
func (s *Reader[T]) listMatchingParquet(
	ctx context.Context, plan *core.ReadPlan,
) ([]core.KeyMeta, error) {
	paginator := s3.NewListObjectsV2Paginator(s.cfg.Target.S3Client(),
		&s3.ListObjectsV2Input{
			Bucket: aws.String(s.cfg.Target.Bucket()),
			Prefix: aws.String(plan.ListPrefix),
		})

	var apiOpts []func(*middleware.Stack) error
	if s.cfg.ConsistencyControl != "" {
		apiOpts = []func(*middleware.Stack) error{
			core.AddHeaderMiddleware(
				"Consistency-Control", string(s.cfg.ConsistencyControl)),
		}
	}

	var out []core.KeyMeta
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx, func(o *s3.Options) {
			o.APIOptions = append(o.APIOptions, apiOpts...)
		})
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
				out = append(out, core.KeyMeta{
					Key:        objKey,
					InsertedAt: aws.ToTime(obj.LastModified),
				})
			}
		}
	}
	return out, nil
}

// listAllMatchingURIs runs listMatchingParquet across every
// pattern with bounded concurrency, applies the idempotent-read
// filter when opts.IdempotentReadToken is set, and returns the
// deduplicated union of file URIs (s3://bucket/key form, ready to
// pass into DuckDB's read_parquet).
//
// method identifies the caller (e.g. "ReadMany", "QueryMany")
// so pattern-validation errors surface with the right entry
// point in the message. Callers should already have dropped
// literal-duplicate patterns via core.DedupePatterns — this
// function doesn't repeat that work.
func (s *Reader[T]) listAllMatchingURIs(
	ctx context.Context, patterns []string,
	opts *core.QueryOpts, method string,
) ([]string, error) {
	plans := make([]*core.ReadPlan, len(patterns))
	for i, p := range patterns {
		plan, err := core.BuildReadPlan(
			p, s.dataPath, s.cfg.Target.PartitionKeyParts())
		if err != nil {
			return nil, fmt.Errorf(
				"s3sql: %s pattern %d %q: %w",
				method, i, p, err)
		}
		plans[i] = plan
	}

	keys, err := core.RunPlansConcurrent(ctx, plans,
		s.cfg.Target.EffectiveMaxInflightRequests(),
		s.listMatchingParquet,
		func(k core.KeyMeta) string { return k.Key })
	if err != nil {
		return nil, err
	}
	keys, err = s.applyIdempotentRead(keys, opts)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, nil
	}

	uris := make([]string, len(keys))
	for i, k := range keys {
		uris[i] = s.s3URI(k.Key)
	}
	return uris, nil
}

// applyIdempotentRead validates opts.IdempotentReadToken when set
// and filters keys accordingly. Runs at LIST time with no S3 call;
// see core.ApplyIdempotentRead for the per-partition self-
// exclusion + later-write-exclusion contract.
func (s *Reader[T]) applyIdempotentRead(
	keys []core.KeyMeta, opts *core.QueryOpts,
) ([]core.KeyMeta, error) {
	if opts.IdempotentReadToken == "" {
		return keys, nil
	}
	if err := core.ValidateIdempotencyToken(
		opts.IdempotentReadToken); err != nil {
		return nil, fmt.Errorf(
			"s3sql: WithIdempotentRead: %w", err)
	}
	return core.ApplyIdempotentRead(
		keys, s.dataPath, opts.IdempotentReadToken), nil
}
