package s3sql

import (
	"context"
	"fmt"

	"github.com/ueisele/s3store/internal/core"
)

// listAllMatchingURIs resolves every pattern to its file set via
// the shared S3Target.ListDataFilesMany helper, applies the
// idempotent-read filter when opts.IdempotentReadToken is set,
// and returns the deduplicated union of file URIs (s3://bucket/key
// form, ready to pass into DuckDB's read_parquet).
//
// method identifies the caller (e.g. "Query", "QueryMany") so
// pattern-validation errors surface with the right entry point in
// the message. Callers should already have dropped literal-
// duplicate patterns via core.DedupePatterns — this function
// doesn't repeat that work.
func (s *Reader) listAllMatchingURIs(
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

	keys, err := s.cfg.Target.ListDataFilesMany(
		ctx, plans, s.cfg.ConsistencyControl)
	if err != nil {
		return nil, fmt.Errorf("s3sql: %w", err)
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
func (s *Reader) applyIdempotentRead(
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
