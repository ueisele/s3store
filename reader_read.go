package s3store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync/atomic"

	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/parquet-go/parquet-go"
)

// Read returns all records whose data files match any of the
// given key patterns. Patterns use the grammar described in
// validateKeyPattern; pass multiple when the target set isn't
// a Cartesian product (e.g. (period=A, customer=X) and
// (period=B, customer=Y) but not the off-diagonal pairs).
//
// When EntityKeyOf and VersionOf are configured, the result is
// deduplicated globally to the latest version per entity across
// the union (pass WithHistory to opt out). Overlapping patterns
// are safe — each parquet file is fetched and decoded at most once.
//
// All records are buffered before return — for unbounded reads,
// use ReadIter instead. Empty patterns slice returns (nil, nil);
// a malformed pattern fails with the offending index.
func (s *Reader[T]) Read(
	ctx context.Context, keyPatterns []string, opts ...QueryOption,
) (out []T, err error) {
	scope := s.cfg.Target.metrics.methodScope(ctx, methodRead)
	defer scope.end(&err)
	var o QueryOpts
	o.Apply(opts...)

	keys, err := ResolvePatterns(
		ctx, s.cfg.Target, keyPatterns, &o)
	if err != nil {
		return nil, fmt.Errorf("s3store: Read %w", err)
	}
	if len(keys) == 0 {
		return nil, nil
	}

	records, bytesTotal, err := s.downloadAndDecodeAll(ctx, keys)
	if err != nil {
		return nil, err
	}
	out = s.sortAndCollect(records, o.IncludeHistory)
	scope.addRecords(int64(len(out)))
	scope.addFiles(int64(len(keys)))
	scope.addBytes(bytesTotal)
	return out, nil
}

// sortKeyMetasByKey orders a partition's files by their S3 key
// for deterministic download order. Emission order is then
// decided by emitPartition's record-content sort, so this only
// affects the internal download pipeline's determinism.
func sortKeyMetasByKey(files []KeyMeta) {
	sort.Slice(files, func(i, j int) bool {
		return files[i].Key < files[j].Key
	})
}

// identityKey is the keyOf function for []string fan-outs — the
// element is itself the dedup key. Used by index/backfill
// callers that union per-pattern lookup results.
func identityKey(s string) string { return s }

// downloadAndDecodeAll fans out a bounded set of parallel
// downloads, decodes each parquet file into []T, and returns
// the concatenated result plus the sum of compressed body bytes
// downloaded across every file. The input is sorted by S3 key
// for deterministic download order; user-visible emission order
// is set later by the caller's sortAndIterate.
func (s *Reader[T]) downloadAndDecodeAll(
	ctx context.Context, keys []KeyMeta,
) ([]T, int64, error) {
	if len(keys) == 0 {
		return nil, 0, nil
	}

	sortKeyMetasByKey(keys)

	results := make([][]T, len(keys))
	var bytesTotal atomic.Int64
	if err := fanOut(ctx, keys,
		s.cfg.Target.EffectiveMaxInflightRequests(),
		s.cfg.Target.metrics,
		func(ctx context.Context, i int, km KeyMeta) error {
			recs, n, err := s.downloadAndDecodeOne(ctx, km)
			if err != nil {
				return err
			}
			bytesTotal.Add(n)
			results[i] = recs
			return nil
		}); err != nil {
		return nil, 0, err
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
	return out, bytesTotal.Load(), nil
}

// downloadAndDecodeOne is the per-file body shared by
// downloadAndDecodeAll. Pulls one parquet object from S3 and
// decodes it into []T. Returns the body byte count alongside
// the records so the caller can sum bytes across the fan-out for
// the s3store.read.bytes metric.
//
// Returns (nil, 0, nil) when the object is missing — a dangling
// ref or a LIST-to-GET race. The OnMissingData hook is invoked
// so the caller can log/count without failing the read.
func (s *Reader[T]) downloadAndDecodeOne(
	ctx context.Context, km KeyMeta,
) ([]T, int64, error) {
	key := km.Key

	data, err := s.cfg.Target.get(ctx, key)
	if err != nil {
		if _, ok := errors.AsType[*s3types.NoSuchKey](err); ok {
			if s.cfg.OnMissingData != nil {
				s.cfg.OnMissingData(key)
			}
			return nil, 0, nil
		}
		return nil, 0, fmt.Errorf("s3store: get %s: %w", key, err)
	}
	recs, err := decodeParquet[T](data)
	if err != nil {
		return nil, 0, fmt.Errorf("s3store: decode %s: %w", key, err)
	}
	return recs, int64(len(data)), nil
}

// decodeParquet reads all rows of a parquet file into []T. T
// must be parquet-go-friendly (field-tagged, primitive-backed).
func decodeParquet[T any](data []byte) ([]T, error) {
	reader := parquet.NewGenericReader[T](bytes.NewReader(data))
	defer func() { _ = reader.Close() }()

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
