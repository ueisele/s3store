package s3parquet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"maps"
	"path"
	"reflect"
	"slices"
	"sync"
	"time"

	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/parquet-go/parquet-go"
	"github.com/ueisele/s3store/internal/core"
)

// ReadIterWhere streams records matching pattern, filtering by a
// predicate evaluated against each parquet row group's chunk-
// level statistics. Row groups rejected by the predicate are
// never fetched — only the footer + accepted groups' column
// chunks cross the wire.
//
// Use when files are large (>~10 MB) and the filter has high
// selectivity (e.g. one customer out of hundreds, where files
// are written with a row group per customer and the customer
// column's min/max prunes cleanly). For small files the ranged-
// GET overhead makes this slower than plain ReadIter; keep the
// unfiltered method in that case.
//
// Dedup and ordering semantics match ReadIter: per-partition
// dedup by default (with the same "EntityKeyOf doesn't span
// partitions" invariant), or no dedup under WithHistory().
// WithReadAheadPartitions is honored identically.
//
// A nil predicate accepts every row group — use plain ReadIter
// in that case to avoid the HEAD+range-GET overhead.
func (s *Reader[T]) ReadIterWhere(
	ctx context.Context, pattern string,
	predicate RowGroupPredicate, opts ...QueryOption,
) iter.Seq2[T, error] {
	return s.ReadManyIterWhere(ctx, []string{pattern}, predicate, opts...)
}

// ReadManyIterWhere is the multi-pattern sibling of
// ReadIterWhere. Same per-partition dedup contract and row-group
// filtering.
func (s *Reader[T]) ReadManyIterWhere(
	ctx context.Context, patterns []string,
	predicate RowGroupPredicate, opts ...QueryOption,
) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var o core.QueryOpts
		o.Apply(opts...)

		patterns = dedupePatterns(patterns)
		if len(patterns) == 0 {
			return
		}

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		plans := make([]*readPlan, len(patterns))
		for i, p := range patterns {
			plan, err := buildReadPlan(p, s.dataPath, s.cfg.Target.PartitionKeyParts)
			if err != nil {
				yield(*new(T), fmt.Errorf(
					"s3parquet: ReadManyIterWhere pattern %d %q: %w",
					i, p, err))
				return
			}
			plans[i] = plan
		}

		keys, err := s.listAllMatchingParquet(ctx, plans)
		if err != nil {
			yield(*new(T), err)
			return
		}
		if len(keys) == 0 {
			return
		}

		readAhead := o.ReadAheadPartitions
		if readAhead < 0 {
			readAhead = 0
		}
		s.streamByPartitionFiltered(
			ctx, keys, o.IncludeHistory, readAhead, predicate, yield)
	}
}

// streamByPartitionFiltered mirrors streamByPartition but uses
// the row-group-filtered file reader for each partition's files.
// Inlined here rather than factored through streamByPartition so
// the filtered code path stays fully separated from the
// unfiltered one — zero risk of regressing existing Read/
// ReadIter behavior.
func (s *Reader[T]) streamByPartitionFiltered(
	ctx context.Context, keys []core.KeyMeta,
	includeHistory bool, readAhead int,
	predicate RowGroupPredicate, yield func(T, error) bool,
) {
	byPartition := s.groupKeysByPartition(keys)
	partitions := slices.Sorted(maps.Keys(byPartition))

	if readAhead <= 0 {
		for _, p := range partitions {
			files := byPartition[p]
			sortKeyMetasByKey(files)
			recs, err := s.downloadFilteredAll(ctx, files, predicate)
			if err != nil {
				yield(*new(T), err)
				return
			}
			if !s.emitPartition(recs, includeHistory, yield) {
				return
			}
		}
		return
	}

	pipeline := make(chan partitionBatch[T], readAhead)
	go func() {
		defer close(pipeline)
		for _, p := range partitions {
			files := byPartition[p]
			sortKeyMetasByKey(files)
			recs, err := s.downloadFilteredAll(ctx, files, predicate)
			select {
			case pipeline <- partitionBatch[T]{recs: recs, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	for b := range pipeline {
		if b.err != nil {
			yield(*new(T), b.err)
			return
		}
		if !s.emitPartition(b.recs, includeHistory, yield) {
			return
		}
	}
}

// downloadFilteredAll is the filtered counterpart of
// downloadAndDecodeAll: fans out one goroutine per file, each
// opens the file through a ranged-GET ReaderAt, applies the
// row-group predicate, and decodes only the accepted row
// groups. Sorts input by key for deterministic download order.
func (s *Reader[T]) downloadFilteredAll(
	ctx context.Context, keys []core.KeyMeta, predicate RowGroupPredicate,
) ([]versionedRecord[T], error) {
	if len(keys) == 0 {
		return nil, nil
	}

	sortKeyMetasByKey(keys)

	results := make([][]versionedRecord[T], len(keys))
	errs := make([]error, len(keys))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sem := make(chan struct{}, pollDownloadConcurrency)
	var wg sync.WaitGroup
	for i, km := range keys {
		wg.Add(1)
		go func(i int, km core.KeyMeta) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				errs[i] = ctx.Err()
				return
			}
			defer func() { <-sem }()

			recs, err := s.downloadFilteredOne(ctx, km, predicate)
			if err != nil {
				errs[i] = err
				cancel()
				return
			}
			results[i] = recs
		}(i, km)
	}
	wg.Wait()

	for _, e := range errs {
		if e == nil || errors.Is(e, context.Canceled) {
			continue
		}
		return nil, e
	}

	total := 0
	for _, r := range results {
		total += len(r)
	}
	out := make([]versionedRecord[T], 0, total)
	for _, r := range results {
		out = append(out, r...)
	}
	return out, nil
}

// downloadFilteredOne is the per-file body for the filtered
// path: HEAD for the file size, open via parquet.OpenFile over a
// ranged-GET ReaderAt, iterate row groups applying the
// predicate, decode only accepted groups.
//
// NoSuchKey and OnMissingData follow the same skip-and-notify
// semantics as downloadAndDecodeOne so a dangling ref or
// LIST-to-GET race doesn't poison the read.
func (s *Reader[T]) downloadFilteredOne(
	ctx context.Context, km core.KeyMeta, predicate RowGroupPredicate,
) ([]versionedRecord[T], error) {
	key := km.Key
	fileName := path.Base(key)

	size, err := s.cfg.Target.size(
		ctx, key, withConsistencyControl(s.cfg.ConsistencyControl))
	if err != nil {
		if _, ok := errors.AsType[*s3types.NotFound](err); ok {
			if s.cfg.OnMissingData != nil {
				s.cfg.OnMissingData(key)
			}
			return nil, nil
		}
		return nil, fmt.Errorf("s3parquet: head %s: %w", key, err)
	}

	ra := &s3ReaderAt{
		ctx:         ctx,
		target:      s.cfg.Target,
		key:         key,
		consistency: s.cfg.ConsistencyControl,
	}
	f, err := parquet.OpenFile(ra, size)
	if err != nil {
		// A LIST-to-HEAD-to-GET race can land NoSuchKey here if
		// cleanup ran between HEAD and footer-fetch. Skip-and-
		// notify matches downloadAndDecodeOne's behavior so one
		// disappearing ref doesn't poison the whole read.
		if _, ok := errors.AsType[*s3types.NoSuchKey](err); ok {
			if s.cfg.OnMissingData != nil {
				s.cfg.OnMissingData(key)
			}
			return nil, nil
		}
		return nil, fmt.Errorf(
			"s3parquet: open file %s: %w", key, err)
	}

	var recs []T
	for _, rg := range f.RowGroups() {
		if predicate != nil && !predicate(RowGroup{
			inner:  rg,
			schema: f.Schema(),
		}) {
			continue
		}
		buf := make([]T, rg.NumRows())
		reader := parquet.NewGenericRowGroupReader[T](rg)
		n, err := reader.Read(buf)
		if err != nil && !errors.Is(err, io.EOF) {
			_ = reader.Close()
			// Same LIST-to-GET race as above — if cleanup ran
			// between footer-fetch and this row group's column
			// chunks, NoSuchKey can surface here too.
			if _, ok := errors.AsType[*s3types.NoSuchKey](err); ok {
				if s.cfg.OnMissingData != nil {
					s.cfg.OnMissingData(key)
				}
				return nil, nil
			}
			return nil, fmt.Errorf(
				"s3parquet: decode row group in %s: %w", key, err)
		}
		_ = reader.Close()
		recs = append(recs, buf[:n]...)
	}

	versioned := make([]versionedRecord[T], len(recs))
	for j, r := range recs {
		ia := km.InsertedAt
		if s.insertedAtFieldIndex != nil {
			ia = reflect.ValueOf(&recs[j]).Elem().
				FieldByIndex(s.insertedAtFieldIndex).
				Interface().(time.Time)
		}
		versioned[j] = versionedRecord[T]{
			rec:        r,
			insertedAt: ia,
			fileName:   fileName,
		}
	}
	return versioned, nil
}

// s3ReaderAt adapts S3Target.getRange to io.ReaderAt so parquet-
// go can do random-access reads on an S3 object. One instance
// per file: ctx is bound at construction so the reader doesn't
// need ctx on every ReadAt call (interface signature forbids it).
type s3ReaderAt struct {
	ctx         context.Context
	target      S3Target
	key         string
	consistency ConsistencyLevel
}

// ReadAt issues a ranged GET for [off, off+len(p)) and copies
// the response into p. parquet-go calls this many times per
// file: once for the footer, then once per column chunk in each
// accepted row group.
func (r *s3ReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	body, err := r.target.getRange(
		r.ctx, r.key, off, off+int64(len(p)),
		withConsistencyControl(r.consistency))
	if err != nil {
		return 0, err
	}
	n := copy(p, body)
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}
