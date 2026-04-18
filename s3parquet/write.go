package s3parquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/ueisele/s3store/internal/core"
)

// writeCleanupTimeout bounds best-effort cleanup work (HEAD /
// DELETE) on the ref-PUT failure path, so the caller's context
// being cancelled doesn't prevent us from either confirming a
// lost ack or removing an orphan parquet.
const writeCleanupTimeout = 5 * time.Second

// Write extracts the key from each record via PartitionKeyOf,
// groups by key, and writes one Parquet file + stream ref per
// key. Returns a WriteResult per group.
//
// An empty records slice is a no-op: (nil, nil) is returned so
// callers don't have to guard against batch-pipeline edge
// cases.
func (s *Store[T]) Write(
	ctx context.Context, records []T,
) ([]WriteResult, error) {
	if len(records) == 0 {
		return nil, nil
	}
	if s.cfg.PartitionKeyOf == nil {
		return nil, fmt.Errorf(
			"s3parquet: PartitionKeyOf is required for Write; " +
				"use WriteWithKey for explicit keys")
	}

	grouped := s.groupByKey(records)

	// Sorted key iteration keeps the returned results (and
	// the sequence of S3 PUTs) deterministic across runs,
	// instead of following map iteration order.
	var results []WriteResult
	for _, key := range slices.Sorted(maps.Keys(grouped)) {
		result, err := s.WriteWithKey(ctx, key, grouped[key])
		if err != nil {
			return results, err
		}
		results = append(results, *result)
	}
	return results, nil
}

// WriteWithKey encodes records as Parquet, uploads to S3, and
// writes an empty ref file with all metadata in the key name.
// Ref timestamp is generated AFTER the data PUT completes.
//
// If the ref PUT fails after the data PUT succeeded, WriteWithKey
// issues a HEAD on the ref key to disambiguate a lost ack (ref
// actually got written, we just lost the response) from a real
// failure. On a real failure it best-effort deletes the orphan
// parquet; if that cleanup also fails, the returned error
// includes the orphan data path so the operator can clean up.
func (s *Store[T]) WriteWithKey(
	ctx context.Context, key string, records []T,
) (*WriteResult, error) {
	if len(records) == 0 {
		return nil, nil
	}
	if err := s.validateKey(key); err != nil {
		return nil, err
	}

	parquetBytes, err := encodeParquet(
		records, s.cfg.BloomFilterColumns, s.compressionCodec)
	if err != nil {
		return nil, fmt.Errorf(
			"s3parquet: parquet encode: %w", err)
	}

	// Compute marker paths up-front so a bad IndexDef.Of fails the
	// whole Write before we touch S3, matching how validateKey
	// aborts on a malformed partition key.
	markerPaths, err := s.collectIndexMarkerPaths(records)
	if err != nil {
		return nil, err
	}

	shortID := uuid.New().String()[:8]

	// Capture the write timestamp before the data PUT so both
	// the data filename and the ref filename share a single
	// tsMicros — the data file is chronologically sortable in
	// S3 LIST without consulting refs, and the invariant
	// "ref visible implies data exists" still holds because
	// the ref PUT is sequenced after the data PUT.
	tsMicros := time.Now().UnixMicro()

	dataKey := core.BuildDataFilePath(s.dataPath, key, tsMicros, shortID)
	if err := s.putObject(
		ctx, dataKey, parquetBytes,
		"application/octet-stream",
	); err != nil {
		return nil, fmt.Errorf(
			"s3parquet: put data: %w", err)
	}

	// Index markers are written after data (so a successful
	// marker implies the backing data file exists) and before
	// the ref (so Poll's commit semantics are unchanged). If any
	// marker PUT fails we delete the orphan data and return —
	// any markers that landed before the failure stay as
	// orphans, which Lookup tolerates.
	if err := s.putMarkersParallel(ctx, markerPaths); err != nil {
		cleanupCtx, cancel := context.WithTimeout(
			context.Background(), writeCleanupTimeout)
		defer cancel()
		if delErr := s.deleteObject(
			cleanupCtx, dataKey,
		); delErr != nil {
			return nil, fmt.Errorf(
				"s3parquet: put index markers: %w "+
					"(orphan data at %s: %v)",
				err, dataKey, delErr)
		}
		return nil, fmt.Errorf(
			"s3parquet: put index markers: %w", err)
	}

	refKey := core.EncodeRefKey(s.refPath, tsMicros, shortID, key)

	result := &WriteResult{
		Offset:   Offset(refKey),
		DataPath: dataKey,
		RefPath:  refKey,
	}

	putErr := s.putObject(
		ctx, refKey, []byte{}, "application/octet-stream")
	if putErr == nil {
		return result, nil
	}

	// Ref PUT failed. Disambiguate lost-ack from a real failure
	// using a bounded, caller-independent context so cleanup
	// still completes if the caller has cancelled.
	cleanupCtx, cancel := context.WithTimeout(
		context.Background(), writeCleanupTimeout)
	defer cancel()

	if exists, headErr := s.objectExists(
		cleanupCtx, refKey,
	); headErr == nil && exists {
		// Ref actually got written — we just lost the ack.
		return result, nil
	}

	if delErr := s.deleteObject(
		cleanupCtx, dataKey,
	); delErr != nil {
		return nil, fmt.Errorf(
			"s3parquet: put ref: %w (orphan data at %s: %v)",
			putErr, dataKey, delErr)
	}

	return nil, fmt.Errorf("s3parquet: put ref: %w", putErr)
}

func (s *Store[T]) groupByKey(records []T) map[string][]T {
	grouped := make(map[string][]T)
	for _, r := range records {
		key := s.cfg.PartitionKeyOf(r)
		grouped[key] = append(grouped[key], r)
	}
	return grouped
}

// validateKey enforces that the key is a "/"-delimited sequence
// of exactly len(PartitionKeyParts) Hive-style segments, each in the
// form "PartitionKeyParts[i]=<non-empty value>", in the configured order.
//
// Values may contain '=' (we split on the first '=' only) but
// cannot contain '/', be empty, or contain ".." — the latter is
// reserved by the key-pattern grammar as the range separator
// (FROM..TO), so a partition value containing ".." would be
// unaddressable on read. Catches PartitionKeyOf bugs before they
// corrupt the S3 layout.
func (s *Store[T]) validateKey(key string) error {
	segments := strings.Split(key, "/")
	if len(segments) != len(s.cfg.PartitionKeyParts) {
		return fmt.Errorf(
			"s3parquet: key %q has %d segments, "+
				"expected %d (%v)",
			key, len(segments),
			len(s.cfg.PartitionKeyParts), s.cfg.PartitionKeyParts)
	}
	for i, seg := range segments {
		part := s.cfg.PartitionKeyParts[i]
		prefix := part + "="
		if !strings.HasPrefix(seg, prefix) {
			return fmt.Errorf(
				"s3parquet: key %q segment %d is %q, "+
					"expected prefix %q",
				key, i, seg, prefix)
		}
		value := seg[len(prefix):]
		if err := core.ValidateHivePartitionValue(value); err != nil {
			return fmt.Errorf(
				"s3parquet: key %q segment %d (%q): %w",
				key, i, part, err)
		}
	}
	return nil
}

// markerPutConcurrency caps the number of parallel marker PUTs
// per WriteWithKey. Markers are tiny (empty objects), so request
// rate rather than bandwidth is the limit. 8 matches the AWS SDK
// default MaxConnsPerHost, mirroring pollDownloadConcurrency.
const markerPutConcurrency = 8

// collectIndexMarkerPaths iterates every registered index over
// every record in the batch and returns the deduplicated set of
// marker S3 keys. Dedup is via map[string]struct{} on the full
// path, which is correct because different indexes live under
// different _index/<name>/ prefixes — no cross-index collisions.
func (s *Store[T]) collectIndexMarkerPaths(records []T) ([]string, error) {
	if len(s.indexes) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{})
	for _, idx := range s.indexes {
		for _, rec := range records {
			paths, err := idx.pathsOf(rec)
			if err != nil {
				return nil, fmt.Errorf(
					"s3parquet: index %q: %w", idx.name, err)
			}
			for _, p := range paths {
				seen[p] = struct{}{}
			}
		}
	}
	if len(seen) == 0 {
		return nil, nil
	}
	out := make([]string, 0, len(seen))
	for p := range seen {
		out = append(out, p)
	}
	return out, nil
}

// putMarkersParallel issues PUTs for every path with bounded
// concurrency and cancel-on-first-error. Returns the earliest
// real (non-cancellation) error observed; already-started PUTs
// run to completion, those still blocked on the semaphore see
// the cancelled context and bail. Partial success is an accepted
// outcome — Lookup tolerates orphan markers.
func (s *Store[T]) putMarkersParallel(
	ctx context.Context, paths []string,
) error {
	if len(paths) == 0 {
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sem := make(chan struct{}, markerPutConcurrency)
	errs := make([]error, len(paths))
	var wg sync.WaitGroup
	for i, p := range paths {
		wg.Add(1)
		go func(i int, p string) {
			defer wg.Done()
			// Acquire the semaphore inside the goroutine so a
			// parent-ctx cancel or sibling-goroutine failure
			// unblocks us promptly instead of letting the main
			// loop keep spawning PUTs.
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				errs[i] = ctx.Err()
				return
			}
			defer func() { <-sem }()

			if err := s.putObject(
				ctx, p, nil, "application/octet-stream",
			); err != nil {
				errs[i] = err
				cancel()
			}
		}(i, p)
	}
	wg.Wait()

	// First real error wins; skip cancellations so we report the
	// root-cause failure instead of the cancellation it triggered
	// in sibling goroutines.
	for _, err := range errs {
		if err == nil || errors.Is(err, context.Canceled) {
			continue
		}
		return err
	}
	return nil
}

// encodeParquet writes records to a parquet byte stream using
// the given compression codec (never nil — Store.New resolves a
// snappy default). When bloomFilterColumns is non-empty,
// per-row-group split-block bloom filters are emitted for those
// columns at bloomFilterBitsPerValue (10 bits/value ≈ 1%
// false-positive rate).
func encodeParquet[T any](
	records []T,
	bloomFilterColumns []string,
	codec compress.Codec,
) ([]byte, error) {
	var buf bytes.Buffer
	opts := []parquet.WriterOption{parquet.Compression(codec)}
	if len(bloomFilterColumns) > 0 {
		filters := make(
			[]parquet.BloomFilterColumn, len(bloomFilterColumns))
		for i, col := range bloomFilterColumns {
			filters[i] = parquet.SplitBlockFilter(
				bloomFilterBitsPerValue, col)
		}
		opts = append(opts, parquet.BloomFilters(filters...))
	}
	writer := parquet.NewGenericWriter[T](&buf, opts...)
	if _, err := writer.Write(records); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
