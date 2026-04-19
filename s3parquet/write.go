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

// partitionWriteConcurrency caps how many partitions Write fans
// out in parallel. Each partition runs an independent encode +
// PUT(data) + PUT(markers…) + PUT(ref) sequence, so the cap
// bounds in-flight memory (sum of parquet buffers) and outbound
// S3 request rate. Matches markerPutConcurrency.
const partitionWriteConcurrency = 8

// Write extracts the key from each record via PartitionKeyOf,
// groups by key, and writes one Parquet file + stream ref per
// key in parallel (bounded by partitionWriteConcurrency).
// Returns one WriteResult per partition that completed, in
// sorted-key order regardless of completion order.
//
// On failure, cancels remaining partitions and returns whatever
// results landed first, with the first real (non-cancel) error.
// Partial success is the accepted outcome — each partition's
// data+markers+ref sequence is self-contained.
//
// An empty records slice is a no-op: (nil, nil) is returned so
// callers don't have to guard against batch-pipeline edge
// cases.
func (s *Writer[T]) Write(
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
	keys := slices.Sorted(maps.Keys(grouped))

	// Slot i holds the result for keys[i], so completion order
	// cannot leak into the returned slice even under parallel
	// execution.
	results := make([]*WriteResult, len(keys))
	errs := make([]error, len(keys))

	parentCtx := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sem := make(chan struct{}, partitionWriteConcurrency)
	var wg sync.WaitGroup
	for i, key := range keys {
		wg.Add(1)
		go func(i int, key string) {
			defer wg.Done()
			// Acquire inside the goroutine so a sibling failure
			// or caller cancel unblocks us promptly rather than
			// letting the main loop dispatch every partition
			// upfront.
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				errs[i] = ctx.Err()
				return
			}
			defer func() { <-sem }()

			r, err := s.WriteWithKey(ctx, key, grouped[key])
			if err != nil {
				errs[i] = err
				cancel()
				return
			}
			results[i] = r
		}(i, key)
	}
	wg.Wait()

	// Compact successful results in sorted-key order.
	var out []WriteResult
	for i := range keys {
		if results[i] != nil {
			out = append(out, *results[i])
		}
	}

	// Prefer a real failure over the cancel it triggered in
	// siblings. If no real failure surfaced but the caller's
	// context was cancelled, report that — otherwise callers
	// could mistake a partial write for full success.
	for _, err := range errs {
		if err == nil || errors.Is(err, context.Canceled) {
			continue
		}
		return out, err
	}
	if err := parentCtx.Err(); err != nil {
		return out, err
	}
	return out, nil
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
func (s *Writer[T]) WriteWithKey(
	ctx context.Context, key string, records []T,
) (*WriteResult, error) {
	if len(records) == 0 {
		return nil, nil
	}
	if err := s.validateKey(key); err != nil {
		return nil, err
	}

	parquetBytes, err := encodeParquet(
		records, s.compressionCodec)
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
	if err := s.cfg.Target.put(
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
		if delErr := s.cfg.Target.del(
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

	// DisableRefStream: skip the ref PUT entirely. Offset and
	// RefPath go empty so callers can't mistake the returned value
	// for a Poll-visible stream position.
	if s.cfg.Target.DisableRefStream {
		return &WriteResult{
			Offset:   "",
			DataPath: dataKey,
			RefPath:  "",
		}, nil
	}

	refKey := core.EncodeRefKey(s.refPath, tsMicros, shortID, key)

	result := &WriteResult{
		Offset:   Offset(refKey),
		DataPath: dataKey,
		RefPath:  refKey,
	}

	putErr := s.cfg.Target.put(
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

	if exists, headErr := s.cfg.Target.exists(
		cleanupCtx, refKey,
	); headErr == nil && exists {
		// Ref actually got written — we just lost the ack.
		return result, nil
	}

	if delErr := s.cfg.Target.del(
		cleanupCtx, dataKey,
	); delErr != nil {
		return nil, fmt.Errorf(
			"s3parquet: put ref: %w (orphan data at %s: %v)",
			putErr, dataKey, delErr)
	}

	return nil, fmt.Errorf("s3parquet: put ref: %w", putErr)
}

func (s *Writer[T]) groupByKey(records []T) map[string][]T {
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
func (s *Writer[T]) validateKey(key string) error {
	segments := strings.Split(key, "/")
	if len(segments) != len(s.cfg.Target.PartitionKeyParts) {
		return fmt.Errorf(
			"s3parquet: key %q has %d segments, "+
				"expected %d (%v)",
			key, len(segments),
			len(s.cfg.Target.PartitionKeyParts), s.cfg.Target.PartitionKeyParts)
	}
	for i, seg := range segments {
		part := s.cfg.Target.PartitionKeyParts[i]
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
func (s *Writer[T]) collectIndexMarkerPaths(records []T) ([]string, error) {
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
func (s *Writer[T]) putMarkersParallel(
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

			if err := s.cfg.Target.put(
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
// snappy default).
func encodeParquet[T any](
	records []T,
	codec compress.Codec,
) ([]byte, error) {
	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[T](
		&buf, parquet.Compression(codec))
	if _, err := writer.Write(records); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
