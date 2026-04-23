package s3parquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
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

// defaultPartitionWriteConcurrency is the fallback cap on how
// many partitions Write fans out in parallel when
// WriterConfig.PartitionWriteConcurrency is zero. Each partition
// runs an independent encode + PUT(data) + PUT(markers…) +
// PUT(ref) sequence, so the cap bounds in-flight memory (sum of
// parquet buffers) and outbound S3 request rate. Matches
// markerPutConcurrency.
const defaultPartitionWriteConcurrency = 8

// partitionConcurrency returns the effective fan-out cap — the
// user-supplied WriterConfig.PartitionWriteConcurrency when set
// to a positive value, otherwise the default.
func (s *Writer[T]) partitionConcurrency() int {
	if s.cfg.PartitionWriteConcurrency > 0 {
		return s.cfg.PartitionWriteConcurrency
	}
	return defaultPartitionWriteConcurrency
}

// Write extracts the key from each record via PartitionKeyOf,
// groups by key, and writes one Parquet file + stream ref per
// key in parallel (bounded by WriterConfig.PartitionWriteConcurrency,
// default 8). Returns one WriteResult per partition that
// completed, in sorted-key order regardless of completion order.
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
	ctx context.Context, records []T, opts ...WriteOption,
) ([]WriteResult, error) {
	if len(records) == 0 {
		return nil, nil
	}
	if s.cfg.PartitionKeyOf == nil {
		return nil, fmt.Errorf(
			"s3parquet: PartitionKeyOf is required for Write; " +
				"use WriteWithKey for explicit keys")
	}
	writeOpts, err := resolveWriteOpts(opts)
	if err != nil {
		return nil, err
	}
	return s.writeGroupedFanOut(ctx, records,
		func(ctx context.Context, key string, recs []T) (*WriteResult, error) {
			return s.writeWithKeyResolved(ctx, key, recs, writeOpts)
		})
}

// resolveWriteOpts folds the variadic WriteOption chain into a
// core.WriteOpts and validates embedded values (IdempotencyToken
// passes ValidateIdempotencyToken). Done once per Write call so
// per-partition dispatch doesn't re-validate on every goroutine.
func resolveWriteOpts(opts []WriteOption) (core.WriteOpts, error) {
	var w core.WriteOpts
	w.Apply(opts...)
	if w.IdempotencyToken != "" {
		if err := core.ValidateIdempotencyToken(
			w.IdempotencyToken); err != nil {
			return core.WriteOpts{}, err
		}
		if w.MaxRetryAge < 0 {
			return core.WriteOpts{}, fmt.Errorf(
				"s3parquet: MaxRetryAge must be >= 0 (got %s); "+
					"zero disables scoped-LIST ref dedup",
				w.MaxRetryAge)
		}
	}
	return w, nil
}

// writeGroupedFanOut is the partition-level fan-out shared by
// Write and WriteRowGroupsBy. Groups records by PartitionKeyOf,
// spawns up to partitionConcurrency() goroutines, each invoking
// perPartition with its key and records. Returns results in
// sorted-key order regardless of completion order; first real
// (non-cancel) failure wins; caller-cancel surfaces as an error
// even when no real failure occurred.
func (s *Writer[T]) writeGroupedFanOut(
	ctx context.Context, records []T,
	perPartition func(
		ctx context.Context, key string, recs []T,
	) (*WriteResult, error),
) ([]WriteResult, error) {
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

	sem := make(chan struct{}, s.partitionConcurrency())
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

			r, err := perPartition(ctx, key, grouped[key])
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
//
// Passing WithIdempotencyToken makes this call retry-safe: the
// data filename is derived from the token, so retries produce
// the same path and the backend's overwrite-prevention triggers
// without re-uploading the parquet body. See
// core.WithIdempotencyToken for the full contract.
func (s *Writer[T]) WriteWithKey(
	ctx context.Context, key string, records []T, opts ...WriteOption,
) (*WriteResult, error) {
	if len(records) == 0 {
		return nil, nil
	}
	writeOpts, err := resolveWriteOpts(opts)
	if err != nil {
		return nil, err
	}
	return s.writeWithKeyResolved(ctx, key, records, writeOpts)
}

// writeWithKeyResolved is the post-option-resolution shared entry
// point for Write (per-partition dispatch) and WriteWithKey (direct
// call). Lets Write resolve options once and avoid the per-
// partition revalidation that calling WriteWithKey in the fan-out
// closure would imply.
func (s *Writer[T]) writeWithKeyResolved(
	ctx context.Context, key string, records []T, opts core.WriteOpts,
) (*WriteResult, error) {
	if err := s.validateKey(key); err != nil {
		return nil, err
	}

	// Capture writeStartTime here (before encode) so the same value
	// is used to populate the InsertedAtField column AND to stamp
	// the data filename / x-amz-meta-created-at header — a single
	// "when was this batch written" value propagates to every
	// downstream surface.
	writeStartTime := time.Now()
	if s.insertedAtFieldIndex != nil {
		populateInsertedAt(records, s.insertedAtFieldIndex, writeStartTime)
	}

	parquetBytes, err := encodeParquet(
		records, s.compressionCodec)
	if err != nil {
		return nil, fmt.Errorf(
			"s3parquet: parquet encode: %w", err)
	}
	return s.writeEncodedPayload(
		ctx, key, records, parquetBytes, writeStartTime, opts)
}

// writeEncodedPayload is the post-encode tail shared between
// WriteWithKey and WriteWithKeyRowGroupsBy: collect marker paths,
// PUT data, PUT markers in parallel, PUT ref (unless
// DisableRefStream), and run cleanup on failure. Keeping this
// factored ensures the two write entry points can't drift on
// ordering guarantees (data before ref, markers before ref,
// orphan cleanup on ref-PUT failure).
//
// writeStartTime is the wall clock captured by the caller just
// before parquet encoding — used to stamp the data filename
// tsMicros AND the x-amz-meta-created-at header so external
// tooling sees the same value that's in the InsertedAtField
// column.
//
// opts carries the resolved WriteOpts from the caller; when
// IdempotencyToken is set the write path uses the deterministic
// token-based id, detects retries via overwrite-prevention or
// HEAD-before-PUT, and scopes a LIST on the ref stream to dedup
// the ref emission.
func (s *Writer[T]) writeEncodedPayload(
	ctx context.Context, key string, records []T, parquetBytes []byte,
	writeStartTime time.Time, opts core.WriteOpts,
) (*WriteResult, error) {
	// Compute marker paths up-front so a bad IndexDef.Of fails the
	// whole Write before we touch S3, matching how validateKey
	// aborts on a malformed partition key.
	markerPaths, err := s.collectIndexMarkerPaths(records)
	if err != nil {
		return nil, err
	}

	// Compute the data-file id. With a token, use the token verbatim
	// as the id so retries produce deterministic data paths (the
	// retry-detection path can rely on equality of dataKey). Without
	// a token, fall back to the library's {tsMicros}-{shortID}
	// scheme — still lex-sortable by time within a partition.
	tsMicros := writeStartTime.UnixMicro()
	var id string
	idempotent := opts.IdempotencyToken != ""
	if idempotent {
		id = opts.IdempotencyToken
	} else {
		id = core.MakeAutoID(tsMicros, uuid.New().String()[:8])
	}

	dataKey := core.BuildDataFilePath(s.dataPath, key, id)
	meta := map[string]string{
		"created-at": writeStartTime.Format(time.RFC3339Nano),
	}

	// isRetry drives the post-data-PUT flow: fresh writes unconditionally
	// emit markers + ref; retries run the scoped-LIST dedup to avoid
	// re-emitting refs that already landed.
	var isRetry bool
	if idempotent {
		putErr := s.putDataIdempotent(ctx, dataKey, parquetBytes, meta)
		switch {
		case errors.Is(putErr, ErrAlreadyExists):
			isRetry = true
		case putErr != nil:
			return nil, fmt.Errorf(
				"s3parquet: put data: %w", putErr)
		}
	} else {
		if err := s.cfg.Target.putWithMeta(
			ctx, dataKey, parquetBytes,
			"application/octet-stream", meta,
			withConsistencyControl(s.cfg.ConsistencyControl),
		); err != nil {
			return nil, fmt.Errorf(
				"s3parquet: put data: %w", err)
		}
	}

	// On retry, scoped-LIST the ref stream for a ref carrying
	// this id. Found → full-success retry, skip markers + ref.
	// Not found → scenario B (data landed but ref didn't on the
	// original attempt), continue to emit markers + ref.
	if isRetry {
		existingRefKey, err := s.findExistingRef(
			ctx, opts.IdempotencyToken, opts.MaxRetryAge)
		if err != nil {
			return nil, fmt.Errorf(
				"s3parquet: scoped LIST for retry: %w", err)
		}
		if existingRefKey != "" {
			return &WriteResult{
				Offset:     Offset(existingRefKey),
				DataPath:   dataKey,
				RefPath:    existingRefKey,
				InsertedAt: writeStartTime,
			}, nil
		}
	}

	// Index markers are written after data (so a successful
	// marker implies the backing data file exists) and before
	// the ref (so Poll's commit semantics are unchanged). If any
	// marker PUT fails we delete the orphan data and return —
	// any markers that landed before the failure stay as
	// orphans, which Lookup tolerates.
	if err := s.putMarkersParallel(ctx, markerPaths); err != nil {
		s.cleanupOrphanData(dataKey, idempotent)
		return nil, fmt.Errorf(
			"s3parquet: put index markers: %w", err)
	}

	// DisableRefStream: skip the ref PUT entirely. Offset and
	// RefPath go empty so callers can't mistake the returned value
	// for a Poll-visible stream position.
	if s.cfg.Target.DisableRefStream {
		return &WriteResult{
			Offset:     "",
			DataPath:   dataKey,
			RefPath:    "",
			InsertedAt: writeStartTime,
		}, nil
	}

	// Capture a second timestamp immediately before the ref PUT so
	// the ref filename reflects publication time (when the ref
	// became visible) rather than write-start time. SettleWindow
	// then only needs to cover ref-PUT latency + LIST propagation,
	// independent of marker count.
	refTsMicros := time.Now().UnixMicro()
	refKey := core.EncodeRefKey(s.refPath, refTsMicros, id, tsMicros, key)

	result := &WriteResult{
		Offset:     Offset(refKey),
		DataPath:   dataKey,
		RefPath:    refKey,
		InsertedAt: writeStartTime,
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

	if !idempotent && !s.cfg.DisableCleanup {
		if delErr := s.cfg.Target.del(
			cleanupCtx, dataKey,
		); delErr != nil {
			return nil, fmt.Errorf(
				"s3parquet: put ref: %w (orphan data at %s: %v)",
				putErr, dataKey, delErr)
		}
	}
	// Idempotent writes leave orphan data in place by design:
	// a retry with the same token reuses the same data path and
	// overwrite-prevention triggers, so deletion would make the
	// retry re-upload the body. DisableCleanup likewise preserves
	// orphans for lifecycle policies to garbage-collect.

	return nil, fmt.Errorf("s3parquet: put ref: %w", putErr)
}

// putDataIdempotent issues the idempotent data PUT using whichever
// detection strategy the Writer resolved — putIfAbsent for
// overwrite-prevention backends (honours If-None-Match: *
// natively OR via a bucket policy denying s3:PutOverwriteObject),
// or headThenPut for backends without either mechanism.
//
// Returns ErrAlreadyExists on retry (callers interpret as "skip
// the body re-upload, scope-LIST refs"), nil on fresh write, and
// any other error verbatim.
func (s *Writer[T]) putDataIdempotent(
	ctx context.Context, dataKey string, parquetBytes []byte,
	meta map[string]string,
) error {
	opts := withConsistencyControl(s.cfg.ConsistencyControl)
	if s.overwritePreventionActive {
		return s.cfg.Target.putIfAbsent(
			ctx, dataKey, parquetBytes,
			"application/octet-stream", meta, opts)
	}
	return s.cfg.Target.headThenPut(
		ctx, dataKey, parquetBytes,
		"application/octet-stream", meta, opts)
}

// findExistingRef scans the ref stream for a ref whose id field
// equals token, within the lexical range [now - maxRetryAge, now].
// Returns the full ref key when found, empty string when not.
//
// Bounded by maxRetryAge so the scan cost is O(refs in window)
// rather than O(all refs). When maxRetryAge == 0 the function
// returns "" immediately — the caller explicitly disabled ref
// dedup for this retry.
//
// Uses the Writer's ConsistencyControl on the LIST so the scan
// sees all prior refs on StorageGRID-style backends — a weak-
// consistency LIST can miss a ref the writer just published on
// another node, silently breaking dedup.
func (s *Writer[T]) findExistingRef(
	ctx context.Context, token string, maxRetryAge time.Duration,
) (string, error) {
	if maxRetryAge <= 0 {
		return "", nil
	}
	lo, _ := core.RefRangeForRetry(s.refPath, time.Now(), maxRetryAge)
	paginator := s.cfg.Target.listRange(s.refPath+"/", lo)
	for paginator.HasMorePages() {
		page, err := s.cfg.Target.listPage(
			ctx, paginator,
			withConsistencyControl(s.cfg.ConsistencyControl))
		if err != nil {
			return "", err
		}
		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}
			id, err := core.ExtractRefID(*obj.Key)
			if err != nil {
				// Malformed ref keys (externally written or a
				// future schema the parser doesn't understand)
				// aren't our retry target — skip rather than fail
				// the write.
				continue
			}
			if id == token {
				return *obj.Key, nil
			}
		}
	}
	return "", nil
}

// cleanupOrphanData runs the best-effort delete for an orphaned
// data object on the failure paths. No-op when the write is
// idempotent (retries reuse the same path; deleting would force
// body re-upload) or when DisableCleanup is set (operator opted
// into lifecycle-based garbage collection).
//
// Errors are silently swallowed: the caller already has a richer
// error to surface, and compounding it with a delete failure
// obscures the root cause. Operators running without lifecycle
// policies should monitor for orphan data/ref drift separately.
func (s *Writer[T]) cleanupOrphanData(dataKey string, idempotent bool) {
	if idempotent || s.cfg.DisableCleanup {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(
		context.Background(), writeCleanupTimeout)
	defer cancel()
	_ = s.cfg.Target.del(cleanupCtx, dataKey)
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

// populateInsertedAt reflectively writes t into every record's
// InsertedAtField (at path fieldIdx). Called by both
// WriteWithKey and WriteWithKeyRowGroupsBy before parquet encode
// so the value lands in the file as a real column. Package-
// level rather than a method so the row-groups writer can reuse
// it without giving a reflective helper Writer[T] access.
//
// fieldIdx must not be nil — callers guard on s.insertedAtFieldIndex
// at the call site.
func populateInsertedAt[T any](recs []T, fieldIdx []int, t time.Time) {
	tsVal := reflect.ValueOf(t)
	for i := range recs {
		rv := reflect.ValueOf(&recs[i]).Elem()
		rv.FieldByIndex(fieldIdx).Set(tsVal)
	}
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
