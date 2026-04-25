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

// writeCleanupTimeout bounds the detached cleanup work on the
// write failure paths:
//
//   - HEAD to disambiguate a ref PUT that errored client-side
//     but may have landed server-side.
//   - DELETE of a stale ref after a successful budget-recovery
//     PUT.
//   - DELETE of an orphan data file after a marker or ref PUT
//     failed.
//
// Detached from the caller's context so cleanup still completes
// when the caller cancels — we already committed to the best-
// effort cleanup before the cancellation, and bailing halfway
// leaves worse state than finishing.
//
// 30s is generous: it accommodates a large-file DELETE on a
// slow backend (MB+ parquet files can take seconds to remove on
// StorageGRID) without pathologically extending the lifetime of
// a failed Write if the backend is truly unreachable.
const writeCleanupTimeout = 30 * time.Second

// refPutBudget returns the client-side timeout for a ref PUT:
// half of SettleWindow, regardless of ConsistencyControl. The
// reserved half covers two things that otherwise push the ref's
// effective visibility time past SettleWindow:
//
//   - HEAD time on the post-PUT disambiguation. Non-zero on every
//     backend. Without headroom, even a PUT that genuinely
//     completes at exactly SettleWindow lands the HEAD past the
//     budget, forcing unnecessary recovery.
//   - LIST propagation between the node that accepted the PUT
//     and the node a concurrent Poll hits. Zero on strong
//     consistency, nonzero on weak backends.
//
// An earlier version gated this on ConsistencyControl and handed
// strong-consistency writers the full SettleWindow. That only
// accounted for LIST propagation and ignored HEAD, so borderline-
// slow PUTs that actually landed in budget triggered the recovery
// path anyway. Uniform settle/2 is simpler and strictly safer —
// real PUT latencies (tens of ms) fit comfortably inside settle/2
// for any reasonable SettleWindow, so the halved budget doesn't
// cost the happy path anything.
func refPutBudget(settleWindow time.Duration) time.Duration {
	return settleWindow / 2
}

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

// writeEncodedPayload is the post-encode orchestration shared
// between WriteWithKey and WriteWithKeyRowGroupsBy. The body is
// a four-phase commit sequence — data → retry-dedup → markers →
// ref — so the at-least-once contract can be checked phase by
// phase. Phase 4 still lives in commitRefOrRecover because it
// owns the SettleWindow/2 budget logic and the budget-exceeded
// recovery branch (non-trivial state machine of its own).
//
// writeStartTime is the wall clock captured by the caller just
// before parquet encoding — used to stamp the data filename
// tsMicros, the InsertedAtField column, and the
// x-amz-meta-created-at header so every downstream surface sees
// the same "when was this written" value.
//
// opts.IdempotencyToken, when set, replaces the default
// {tsMicros}-{shortID} id so retries produce deterministic data
// paths; opts.MaxRetryAge bounds the scoped LIST issued on the
// retry-dedup branch.
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
	idempotent := opts.IdempotencyToken != ""
	var id string
	if idempotent {
		id = opts.IdempotencyToken
	} else {
		id = core.MakeAutoID(tsMicros, uuid.New().String()[:8])
	}
	dataKey := core.BuildDataFilePath(s.dataPath, key, id)

	// Phase 1: data PUT (always conditional via If-None-Match: *).
	// Auto-generated dataKeys are unique per attempt so the check
	// trivially passes; token-derived dataKeys collide with prior
	// attempts and surface ErrAlreadyExists, which we route to the
	// retry-dedup branch instead of failing.
	putErr := s.cfg.Target.putIfAbsent(
		ctx, dataKey, parquetBytes,
		"application/octet-stream",
		map[string]string{
			"created-at": writeStartTime.Format(time.RFC3339Nano),
		},
		s.cfg.ConsistencyControl)
	isRetry := errors.Is(putErr, ErrAlreadyExists)
	if putErr != nil && !isRetry {
		return nil, fmt.Errorf("s3parquet: put data: %w", putErr)
	}

	// Phase 2: on retry, scoped-LIST the ref stream for a still-
	// fresh ref this token already published. Found → prior attempt
	// already made the write consumable; return its ref as the
	// result. findExistingRef filters stale refs (past the settle
	// cutoff) so a "found" never silently misses consumers who
	// advanced past it.
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

	// Phase 3: markers. Sequenced after data so a landed marker
	// implies the backing file exists, and before ref so Poll's
	// commit semantics are unchanged. On marker-PUT failure,
	// best-effort cleanup of the orphan data (non-idempotent
	// writes only; idempotent retries reuse the same data file).
	if err := s.putMarkersParallel(ctx, markerPaths); err != nil {
		_ = s.cleanupOrphanData(dataKey, idempotent)
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

	// Phase 4: ref PUT under a SettleWindow/2 budget, with
	// internal budget-exceeded recovery — see commitRefOrRecover.
	return s.commitRefOrRecover(
		ctx, dataKey, id, tsMicros, key, writeStartTime, idempotent)
}

// commitRefOrRecover is phase 4 of writeEncodedPayload: PUT the
// ref under a SettleWindow/2 budget. Three branches, in order:
//
//  1. Happy path — PUT returned nil and elapsed <= settle:
//     ref's refTsMicros is fresh enough that any concurrent Poll
//     respecting SettleWindow will yield it. Return success.
//  2. Slow path — PUT returned nil but elapsed > settle: the
//     ref's refTsMicros sits at an offset some Poll may have
//     already advanced past. Recover via a fresh in-budget ref +
//     best-effort delete of the stale one. Reader dedup absorbs
//     the rare visible duplicate.
//  3. Hard failure — PUT returned an error: cleanup the orphan
//     data (non-idempotent only) and return the wrapped error.
//     The caller's retry — under WithIdempotencyToken — picks up
//     a previously-landed ref via findExistingRef; without a
//     token the retry writes fresh data + ref and any
//     dangling-ref left behind by a server-side-success +
//     client-side-error case is silently absorbed by the
//     OnMissingData skip-on-NoSuchKey contract.
//
// The library deliberately does not do post-hoc HEAD on a
// PUT-failure to disambiguate lost-ack — under any backend
// honouring read-after-new-write (i.e. anything we support), the
// caller's retry path achieves the same outcome with one extra
// round-trip on a rare event, in exchange for a much simpler
// commit phase. A weakly-consistent backend would defeat the
// HEAD too; correctness on this library requires
// ConsistencyStrongGlobal / ConsistencyStrongSite when running
// on backends like StorageGRID.
//
// At-least-once invariant on nil return: the returned RefPath is
// LIST-visible and its refTsMicros was captured < SettleWindow
// ago, so any Poll whose `since` is lex-earlier will yield it.
// On ErrRefSettleBudgetExceeded, findExistingRef's freshness
// filter ensures the caller's retry emits a fresh ref rather
// than silently matching the stale one.
func (s *Writer[T]) commitRefOrRecover(
	ctx context.Context,
	dataKey, id string, dataTsMicros int64, hiveKey string,
	writeStartTime time.Time, idempotent bool,
) (*WriteResult, error) {
	// Capture a second timestamp immediately before the ref PUT so
	// the ref filename reflects publication time (when the ref
	// became visible) rather than write-start time. SettleWindow
	// then only needs to cover ref-PUT latency + LIST propagation,
	// independent of marker count.
	refCaptureTime := time.Now()
	refTsMicros := refCaptureTime.UnixMicro()
	refKey := core.EncodeRefKey(
		s.refPath, refTsMicros, id, dataTsMicros, hiveKey)

	result := &WriteResult{
		Offset:     Offset(refKey),
		DataPath:   dataKey,
		RefPath:    refKey,
		InsertedAt: writeStartTime,
	}

	settle := s.cfg.Target.EffectiveSettleWindow()
	elapsed, putErr := s.putRefWithBudget(ctx, refKey, settle, refCaptureTime)

	switch {
	case putErr == nil && elapsed <= settle:
		return result, nil
	case putErr == nil:
		// PUT succeeded but past budget — recover with a fresh in-
		// budget ref so a concurrent Poll's cutoff can't have
		// advanced past it.
		return s.recoverRefAfterBudgetExceeded(
			ctx, result, id, dataTsMicros, hiveKey, settle, refKey)
	}

	// Hard failure: PUT errored. Cleanup orphan data (non-idempotent
	// only) and surface the wrapped error so the caller retries.
	if delErr := s.cleanupOrphanData(dataKey, idempotent); delErr != nil {
		return nil, fmt.Errorf(
			"s3parquet: put ref: %w (orphan data at %s: %v)",
			putErr, dataKey, delErr)
	}
	return nil, fmt.Errorf("s3parquet: put ref: %w", putErr)
}

// putRefWithBudget issues the empty-body ref PUT under a
// SettleWindow/2 client-side timeout and returns the PUT error
// plus the elapsed time measured from refCaptureTime. The post-
// hoc elapsed check guards against cases where the timeout was
// ignored or masked by SDK internals — caller branches on
// elapsed > settle even when putErr == nil.
//
// Shared by the initial commit and the budget-recovery PUT so
// both paths see identical budgeting + consistency-header
// semantics.
func (s *Writer[T]) putRefWithBudget(
	ctx context.Context, refKey string,
	settle time.Duration, refCaptureTime time.Time,
) (elapsed time.Duration, err error) {
	putCtx, cancel := context.WithTimeout(ctx, refPutBudget(settle))
	defer cancel()
	err = s.cfg.Target.put(
		putCtx, refKey, []byte{}, "application/octet-stream",
		s.cfg.ConsistencyControl)
	return time.Since(refCaptureTime), err
}

// recoverRefAfterBudgetExceeded runs the settle-budget recovery
// path: the initial ref PUT took longer than SettleWindow, so its
// refTsMicros sits at an offset some consumers may have already
// advanced past. Write a fresh ref with a new refTsMicros well
// inside budget so subsequent polls pick it up, then best-effort
// delete the stale one to narrow the duplicate window for any
// consumer that polled between "stale ref landed" and "delete
// took effect".
//
// Duplicate semantics: when a consumer reads both the stale and
// the fresh ref, both point at the same idempotent data file. The
// records share (entity, version), so reader-layer dedup
// (EntityKeyOf + VersionOf) collapses them to one. Callers without
// reader dedup see the raw duplicate — documented tradeoff; the
// library explicitly chooses "rare duplicate absorbed by dedup"
// over "silent missed write" here.
//
// On failure of the recovery PUT (error or its own budget blown),
// returns ErrRefSettleBudgetExceeded so the caller retries. A
// caller retry's findExistingRef filters stale refs out of the
// dedup window (by refTsMicros age), so the retry writes cleanly
// rather than matching the already-stale ref and silently
// succeeding.
//
// Mirrors commitRefOrRecover's PUT semantics via putRefWithBudget
// — same client-side timeout + consistency header — so initial
// and recovery paths share their critical-section behaviour.
func (s *Writer[T]) recoverRefAfterBudgetExceeded(
	ctx context.Context,
	result *WriteResult,
	id string, dataTsMicros int64, hiveKey string,
	settle time.Duration, staleRefKey string,
) (*WriteResult, error) {
	refCaptureTime := time.Now()
	refTsMicros := refCaptureTime.UnixMicro()
	newRefKey := core.EncodeRefKey(
		s.refPath, refTsMicros, id, dataTsMicros, hiveKey)

	elapsed, putErr := s.putRefWithBudget(
		ctx, newRefKey, settle, refCaptureTime)

	if putErr != nil || elapsed > settle {
		// Recovery PUT also missed budget. Surface the sentinel so
		// the caller retries; their retry's scoped LIST rejects the
		// stale ref by age and writes a fresh one.
		return result, ErrRefSettleBudgetExceeded
	}

	// Best-effort delete. S3 DeleteObject is idempotent (204 even
	// for missing keys), so this is safe whether the stale ref
	// actually landed or not. A failing DELETE leaves a garbage ref
	// that reader dedup absorbs — not a correctness problem.
	cleanupCtx, cancel := context.WithTimeout(
		context.Background(), writeCleanupTimeout)
	defer cancel()
	_ = s.cfg.Target.del(cleanupCtx, staleRefKey)

	result.RefPath = newRefKey
	result.Offset = Offset(newRefKey)
	return result, nil
}

// findExistingRef scans the ref stream for a ref whose id field
// equals token and whose refTsMicros is still fresh enough to be
// a safe dedup target. Returns the full ref key when found, empty
// string when not.
//
// Bounded on three axes:
//
//   - Lower bound via listRange(startAfter=lo) so the paginator
//     starts at (now - maxRetryAge).
//   - Upper bound via an in-loop compare against hi so we stop as
//     soon as a page yields a key past the retry window. Without
//     this the paginator walks every ref newer than "now" —
//     concurrent writers' refs, the store's tail — which costs
//     additional LIST pages proportional to traffic beyond "now"
//     without adding any chance of a match (our token can't
//     appear with a future refTsMicros).
//   - Freshness via the settle-cutoff filter: a ref with
//     refTsMicros < now - SettleWindow sits at an offset some
//     consumers may have advanced past. Treating it as a dedup
//     match would silently miss those consumers, so the scan
//     skips stale matches and lets the caller emit a fresh ref.
//     The in-flight settle-budget recovery (see
//     recoverRefAfterBudgetExceeded) normally prevents a stale
//     ref from existing at all; this filter is the backstop for
//     the cascading-failure case where recovery also failed and
//     the caller retried.
//
// When maxRetryAge == 0 the function returns "" immediately — the
// caller explicitly disabled ref dedup for this retry.
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
	now := time.Now()
	lo, hi := core.RefRangeForRetry(s.refPath, now, maxRetryAge)
	settleCutoffUs := now.Add(
		-s.cfg.Target.EffectiveSettleWindow()).UnixMicro()
	paginator := s.cfg.Target.listRange(s.refPath+"/", lo)
	for paginator.HasMorePages() {
		page, err := s.cfg.Target.listPage(
			ctx, paginator,
			s.cfg.ConsistencyControl)
		if err != nil {
			return "", err
		}
		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}
			if *obj.Key > hi {
				// Past the retry window. Lex compare holds because
				// all real tsMicros share the same decimal width
				// (16 digits post-2001), so lex order matches
				// numeric order — same assumption RefCutoff and
				// RefRangeForRetry's lo already rely on.
				return "", nil
			}
			_, refTsMicros, id, _, err := core.ParseRefKey(*obj.Key)
			if err != nil {
				// Malformed ref keys (externally written or a
				// future schema the parser doesn't understand)
				// aren't our retry target — skip rather than fail
				// the write.
				continue
			}
			if id != token {
				continue
			}
			if refTsMicros < settleCutoffUs {
				// Stale match: the ref sits past the settle cutoff
				// a concurrent Poll would compute right now, so
				// some consumers may have advanced past it. Skip,
				// so the caller emits a fresh ref that every
				// consumer can still yield.
				continue
			}
			return *obj.Key, nil
		}
	}
	return "", nil
}

// cleanupOrphanData runs the best-effort delete for an orphaned
// data object on the failure paths. No-op (nil return) when the
// write is idempotent (retries reuse the same path; deleting
// would force body re-upload) or when DisableCleanup is set
// (operator opted into lifecycle-based garbage collection).
//
// Uses its own detached context so the cleanup survives caller-
// context cancellation — the caller already decided to bail, but
// we'd still rather not leave orphan objects behind.
//
// Returns the del error when the delete actually ran and failed.
// Fire-and-forget sites (commitMarkers) discard the returned
// error because they already have a richer error to surface and
// compounding obscures the root cause. The ref-PUT failure path
// folds the returned error into its user-facing message so
// operators without lifecycle policies can find the orphan.
func (s *Writer[T]) cleanupOrphanData(
	dataKey string, idempotent bool,
) error {
	if idempotent || s.cfg.DisableCleanup {
		return nil
	}
	cleanupCtx, cancel := context.WithTimeout(
		context.Background(), writeCleanupTimeout)
	defer cancel()
	return s.cfg.Target.del(cleanupCtx, dataKey)
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
	parentCtx := ctx
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
				s.cfg.ConsistencyControl,
			); err != nil {
				errs[i] = err
				cancel()
			}
		}(i, p)
	}
	wg.Wait()

	// First real error wins; skip cancellations so we report the
	// root-cause failure instead of the cancellation it triggered
	// in sibling goroutines. If every goroutine bailed with
	// Canceled, check parentCtx so a caller-triggered cancel
	// surfaces as an error rather than a nil-return that would
	// misrepresent a cancelled marker commit as a successful one
	// to any future caller refactoring this site.
	for _, err := range errs {
		if err == nil || errors.Is(err, context.Canceled) {
			continue
		}
		return err
	}
	if err := parentCtx.Err(); err != nil {
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
