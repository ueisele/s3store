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
// between WriteWithKey and WriteWithKeyRowGroupsBy. The body
// reads as a commit sequence so a reviewer can verify the
// at-least-once contract one phase at a time:
//
//  1. data    — writeData PUTs the parquet bytes (idempotent or
//     fresh). On a retry-detected idempotent PUT it
//     returns isRetry=true instead of failing.
//  2. retry   — when isRetry, claimExistingRef scoped-LISTs the
//     ref stream for a still-fresh ref with this token.
//     Found → return that ref as the result (the prior
//     attempt already made the write consumable). Not
//     found → continue.
//  3. markers — commitMarkers PUTs the index markers (with
//     orphan-data cleanup on failure). Sequenced after
//     data so a landed marker implies the backing file
//     exists, and before ref so Poll's commit semantics
//     are unchanged.
//  4. ref     — commitRefOrRecover PUTs the ref with a
//     SettleWindow/2 budget. On budget-blown or lost-
//     ack past the budget, recovers internally (fresh
//     ref + best-effort delete of stale). Only surfaces
//     ErrRefSettleBudgetExceeded when both the initial
//     and the recovery PUT miss budget.
//
// writeStartTime is the wall clock captured by the caller just
// before parquet encoding — used to stamp the data filename
// tsMicros, the InsertedAtField column, and the
// x-amz-meta-created-at header so every downstream surface sees
// the same "when was this written" value.
//
// opts.IdempotencyToken, when set, replaces the default
// {tsMicros}-{shortID} id so retries produce deterministic data
// paths; opts.MaxRetryAge bounds the scoped LIST in phase 2.
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
	meta := map[string]string{
		"created-at": writeStartTime.Format(time.RFC3339Nano),
	}

	// Phase 1: data.
	isRetry, err := s.writeData(
		ctx, dataKey, parquetBytes, meta, idempotent)
	if err != nil {
		return nil, err
	}

	// Phase 2: on retry, check for a still-fresh ref already
	// emitted by the prior attempt. Found → return early.
	if isRetry {
		existing, err := s.claimExistingRef(
			ctx, opts, dataKey, writeStartTime)
		if err != nil {
			return nil, err
		}
		if existing != nil {
			return existing, nil
		}
	}

	// Phase 3: markers.
	if err := s.commitMarkers(
		ctx, markerPaths, dataKey, idempotent); err != nil {
		return nil, err
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

	// Phase 4: ref (with internal budget-exceeded recovery).
	return s.commitRefOrRecover(
		ctx, dataKey, id, tsMicros, key, writeStartTime, idempotent)
}

// writeData is phase 1 of writeEncodedPayload: PUT the parquet
// bytes to dataKey. Routes through the overwrite-prevention
// detection path (putIfAbsent or headThenPut) when idempotent is
// true, or a plain metadata-carrying PUT when it isn't.
//
// Returns isRetry=true when the data object already existed,
// signalling that markers and ref may already have been emitted
// by a prior attempt. Any other PUT error is wrapped and returned.
//
// At-least-once invariant on (isRetry=false, err=nil): the data
// file at dataKey carries exactly the caller's parquet bytes,
// just uploaded. On (isRetry=true, err=nil): the data file
// exists with bytes from a prior attempt (idempotent token →
// same logical content).
func (s *Writer[T]) writeData(
	ctx context.Context, dataKey string, parquetBytes []byte,
	meta map[string]string, idempotent bool,
) (isRetry bool, err error) {
	if idempotent {
		putErr := s.putDataIdempotent(
			ctx, dataKey, parquetBytes, meta)
		switch {
		case errors.Is(putErr, ErrAlreadyExists):
			return true, nil
		case putErr != nil:
			return false, fmt.Errorf(
				"s3parquet: put data: %w", putErr)
		}
		return false, nil
	}
	if err := s.cfg.Target.putWithMeta(
		ctx, dataKey, parquetBytes,
		"application/octet-stream", meta,
		withConsistencyControl(s.cfg.ConsistencyControl),
	); err != nil {
		return false, fmt.Errorf(
			"s3parquet: put data: %w", err)
	}
	return false, nil
}

// claimExistingRef is phase 2 of writeEncodedPayload: on a
// retry-detected idempotent write, scoped-LIST the ref stream
// for a still-fresh ref carrying this token. Returns a populated
// *WriteResult when such a ref exists (prior attempt already
// made the write consumable), or (nil, nil) when no fresh match
// exists (caller proceeds to emit markers + ref).
//
// Stale refs (refTsMicros past the settle cutoff) are filtered
// by findExistingRef so they never satisfy dedup — treating one
// as "found" would silently miss consumers who advanced past its
// offset. Only called when opts.IdempotencyToken != "".
func (s *Writer[T]) claimExistingRef(
	ctx context.Context, opts core.WriteOpts,
	dataKey string, writeStartTime time.Time,
) (*WriteResult, error) {
	existingRefKey, err := s.findExistingRef(
		ctx, opts.IdempotencyToken, opts.MaxRetryAge)
	if err != nil {
		return nil, fmt.Errorf(
			"s3parquet: scoped LIST for retry: %w", err)
	}
	if existingRefKey == "" {
		return nil, nil
	}
	return &WriteResult{
		Offset:     Offset(existingRefKey),
		DataPath:   dataKey,
		RefPath:    existingRefKey,
		InsertedAt: writeStartTime,
	}, nil
}

// commitMarkers is phase 3 of writeEncodedPayload: PUT every
// registered index marker in parallel. On any marker-PUT
// failure, best-effort cleanup of the orphan data (non-
// idempotent writes only; idempotent retries reuse the existing
// data file) and surface the wrapped error. Partial marker
// landings on failure are tolerated: orphan markers that outlive
// their data file fall out at Lookup time via the same LIST-to-
// GET race handling as dangling refs.
//
// At-least-once invariant on nil return: every required marker
// is durably stored in S3; Lookup calls respecting SettleWindow
// will yield them on the same contract as Poll.
func (s *Writer[T]) commitMarkers(
	ctx context.Context, markerPaths []string,
	dataKey string, idempotent bool,
) error {
	if err := s.putMarkersParallel(ctx, markerPaths); err != nil {
		s.cleanupOrphanData(dataKey, idempotent)
		return fmt.Errorf(
			"s3parquet: put index markers: %w", err)
	}
	return nil
}

// commitRefOrRecover is phase 4 of writeEncodedPayload: PUT the
// ref under a SettleWindow/2 budget, with two recovery gates so
// the at-least-once contract is preserved across backend slowness
// and weak-consistency races.
//
// Happy path: PUT returns nil within budget → return success.
//
// Recovery gate A (ref landed, visibility > settle): the stale
// ref sits at an offset a concurrent Poll may have already
// advanced past. recoverRefAfterBudgetExceeded writes a fresh ref
// inside budget and best-effort deletes the stale.
//
// Recovery gate B (PUT reported success but HEAD can't see it,
// weak-consistency propagation): same recovery — emit a fresh,
// visible ref and let any late-propagating stale be absorbed by
// reader dedup.
//
// Hard failure: PUT failed AND HEAD confirms ref absent. Cleanup
// the orphan data (non-idempotent) and return a wrapped put-ref
// error.
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

	// Bound the ref PUT's wall-clock time so its refTsMicros stays
	// inside the settle-window budget a concurrent Poll uses to
	// compute its cutoff. Budget is SettleWindow/2 — see
	// refPutBudget for why. The ctx timeout fires client-side; the
	// post-hoc elapsed check guards against cases where the
	// timeout was ignored or masked by internal retries.
	settle := s.cfg.Target.EffectiveSettleWindow()
	putCtx, cancelPut := context.WithTimeout(
		ctx, refPutBudget(settle))
	putErr := s.cfg.Target.put(
		putCtx, refKey, []byte{}, "application/octet-stream")
	cancelPut()
	elapsed := time.Since(refCaptureTime)

	if putErr == nil && elapsed <= settle {
		return result, nil
	}

	// Ref PUT failed or blew the settle budget. Disambiguate lost-
	// ack from a real failure using a bounded, caller-independent
	// context so cleanup still completes if the caller has
	// cancelled.
	cleanupCtx, cancel := context.WithTimeout(
		context.Background(), writeCleanupTimeout)
	defer cancel()

	if exists, headErr := s.cfg.Target.exists(
		cleanupCtx, refKey,
	); headErr == nil && exists {
		// Ref is present in S3. The leapfrog window for a
		// concurrent Poll opens at refCaptureTime + settle, so the
		// safe question is "is *now* still within that window?",
		// not "did the PUT itself finish in time?". For putErr ==
		// nil the two are equivalent (PUT-return time is the
		// visibility time). For putErr != nil (timeout) the actual
		// visibility time is unobservable but bounded above by
		// HEAD-completion time — re-measuring after the HEAD gives
		// the conservative upper bound.
		//
		// Two sub-cases on visibilityElapsed:
		//   - <= settle: genuine lost-ack within budget — the
		//     ref's refTsMicros is fresh enough that no consumer
		//     can have yielded another writer's later ref past
		//     ours. Return success.
		//   - >  settle: the ref's refTsMicros sits at an offset
		//     that a concurrent Poll may have already advanced
		//     past (after yielding fresher refs from other
		//     partitions). Run recovery: write a fresh ref well
		//     inside budget, best-effort delete the stale one.
		visibilityElapsed := time.Since(refCaptureTime)
		if visibilityElapsed > settle {
			return s.recoverRefAfterBudgetExceeded(
				ctx, cleanupCtx, result,
				id, dataTsMicros, hiveKey, settle, refKey)
		}
		return result, nil
	}

	// HEAD failed or reported the ref absent. When the PUT itself
	// claimed success (putErr == nil) the ref may still be
	// propagating on a weakly-consistent backend; the budget-
	// blown case is the same — run recovery so consumers have a
	// fresh, in-budget ref regardless of whether the original
	// propagates later (reader dedup absorbs the rare duplicate).
	if putErr == nil {
		return s.recoverRefAfterBudgetExceeded(
			ctx, cleanupCtx, result,
			id, dataTsMicros, hiveKey, settle, refKey)
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

// recoverRefAfterBudgetExceeded runs the settle-budget recovery
// path: the initial ref PUT either took longer than SettleWindow
// or landed with an inconclusive HEAD, so its refTsMicros sits at
// an offset some consumers may have already advanced past. Write
// a fresh ref with a new refTsMicros well inside budget so
// subsequent polls pick it up, then best-effort delete the stale
// one to narrow the duplicate window for any consumer that polled
// between "stale ref landed" and "delete took effect".
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
func (s *Writer[T]) recoverRefAfterBudgetExceeded(
	ctx, cleanupCtx context.Context,
	result *WriteResult,
	id string, dataTsMicros int64, hiveKey string,
	settle time.Duration, staleRefKey string,
) (*WriteResult, error) {
	refCaptureTime := time.Now()
	refTsMicros := refCaptureTime.UnixMicro()
	newRefKey := core.EncodeRefKey(
		s.refPath, refTsMicros, id, dataTsMicros, hiveKey)

	putCtx, cancelPut := context.WithTimeout(
		ctx, refPutBudget(settle))
	putErr := s.cfg.Target.put(
		putCtx, newRefKey, []byte{}, "application/octet-stream")
	cancelPut()
	elapsed := time.Since(refCaptureTime)

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
	_ = s.cfg.Target.del(cleanupCtx, staleRefKey)

	result.RefPath = newRefKey
	result.Offset = Offset(newRefKey)
	return result, nil
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
			withConsistencyControl(s.cfg.ConsistencyControl))
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
