package s3store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
)

// WriteResult contains metadata about a completed write.
//
// InsertedAt is the writer's pre-encode wall-clock at write-start
// (microsecond precision) by default — or the caller's
// WithInsertedAt override, when supplied — the same value stamped
// into the parquet's InsertedAtField column. Surfaced on the
// result so callers can log / persist it (e.g., into an outbox
// table) without parsing the data path or issuing a HEAD.
//
// Under WithIdempotencyToken, a same-token retry whose upfront
// HEAD on `<token>.commit` finds a prior commit returns *that*
// commit's WriteResult (DataPath, RefPath, InsertedAt all
// reflect the prior attempt — InsertedAt is recovered from the
// token-commit's `insertedat` user-metadata, so it agrees with
// the prior attempt's column value byte-for-byte). Callers
// comparing two results from the same token across retries will
// therefore see identical values whenever a prior attempt's
// token-commit is still in place.
type WriteResult struct {
	Offset     Offset
	DataPath   string
	RefPath    string
	InsertedAt time.Time
	// RowCount is the number of records persisted in this commit's
	// parquet file. On a fresh write this is len(records) for the
	// partition; on the retry-finds-prior-commit path it is recovered
	// from the token-commit's `rowcount` user-metadata so the value
	// matches the original attempt's parquet exactly. Surfaced so
	// retry-recovery callers — who don't have the original records
	// slice — can still report batch size without GETting the parquet.
	RowCount int64
}

// LookupCommit returns the WriteResult of the canonical write
// committed under (partition, token), if any. One HEAD against
// `<dataPath>/<partition>/<token>.commit`; no LIST, no parquet
// re-encode, no parquet GET.
//
// Sits on Writer because the use case is write-side: a service
// that stores the (partition, token) pair alongside its outbox
// row and, on retry, wants to know "did the prior attempt
// commit?" before re-fetching records and re-encoding parquet.
// WriteWithKey under the same token would do the same upfront
// HEAD internally, but only after re-encoding parquet — calling
// LookupCommit first lets retries skip the encode entirely on
// the recovery path. The HEAD + reconstruction primitives
// (headTokenCommit / reconstructWriteResult) are exactly the
// ones writeEncodedPayload's Step 2 calls; this method is the
// externalized form of that step.
//
// On a hit, the returned WriteResult is byte-identical to what
// the original Write returned (DataPath / RefPath / Offset
// reconstructed from the path scheme + the marker's metadata;
// InsertedAt sourced from the `insertedat` user-metadata so it
// matches the parquet's InsertedAtField column value).
//
// ok=false signals 404: no commit exists for that (partition,
// token). If the caller knows a Write was attempted for this
// token and ok=false, the write either crashed or returned an
// error to its original caller.
//
// Validates token via validateIdempotencyToken so callers fail
// loudly on garbage rather than HEADing nonsensical keys.
func (w *Writer[T]) LookupCommit(
	ctx context.Context, partition, token string,
) (wr WriteResult, ok bool, err error) {
	scope := w.cfg.Target.metrics.methodScope(ctx, methodLookupCommit)
	defer scope.end(&err)
	if err := validateIdempotencyToken(token); err != nil {
		return WriteResult{}, false, fmt.Errorf("LookupCommit: %w", err)
	}
	w.cfg.Target.metrics.recordReadCommitHead(ctx, methodLookupCommit)
	meta, exists, err := headTokenCommit(ctx, w.cfg.Target,
		w.dataPath, partition, token)
	if err != nil {
		return WriteResult{}, false, fmt.Errorf("LookupCommit: %w", err)
	}
	if !exists {
		return WriteResult{}, false, nil
	}
	return reconstructWriteResult(w.dataPath, w.refPath,
		partition, token, meta), true, nil
}

// Write extracts the key from each record via PartitionKeyOf,
// groups by key, and writes one Parquet file + stream ref per
// key in parallel (bounded by Target.MaxInflightRequests,
// default 32). Returns one WriteResult per partition that
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
) (results []WriteResult, err error) {
	scope := s.cfg.Target.metrics.methodScope(ctx, methodWrite)
	defer scope.end(&err)
	if len(records) == 0 {
		return nil, nil
	}
	if s.cfg.PartitionKeyOf == nil {
		return nil, errors.New(
			"PartitionKeyOf is required for Write; " +
				"use WriteWithKey for explicit keys")
	}
	results, err = s.writeGroupedFanOut(ctx, records,
		func(ctx context.Context, key string, recs []T) (*WriteResult, error) {
			r, _, err := s.writeWithKeyResolved(ctx, key, recs, opts, scope)
			return r, err
		})
	return results, err
}

// resolveWriteOpts folds the variadic WriteOption chain into a
// writeOpts and resolves the per-partition idempotency token. The
// records slice is pass-by-header (no copy of the underlying
// array) and is consumed by idempotencyTokenFn — when set — to
// derive the per-partition token; the static idempotencyToken
// branch ignores records entirely. Mutual-exclusion between the
// two options is enforced first so the resolution path is
// unambiguous.
//
// The type-assertion on idempotencyTokenFn (any → func([]T)
// (string, error)) lives here because writeOpts can't be generic
// — it's the boundary type WriteOption closures write into. A
// closure whose T doesn't match the writer's surfaces a clear
// error naming the mismatch.
func resolveWriteOpts[T any](opts []WriteOption, records []T) (writeOpts, error) {
	var w writeOpts
	w.apply(opts...)
	if w.idempotencyToken != "" && w.idempotencyTokenFn != nil {
		return writeOpts{}, errors.New(
			"WithIdempotencyToken and WithIdempotencyTokenOf " +
				"are mutually exclusive")
	}
	if w.idempotencyTokenFn != nil {
		fn, ok := w.idempotencyTokenFn.(func([]T) (string, error))
		if !ok {
			return writeOpts{}, fmt.Errorf(
				"WithIdempotencyTokenOf: closure type %T does not "+
					"match writer's record type — pass "+
					"WithIdempotencyTokenOf[T] with the same T as the Writer",
				w.idempotencyTokenFn)
		}
		token, err := fn(records)
		if err != nil {
			return writeOpts{}, fmt.Errorf(
				"WithIdempotencyTokenOf: %w", err)
		}
		if err := validateIdempotencyToken(token); err != nil {
			return writeOpts{}, fmt.Errorf(
				"WithIdempotencyTokenOf: %w", err)
		}
		w.idempotencyToken = token
	} else if w.idempotencyTokenSet {
		// User explicitly invoked WithIdempotencyToken — validate
		// even if the value is empty so a wiring bug doesn't
		// silently drop the idempotency contract.
		if err := validateIdempotencyToken(
			w.idempotencyToken); err != nil {
			return writeOpts{}, err
		}
	}
	return w, nil
}

// writeGroupedFanOut is the partition-level fan-out used by
// Write. Groups records by PartitionKeyOf, runs perPartition
// through fanOut bounded by
// Target.MaxInflightRequests. Returns results in sorted-key order
// regardless of completion order; first real (non-cancel) failure
// wins; caller-cancel surfaces as an error even when no real
// failure occurred (handled in fanOut).
//
// Partial success is the accepted outcome: on error, results that
// committed before the cancel still appear in the returned slice.
func (s *Writer[T]) writeGroupedFanOut(
	ctx context.Context, records []T,
	perPartition func(
		ctx context.Context, key string, recs []T,
	) (*WriteResult, error),
) ([]WriteResult, error) {
	parts := s.GroupByPartition(records)

	// Slot i holds the result for parts[i] so completion order
	// cannot leak into the returned slice even under parallel
	// execution.
	results := make([]*WriteResult, len(parts))

	err := fanOut(ctx, parts,
		s.cfg.Target.EffectiveMaxInflightRequests(),
		s.cfg.Target.metrics,
		func(ctx context.Context, i int, p HivePartition[T]) error {
			r, err := perPartition(ctx, p.Key, p.Rows)
			// Capture the result even on err: writeEncodedPayload
			// returns a non-nil WriteResult alongside a non-nil
			// error on the CommitAfterTimeout path (data is durable;
			// stream reader's SettleWindow may have moved past it),
			// and the caller wants the offset/ref for outbox
			// journaling on those partitions too.
			if r != nil {
				results[i] = r
			}
			return err
		})

	// Compact successful results in sorted-key order regardless
	// of err — partial success on failure is documented behaviour.
	var out []WriteResult
	for i := range parts {
		if results[i] != nil {
			out = append(out, *results[i])
		}
	}
	return out, err
}

// WriteWithKey encodes records as Parquet, uploads to S3, writes
// the ref file, and lands the token-commit marker that flips
// visibility for both the snapshot and stream read paths
// atomically.
//
// Each attempt writes to a per-attempt data path
// (`{partition}/{token}-{attemptID}.parquet`), so no data PUT
// ever overwrites — sidesteps multi-site StorageGRID's
// eventual-consistency exposure on data-file overwrites. token
// is the caller's WithIdempotencyToken value, or a
// writer-generated UUIDv7 (used as both token and attemptID) for
// non-idempotent writes. The trade is a per-attempt orphan triple
// (data + ref + possibly token-commit) on failure; the reader's
// commit-marker gate filters them out, and the operator-driven
// sweeper reclaims them.
//
// On any failure mid-sequence the returned error is wrapped and
// nothing is deleted. This is the at-least-once contract: a
// failed Write may leave per-attempt orphans, never deletes one.
// Reads ignore the orphans because their token-commits either
// didn't land or name a different attempt-id.
//
// Passing WithIdempotencyToken makes this call retry-safe across
// arbitrary outages: an upfront HEAD on `<token>.commit` returns
// any prior commit's WriteResult reconstructed from metadata
// (no body re-upload, no new PUTs). If no prior commit exists,
// the retry proceeds with a fresh attempt-id; recovery is
// automatic regardless of how long ago the original landed.
// **Concurrent writes that share the same token are out of
// contract** — see README's Concurrency contract section.
func (s *Writer[T]) WriteWithKey(
	ctx context.Context, key string, records []T, opts ...WriteOption,
) (result *WriteResult, err error) {
	scope := s.cfg.Target.metrics.methodScope(ctx, methodWriteWithKey)
	defer scope.end(&err)
	if len(records) == 0 {
		return nil, nil
	}
	result, _, err = s.writeWithKeyResolved(ctx, key, records, opts, scope)
	return result, err
}

// writeWithKeyResolved is the shared per-partition entry point
// for Write (per-partition dispatch via writeGroupedFanOut) and
// WriteWithKey (direct single-partition call). It owns the option-
// resolution step so both entry points see consistent semantics:
// the static idempotencyToken is pre-validated, the per-partition
// idempotencyTokenFn (if set) is invoked here with the partition's
// records, and the resulting token is validated and substituted
// into a local writeOpts before encode/PUT.
//
// scope is the caller's methodScope. On commit (ref PUT succeeded),
// this function increments the scope's record / byte / partition
// counters via the additive addX methods. Failures before commit
// don't touch the scope, so the scope reports "what actually
// landed in S3," not "what we attempted to write." Safe under
// Write's parallel partition fan-out — addX is atomic.
//
// Returns the parquet body byte count alongside the WriteResult
// because writeEncodedPayload also exposes it; the caller doesn't
// need it (the scope already has it on commit) but pre-existing
// signatures are preserved.
func (s *Writer[T]) writeWithKeyResolved(
	ctx context.Context, key string, records []T, opts []WriteOption,
	scope *methodScope,
) (*WriteResult, int, error) {
	// Validate the partition key first — surfacing a malformed key
	// before resolveWriteOpts means the user's idempotencyTokenFn
	// closure (which may have side effects: outbox-row reads, log
	// lines) doesn't run on a write that's about to fail anyway.
	if err := s.validateKey(key); err != nil {
		return nil, 0, err
	}
	o, err := resolveWriteOpts(opts, records)
	if err != nil {
		return nil, 0, err
	}

	// Resolve token + check for prior commit before any work that
	// mutates caller state or burns CPU. populateInsertedAt writes
	// into records via reflection; encodeParquet builds bytes that
	// won't be PUT on retry-found-prior-commit. Lifting the HEAD
	// here keeps the caller's slice untouched on the dedup path.
	token, autoToken, prior, err := s.resolveTokenAndCheckCommit(
		ctx, key, o.idempotencyToken)
	if err != nil {
		return nil, 0, err
	}
	if prior != nil {
		// Retry-found-prior-commit short-circuit: no encode, no
		// caller-slice mutation, no PUT. Account for the prior
		// commit's RowCount so partial-success Write metrics still
		// reflect what's durable.
		scope.addRecords(prior.RowCount)
		scope.addPartitions(1)
		return prior, 0, nil
	}

	// Capture writeStartTime here (before encode) so the same value
	// is used to populate the InsertedAtField column AND to stamp
	// the token-commit's `insertedat` metadata — a single "when
	// was this batch written" value propagates to every downstream
	// surface. WithInsertedAt overrides the auto-capture; a
	// zero-value option falls back to time.Now(). Truncated to
	// microsecond precision so a LookupCommit round-trip (which
	// reads back through UnixMicro / time.UnixMicro) yields a
	// time.Time that compares Equal to the value embedded in the
	// original WriteResult; without truncation, sub-µs nanoseconds
	// are lost on the wire and the round-trip mismatches on
	// platforms whose clocks have sub-µs resolution (Linux).
	writeStartTime := o.insertedAt
	if writeStartTime.IsZero() {
		writeStartTime = time.Now()
	}
	writeStartTime = writeStartTime.Truncate(time.Microsecond)
	if s.insertedAtFieldIndex != nil {
		populateInsertedAt(records, s.insertedAtFieldIndex, writeStartTime)
	}

	parquetBytes, err := encodeParquet(
		records, s.compressionCodec)
	if err != nil {
		return nil, 0, fmt.Errorf("parquet encode: %w", err)
	}
	r, err := s.writeEncodedPayload(
		ctx, key, records, parquetBytes, writeStartTime,
		token, autoToken)
	if r != nil {
		// Commit semantics: writeEncodedPayload returned a non-nil
		// WriteResult ⇒ data is durable, markers are written, ref
		// PUT succeeded, and the token-commit landed. err may still
		// be non-nil (CommitAfterTimeout — durable but the stream
		// reader's SettleWindow may have advanced past refMicroTs);
		// the records ARE persisted either way, so the scope
		// reflects what's in S3, not "what we attempted to write."
		// Source from r.RowCount so this path aligns symmetrically
		// with the retry-found-prior-commit branch above.
		scope.addRecords(r.RowCount)
		scope.addBytes(int64(len(parquetBytes)))
		scope.addPartitions(1)
	}
	return r, len(parquetBytes), err
}

// resolveTokenAndCheckCommit owns step 1+2 of the write sequence
// in isolation so writeWithKeyResolved can short-circuit on the
// retry-found-prior-commit path before any CPU- or caller-state-
// touching work (populateInsertedAt, encodeParquet).
//
// Returns:
//   - token: the resolved token. Caller's explicit token verbatim,
//     or a freshly-generated UUIDv7 on the auto-token path (used
//     as both token and attempt-id downstream so the path layout
//     is uniform: <token>-<attemptID>).
//   - autoToken: true when the writer generated the token.
//   - prior: non-nil when a same-token commit already exists. The
//     reconstructed WriteResult is the original commit's payload —
//     RefPath, Offset, InsertedAt, RowCount round-tripped through
//     `<token>.commit` user-metadata. Caller returns this without
//     any further work.
//   - err: HEAD failure or auto-token generation failure.
//
// The auto-token path skips the HEAD entirely — a freshly-generated
// UUIDv7 is guaranteed to 404 by construction.
func (s *Writer[T]) resolveTokenAndCheckCommit(
	ctx context.Context, key, callerToken string,
) (token string, autoToken bool, prior *WriteResult, err error) {
	if callerToken == "" {
		auto, err := newAttemptID()
		if err != nil {
			return "", false, nil, fmt.Errorf("generate auto-token: %w", err)
		}
		return auto, true, nil, nil
	}
	meta, exists, err := headTokenCommit(ctx, s.cfg.Target,
		s.dataPath, key, callerToken)
	if err != nil {
		return "", false, nil, fmt.Errorf("head token-commit: %w", err)
	}
	if exists {
		wr := reconstructWriteResult(s.dataPath, s.refPath,
			key, callerToken, meta)
		return callerToken, false, &wr, nil
	}
	return callerToken, false, nil, nil
}

// writeEncodedPayload is the post-encode orchestration for
// WriteWithKey. The new sequence (3 PUTs, 0 HEADs in the
// happy-path-without-idempotency-token, 1 HEAD on idempotent
// retries) is the heart of the token-commit redesign:
//
//  1. Token resolution. Use the caller's WithIdempotencyToken
//     verbatim; otherwise generate a fresh UUIDv7 and use it as
//     both the token and the attempt-id (the path layout stays
//     uniform, parsing is one-case).
//  2. Upfront commit check (idempotent path only). HEAD
//     `<dataPath>/<partition>/<token>.commit`. 200 → reconstruct
//     WriteResult from the metadata and return without re-issuing
//     any PUT. 404 → proceed with a fresh attempt. Skipped on the
//     auto-token path: the UUIDv7 was just generated, so the HEAD
//     is guaranteed to 404 by construction.
//  3. Generate attempt-id. UUIDv7 hex (32 lowercase hex chars).
//     For non-idempotent writes the auto-token doubles as the
//     attempt-id; for idempotent writes it's freshly generated.
//  4. Projection markers PUT (Phase 3 ordering: before data, so
//     any data file on S3 implies its R1 markers landed).
//  5. Data PUT to `<dataPath>/<partition>/<token>-<attemptID>.parquet`.
//     Unconditional; path is unique per attempt by construction.
//  6. Ref PUT to
//     `<refPath>/<refMicroTs>-<token>-<attemptID>;<hiveEsc>.ref`.
//     refMicroTs is captured immediately before this PUT so the
//     encoded value tracks ref-LIST-visibility as tightly as
//     possible — bounds the writer↔reader skew SettleWindow has
//     to absorb. Issued under a context.WithoutCancel of the
//     caller's ctx (see step 6+7 note below).
//  7. Token-commit PUT at `<dataPath>/<partition>/<token>.commit`,
//     zero-byte body, with user-metadata `attemptid` + `refmicrots`
//     so reads can reconstruct the WriteResult on retry without a
//     LIST. Single atomic event flipping read-side visibility.
//     Same detached context as step 6: once the data PUT lands,
//     these two zero-byte PUTs run to completion regardless of
//     caller cancellation, so a cancel-after-data doesn't leave
//     an orphan parquet that's invisible-but-durable. Bounded by
//     retryMaxAttempts plus the AWS SDK's per-request timeouts.
//  8. Sanity check (writer-local). When the elapsed time from
//     refMicroTs (step 6) to now exceeds CommitTimeout, increment
//     s3store.write.commit_after_timeout and return an error. The
//     commit landed and the data is durable for snapshot reads;
//     the error signals that a stream reader's SettleWindow
//     (= CommitTimeout + MaxClockSkew) may have already advanced
//     past refMicroTs before the token-commit became visible.
//     Pre-ref work (parquet encoding, marker PUTs, data PUT — the
//     last scaling with payload size) is deliberately excluded:
//     the SettleWindow contract is bounded by ref-LIST-visible →
//     token-commit-visible only.
//
// Per-attempt data paths sidestep multi-site StorageGRID's
// eventual-consistency exposure on data-file overwrites. The
// token-commit IS overwriteable across concurrent retries of the
// same token, but **concurrent writes per (partition, token) are
// out of contract** (see README's Concurrency contract section);
// under sequential retries the upfront HEAD short-circuits before
// any second token-commit PUT lands.
//
// writeStartTime is the wall clock captured by the caller just
// before parquet encoding — used for the InsertedAtField column
// only. The WriteResult's InsertedAt comes from refMicroTs (so
// retries that find a prior commit return the original commit's
// InsertedAt unchanged via the token-commit metadata).
//
// token / autoToken come from resolveTokenAndCheckCommit, which
// the caller invoked before encode to short-circuit retry-found-
// prior-commit without touching caller state. autoToken=true
// signals "writer generated this token, use it as the attempt-id
// too" (uniform path layout: <token>-<attemptID>).
func (s *Writer[T]) writeEncodedPayload(
	ctx context.Context, key string, records []T, parquetBytes []byte,
	writeStartTime time.Time, token string, autoToken bool,
) (*WriteResult, error) {
	// Compute marker paths up-front so a bad ProjectionDef.Of
	// fails the whole Write before we touch S3, matching how
	// validateKey aborts on a malformed partition key.
	markerPaths, err := s.collectProjectionMarkerPaths(records)
	if err != nil {
		return nil, err
	}

	// Step 3: attempt-id. The auto-token path reuses the token
	// (same UUIDv7) so id == "<UUIDv7>-<UUIDv7>"; the idempotent
	// path generates a fresh UUIDv7 distinct from the token.
	var attemptID string
	if autoToken {
		attemptID = token
	} else {
		fresh, err := newAttemptID()
		if err != nil {
			return nil, fmt.Errorf("generate attempt-id: %w", err)
		}
		attemptID = fresh
	}
	id := makeID(token, attemptID)
	dataKey := buildDataFilePath(s.dataPath, key, id)

	// Step 4: projection markers, before data (Phase 3 ordering).
	if err := s.putMarkers(ctx, markerPaths); err != nil {
		return nil, fmt.Errorf("put projection markers: %w", err)
	}

	// Step 5: data PUT to fresh path. Unconditional (path is
	// unique per attempt by construction).
	if err := s.cfg.Target.put(ctx, dataKey, parquetBytes,
		"application/octet-stream"); err != nil {
		return nil, fmt.Errorf("put data: %w", err)
	}

	// Step 6 + 7: ref PUT and token-commit PUT issue under a
	// detached context that ignores caller cancellation. Once the
	// data PUT has landed, a cancellation here would leave an
	// orphan parquet (invisible to readers via the commit gate,
	// but dead weight on S3) when the work to make it visible is
	// two zero-byte PUTs away. Bounded by retryMaxAttempts (4)
	// per call plus the AWS SDK's per-request timeouts; no extra
	// deadline needed. A caller that genuinely needs to abort
	// must do so before the data PUT (Step 5) returns.
	commitCtx := context.WithoutCancel(ctx)

	// Step 6: ref PUT. Capture refMicroTs immediately before the
	// PUT so the encoded value tracks ref-LIST-visibility as
	// tightly as possible. Writer wall-clock is now in the
	// protocol via this field — MaxClockSkew bounds writer↔reader
	// skew (see CLAUDE.md "Backend assumptions").
	refMicroTs := time.Now().UnixMicro()
	refKey := encodeRefKey(s.refPath, refMicroTs, token, attemptID, key)
	if err := s.cfg.Target.put(commitCtx, refKey, []byte{},
		"application/octet-stream"); err != nil {
		return nil, fmt.Errorf("put ref: %w", err)
	}

	// Step 7: token-commit PUT with attempt-id, refMicroTs,
	// writeStartTime (for InsertedAt round-tripping on retry), and
	// the parquet's row count (so LookupCommit / retry-recovery /
	// Poll surface RowCount without a parquet GET). The single
	// atomic event that flips visibility for both read paths.
	rowCount := int64(len(records))
	if err := putTokenCommit(commitCtx, s.cfg.Target,
		s.dataPath, key, token, attemptID,
		refMicroTs, writeStartTime.UnixMicro(), rowCount); err != nil {
		return nil, fmt.Errorf("put token-commit: %w", err)
	}

	// Step 8: writer-local contract enforcement. Past
	// CommitTimeout, the reader's SettleWindow (= CommitTimeout +
	// MaxClockSkew) may have already advanced past this write's
	// `refMicroTs` and emitted the ref before the token-commit
	// became visible — i.e., a stream reader could miss it. The
	// writer surfaces this as an error so the caller knows their
	// write is at risk; the token-commit IS in place (the data is
	// durable + committed for snapshot reads), so an idempotent
	// retry recovers automatically via the upfront-HEAD path.
	//
	// The WriteResult is returned alongside the error so the caller
	// retains the offset / refKey / row count for outbox journaling
	// and `LookupCommit` use cases — the data IS in S3 at the
	// returned paths, just maybe past the stream reader's window.
	//
	// Measured from `refMicroTs` (the wall-clock stamped just before
	// the ref PUT), not from writeStartTime: the contract-relevant
	// interval is ref-LIST-visible → token-commit-visible. Anything
	// before the ref PUT (parquet encoding, marker PUTs, data PUT —
	// the last of which scales with payload size) cannot put the
	// SettleWindow contract at risk, so it doesn't belong in the
	// budget.
	result := &WriteResult{
		Offset:     Offset(refKey),
		DataPath:   dataKey,
		RefPath:    refKey,
		InsertedAt: writeStartTime,
		RowCount:   rowCount,
	}
	commitTimeout := s.cfg.Target.CommitTimeout()
	tCommitWindowStart := time.UnixMicro(refMicroTs)
	if elapsed := time.Since(tCommitWindowStart); elapsed > commitTimeout {
		// Use commitCtx (not the caller's ctx) so a caller cancel
		// during the commit PUTs doesn't suppress the only signal
		// that this write committed past CommitTimeout.
		s.cfg.Target.metrics.recordCommitAfterTimeout(commitCtx)
		return result, fmt.Errorf(
			"write committed after CommitTimeout "+
				"(elapsed %v > %v from ref PUT) — stream reader's "+
				"SettleWindow may not include this write; data is "+
				"durable at %s, ref at %s, retry with the same "+
				"WithIdempotencyToken recovers via upfront-HEAD",
			elapsed, commitTimeout, dataKey, refKey)
	}

	return result, nil
}

// GroupByPartition splits records by their Hive partition key
// (PartitionKeyOf) and returns one HivePartition per distinct
// key in lex-ascending order of Key. Deterministic across calls
// — same input produces byte-identical output, mirroring the
// read-side emission-order invariant. Same HivePartition[T]
// type the ReadPartition* methods yield.
//
// Use to preview partitioning without paying the write cost
// (logging, sharding decisions, dry-run validation) or as a
// building block for custom write strategies (filter some
// partitions, write the rest; route partitions to different
// Targets; etc.).
//
// Empty records returns nil. Records whose PartitionKeyOf
// returns the same string land in the same HivePartition;
// per-partition record order is preserved (insertion order from
// the input slice).
//
// Public contract: partition emission is lex-ordered. See
// "Deterministic emission order across read and write paths"
// in CLAUDE.md — GroupByPartition is the write-side
// counterpart of that invariant.
func (s *Writer[T]) GroupByPartition(records []T) []HivePartition[T] {
	if len(records) == 0 {
		return nil
	}
	grouped := make(map[string][]T)
	for _, r := range records {
		key := s.cfg.PartitionKeyOf(r)
		grouped[key] = append(grouped[key], r)
	}
	keys := slices.Sorted(maps.Keys(grouped))
	out := make([]HivePartition[T], len(keys))
	for i, k := range keys {
		out[i] = HivePartition[T]{Key: k, Rows: grouped[k]}
	}
	return out
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
	if len(segments) != len(s.cfg.Target.PartitionKeyParts()) {
		return fmt.Errorf(
			"key %q has %d segments, expected %d (%v)",
			key, len(segments),
			len(s.cfg.Target.PartitionKeyParts()), s.cfg.Target.PartitionKeyParts())
	}
	for i, seg := range segments {
		part := s.cfg.Target.PartitionKeyParts()[i]
		prefix := part + "="
		if !strings.HasPrefix(seg, prefix) {
			return fmt.Errorf(
				"key %q segment %d is %q, expected prefix %q",
				key, i, seg, prefix)
		}
		value := seg[len(prefix):]
		if err := validateHivePartitionValue(value); err != nil {
			return fmt.Errorf(
				"key %q segment %d (%q): %w",
				key, i, part, err)
		}
	}
	return nil
}

// populateInsertedAt reflectively writes t into every record's
// InsertedAtField (at path fieldIdx). Called by WriteWithKey
// before parquet encode so the value lands in the file as a real
// column.
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
