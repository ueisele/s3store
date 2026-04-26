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
	"time"

	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
)

// WriteResult contains metadata about a completed write.
// InsertedAt is the writer's wall-clock capture at write-start
// — the same value that populates the configured InsertedAtField
// column, the x-amz-meta-created-at header, and the dataTsMicros
// component of the ref filename. Exposed on the result so callers
// can log / persist it (e.g., into an outbox table) without
// parsing the data path or issuing a HEAD.
type WriteResult struct {
	Offset     Offset
	DataPath   string
	RefPath    string
	InsertedAt time.Time
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
// WriteOpts and validates embedded values (IdempotencyToken
// passes ValidateIdempotencyToken). Done once per Write call so
// per-partition dispatch doesn't re-validate on every goroutine.
func resolveWriteOpts(opts []WriteOption) (WriteOpts, error) {
	var w WriteOpts
	w.Apply(opts...)
	if w.IdempotencyToken != "" {
		if err := validateIdempotencyToken(
			w.IdempotencyToken); err != nil {
			return WriteOpts{}, err
		}
		if w.MaxRetryAge <= 0 {
			return WriteOpts{}, fmt.Errorf(
				"s3parquet: MaxRetryAge must be > 0 (got %s) "+
					"when IdempotencyToken is set",
				w.MaxRetryAge)
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
	grouped := s.groupByKey(records)
	keys := slices.Sorted(maps.Keys(grouped))

	// Slot i holds the result for keys[i] so completion order
	// cannot leak into the returned slice even under parallel
	// execution.
	results := make([]*WriteResult, len(keys))

	err := fanOut(ctx, keys,
		s.cfg.Target.EffectiveMaxInflightRequests(),
		func(ctx context.Context, i int, key string) error {
			r, err := perPartition(ctx, key, grouped[key])
			if err != nil {
				return err
			}
			results[i] = r
			return nil
		})

	// Compact successful results in sorted-key order regardless
	// of err — partial success on failure is documented behaviour.
	var out []WriteResult
	for i := range keys {
		if results[i] != nil {
			out = append(out, *results[i])
		}
	}
	return out, err
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
// WithIdempotencyToken for the full contract.
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
	ctx context.Context, key string, records []T, opts WriteOpts,
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

// writeEncodedPayload is the post-encode orchestration for
// WriteWithKey. The body is a four-phase commit sequence —
// data → retry-dedup → markers →
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
	writeStartTime time.Time, opts WriteOpts,
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
		id = makeAutoID(tsMicros, uuid.New().String()[:8])
	}
	dataKey := buildDataFilePath(s.dataPath, key, id)

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
		})
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
	if err := s.putMarkers(ctx, markerPaths); err != nil {
		_ = s.cleanupOrphanData(ctx, dataKey, idempotent)
		return nil, fmt.Errorf(
			"s3parquet: put index markers: %w", err)
	}

	// DisableRefStream: skip the ref PUT entirely. Offset and
	// RefPath go empty so callers can't mistake the returned value
	// for a Poll-visible stream position.
	if s.cfg.Target.DisableRefStream() {
		return &WriteResult{
			Offset:     "",
			DataPath:   dataKey,
			RefPath:    "",
			InsertedAt: writeStartTime,
		}, nil
	}

	// Phase 4: ref PUT under a SettleWindow/2 client-side timeout.
	return s.commitRef(
		ctx, dataKey, id, tsMicros, key, writeStartTime, idempotent)
}

// commitRef is phase 4 of writeEncodedPayload: PUT the ref under
// a SettleWindow/2 client-side timeout. On success returns the
// WriteResult; on PUT failure (timeout, transport error, etc.)
// runs best-effort orphan-data cleanup and surfaces the wrapped
// error so the caller retries.
//
// refCaptureTime is captured just before the PUT so the ref
// filename's refTsMicros reflects publication time, not
// write-start. SettleWindow only needs to cover ref-PUT latency
// + LIST propagation, independent of marker count.
//
// The library does not do post-hoc HEAD on a PUT failure to
// disambiguate lost-ack: under any backend honouring
// read-after-new-write (which we require), the caller's retry
// achieves the same outcome with one extra round-trip on a rare
// event. A weakly-consistent backend would defeat the HEAD too;
// correctness requires ConsistencyStrongGlobal /
// ConsistencyStrongSite on StorageGRID-style backends.
//
// At-least-once invariant on nil return: the returned RefPath is
// LIST-visible and its refTsMicros was captured under SettleWindow/2
// ago, well inside any concurrent Poll's cutoff.
func (s *Writer[T]) commitRef(
	ctx context.Context,
	dataKey, id string, dataTsMicros int64, hiveKey string,
	writeStartTime time.Time, idempotent bool,
) (*WriteResult, error) {
	refCaptureTime := time.Now()
	refTsMicros := refCaptureTime.UnixMicro()
	refKey := encodeRefKey(
		s.refPath, refTsMicros, id, dataTsMicros, hiveKey)

	settle := s.cfg.Target.EffectiveSettleWindow()
	putCtx, cancel := context.WithTimeout(ctx, settle/2)
	defer cancel()

	if err := s.cfg.Target.put(
		putCtx, refKey, []byte{}, "application/octet-stream",
	); err != nil {
		if delErr := s.cleanupOrphanData(ctx, dataKey, idempotent); delErr != nil {
			return nil, fmt.Errorf(
				"s3parquet: put ref: %w (orphan data at %s: %v)",
				err, dataKey, delErr)
		}
		return nil, fmt.Errorf("s3parquet: put ref: %w", err)
	}

	return &WriteResult{
		Offset:     Offset(refKey),
		DataPath:   dataKey,
		RefPath:    refKey,
		InsertedAt: writeStartTime,
	}, nil
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
//     A stale ref can only exist when an earlier attempt's PUT
//     ack was lost after server-side persistence; the retry then
//     emits a fresh in-budget ref.
//
// Inherits the target's ConsistencyControl on the LIST (every
// listEach call does) so the scan sees all prior refs on
// StorageGRID-style backends — a weak-consistency LIST can miss
// a ref the writer just published on another node, silently
// breaking dedup.
//
// resolveWriteOpts validates that maxRetryAge > 0 when an
// idempotency token is set, so callers never reach here with a
// non-positive value.
func (s *Writer[T]) findExistingRef(
	ctx context.Context, token string, maxRetryAge time.Duration,
) (string, error) {
	now := time.Now()
	lo, hi := refRangeForRetry(s.refPath, now, maxRetryAge)
	settleCutoffUs := now.Add(
		-s.cfg.Target.EffectiveSettleWindow()).UnixMicro()
	var found string
	err := s.cfg.Target.listEach(ctx, s.refPath+"/", lo, 0,
		func(obj s3types.Object) (bool, error) {
			if obj.Key == nil {
				return true, nil
			}
			if *obj.Key > hi {
				// Past the retry window. Lex compare holds because
				// all real tsMicros share the same decimal width
				// (16 digits post-2001), so lex order matches
				// numeric order — same assumption RefCutoff and
				// RefRangeForRetry's lo already rely on.
				return false, nil
			}
			_, refTsMicros, id, _, err := parseRefKey(*obj.Key)
			if err != nil {
				// Malformed ref keys (externally written or a
				// future schema the parser doesn't understand)
				// aren't our retry target — skip rather than fail
				// the write.
				return true, nil //nolint:nilerr // intentional: skip malformed
			}
			if id != token {
				return true, nil
			}
			if refTsMicros < settleCutoffUs {
				// Stale match: the ref sits past the settle cutoff
				// a concurrent Poll would compute right now, so
				// some consumers may have advanced past it. Skip,
				// so the caller emits a fresh ref that every
				// consumer can still yield.
				return true, nil
			}
			found = *obj.Key
			return false, nil
		})
	if err != nil {
		return "", err
	}
	return found, nil
}

// cleanupOrphanData runs the best-effort delete for an orphaned
// data object on the failure paths. No-op (nil return) when the
// write is idempotent (retries reuse the same path; deleting
// would force body re-upload) or when DisableCleanup is set
// (operator opted into lifecycle-based garbage collection).
//
// Uses the caller's ctx — if the caller cancels, the DELETE is
// interrupted and the orphan stays for bucket lifecycle to
// collect. Same posture as DisableCleanup, just opportunistic.
//
// Returns the del error when the delete actually ran and failed.
// Fire-and-forget sites (commitMarkers) discard the returned
// error because they already have a richer error to surface and
// compounding obscures the root cause. The ref-PUT failure path
// folds the returned error into its user-facing message so
// operators without lifecycle policies can find the orphan.
func (s *Writer[T]) cleanupOrphanData(
	ctx context.Context, dataKey string, idempotent bool,
) error {
	if idempotent || s.cfg.DisableCleanup {
		return nil
	}
	return s.cfg.Target.del(ctx, dataKey)
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
	if len(segments) != len(s.cfg.Target.PartitionKeyParts()) {
		return fmt.Errorf(
			"s3parquet: key %q has %d segments, "+
				"expected %d (%v)",
			key, len(segments),
			len(s.cfg.Target.PartitionKeyParts()), s.cfg.Target.PartitionKeyParts())
	}
	for i, seg := range segments {
		part := s.cfg.Target.PartitionKeyParts()[i]
		prefix := part + "="
		if !strings.HasPrefix(seg, prefix) {
			return fmt.Errorf(
				"s3parquet: key %q segment %d is %q, "+
					"expected prefix %q",
				key, i, seg, prefix)
		}
		value := seg[len(prefix):]
		if err := validateHivePartitionValue(value); err != nil {
			return fmt.Errorf(
				"s3parquet: key %q segment %d (%q): %w",
				key, i, part, err)
		}
	}
	return nil
}

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
			p, err := idx.pathOf(rec)
			if err != nil {
				return nil, fmt.Errorf(
					"s3parquet: index %q: %w", idx.name, err)
			}
			if p == "" {
				continue
			}
			seen[p] = struct{}{}
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

// putMarkers fans marker PUTs out through fanOut. The
// per-target MaxInflightRequests semaphore inside target.put caps
// net in-flight S3 requests, so the fan-out can't overshoot
// http.Transport.MaxConnsPerHost — extra goroutines just queue at
// the semaphore. Returns the first PUT error; partial success on
// failure is accepted — orphan markers are tolerated at Lookup
// time.
func (s *Writer[T]) putMarkers(
	ctx context.Context, paths []string,
) error {
	return fanOut(ctx, paths,
		s.cfg.Target.EffectiveMaxInflightRequests(),
		func(ctx context.Context, _ int, p string) error {
			return s.cfg.Target.put(
				ctx, p, nil, "application/octet-stream",
			)
		})
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
