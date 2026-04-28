package s3store

import (
	"bytes"
	"context"
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
// InsertedAt is the writer's wall-clock capture at write-start
// (pre-encode), microsecond precision — the same value stamped
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
		return nil, fmt.Errorf(
			"s3store: PartitionKeyOf is required for Write; " +
				"use WriteWithKey for explicit keys")
	}
	writeOpts, err := resolveWriteOpts(opts)
	if err != nil {
		return nil, err
	}
	results, err = s.writeGroupedFanOut(ctx, records,
		func(ctx context.Context, key string, recs []T) (*WriteResult, error) {
			r, _, err := s.writeWithKeyResolved(ctx, key, recs, writeOpts, scope)
			return r, err
		})
	return results, err
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
		s.cfg.Target.metrics,
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
	writeOpts, err := resolveWriteOpts(opts)
	if err != nil {
		return nil, err
	}
	result, _, err = s.writeWithKeyResolved(ctx, key, records, writeOpts, scope)
	return result, err
}

// writeWithKeyResolved is the post-option-resolution shared entry
// point for Write (per-partition dispatch) and WriteWithKey (direct
// call). Lets Write resolve options once and avoid the per-
// partition revalidation that calling WriteWithKey in the fan-out
// closure would imply.
//
// scope is the caller's methodScope. On commit (ref PUT succeeded),
// this function increments the
// scope's record / byte / partition counters via the additive
// addX methods. Failures before commit don't touch the scope, so
// the scope reports "what actually landed in S3," not "what we
// attempted to write." Safe under Write's parallel partition
// fan-out — addX is atomic.
//
// Returns the parquet body byte count alongside the WriteResult
// because writeEncodedPayload also exposes it; the caller doesn't
// need it (the scope already has it on commit) but pre-existing
// signatures are preserved.
func (s *Writer[T]) writeWithKeyResolved(
	ctx context.Context, key string, records []T, opts WriteOpts,
	scope *methodScope,
) (*WriteResult, int, error) {
	if err := s.validateKey(key); err != nil {
		return nil, 0, err
	}

	// Capture writeStartTime here (before encode) so the same value
	// is used to populate the InsertedAtField column AND to stamp
	// the data filename's tsMicros — a single "when was this batch
	// written" value propagates to every downstream surface.
	writeStartTime := time.Now()
	if s.insertedAtFieldIndex != nil {
		populateInsertedAt(records, s.insertedAtFieldIndex, writeStartTime)
	}

	parquetBytes, err := encodeParquet(
		records, s.compressionCodec)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"s3store: parquet encode: %w", err)
	}
	r, err := s.writeEncodedPayload(
		ctx, key, records, parquetBytes, writeStartTime, opts)
	if err == nil && r != nil {
		// Commit semantics: writeEncodedPayload returned a non-nil
		// WriteResult ⇒ data is durable, markers are written, and
		// the ref PUT succeeded. Update the scope here so partial-success Write calls
		// (some partitions committed, others failed) report the
		// committed records/bytes/partitions, not the attempted
		// totals.
		scope.addRecords(int64(len(records)))
		scope.addBytes(int64(len(parquetBytes)))
		scope.addPartitions(1)
	}
	return r, len(parquetBytes), err
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
//     to absorb.
//  7. Token-commit PUT at `<dataPath>/<partition>/<token>.commit`,
//     zero-byte body, with user-metadata `attemptid` + `refmicrots`
//     so reads can reconstruct the WriteResult on retry without a
//     LIST. Single atomic event flipping read-side visibility.
//  8. Sanity check (writer-local). When the writer's local
//     elapsed exceeds CommitTimeout, increment
//     s3store.write.commit_after_timeout and warn. Not a failure
//     — the commit landed; this just signals that a SettleWindow
//     tuned to this CommitTimeout may not yet have included this
//     write in the stream window.
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
func (s *Writer[T]) writeEncodedPayload(
	ctx context.Context, key string, records []T, parquetBytes []byte,
	writeStartTime time.Time, opts WriteOpts,
) (*WriteResult, error) {
	// Compute marker paths up-front so a bad ProjectionDef.Of
	// fails the whole Write before we touch S3, matching how
	// validateKey aborts on a malformed partition key.
	markerPaths, err := s.collectProjectionMarkerPaths(records)
	if err != nil {
		return nil, err
	}

	// Step 1: token resolution. Auto-generate a UUIDv7 for the
	// no-token path and use it as both the token and the
	// attempt-id (path shape is uniform: <token>-<attemptID>).
	token := opts.IdempotencyToken
	autoToken := false
	if token == "" {
		auto, err := newAttemptID()
		if err != nil {
			return nil, fmt.Errorf(
				"s3store: generate auto-token: %w", err)
		}
		token = auto
		autoToken = true
	}

	// Step 2: upfront HEAD on <token>.commit. Skipped on the
	// auto-token path (HEAD would always 404 — we just generated
	// the token).
	if !autoToken {
		meta, exists, err := headTokenCommit(ctx, s.cfg.Target,
			s.dataPath, key, token)
		if err != nil {
			return nil, fmt.Errorf(
				"s3store: head token-commit: %w", err)
		}
		if exists {
			wr := reconstructWriteResult(s.dataPath, s.refPath,
				key, token, meta)
			return &wr, nil
		}
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
			return nil, fmt.Errorf(
				"s3store: generate attempt-id: %w", err)
		}
		attemptID = fresh
	}
	id := makeID(token, attemptID)
	dataKey := buildDataFilePath(s.dataPath, key, id)

	// Step 4: projection markers, before data (Phase 3 ordering).
	if err := s.putMarkers(ctx, markerPaths); err != nil {
		return nil, fmt.Errorf(
			"s3store: put projection markers: %w", err)
	}

	// Step 5: data PUT to fresh path. Unconditional (path is
	// unique per attempt by construction).
	if err := s.cfg.Target.put(ctx, dataKey, parquetBytes,
		"application/octet-stream"); err != nil {
		return nil, fmt.Errorf("s3store: put data: %w", err)
	}

	// Step 6: ref PUT. Capture refMicroTs immediately before the
	// PUT so the encoded value tracks ref-LIST-visibility as
	// tightly as possible. Writer wall-clock is now in the
	// protocol via this field — MaxClockSkew bounds writer↔reader
	// skew (see CLAUDE.md "Backend assumptions").
	refMicroTs := time.Now().UnixMicro()
	refKey := encodeRefKey(s.refPath, refMicroTs, token, attemptID, key)
	if err := s.cfg.Target.put(ctx, refKey, []byte{},
		"application/octet-stream"); err != nil {
		return nil, fmt.Errorf("s3store: put ref: %w", err)
	}

	// Step 7: token-commit PUT with attempt-id, refMicroTs, and
	// writeStartTime (for InsertedAt round-tripping on retry).
	// The single atomic event that flips visibility for both
	// read paths.
	if err := putTokenCommit(ctx, s.cfg.Target,
		s.dataPath, key, token, attemptID,
		refMicroTs, writeStartTime.UnixMicro()); err != nil {
		return nil, fmt.Errorf(
			"s3store: put token-commit: %w", err)
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
	commitTimeout := s.cfg.Target.CommitTimeout()
	if elapsed := time.Since(writeStartTime); elapsed > commitTimeout {
		s.cfg.Target.metrics.recordCommitAfterTimeout(ctx)
		return nil, fmt.Errorf(
			"s3store: write committed after CommitTimeout "+
				"(elapsed %v > %v) — stream reader's SettleWindow "+
				"may not include this write; data is durable at "+
				"%s, retry with the same WithIdempotencyToken "+
				"recovers via upfront-HEAD",
			elapsed, commitTimeout, dataKey)
	}

	return &WriteResult{
		Offset:     Offset(refKey),
		DataPath:   dataKey,
		RefPath:    refKey,
		InsertedAt: writeStartTime,
	}, nil
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
			"s3store: key %q has %d segments, "+
				"expected %d (%v)",
			key, len(segments),
			len(s.cfg.Target.PartitionKeyParts()), s.cfg.Target.PartitionKeyParts())
	}
	for i, seg := range segments {
		part := s.cfg.Target.PartitionKeyParts()[i]
		prefix := part + "="
		if !strings.HasPrefix(seg, prefix) {
			return fmt.Errorf(
				"s3store: key %q segment %d is %q, "+
					"expected prefix %q",
				key, i, seg, prefix)
		}
		value := seg[len(prefix):]
		if err := validateHivePartitionValue(value); err != nil {
			return fmt.Errorf(
				"s3store: key %q segment %d (%q): %w",
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
