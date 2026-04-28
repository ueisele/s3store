package s3store

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
)

// WriteResult contains metadata about a completed write.
// InsertedAt is the writer's wall-clock capture at write-start —
// the same value that populates the configured InsertedAtField
// column and the dataTsMicros component of the ref filename.
// Exposed on the result so callers can log / persist it (e.g.,
// into an outbox table) without parsing the data path or issuing
// a HEAD.
//
// Under WithIdempotencyToken, a same-token retry whose upfront
// LIST finds a prior valid commit returns *that* commit's
// WriteResult (DataPath, RefPath, InsertedAt all reflect the
// prior attempt). Callers comparing two results from the same
// token across retries will see identical values when the prior
// attempt landed within CommitTimeout, and different per-attempt
// values when an earlier attempt aged out.
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
// the ref file, and lands the commit marker that flips visibility
// for both the snapshot and stream read paths atomically.
//
// Each attempt writes to a per-attempt path (id =
// {token}-{tsMicros}-{shortID} under WithIdempotencyToken,
// {tsMicros}-{shortID} otherwise), so no PUT in this method ever
// overwrites — sidesteps multi-site StorageGRID's eventual-
// consistency exposure on overwrites. The trade is a per-attempt
// orphan triple (data + ref + possibly marker) on failure; the
// reader's timeliness check filters them out and the operator-
// driven sweeper reclaims them.
//
// On any failure mid-sequence the returned error is wrapped and
// nothing is deleted. This is the at-least-once contract: a
// failed Write may leave per-attempt orphans, never deletes one.
// Snapshot and stream reads ignore the orphans because their
// commit markers either didn't land or fail the timeliness check.
//
// Passing WithIdempotencyToken makes this call retry-safe across
// arbitrary outages: an upfront LIST under
// {partition}/{token}- finds any prior valid commit and returns
// its WriteResult unchanged (no body re-upload, no new PUTs).
// If no valid prior commit exists, the retry proceeds with a
// fresh attempt-id and a fresh server-stamped data.LM —
// recovery is automatic regardless of how long ago the original
// landed. See WithIdempotencyToken for the full contract.
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
// WriteWithKey. The eight-step sequence is the heart of the
// commit-marker design:
//
//  1. Upfront LIST under {partition}/{token}- (only when
//     WithIdempotencyToken is set). Pair .parquet with .commit
//     by id, apply isCommitValid using LIST-page LMs. Any valid
//     pair → reconstruct WriteResult and return success without
//     re-issuing PUTs.
//  2. Generate fresh attempt-id. id = {token}-{tsMicros}-{shortID}
//     under WithIdempotencyToken, or {tsMicros}-{shortID} for
//     token-less writes. tsMicros is the writer's wall clock at
//     write-start; shortID is a UUID-derived 8 hex-char fragment.
//  3. Projection markers PUT (Phase 3 ordering: before data, so
//     any data file on S3 implies its R1 markers landed).
//  4. Data PUT to fresh path. Unconditional — but the per-attempt
//     id makes the path unique by construction, so nothing to
//     overwrite. Server stamps data.LM.
//  5. HEAD data file → server-stamped data.LastModified. This
//     value becomes the ref filename's refTsMicros and is stamped
//     into the marker's dataLM user metadata (consumed by stream
//     reads only).
//  6. Ref PUT at the path computed from
//     (refPath, data.LM, id, dataTsMicros, hiveKey) — refTsMicros
//     is server-stamped, so the reader's refCutoff comparison is
//     reader↔server only (writer's clock is not in the protocol).
//  7. Commit-marker PUT at {partition}/{id}.commit with user
//     metadata {dataLM}. Sibling of the .parquet, single source
//     of atomic visibility for Phases 5–6 read paths.
//  8. HEAD marker → server-stamped marker.LastModified. Verify
//     marker.LM - data.LM < CommitTimeout (same check the reader
//     runs, same server-stamped values). On violation: explicit
//     error; the per-attempt orphan stays on S3, filtered by
//     every read path's timeliness check.
//
// Per-attempt-paths sidestep multi-site StorageGRID's eventual-
// consistency exposure on overwrites: read-after-new-write is
// strongly consistent on every supported backend at any
// consistency level, so the post-PUT HEADs on data and marker
// always observe the values they just wrote. No If-None-Match,
// no s3:PutOverwriteObject deny policy — uniform behaviour
// across AWS S3, MinIO, and StorageGRID.
//
// writeStartTime is the wall clock captured by the caller just
// before parquet encoding — used to stamp the data filename
// tsMicros and the InsertedAtField column.
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

	commitTimeout := s.cfg.Target.CommitTimeout()

	// Step 1 (token-only): upfront LIST for a prior valid commit.
	// LIST returns both .parquet and .commit siblings with
	// LastModified — enough to run the timeliness check and
	// reconstruct WriteResult. No HEADs at this stage; same
	// primitive Phase 7's LookupCommit will use.
	if opts.IdempotencyToken != "" {
		ci, ok, err := findValidCommitForToken(ctx, s.cfg.Target,
			s.dataPath, key, opts.IdempotencyToken, commitTimeout)
		if err != nil {
			return nil, fmt.Errorf(
				"s3store: upfront LIST for token: %w", err)
		}
		if ok {
			wr, err := reconstructWriteResult(s.refPath, ci, key)
			if err != nil {
				return nil, fmt.Errorf(
					"s3store: reconstruct prior commit: %w", err)
			}
			return &wr, nil
		}
	}

	// Step 2: generate fresh per-attempt id.
	tsMicros := writeStartTime.UnixMicro()
	autoID := makeAutoID(tsMicros, uuid.New().String()[:8])
	id := autoID
	if opts.IdempotencyToken != "" {
		id = opts.IdempotencyToken + "-" + autoID
	}
	dataKey := buildDataFilePath(s.dataPath, key, id)

	// Step 3: projection markers, before data (Phase 3 ordering).
	if err := s.putMarkers(ctx, markerPaths); err != nil {
		return nil, fmt.Errorf(
			"s3store: put projection markers: %w", err)
	}

	// Step 4: data PUT to fresh path. Unconditional (path is
	// unique per attempt by construction).
	if err := s.cfg.Target.put(ctx, dataKey, parquetBytes,
		"application/octet-stream"); err != nil {
		return nil, fmt.Errorf("s3store: put data: %w", err)
	}

	// Step 5: HEAD data file to read server-stamped data.LM.
	dataLM, _, err := s.cfg.Target.head(ctx, dataKey)
	if err != nil {
		return nil, fmt.Errorf(
			"s3store: head data file: %w", err)
	}
	dataLMMicros := dataLM.UnixMicro()

	// Step 6: ref PUT. refTsMicros = server-stamped data.LM, so
	// the reader's refCutoff comparison is reader↔server only.
	refKey := encodeRefKey(s.refPath, dataLMMicros, id, tsMicros, key)
	if err := s.cfg.Target.put(ctx, refKey, []byte{},
		"application/octet-stream"); err != nil {
		return nil, fmt.Errorf("s3store: put ref: %w", err)
	}

	// Step 7: commit-marker PUT with dataLM metadata. The single
	// atomic event that flips visibility for both read paths.
	markerKey := commitMarkerKey(s.dataPath, key, id)
	if err := s.cfg.Target.putWithMeta(ctx, markerKey, []byte{},
		"application/octet-stream",
		map[string]string{
			dataLMMetaKey: strconv.FormatInt(dataLMMicros, 10),
		}); err != nil {
		return nil, fmt.Errorf(
			"s3store: put commit marker: %w", err)
	}

	// Step 8: HEAD marker, verify timeliness (same check the
	// reader runs, same server-stamped values, no clock-skew
	// sensitivity).
	markerLM, _, err := s.cfg.Target.head(ctx, markerKey)
	if err != nil {
		return nil, fmt.Errorf(
			"s3store: head commit marker: %w", err)
	}
	if !isCommitValid(dataLM, markerLM, commitTimeout) {
		return nil, fmt.Errorf(
			"s3store: commit marker %q stale: marker.LM - data.LM "+
				"= %v, want < CommitTimeout %v",
			markerKey, markerLM.Sub(dataLM), commitTimeout)
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
