package s3parquet

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go/compress"
	"github.com/ueisele/s3store/internal/core"
)

// probeTimeout caps the wall-clock duration of the eager
// overwrite-prevention probe in NewWriter. Defends against a
// misconfigured endpoint that would otherwise hang construction
// indefinitely. 10s leaves room for a slow first TLS handshake +
// 2-3 S3 round-trips while still failing fast on a real outage.
const probeTimeout = 10 * time.Second

// WriterConfig is the narrower Config form for constructing a
// Writer directly (without a Reader). Holds the S3-wiring bundle
// (Target) plus write-side-only knobs. Use NewWriter(cfg) when a
// service writes but never reads.
//
// Target carries Bucket / Prefix / S3Client / PartitionKeyParts /
// SettleWindow — shared with ReaderConfig through the same type,
// so a Writer and a Reader built from the same Target cannot
// drift on those fields. SettleWindow sits on Target because it
// governs marker visibility from Lookup (a read concern) but
// needs to be expressible at a pure-write call site.
type WriterConfig[T any] struct {
	Target         S3Target
	PartitionKeyOf func(T) string
	Compression    CompressionCodec

	// PartitionWriteConcurrency caps how many partitions Write
	// fans out in parallel per call. Zero → default (8). Raise
	// for workloads with many small partitions per Write (a
	// single poll from a DB fanning out to dozens of Hive keys)
	// where the default leaves S3 I/O idle. Lower if parquet
	// buffers are large enough that N × buffer-size would
	// dominate memory.
	PartitionWriteConcurrency int

	// InsertedAtField names a time.Time field on T that the writer
	// populates with its wall-clock time.Now() just before parquet
	// encoding, so the value becomes a real parquet column in every
	// written file. The field must carry a non-empty, non-"-"
	// parquet tag (e.g. `parquet:"inserted_at"`) — the value is
	// persisted on disk, not library-managed metadata. Empty
	// disables the feature; there is no reflection cost when unset.
	//
	// Paired with ReaderConfig.InsertedAtField so the same struct
	// field round-trips unchanged end-to-end. Callers that want the
	// write-time stamp surfaced at read time must set both sides.
	InsertedAtField string

	// DuplicateWriteDetection selects the strategy for detecting
	// retries of idempotent writes. Only consulted when a write
	// carries WithIdempotencyToken — without a token the field is
	// irrelevant and the writer never probes.
	//
	// Three options (see the DuplicateWriteDetectionBy* factory
	// functions for details):
	//
	//   - DuplicateWriteDetectionByOverwritePrevention(): always
	//     send If-None-Match: *. Use when you know the backend
	//     rejects overwrites (AWS, recent MinIO, or StorageGRID
	//     with the s3:PutOverwriteObject deny policy).
	//   - DuplicateWriteDetectionByHEAD(): pre-flight HEAD on
	//     every idempotent write. Use when the backend has no
	//     overwrite prevention.
	//   - DuplicateWriteDetectionByProbe(deleteScratch): auto-
	//     detect on the first idempotent write. Default.
	//
	// Nil resolves to DuplicateWriteDetectionByProbe(true) —
	// auto-detect, clean up the scratch object after probing.
	DuplicateWriteDetection DuplicateWriteDetection

	// DisableCleanup opts out of the best-effort DeleteObject
	// calls on partial-write failure paths (orphan data after a
	// marker or ref PUT failure). When true, orphan objects
	// remain at their S3 keys for bucket lifecycle policies to
	// garbage-collect. Set when the writer lacks DELETE permission
	// (common in STACKIT / StorageGRID deployments with a narrowly-
	// scoped service account).
	//
	// Independent of DuplicateWriteDetectionByProbe's deleteScratch
	// flag: probe cleanup and write-orphan cleanup are separate
	// concerns. Pair DisableCleanup=true with Probe(false) (or
	// an explicit OverwritePrevention() / HEAD() strategy) when
	// DELETE is withheld globally.
	DisableCleanup bool

	// ConsistencyControl sets the Consistency-Control HTTP header
	// on S3 operations where the library's correctness depends on
	// strong read-after-write / list-after-write visibility: the
	// data PUT of an idempotent write, the scoped LIST used to
	// dedup refs on the retry path, and every registered index's
	// marker PUT (so a paired Lookup sees it read-after-write).
	//
	// Zero value (ConsistencyDefault) sends no header — bucket
	// default applies. On AWS S3 / MinIO that's strongly
	// consistent by default so the field can stay empty. On
	// NetApp StorageGRID the bucket default is typically
	// read-after-new-write, which is insufficient for Phase 3's
	// correctness conditions — set ConsistencyStrongGlobal (multi-
	// site) or ConsistencyStrongSite (single-site) explicitly.
	//
	// NetApp requires PUT and paired GET to use matching
	// consistency levels, so WriterConfig.ConsistencyControl and
	// ReaderConfig.ConsistencyControl should match. NewWriter /
	// NewReader can't cross-validate (they're independent
	// constructors), so this is a caller contract documented
	// here. NewIndexWithRegister copies this value onto the
	// returned Index so the marker-LIST side matches automatically.
	ConsistencyControl ConsistencyLevel
}

// Writer is the write-side half of a Store. Owns the write path
// (Write / WriteWithKey) and the in-writer registration list that
// drives marker emission on Write.
//
// Construct directly via NewWriter when a service only writes;
// embed in Store when it also reads.
type Writer[T any] struct {
	cfg      WriterConfig[T]
	dataPath string
	refPath  string

	// compressionCodec is Config.Compression resolved to the
	// parquet-go codec once at New(), so the hot path doesn't
	// re-switch on the string.
	compressionCodec compress.Codec

	// indexes is the list of registered secondary-index writers
	// that the write path iterates per record to emit marker
	// objects. RegisterIndex appends here; the entry type K is
	// erased at the closure boundary so the slice stays
	// homogeneous over T.
	indexes []indexWriter[T]

	// insertedAtFieldIndex is the reflect struct-field path for
	// WriterConfig.InsertedAtField, resolved once at NewWriter so
	// the write hot path doesn't re-parse the type. nil when unset
	// — populateInsertedAt is then skipped entirely.
	insertedAtFieldIndex []int

	// overwritePreventionActive is the resolved capability decided
	// by NewWriter from the configured DuplicateWriteDetection:
	//
	//   - DuplicateWriteDetectionByOverwritePrevention(): always true
	//     (caller asserted backend support; no probe).
	//   - DuplicateWriteDetectionByHEAD(): always false (caller opted
	//     into pre-flight HEAD; no probe).
	//   - DuplicateWriteDetectionByProbe(deleteScratch): result of the
	//     eager probe NewWriter ran against the bucket. true means
	//     putIfAbsent path; false falls back to headThenPut.
	//
	// Read by the idempotent write path; never mutated after
	// NewWriter returns, so no synchronisation needed.
	overwritePreventionActive bool
}

// indexWriter is the internal, entry-type-erased contract
// between RegisterIndex (called once per index) and the write
// path. Given a record, it returns the S3 object keys of the
// markers that record produces, already validated and ready to
// PUT.
type indexWriter[T any] struct {
	name    string
	pathsOf func(T) ([]string, error)
}

// registerIndex appends an indexWriter closure to the write
// path's iteration list. Called by RegisterIndex. Not
// concurrency-safe: registration must happen before the first
// Write.
func (w *Writer[T]) registerIndex(iw indexWriter[T]) {
	w.indexes = append(w.indexes, iw)
}

// Target returns the untyped S3Target this Writer is bound to.
// Use when constructing read-only tools (NewIndex, BackfillIndex)
// against the same dataset without carrying the Writer's T into
// their call graph.
func (w *Writer[T]) Target() S3Target {
	return w.cfg.Target
}

// PartitionKey applies the configured PartitionKeyOf to rec and
// returns the resulting partition key. Intended for callers that
// want the single-partition WriteWithKey path without having to
// re-implement the key format at the call site:
//
//	_, err := w.WriteWithKey(ctx, w.PartitionKey(recs[0]), recs)
//
// Panics if PartitionKeyOf was not set at construction — the same
// nil-func-call semantics Write gets, just surfaced at a clearer
// site.
func (w *Writer[T]) PartitionKey(rec T) string {
	return w.cfg.PartitionKeyOf(rec)
}

// NewWriter constructs a Writer directly from WriterConfig. Use
// this in services that only write; use New(ctx, Config) when the
// same process also reads through a Reader/Store.
//
// Validation mirrors the writer-side half of New: the Target
// must carry Bucket / Prefix / S3Client / PartitionKeyParts;
// Compression resolves to a codec (zero value → snappy).
// PartitionKeyOf is optional at construction — Write errors if
// called without it, but WriteWithKey works regardless.
//
// ctx bounds the optional overwrite-prevention probe: when
// DuplicateWriteDetection is the default Probe strategy, NewWriter
// runs an eager probe against the bucket so the capability is
// known before the first write. The probe is wrapped in an
// internal 10s timeout (probeTimeout) layered onto ctx so a
// caller-supplied context.Background() doesn't hang construction
// indefinitely on a misconfigured endpoint. The other two
// strategies (OverwritePrevention, HEAD) skip the probe entirely
// — ctx is consulted only by the probe path.
func NewWriter[T any](
	ctx context.Context, cfg WriterConfig[T],
) (*Writer[T], error) {
	if err := cfg.Target.Validate(); err != nil {
		return nil, err
	}
	if cfg.PartitionWriteConcurrency < 0 {
		return nil, fmt.Errorf(
			"s3parquet: PartitionWriteConcurrency must be "+
				">= 0 (got %d); zero means default",
			cfg.PartitionWriteConcurrency)
	}
	codec, err := resolveCompression(cfg.Compression)
	if err != nil {
		return nil, err
	}
	insertedAtIdx, err := validateWriterInsertedAtField[T](cfg.InsertedAtField)
	if err != nil {
		return nil, err
	}
	if cfg.ConsistencyControl != "" && !cfg.ConsistencyControl.IsKnown() {
		log.Printf(
			"s3parquet: WriterConfig.ConsistencyControl %q is not one "+
				"of the known levels (all, strong-global, strong-site, "+
				"read-after-new-write, available) — header will be "+
				"sent verbatim; verify the backend accepts it",
			cfg.ConsistencyControl)
	}
	detection := asConcrete(cfg.DuplicateWriteDetection)
	active, err := resolveOverwritePreventionActive(
		ctx, cfg.Target, detection, cfg.ConsistencyControl)
	if err != nil {
		return nil, err
	}
	return &Writer[T]{
		cfg:                       cfg,
		dataPath:                  core.DataPath(cfg.Target.Prefix),
		refPath:                   core.RefPath(cfg.Target.Prefix),
		compressionCodec:          codec,
		insertedAtFieldIndex:      insertedAtIdx,
		overwritePreventionActive: active,
	}, nil
}

// resolveOverwritePreventionActive runs at NewWriter time to
// resolve the configured DuplicateWriteDetection strategy into a
// concrete capability:
//
//   - ByOverwritePrevention: returns true unconditionally; the
//     caller asserted that the backend rejects re-PUTs.
//   - ByHEAD: returns false unconditionally; the caller wants
//     pre-flight HEAD on every idempotent write.
//   - ByProbe: PUTs a scratch object twice against the bucket and
//     observes whether the second PUT is rejected. Wrapped in a
//     probeTimeout-bounded context so a misconfigured endpoint
//     doesn't hang NewWriter.
//
// The probe call carries the Writer's configured
// ConsistencyControl so StorageGRID deployments that gate the
// overwrite-deny policy on strong consistency see the same
// header on the probe.
func resolveOverwritePreventionActive(
	ctx context.Context,
	target S3Target,
	detection duplicateWriteDetection,
	consistency ConsistencyLevel,
) (bool, error) {
	switch detection.kind {
	case detectKindOverwritePrevention:
		return true, nil
	case detectKindHEAD:
		return false, nil
	case detectKindProbe:
		probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
		defer cancel()
		return target.probeOverwritePrevention(
			probeCtx, detection.deleteScratch,
			withConsistencyControl(consistency))
	}
	return false, fmt.Errorf(
		"s3parquet: unhandled DuplicateWriteDetection kind %d",
		detection.kind)
}

// validateWriterInsertedAtField resolves WriterConfig.InsertedAtField
// to a struct-field index on T. Mirrors the reader's
// validateInsertedAtField but enforces the *column* contract: the
// field must exist, be time.Time, and carry a non-empty, non-"-"
// parquet tag, because the writer persists the value as a real
// parquet column (not library-managed metadata). Returns nil when
// name is empty.
func validateWriterInsertedAtField[T any](name string) ([]int, error) {
	if name == "" {
		return nil, nil
	}
	rt := reflect.TypeFor[T]()
	if rt.Kind() != reflect.Struct {
		return nil, fmt.Errorf(
			"s3parquet: InsertedAtField requires T to be a struct, got %s",
			rt)
	}
	f, ok := rt.FieldByName(name)
	if !ok {
		return nil, fmt.Errorf(
			"s3parquet: InsertedAtField %q: no such field on %s",
			name, rt)
	}
	if f.Type != reflect.TypeFor[time.Time]() {
		return nil, fmt.Errorf(
			"s3parquet: InsertedAtField %q: must be time.Time, got %s",
			name, f.Type)
	}
	tag := f.Tag.Get("parquet")
	name0, _, _ := strings.Cut(tag, ",")
	if name0 == "" || name0 == "-" {
		return nil, fmt.Errorf(
			"s3parquet: InsertedAtField %q: must carry a non-empty, "+
				"non-\"-\" parquet tag so the writer can populate it "+
				"as a real parquet column (got %q)", name, tag)
	}
	return f.Index, nil
}
