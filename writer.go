package s3store

import (
	"sync"

	"github.com/parquet-go/parquet-go/compress"
)

// WriterConfig is the narrower Config form for constructing a
// Writer directly (without a Reader). Holds the S3-wiring bundle
// (Target — a constructed S3Target) plus write-side-only knobs.
// Use NewWriter(cfg) when a service writes but never reads.
//
// Target is built once via NewS3Target and can be passed to both
// WriterConfig.Target and ReaderConfig.Target so the resulting
// Writer and Reader share the same MaxInflightRequests semaphore
// — net in-flight S3 requests across both halves stay bounded by
// one cap.
type WriterConfig[T any] struct {
	Target         S3Target
	PartitionKeyOf func(T) string
	Compression    CompressionCodec

	// MaterializedViews lists the secondary materialized views the
	// writer should maintain. Every Write iterates each entry,
	// calls Of per record, and PUTs one empty marker per distinct
	// (view, column-values) tuple in the batch under
	// <Prefix>/_matview/<Name>/. Validation runs at NewWriter:
	// Name non-empty + free of '/', Columns valid + unique,
	// Of non-nil, Names unique across the slice.
	//
	// Constructed at writer-creation time so registration cannot
	// race with Write and "registered after the first Write" is
	// not a reachable state. Use BackfillMaterializedView to
	// retroactively cover records written before a view existed.
	MaterializedViews []MaterializedViewDef[T]

	// InsertedAtField names a time.Time field on T that the writer
	// populates with its wall-clock time.Now() just before parquet
	// encoding, so the value becomes a real parquet column in every
	// written file. The field must carry a non-empty, non-"-"
	// parquet tag (e.g. `parquet:"inserted_at"`) — the value is
	// persisted on disk, not library-managed metadata. Empty
	// disables the feature; there is no reflection cost when unset.
	//
	// On the read side the column shows up on T like any other
	// parquet field — no special reader configuration needed.
	// Reference it from VersionOf to use the write-time stamp as
	// the dedup version:
	//
	//	VersionOf: func(r T) int64 { return r.InsertedAt.UnixMicro() }
	InsertedAtField string

	// EncodeBufPoolMaxBytes caps the *bytes.Buffer capacity that the
	// internal encode pool retains across writes. Buffers that grew
	// beyond this on a single Write are dropped on Put — preventing
	// one outlier batch from ballooning the pool's steady-state
	// footprint, at the cost of losing reuse on the next Write of
	// similar size.
	//
	// Set this slightly above the largest typical produced parquet
	// size for the workload. If too low, large writes pay
	// grow-from-zero plus copy-out on every Write (the encode
	// becomes net negative vs no pool — see BenchmarkEncode_Pooled
	// at sizes above the cap). If too high, idle pool retention
	// grows in proportion to GOMAXPROCS, multiplied by the number
	// of Writer instances in the process.
	//
	// The s3store.write.encode_buf_dropped counter increments every
	// time a buffer is dropped due to this cap; a non-zero rate
	// indicates the cap is undersized for the workload.
	//
	// Zero or negative selects the default (48 MiB).
	EncodeBufPoolMaxBytes int64
}

// Writer is the write-side half of a Store. Owns the write path
// (Write / WriteWithKey) and the materialized-view list that
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

	// matviews is the resolved per-view marker emitter list, built
	// once at NewWriter from cfg.MaterializedViews. Immutable after
	// construction — no concurrency story needed on the write path.
	matviews []matviewWriter[T]

	// insertedAtFieldIndex is the reflect struct-field path for
	// WriterConfig.InsertedAtField, resolved once at NewWriter so
	// the write hot path doesn't re-parse the type. nil when unset
	// — populateInsertedAt is then skipped entirely.
	insertedAtFieldIndex []int

	// pqWriterPool holds *parquet.GenericWriter[T] across encode
	// calls. Reset(buf) before reuse rebinds the writer to a fresh
	// output buffer; the codec is fixed at first construction
	// (compressionCodec is constant per Writer). Output bytes are
	// copied out before Put, so nothing pinned by the caller points
	// into pooled state.
	pqWriterPool sync.Pool

	// encodeBufPool holds *bytes.Buffer used as the parquet writer's
	// output target. Reset() before reuse; oversized buffers (Cap
	// above encodeBufPoolMaxBytes) are dropped on Put so a single
	// huge Write doesn't balloon the pool permanently.
	encodeBufPool sync.Pool

	// encodeBufPoolMaxBytes is the resolved
	// WriterConfig.EncodeBufPoolMaxBytes — always > 0 after
	// NewWriter. Zero/negative input is replaced with
	// defaultEncodeBufPoolMaxBytes during construction so the
	// hot path can compare buf.Cap() against this directly.
	encodeBufPoolMaxBytes int64
}

// defaultEncodeBufPoolMaxBytes is the fallback value applied when
// WriterConfig.EncodeBufPoolMaxBytes is zero or negative. 48 MiB
// covers parquet outputs from a few KB up to ~20 MiB with comfort,
// which matches the common workload distribution. Workloads with
// regular ≥ 50 MiB writes should override the field — leaving it
// at the default makes those writes silently lose pool benefit.
const defaultEncodeBufPoolMaxBytes int64 = 48 << 20

// Target returns the untyped S3Target this Writer is bound to.
// Use when constructing read-only tools
// (NewMaterializedViewReader, BackfillMaterializedView) against
// the same dataset without carrying the Writer's T into their
// call graph.
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
// this in services that only write; use New(Config) when the same
// process also reads through a Reader/Store.
//
// Validation mirrors the writer-side half of New: the Target
// must carry Bucket / Prefix / S3Client / PartitionKeyParts;
// Compression resolves to a codec (zero value → snappy);
// every MaterializedViewDef in cfg.MaterializedViews is
// shape-validated and Of must be non-nil. View names must be
// unique across the slice. PartitionKeyOf is optional at
// construction — Write errors if called without it, but
// WriteWithKey works regardless.
//
// Constructor performs no S3 I/O. Idempotent retries are gated
// by the writer's upfront HEAD on
// `<dataPath>/<partition>/<token>.commit` (see
// WithIdempotencyToken): data and ref PUTs land at fresh
// per-attempt paths and never overwrite, and a prior attempt's
// commit marker reconstructs the original WriteResult unchanged.
// Cross-backend uniformity — no dependency on If-None-Match
// support or bucket-policy denies.
func NewWriter[T any](cfg WriterConfig[T]) (*Writer[T], error) {
	if err := cfg.Target.Validate(); err != nil {
		return nil, err
	}
	codec, err := resolveCompression(cfg.Compression)
	if err != nil {
		return nil, err
	}
	insertedAtIdx, err := validateInsertedAtField[T](cfg.InsertedAtField)
	if err != nil {
		return nil, err
	}
	matviews, err := buildMatviewWriters(cfg.Target, cfg.MaterializedViews)
	if err != nil {
		return nil, err
	}
	bufCap := cfg.EncodeBufPoolMaxBytes
	if bufCap <= 0 {
		bufCap = defaultEncodeBufPoolMaxBytes
	}
	return &Writer[T]{
		cfg:                   cfg,
		dataPath:              dataPath(cfg.Target.Prefix()),
		refPath:               refPath(cfg.Target.Prefix()),
		compressionCodec:      codec,
		insertedAtFieldIndex:  insertedAtIdx,
		matviews:              matviews,
		encodeBufPoolMaxBytes: bufCap,
	}, nil
}
