package s3parquet

import (
	"github.com/parquet-go/parquet-go/compress"
	"github.com/ueisele/s3store/internal/core"
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

	// DisableCleanup opts out of the best-effort DeleteObject
	// calls on partial-write failure paths (orphan data after a
	// marker or ref PUT failure). When true, orphan objects
	// remain at their S3 keys for bucket lifecycle policies to
	// garbage-collect. Set when the writer lacks DELETE permission
	// (common in StorageGRID deployments with a narrowly-
	// scoped service account).
	DisableCleanup bool
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
// this in services that only write; use New(Config) when the same
// process also reads through a Reader/Store.
//
// Validation mirrors the writer-side half of New: the Target
// must carry Bucket / Prefix / S3Client / PartitionKeyParts;
// Compression resolves to a codec (zero value → snappy).
// PartitionKeyOf is optional at construction — Write errors if
// called without it, but WriteWithKey works regardless.
//
// Constructor performs no S3 I/O. Idempotent writes always go
// through If-None-Match: * (handled by S3Target.putIfAbsent),
// which AWS / MinIO honour natively and StorageGRID deployments
// honour via the s3:PutOverwriteObject deny policy. On backends
// without either mechanism the conditional PUT silently succeeds
// on retries — the data path is deterministic + the body is
// byte-identical, so the duplicate write is harmless and any
// extra ref it produces is absorbed by reader dedup.
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
	return &Writer[T]{
		cfg:                  cfg,
		dataPath:             core.DataPath(cfg.Target.Prefix()),
		refPath:              core.RefPath(cfg.Target.Prefix()),
		compressionCodec:     codec,
		insertedAtFieldIndex: insertedAtIdx,
	}, nil
}
