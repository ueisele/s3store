package s3store

import "github.com/parquet-go/parquet-go/compress"

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

	// Projections lists the secondary projections the writer
	// should maintain. Every Write iterates each entry, calls Of
	// per record, and PUTs one empty marker per distinct
	// (projection, column-values) tuple in the batch under
	// <Prefix>/_projection/<Name>/. Validation runs at NewWriter:
	// Name non-empty + free of '/', Columns valid + unique,
	// Of non-nil, Names unique across the slice.
	//
	// Constructed at writer-creation time so registration cannot
	// race with Write and "registered after the first Write" is
	// not a reachable state. Use BackfillProjection to
	// retroactively cover records written before a projection
	// existed.
	Projections []ProjectionDef[T]

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
}

// Writer is the write-side half of a Store. Owns the write path
// (Write / WriteWithKey) and the projection list that drives
// marker emission on Write.
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

	// projections is the resolved per-projection marker emitter
	// list, built once at NewWriter from cfg.Projections.
	// Immutable after construction — no concurrency story needed
	// on the write path.
	projections []projectionWriter[T]

	// insertedAtFieldIndex is the reflect struct-field path for
	// WriterConfig.InsertedAtField, resolved once at NewWriter so
	// the write hot path doesn't re-parse the type. nil when unset
	// — populateInsertedAt is then skipped entirely.
	insertedAtFieldIndex []int
}

// Target returns the untyped S3Target this Writer is bound to.
// Use when constructing read-only tools (NewProjectionReader,
// BackfillProjection) against the same dataset without carrying
// the Writer's T into their call graph.
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
// every ProjectionDef in cfg.Projections is shape-validated and
// Of must be non-nil. Projection names must be unique across the
// slice. PartitionKeyOf is optional at construction — Write
// errors if called without it, but WriteWithKey works regardless.
//
// Constructor performs no S3 I/O. Idempotent retries are gated
// by the writer's upfront LIST under {partition}/{token}- (see
// WithIdempotencyToken): no PUT in the write path ever overwrites,
// every attempt lands at a fresh per-attempt path, and the
// upfront-LIST gate finds any prior valid commit and returns
// its WriteResult unchanged. Cross-backend uniformity — no
// dependency on If-None-Match support or bucket-policy denies.
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
	projections, err := buildProjectionWriters(cfg.Target, cfg.Projections)
	if err != nil {
		return nil, err
	}
	return &Writer[T]{
		cfg:                  cfg,
		dataPath:             dataPath(cfg.Target.Prefix()),
		refPath:              refPath(cfg.Target.Prefix()),
		compressionCodec:     codec,
		insertedAtFieldIndex: insertedAtIdx,
		projections:          projections,
	}, nil
}
