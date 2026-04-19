package s3parquet

import (
	"github.com/parquet-go/parquet-go/compress"
	"github.com/ueisele/s3store/internal/core"
)

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

// NewWriter constructs a Writer directly from WriterConfig. Use
// this in services that only write; use New(Config) when the
// same process also reads through a Reader/Store.
//
// Validation mirrors the writer-side half of New: the Target
// must carry Bucket / Prefix / S3Client / PartitionKeyParts;
// Compression resolves to a codec (zero value → snappy).
// PartitionKeyOf is optional at construction — Write errors if
// called without it, but WriteWithKey works regardless.
func NewWriter[T any](cfg WriterConfig[T]) (*Writer[T], error) {
	if err := cfg.Target.Validate(); err != nil {
		return nil, err
	}
	codec, err := resolveCompression(cfg.Compression)
	if err != nil {
		return nil, err
	}
	return &Writer[T]{
		cfg:              cfg,
		dataPath:         core.DataPath(cfg.Target.Prefix),
		refPath:          core.RefPath(cfg.Target.Prefix),
		compressionCodec: codec,
	}, nil
}
