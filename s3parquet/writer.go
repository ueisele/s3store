package s3parquet

import (
	"fmt"
	"reflect"
	"strings"
	"time"

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
	return &Writer[T]{
		cfg:                  cfg,
		dataPath:             core.DataPath(cfg.Target.Prefix),
		refPath:              core.RefPath(cfg.Target.Prefix),
		compressionCodec:     codec,
		insertedAtFieldIndex: insertedAtIdx,
	}, nil
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
