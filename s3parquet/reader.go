package s3parquet

import (
	"fmt"
	"time"

	"github.com/ueisele/s3store/internal/core"
)

// ReaderConfig is the narrower Config form for constructing a
// Reader directly. Holds the S3-wiring bundle (Target) plus
// read-side-only knobs. Use NewReader(cfg) in read-only services
// that have no PartitionKeyOf / Compression / BloomFilter config
// to supply.
//
// Target carries Bucket / Prefix / S3Client / PartitionKeyParts /
// SettleWindow — shared with WriterConfig through the same type,
// so a Writer and a Reader built from the same Target cannot
// drift on those fields.
type ReaderConfig[T any] struct {
	Target          S3Target
	EntityKeyOf     func(T) string
	VersionOf       func(T, time.Time) int64
	InsertedAtField string
	OnMissingData   func(dataPath string)
}

// dedupEnabled reports whether latest-per-entity dedup applies.
// Gated solely on EntityKeyOf: NewReader populates VersionOf
// with DefaultVersionOf when the user leaves it nil, so by the
// time a Reader exists the VersionOf field is always callable if
// EntityKeyOf is set.
func (c ReaderConfig[T]) dedupEnabled() bool {
	return c.EntityKeyOf != nil
}

// ReaderExtras carries the read-side-only knobs — everything on
// ReaderConfig except the Target. Used by NewReaderFromWriter
// and NewReaderFromStore: the Target comes from the Writer /
// Store; the user supplies just the read-specific fields here.
//
// Drift guard: every field below must also appear on ReaderConfig
// with an identical name and type (enforced by a reflect-based
// unit test).
type ReaderExtras[T any] struct {
	EntityKeyOf     func(T) string
	VersionOf       func(T, time.Time) int64
	InsertedAtField string
	OnMissingData   func(dataPath string)
}

// Reader is the read-side half of a Store. Owns Read / Poll /
// PollRecords / PollRecordsAll / OffsetAt. Construct directly
// via NewReader in read-only services, or via
// NewReaderFromWriter / NewReaderFromStore for a (possibly
// narrower-T) Reader over a Writer's / Store's data.
type Reader[T any] struct {
	cfg      ReaderConfig[T]
	dataPath string
	refPath  string

	// insertedAtFieldIndex is the reflect struct-field path for
	// Config.InsertedAtField, resolved once at New() so the hot
	// path doesn't reparse the type. nil when unset.
	insertedAtFieldIndex []int
}

// Target returns the untyped S3Target this Reader is bound to.
// Use when constructing an Index[K] or running BackfillIndex
// against the same dataset without carrying T through the
// caller's signatures.
func (r *Reader[T]) Target() S3Target {
	return r.cfg.Target
}

// NewReader constructs a Reader directly from ReaderConfig.
// Intended for read-only services that have no write-side config
// to supply (no PartitionKeyOf / Compression / BloomFilters).
//
// Validates the same read-side invariants New(Config) does:
// required Target fields, InsertedAtField (if set) must resolve
// on T, and a default VersionOf is assigned when EntityKeyOf is
// set but VersionOf is nil (wrote-last-wins).
func NewReader[T any](cfg ReaderConfig[T]) (*Reader[T], error) {
	if err := cfg.Target.validate(); err != nil {
		return nil, err
	}
	if cfg.EntityKeyOf != nil && cfg.VersionOf == nil {
		cfg.VersionOf = DefaultVersionOf[T]
	}
	insertedAtIdx, err := validateInsertedAtField[T](cfg.InsertedAtField)
	if err != nil {
		return nil, err
	}
	return &Reader[T]{
		cfg:                  cfg,
		dataPath:             core.DataPath(cfg.Target.Prefix),
		refPath:              core.RefPath(cfg.Target.Prefix),
		insertedAtFieldIndex: insertedAtIdx,
	}, nil
}

// NewReaderFromWriter constructs a Reader[T] over the data a
// Writer[U] produces. T may equal U (same-shape read) or differ
// from U — typically a narrower struct that omits heavy
// write-only columns (parquet-go skips unlisted columns on
// decode). The Writer's Target (Bucket, Prefix, S3Client,
// PartitionKeyParts, SettleWindow) carries over; the user
// supplies read-side knobs via ReaderExtras[T].
//
// Dedup closures (EntityKeyOf / VersionOf) on the Writer are
// typed over U and cannot be auto-transformed to T; the caller
// re-declares them in extras when dedup is needed.
func NewReaderFromWriter[T, U any](
	w *Writer[U], extras ReaderExtras[T],
) (*Reader[T], error) {
	if w == nil {
		return nil, fmt.Errorf(
			"s3parquet: NewReaderFromWriter: writer is nil")
	}
	return NewReader(ReaderConfig[T]{
		Target:          w.cfg.Target,
		EntityKeyOf:     extras.EntityKeyOf,
		VersionOf:       extras.VersionOf,
		InsertedAtField: extras.InsertedAtField,
		OnMissingData:   extras.OnMissingData,
	})
}

// NewReaderFromStore is NewReaderFromWriter with a Store[U]
// instead of a Writer[U] — extracts the embedded Writer and
// forwards. Common shape: one Store writes FullRec, a few
// NewReaderFromStore calls produce narrow Readers for hot-path
// reads without respecifying the shared config.
func NewReaderFromStore[T, U any](
	s *Store[U], extras ReaderExtras[T],
) (*Reader[T], error) {
	if s == nil {
		return nil, fmt.Errorf(
			"s3parquet: NewReaderFromStore: store is nil")
	}
	return NewReaderFromWriter(s.Writer, extras)
}
