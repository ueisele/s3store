package s3parquet

import (
	"cmp"
	"fmt"
	"log"
	"time"

	"github.com/ueisele/s3store/internal/core"
)

// ReaderConfig is the narrower Config form for constructing a
// Reader directly. Holds the S3-wiring bundle (Target) plus
// read-side-only knobs. Use NewReader(cfg) in read-only services
// that have no PartitionKeyOf / Compression config to supply.
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

	// ConsistencyControl sets the Consistency-Control HTTP header
	// on data-file GETs following a LIST, matching the WriterConfig
	// ConsistencyControl value per NetApp's "same consistency for
	// paired PUT and GET" rule. Zero (ConsistencyDefault) sends no
	// header and is correct on AWS S3 / MinIO; on StorageGRID
	// match the writer's setting explicitly.
	//
	// Only the data-file GET applies the header — LIST of the ref
	// stream and of partitions stay at the bucket default (their
	// consistency needs are absorbed by SettleWindow, not the
	// header).
	ConsistencyControl ConsistencyLevel
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
	EntityKeyOf        func(T) string
	VersionOf          func(T, time.Time) int64
	InsertedAtField    string
	OnMissingData      func(dataPath string)
	ConsistencyControl ConsistencyLevel
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

	// sortCmp is the resolved comparison function for emission
	// order. Two-tier cascade:
	//   - EntityKeyOf set: (entityKey, versionOf(rec, insertedAt)) asc
	//   - else:            (insertedAt, fileName) asc
	sortCmp func(a, b versionedRecord[T]) int
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
// to supply (no PartitionKeyOf / Compression).
//
// Validates the same read-side invariants New(Config) does:
// required Target fields, InsertedAtField (if set) must resolve
// on T, and a default VersionOf is assigned when EntityKeyOf is
// set but VersionOf is nil (wrote-last-wins).
func NewReader[T any](cfg ReaderConfig[T]) (*Reader[T], error) {
	if err := cfg.Target.Validate(); err != nil {
		return nil, err
	}
	if cfg.EntityKeyOf != nil && cfg.VersionOf == nil {
		cfg.VersionOf = DefaultVersionOf[T]
	}
	insertedAtIdx, err := validateInsertedAtField[T](cfg.InsertedAtField)
	if err != nil {
		return nil, err
	}
	if cfg.ConsistencyControl != "" && !cfg.ConsistencyControl.IsKnown() {
		log.Printf(
			"s3parquet: ReaderConfig.ConsistencyControl %q is not one "+
				"of the known levels — header will be sent verbatim; "+
				"verify the backend accepts it",
			cfg.ConsistencyControl)
	}
	return &Reader[T]{
		cfg:                  cfg,
		dataPath:             core.DataPath(cfg.Target.Prefix),
		refPath:              core.RefPath(cfg.Target.Prefix),
		insertedAtFieldIndex: insertedAtIdx,
		sortCmp:              resolveSortCmp(cfg.EntityKeyOf, cfg.VersionOf),
	}, nil
}

// resolveSortCmp returns the versionedRecord comparator that
// decides the reader's emission order. The cascade is:
//
//   - EntityKeyOf set: sort by (entityKey, versionOf) ascending
//     so each entity's records land grouped and in version-
//     ascending order (newest last). When VersionOf is nil the
//     NewReader default (DefaultVersionOf = insertedAt.UnixMicro)
//     applies, so this reduces to (entity, LastModified).
//   - EntityKeyOf nil: sort by (insertedAt, fileName) ascending
//     for per-file chronological output. fileName is the base
//     name of the source parquet key — a deterministic tiebreaker
//     when two files share a LastModified.
//
// cmp.Compare gives a stable three-way result on int64 nanoseconds
// so ties fall through to the secondary key cleanly.
func resolveSortCmp[T any](
	entityKeyOf func(T) string,
	versionOf func(T, time.Time) int64,
) func(a, b versionedRecord[T]) int {
	if entityKeyOf != nil {
		return func(a, b versionedRecord[T]) int {
			if c := cmp.Compare(
				entityKeyOf(a.rec), entityKeyOf(b.rec)); c != 0 {
				return c
			}
			return cmp.Compare(
				versionOf(a.rec, a.insertedAt),
				versionOf(b.rec, b.insertedAt))
		}
	}
	return func(a, b versionedRecord[T]) int {
		switch {
		case a.insertedAt.Before(b.insertedAt):
			return -1
		case a.insertedAt.After(b.insertedAt):
			return 1
		}
		return cmp.Compare(a.fileName, b.fileName)
	}
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
		Target:             w.cfg.Target,
		EntityKeyOf:        extras.EntityKeyOf,
		VersionOf:          extras.VersionOf,
		InsertedAtField:    extras.InsertedAtField,
		OnMissingData:      extras.OnMissingData,
		ConsistencyControl: extras.ConsistencyControl,
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
