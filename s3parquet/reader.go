package s3parquet

import (
	"cmp"
	"fmt"

	"github.com/ueisele/s3store/internal/core"
)

// ReaderConfig is the narrower Config form for constructing a
// Reader directly. Holds the S3-wiring bundle (Target — a
// constructed S3Target) plus read-side-only knobs. Use
// NewReader(cfg) in read-only services that have no
// PartitionKeyOf / Compression config to supply.
//
// Target is built once via NewS3Target and can be passed to both
// WriterConfig.Target and ReaderConfig.Target so the resulting
// Writer and Reader share the same MaxInflightRequests semaphore.
//
// Dedup contract: EntityKeyOf and VersionOf are both set or both
// nil — NewReader rejects partial configurations. When set,
// records dedup to the latest-per-entity by VersionOf; when nil,
// every record flows through.
type ReaderConfig[T any] struct {
	Target        S3Target
	EntityKeyOf   func(T) string
	VersionOf     func(T) int64
	OnMissingData func(dataPath string)

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

// Reader is the read-side half of a Store. Owns Read / ReadIter /
// ReadRangeIter / Poll / PollRecords / OffsetAt. Construct directly
// via NewReader in read-only services, or via
// NewReaderFromWriter / NewReaderFromStore for a (possibly
// narrower-T) Reader over a Writer's / Store's data.
type Reader[T any] struct {
	cfg      ReaderConfig[T]
	dataPath string
	refPath  string

	// sortCmp is the resolved comparison function for the dedup
	// path: sort by (entityKey, versionOf) ascending, so each
	// entity's records land grouped and the latest-version record
	// of each entity is the one that survives dedup. Nil when
	// EntityKeyOf is unset — in that case sortAndIterate skips
	// the sort entirely and yields records in input (decode)
	// order.
	sortCmp func(a, b T) int
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
// Validates EntityKeyOf and VersionOf are both set or both nil.
func NewReader[T any](cfg ReaderConfig[T]) (*Reader[T], error) {
	if err := cfg.Target.Validate(); err != nil {
		return nil, err
	}
	if (cfg.EntityKeyOf == nil) != (cfg.VersionOf == nil) {
		return nil, fmt.Errorf(
			"s3parquet: EntityKeyOf and VersionOf must be set together")
	}
	warnIfUnknownConsistency(cfg.ConsistencyControl, "ReaderConfig")
	return &Reader[T]{
		cfg:      cfg,
		dataPath: core.DataPath(cfg.Target.Prefix()),
		refPath:  core.RefPath(cfg.Target.Prefix()),
		sortCmp:  resolveSortCmp(cfg.EntityKeyOf, cfg.VersionOf),
	}, nil
}

// resolveSortCmp returns the comparator sortAndIterate uses on
// the dedup path: records sort by (entityKey, versionOf)
// ascending so each entity's records land grouped and the
// latest-version record per entity is last in the group. Returns
// nil when EntityKeyOf is unset — sortAndIterate then skips the
// sort and emits records in input (decode) order.
func resolveSortCmp[T any](
	entityKeyOf func(T) string,
	versionOf func(T) int64,
) func(a, b T) int {
	if entityKeyOf == nil {
		return nil
	}
	return func(a, b T) int {
		if c := cmp.Compare(entityKeyOf(a), entityKeyOf(b)); c != 0 {
			return c
		}
		return cmp.Compare(versionOf(a), versionOf(b))
	}
}

// NewReaderFromWriter constructs a Reader[T] over the data a
// Writer[U] produces. T may equal U (same-shape read) or differ
// from U — typically a narrower struct that omits heavy
// write-only columns (parquet-go skips unlisted columns on
// decode). The Writer's Target overrides whatever Target cfg
// carries (or doesn't); read-side knobs (EntityKeyOf, VersionOf,
// OnMissingData, ConsistencyControl) come from cfg.
//
// Dedup closures (EntityKeyOf / VersionOf) on the Writer are
// typed over U and cannot be auto-transformed to T; the caller
// re-declares them in cfg when dedup is needed.
func NewReaderFromWriter[T, U any](
	w *Writer[U], cfg ReaderConfig[T],
) (*Reader[T], error) {
	if w == nil {
		return nil, fmt.Errorf(
			"s3parquet: NewReaderFromWriter: writer is nil")
	}
	cfg.Target = w.cfg.Target
	return NewReader(cfg)
}

// NewReaderFromStore is NewReaderFromWriter with a Store[U]
// instead of a Writer[U] — extracts the embedded Writer and
// forwards. Common shape: one Store writes FullRec, a few
// NewReaderFromStore calls produce narrow Readers for hot-path
// reads without respecifying the shared config.
func NewReaderFromStore[T, U any](
	s *Store[U], cfg ReaderConfig[T],
) (*Reader[T], error) {
	if s == nil {
		return nil, fmt.Errorf(
			"s3parquet: NewReaderFromStore: store is nil")
	}
	return NewReaderFromWriter(s.Writer, cfg)
}
