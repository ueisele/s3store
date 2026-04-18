package s3parquet

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"
	"github.com/ueisele/s3store/internal/core"
)

// Config defines how a Store is set up. T is the record type,
// which must be encodable and decodable by parquet-go directly
// (struct fields tagged with `parquet:"..."`, primitive-friendly
// types). Types with fields parquet-go can't encode (e.g.
// decimal.Decimal, custom wrappers) need a companion
// parquet-layout struct and a translation step in the caller's
// package.
type Config[T any] struct {
	// Bucket is the S3 bucket name.
	Bucket string

	// Prefix under which data files are stored.
	Prefix string

	// PartitionKeyParts defines the Hive-partition key segments in order.
	PartitionKeyParts []string

	// S3Client is the AWS S3 client to use. Its endpoint, region,
	// credentials, and path-style setting are used as-is.
	S3Client *s3.Client

	// PartitionKeyOf extracts the Hive-partition key from a
	// record. Required for Write(). The returned string must
	// conform to the PartitionKeyParts layout ("part=value/part=value").
	PartitionKeyOf func(T) string

	// SettleWindow is how far behind the stream tip Poll and
	// PollRecords read. Default: 5s.
	SettleWindow time.Duration

	// EntityKeyOf returns the logical entity identifier for a
	// record. When non-nil, Read and PollRecords deduplicate to
	// the record with the maximum VersionOf per entity. When
	// nil, every record is returned (pure stream semantics).
	EntityKeyOf func(T) string

	// VersionOf returns the monotonic version of a record for
	// dedup ordering. The library passes the source file's
	// write time in insertedAt — useful as a fallback when the
	// record has no domain-level version, or combine it with a
	// business timestamp for hybrid strategies.
	//
	// Nil defaults to DefaultVersionOf (wrote-last-wins). The
	// default is assigned inside New() when EntityKeyOf is
	// also set, so dedupEnabled only checks EntityKeyOf.
	VersionOf func(record T, insertedAt time.Time) int64

	// OnMissingData is invoked when a data-file GET returns S3
	// NoSuchKey (404) during Read, PollRecords, or Index.Backfill.
	// The path is skipped (not treated as an error) and the hook
	// is called with its S3 key so the caller can log, count, or
	// alert. Nil disables the hook and retains skip-on-404 behavior.
	//
	// Intended to mask two rare but known outcomes of the write
	// path: (1) the ref PUT "failed" with a lost ack but the ref
	// is persisted while cleanup deleted the data, leaving a
	// dangling ref; (2) LIST-to-GET race where an object was
	// deleted between listing and reading. In both cases failing
	// the whole read would turn a one-record anomaly into ongoing
	// breakage; skip-and-notify is the at-least-once posture.
	//
	// Called from the S3 download worker goroutine — must be safe
	// for concurrent invocation.
	OnMissingData func(dataPath string)

	// BloomFilterColumns lists parquet column names (top-level)
	// that Write should emit per-row-group split-block bloom
	// filters for. Use this for columns that queries filter on
	// with equality (WHERE sku_id = X) and that partition pruning
	// can't cover.
	//
	// IMPORTANT: only DuckDB (s3sql) consults these filters at
	// read time. The pure-Go s3parquet.Read has no per-column
	// predicate API and decodes every matching file regardless,
	// so configuring BloomFilterColumns for a pure-s3parquet
	// workload adds write cost with no read-side benefit.
	//
	// Column names must match the `parquet:"..."` tag on a
	// top-level struct field of T; New() rejects unknown names
	// so typos don't silently disable the filter.
	BloomFilterColumns []string
}

// bloomFilterBitsPerValue is the bits-per-value for split-block
// bloom filters emitted for every column in BloomFilterColumns.
// 10 is parquet-go's recommended default: ~1% false-positive rate
// at 10 bits/value, scales linearly with N. Not exposed as a knob
// yet; revisit if users need per-column tuning.
const bloomFilterBitsPerValue = 10

// DefaultVersionOf returns insertedAt in microseconds. Assigned
// to Config.VersionOf inside New() when that field is nil and
// EntityKeyOf is set; also exported so users can reference the
// wrote-last-wins default explicitly in their config.
func DefaultVersionOf[T any](_ T, insertedAt time.Time) int64 {
	return insertedAt.UnixMicro()
}

func (c Config[T]) settleWindow() time.Duration {
	if c.SettleWindow > 0 {
		return c.SettleWindow
	}
	return 5 * time.Second
}

// dedupEnabled reports whether latest-per-entity dedup applies.
// Gated solely on EntityKeyOf: New() populates VersionOf with
// DefaultVersionOf when the user leaves it nil, so by the time
// a Store exists the VersionOf field is always callable if
// EntityKeyOf is set.
func (c Config[T]) dedupEnabled() bool {
	return c.EntityKeyOf != nil
}

// Store is the pure-Go entry point to an s3store.
type Store[T any] struct {
	cfg      Config[T]
	s3       *s3.Client
	dataPath string
	refPath  string

	// indexes is the list of registered secondary indexes that the
	// write path iterates per record to emit marker objects. Typed
	// Index[T, K] handles append to this slice via registerIndex
	// at NewIndex time; the entry type K is erased at the closure
	// boundary so the slice can be homogeneous over T.
	indexes []indexWriter[T]
}

// indexWriter is the internal, entry-type-erased contract between
// a typed Index[T, K] and the Store's write path. Given a record,
// it returns the S3 object keys of the markers that record
// produces, already validated and ready to PUT.
type indexWriter[T any] struct {
	name    string
	pathsOf func(T) ([]string, error)
}

// registerIndex appends a typed index's writer to the store's
// iteration list. Called from NewIndex. Not concurrency-safe:
// indexes should be registered before the first Write.
func (s *Store[T]) registerIndex(w indexWriter[T]) {
	s.indexes = append(s.indexes, w)
}

// New constructs a Store. Validates required config fields.
func New[T any](cfg Config[T]) (*Store[T], error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("s3parquet: Bucket is required")
	}
	if cfg.Prefix == "" {
		return nil, fmt.Errorf("s3parquet: Prefix is required")
	}
	if cfg.S3Client == nil {
		return nil, fmt.Errorf("s3parquet: S3Client is required")
	}
	if err := core.ValidatePartitionKeyParts(cfg.PartitionKeyParts); err != nil {
		return nil, err
	}
	// Default VersionOf when the user asked for dedup
	// (EntityKeyOf set) but didn't tell us how to compare
	// versions. Wrote-last-wins is the natural zero-config
	// behaviour for append-only storage.
	if cfg.EntityKeyOf != nil && cfg.VersionOf == nil {
		cfg.VersionOf = DefaultVersionOf[T]
	}
	if err := validateBloomFilterColumns[T](cfg.BloomFilterColumns); err != nil {
		return nil, err
	}
	return &Store[T]{
		cfg:      cfg,
		s3:       cfg.S3Client,
		dataPath: core.DataPath(cfg.Prefix),
		refPath:  core.RefPath(cfg.Prefix),
	}, nil
}

// Close releases resources. Pure-Go Store holds no persistent
// connections — Close is a no-op but present for API symmetry
// with s3sql.Store and for future-proofing.
func (s *Store[T]) Close() error { return nil }

// validateBloomFilterColumns rejects BloomFilterColumns entries
// that aren't top-level parquet columns of T, so a typo fails at
// New() instead of silently producing files without the filter.
func validateBloomFilterColumns[T any](cols []string) error {
	if len(cols) == 0 {
		return nil
	}
	var zero T
	schema := parquet.SchemaOf(zero)
	for _, name := range cols {
		if name == "" {
			return fmt.Errorf(
				"s3parquet: BloomFilterColumns contains an empty name")
		}
		if _, ok := schema.Lookup(name); !ok {
			return fmt.Errorf(
				"s3parquet: BloomFilterColumns[%q] is not a "+
					"top-level parquet column of %T", name, zero)
		}
	}
	return nil
}
