package s3parquet

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
)

// CompressionCodec selects the parquet-level compression applied
// to every column on Write. String-valued for easy config/YAML
// wiring; mapped to the parquet-go codec at Store construction
// time. Zero value ("") resolves to snappy — the de-facto
// ecosystem default (Spark, DuckDB's parquet writer, Trino,
// Athena all emit snappy unless told otherwise).
type CompressionCodec string

const (
	// CompressionSnappy: fast encode/decode, ~2-3× ratio,
	// negligible CPU overhead. Default.
	CompressionSnappy CompressionCodec = "snappy"
	// CompressionZstd: better ratios than snappy at higher CPU
	// cost. Good for cold / archive data.
	CompressionZstd CompressionCodec = "zstd"
	// CompressionGzip: widely compatible, moderate CPU, decent
	// ratio. Mostly a legacy choice today.
	CompressionGzip CompressionCodec = "gzip"
	// CompressionUncompressed: no compression. Largest files;
	// only meaningful when the data is already high-entropy or
	// the CPU tradeoff matters more than S3 cost.
	CompressionUncompressed CompressionCodec = "uncompressed"
)

// Config defines how a Store is set up. T is the record type,
// which must be encodable and decodable by parquet-go directly
// (struct fields tagged with `parquet:"..."`, primitive-friendly
// types). Types with fields parquet-go can't encode (e.g.
// decimal.Decimal, custom wrappers) need a companion
// parquet-layout struct and a translation step in the caller's
// package.
//
// The flat layout is the umbrella ergonomics — fill it in once,
// every field lives at the top level. Internally New() projects
// it onto the narrower WriterConfig[T] and ReaderConfig[T] (both
// of which take an S3Target directly). Advanced users who want
// just one side can skip this type and call NewWriter /
// NewReader with a hand-built WriterConfig / ReaderConfig.
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
	// dedup ordering. Required when EntityKeyOf is set; ignored
	// otherwise. Typical implementations return a domain
	// timestamp / sequence number from a record field
	// (`func(r T) int64 { return r.UpdatedAt.UnixMicro() }`).
	//
	// To use the library's writer-stamped insertedAt as the
	// version, configure InsertedAtField on the writer side and
	// reference the same field here:
	//
	//	VersionOf: func(r T) int64 { return r.InsertedAt.UnixMicro() }
	VersionOf func(record T) int64

	// OnMissingData is invoked when a data-file GET returns S3
	// NoSuchKey (404) during Read, PollRecords, or BackfillIndex.
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

	// InsertedAtField names a time.Time field on T that the writer
	// populates with its wall-clock time.Now() captured just before
	// parquet encoding. The field must carry a non-empty, non-"-"
	// parquet tag (e.g. `parquet:"inserted_at"`) — the value is a
	// real parquet column persisted on disk. Empty disables the
	// feature; no reflection cost when unset.
	//
	// On the read side the column shows up on T as a normal field
	// — no special handling. Use it from VersionOf when you want
	// the writer's stamp to drive dedup ordering:
	//
	//	VersionOf: func(r T) int64 { return r.InsertedAt.UnixMicro() }
	//
	// Forwarded to WriterConfig only.
	InsertedAtField string

	// Compression selects the parquet compression codec used on
	// Write. Zero value is snappy — matches the ecosystem default
	// and produces ~2-3× smaller files than the parquet-go raw
	// default (uncompressed) for no meaningful CPU cost on
	// decode. Set to CompressionUncompressed to opt out,
	// CompressionZstd / CompressionGzip to trade CPU for ratio.
	// New() validates this value and stores the resolved codec so
	// the hot-path Write doesn't reparse it.
	Compression CompressionCodec

	// DisableRefStream opts this dataset out of writing stream ref
	// files. Saves one S3 PUT per distinct partition key touched
	// by a Write. Poll / PollRecords / ReadRangeIter return
	// ErrRefStreamDisabled when set. See S3Target.DisableRefStream
	// for the full contract.
	DisableRefStream bool

	// DisableCleanup disables best-effort orphan cleanup on the
	// write path's failure branches. Forwarded to WriterConfig;
	// see WriterConfig.DisableCleanup.
	DisableCleanup bool

	// ConsistencyControl is the Consistency-Control HTTP header
	// value applied to correctness-critical S3 operations.
	// Forwarded to both WriterConfig and ReaderConfig so the two
	// halves of the Store share the same value. See
	// WriterConfig.ConsistencyControl for the contract.
	ConsistencyControl ConsistencyLevel

	// MaxInflightRequests caps S3 requests in flight per call.
	// Zero → default (32). Forwarded onto the shared S3Target
	// so both Writer and Reader see the same cap. See
	// S3Target.MaxInflightRequests for the full contract.
	MaxInflightRequests int
}

// dedupEnabled reports whether latest-per-entity dedup applies.
// EntityKeyOf and VersionOf must be set together (both required
// for dedup) — New / NewReader reject partial configurations.
func (c Config[T]) dedupEnabled() bool {
	return c.EntityKeyOf != nil
}

// Store is the pure-Go entry point to an s3store. It composes
// an internal Writer + Reader: the two halves own their own
// state and methods, Store re-exposes everything via embedding
// so existing "one Store does both" callers keep working.
type Store[T any] struct {
	*Writer[T]
	*Reader[T]
}

// Target returns the untyped S3Target the Store was built with.
// The Writer and Reader share one S3Target value (set once in
// New) so the choice of which embedded half to delegate to is
// cosmetic.
func (s *Store[T]) Target() S3Target {
	return s.Reader.Target()
}

// New constructs a Store by projecting Config onto WriterConfig
// and ReaderConfig, then delegating to NewWriter + NewReader.
// The unified Config[T] is kept as a back-compat entry point;
// new code that only writes or only reads should prefer
// NewWriter / NewReader directly.
//
// Performs no S3 I/O at construction time.
func New[T any](cfg Config[T]) (*Store[T], error) {
	w, err := NewWriter(writerConfigFrom(cfg))
	if err != nil {
		return nil, err
	}
	r, err := NewReader(readerConfigFrom(cfg))
	if err != nil {
		return nil, err
	}
	return &Store[T]{Writer: w, Reader: r}, nil
}

// targetFrom lifts the S3-wiring fields off a unified Config[T]
// into a constructed S3Target. Called once inside New() so the
// Writer and Reader projections share one S3Target instance —
// and therefore one MaxInflightRequests semaphore — automatically.
func targetFrom[T any](c Config[T]) S3Target {
	return NewS3Target(S3TargetConfig{
		Bucket:              c.Bucket,
		Prefix:              c.Prefix,
		S3Client:            c.S3Client,
		PartitionKeyParts:   c.PartitionKeyParts,
		SettleWindow:        c.SettleWindow,
		DisableRefStream:    c.DisableRefStream,
		MaxInflightRequests: c.MaxInflightRequests,
	})
}

// writerConfigFrom projects a unified Config[T] onto the narrower
// WriterConfig[T]. Central place so drift between the two types
// is easy to spot.
func writerConfigFrom[T any](c Config[T]) WriterConfig[T] {
	return WriterConfig[T]{
		Target:             targetFrom(c),
		PartitionKeyOf:     c.PartitionKeyOf,
		Compression:        c.Compression,
		InsertedAtField:    c.InsertedAtField,
		DisableCleanup:     c.DisableCleanup,
		ConsistencyControl: c.ConsistencyControl,
	}
}

// readerConfigFrom projects a unified Config[T] onto the narrower
// ReaderConfig[T]. InsertedAtField is writer-only — the reader
// sees that column like any other parquet field, decoded
// natively into T by parquet-go.
func readerConfigFrom[T any](c Config[T]) ReaderConfig[T] {
	return ReaderConfig[T]{
		Target:             targetFrom(c),
		EntityKeyOf:        c.EntityKeyOf,
		VersionOf:          c.VersionOf,
		OnMissingData:      c.OnMissingData,
		ConsistencyControl: c.ConsistencyControl,
	}
}

// resolveCompression maps the user-facing CompressionCodec enum
// to the parquet-go codec instance used by the Write path.
// Empty string defaults to snappy — the ecosystem norm — so the
// Config zero value produces small files instead of
// parquet-go's raw default (uncompressed).
func resolveCompression(c CompressionCodec) (compress.Codec, error) {
	switch c {
	case "", CompressionSnappy:
		return &parquet.Snappy, nil
	case CompressionZstd:
		return &parquet.Zstd, nil
	case CompressionGzip:
		return &parquet.Gzip, nil
	case CompressionUncompressed:
		return &parquet.Uncompressed, nil
	}
	return nil, fmt.Errorf(
		"s3parquet: unknown Compression %q (want snappy, "+
			"zstd, gzip, or uncompressed)", c)
}

// validateInsertedAtField resolves an InsertedAtField name (from
// either WriterConfig or ReaderConfig) to a struct-field index on
// T. Rejects typos (no such field), wrong type (not time.Time),
// and a missing or "-" parquet tag — the field carries a real
// parquet column the writer populates and the reader decodes
// through the normal parquet schema, so the tag has to be present
// for both sides to round-trip the value. Returns nil when name
// is empty.
func validateInsertedAtField[T any](name string) ([]int, error) {
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
				"non-\"-\" parquet tag so the value persists as a real "+
				"parquet column (got %q)", name, tag)
	}
	return f.Index, nil
}
