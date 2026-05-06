package s3store

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
)

// CompressionCodec selects the parquet-level compression applied
// to every column on Write. String-valued for easy config/YAML
// wiring; mapped to the parquet-go codec at Store construction
// time. Zero value ("") resolves to snappy — the de-facto
// ecosystem default (Spark, Trino, Athena all emit snappy unless
// told otherwise).
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

// StoreConfig defines how a Store is set up. T is the record type,
// which must be encodable and decodable by parquet-go directly
// (struct fields tagged with `parquet:"..."`, primitive-friendly
// types). Types with fields parquet-go can't encode (e.g.
// decimal.Decimal, custom wrappers) need a companion
// parquet-layout struct and a translation step in the caller's
// package.
//
// Embeds S3TargetConfig so the S3 wiring (Bucket, Prefix, S3Client,
// PartitionKeyParts, MaxInflightRequests, ConsistencyControl,
// MeterProvider) lives in exactly one place. New() reads the
// embedded value, builds an S3Target via NewS3Target, then projects
// the side-specific knobs onto WriterConfig[T] / ReaderConfig[T]
// (both of which take a built S3Target directly). Advanced users
// who only need one side can skip this type and call NewWriter /
// NewReader with a hand-built S3Target — see those constructors.
type StoreConfig[T any] struct {
	S3TargetConfig

	// PartitionKeyOf extracts the Hive-partition key from a
	// record. Required for Write(). The returned string must
	// conform to the PartitionKeyParts layout ("part=value/part=value").
	PartitionKeyOf func(T) string

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

	// MaterializedViews lists the secondary materialized views the
	// writer should maintain. See WriterConfig.MaterializedViews
	// for the full contract. Forwarded to WriterConfig only —
	// readers don't emit markers.
	MaterializedViews []MaterializedViewDef[T]
}

// dedupEnabled reports whether latest-per-entity dedup applies.
// EntityKeyOf and VersionOf must be set together (both required
// for dedup) — New / NewReader reject partial configurations.
func (c StoreConfig[T]) dedupEnabled() bool {
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

// New constructs a Store from StoreConfig: builds an S3Target
// from the embedded S3TargetConfig, then projects the side-specific
// knobs onto WriterConfig[T] / ReaderConfig[T] and delegates to
// NewWriter + NewReader. The Writer and Reader share the one
// constructed S3Target — same MaxInflightRequests semaphore, same
// ConsistencyControl, same resolved CommitTimeout + MaxClockSkew.
//
// Performs two S3 GETs at construction time — the persisted
// timing-config objects at <Prefix>/_config/commit-timeout and
// <Prefix>/_config/max-clock-skew — via NewS3Target. Construction
// fails when either object is missing, unparseable, or below its
// floor (CommitTimeoutFloor / MaxClockSkewFloor).
//
// Services that only write or only read can skip this and call
// NewS3Target + NewWriter / NewReader directly with hand-built
// WriterConfig / ReaderConfig values.
func New[T any](ctx context.Context, cfg StoreConfig[T]) (*Store[T], error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	target, err := NewS3Target(ctx, cfg.S3TargetConfig)
	if err != nil {
		return nil, err
	}
	return newStoreFromTarget(cfg, target)
}

// newStoreFromTarget wires Writer + Reader from cfg + a pre-built
// target. Shared between New (production path, GETs the persisted
// config) and unit-test helpers that bypass the GET.
func newStoreFromTarget[T any](cfg StoreConfig[T], target S3Target) (*Store[T], error) {
	w, err := NewWriter(writerConfigFrom(cfg, target))
	if err != nil {
		return nil, err
	}
	r, err := NewReader(readerConfigFrom(cfg, target))
	if err != nil {
		return nil, err
	}
	return &Store[T]{Writer: w, Reader: r}, nil
}

// writerConfigFrom projects a StoreConfig[T] onto the narrower
// WriterConfig[T]. Central place so drift between the two types
// is easy to spot.
func writerConfigFrom[T any](c StoreConfig[T], target S3Target) WriterConfig[T] {
	return WriterConfig[T]{
		Target:            target,
		PartitionKeyOf:    c.PartitionKeyOf,
		Compression:       c.Compression,
		InsertedAtField:   c.InsertedAtField,
		MaterializedViews: c.MaterializedViews,
	}
}

// readerConfigFrom projects a StoreConfig[T] onto the narrower
// ReaderConfig[T]. InsertedAtField is writer-only — the reader
// sees that column like any other parquet field, decoded
// natively into T by parquet-go.
func readerConfigFrom[T any](c StoreConfig[T], target S3Target) ReaderConfig[T] {
	return ReaderConfig[T]{
		Target:      target,
		EntityKeyOf: c.EntityKeyOf,
		VersionOf:   c.VersionOf,
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
		"unknown Compression %q (want snappy, "+
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
			"InsertedAtField requires T to be a struct, got %s",
			rt)
	}
	f, ok := rt.FieldByName(name)
	if !ok {
		return nil, fmt.Errorf(
			"InsertedAtField %q: no such field on %s",
			name, rt)
	}
	if f.Type != reflect.TypeFor[time.Time]() {
		return nil, fmt.Errorf(
			"InsertedAtField %q: must be time.Time, got %s",
			name, f.Type)
	}
	tag := f.Tag.Get("parquet")
	name0, _, _ := strings.Cut(tag, ",")
	if name0 == "" || name0 == "-" {
		return nil, fmt.Errorf(
			"InsertedAtField %q: must carry a non-empty, "+
				"non-\"-\" parquet tag so the value persists as a real "+
				"parquet column (got %q)", name, tag)
	}
	return f.Index, nil
}
