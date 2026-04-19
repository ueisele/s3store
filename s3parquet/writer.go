package s3parquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/ueisele/s3store/internal/core"
)

// Writer is the write-side half of a Store. Owns the write path
// (Write / WriteWithKey), index-marker emission, and the HEAD /
// DELETE helpers used for write-path cleanup of orphaned data.
// Construct directly via NewWriter when a service only writes;
// embed in Store when it also reads.
type Writer[T any] struct {
	cfg      WriterConfig[T]
	s3       *s3.Client
	dataPath string
	refPath  string

	// compressionCodec is Config.Compression resolved to the
	// parquet-go codec once at New(), so the hot path doesn't
	// re-switch on the string.
	compressionCodec compress.Codec

	// indexes is the list of registered secondary indexes that
	// the write path iterates per record to emit marker objects.
	// Typed Index[T, K] handles append to this slice via
	// registerIndex at NewIndex time; the entry type K is erased
	// at the closure boundary so the slice can be homogeneous
	// over T.
	indexes []indexWriter[T]
}

// indexWriter is the internal, entry-type-erased contract
// between a typed Index[T, K] and the writer's write path.
// Given a record, it returns the S3 object keys of the markers
// that record produces, already validated and ready to PUT.
type indexWriter[T any] struct {
	name    string
	pathsOf func(T) ([]string, error)
}

// registerIndex appends a typed index's writer to the write
// path's iteration list. Called from NewIndex. Not
// concurrency-safe: indexes should be registered before the
// first Write.
func (w *Writer[T]) registerIndex(iw indexWriter[T]) {
	w.indexes = append(w.indexes, iw)
}

// putObject uploads data to S3 under the given key.
func (w *Writer[T]) putObject(
	ctx context.Context,
	key string,
	data []byte,
	contentType string,
) error {
	_, err := w.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(w.cfg.Bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String(contentType),
	})
	return err
}

// objectExists reports whether an S3 object exists, mapping a
// NotFound error to (false, nil) so the caller can distinguish
// missing objects from real S3 failures.
func (w *Writer[T]) objectExists(
	ctx context.Context, key string,
) (bool, error) {
	_, err := w.s3.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(w.cfg.Bucket),
		Key:    aws.String(key),
	})
	if err == nil {
		return true, nil
	}
	var notFound *s3types.NotFound
	if errors.As(err, &notFound) {
		return false, nil
	}
	return false, err
}

// deleteObject removes an S3 object.
func (w *Writer[T]) deleteObject(
	ctx context.Context, key string,
) error {
	_, err := w.s3.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(w.cfg.Bucket),
		Key:    aws.String(key),
	})
	return err
}

// NewWriter constructs a Writer directly from WriterConfig. Use
// this in services that only write; use New(Config) when the
// same process also reads through a Reader/Store.
//
// Validation mirrors the writer-side half of New: Bucket,
// Prefix, S3Client, PartitionKeyParts must be set;
// BloomFilterColumns must map to real parquet columns on T;
// Compression resolves to a codec (zero value → snappy).
// PartitionKeyOf is optional at construction — Write errors if
// called without it, but WriteWithKey works regardless.
func NewWriter[T any](cfg WriterConfig[T]) (*Writer[T], error) {
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
	if err := validateBloomFilterColumns[T](cfg.BloomFilterColumns); err != nil {
		return nil, err
	}
	codec, err := resolveCompression(cfg.Compression)
	if err != nil {
		return nil, err
	}
	return &Writer[T]{
		cfg:              cfg,
		s3:               cfg.S3Client,
		dataPath:         core.DataPath(cfg.Prefix),
		refPath:          core.RefPath(cfg.Prefix),
		compressionCodec: codec,
	}, nil
}

// Reader returns a Reader[T] over the same data this Writer
// produces. Shared wiring (Bucket, Prefix, S3Client,
// PartitionKeyParts) comes from the Writer's config; extras
// carries the read-side knobs (SettleWindow, dedup,
// InsertedAtField, OnMissingData). Pass ReaderExtras[T]{} for
// all defaults.
//
// Convenience wrapper over NewReader — equivalent to building a
// ReaderConfig by hand from this Writer's shared fields and
// extras.
func (w *Writer[T]) Reader(extras ReaderExtras[T]) (*Reader[T], error) {
	return NewReader[T](ReaderConfig[T]{
		Bucket:            w.cfg.Bucket,
		Prefix:            w.cfg.Prefix,
		PartitionKeyParts: w.cfg.PartitionKeyParts,
		S3Client:          w.cfg.S3Client,
		SettleWindow:      extras.SettleWindow,
		EntityKeyOf:       extras.EntityKeyOf,
		VersionOf:         extras.VersionOf,
		InsertedAtField:   extras.InsertedAtField,
		OnMissingData:     extras.OnMissingData,
	})
}
