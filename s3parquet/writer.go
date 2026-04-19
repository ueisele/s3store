package s3parquet

import (
	"bytes"
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/parquet-go/parquet-go/compress"
)

// writer is the internal write-side half of a Store, holding the
// state that Write / WriteWithKey, index-marker emission, and
// write-path cleanup (HEAD / DELETE on orphaned data) need.
// Stage 1 keeps it unexported; Stage 2 promotes it to a public
// Writer[T] with its own narrower Config.
type writer[T any] struct {
	cfg      Config[T]
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
func (w *writer[T]) registerIndex(iw indexWriter[T]) {
	w.indexes = append(w.indexes, iw)
}

// putObject uploads data to S3 under the given key.
func (w *writer[T]) putObject(
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
func (w *writer[T]) objectExists(
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
func (w *writer[T]) deleteObject(
	ctx context.Context, key string,
) error {
	_, err := w.s3.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(w.cfg.Bucket),
		Key:    aws.String(key),
	})
	return err
}
