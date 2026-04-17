package s3parquet

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// putObject uploads data to S3 under the given key.
func (s *Store[T]) putObject(
	ctx context.Context,
	key string,
	data []byte,
	contentType string,
) error {
	_, err := s.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.cfg.Bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String(contentType),
	})
	return err
}

// objectExists reports whether an S3 object exists, mapping a
// NotFound error to (false, nil) so the caller can distinguish
// missing objects from real S3 failures.
func (s *Store[T]) objectExists(
	ctx context.Context, key string,
) (bool, error) {
	_, err := s.s3.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
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
func (s *Store[T]) deleteObject(
	ctx context.Context, key string,
) error {
	_, err := s.s3.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(key),
	})
	return err
}

// getObjectBytes downloads a single object into memory. Used
// by Read and PollRecords; avoids the per-request overhead of
// setting up a ranged io.ReaderAt in exchange for buffering the
// whole file.
func (s *Store[T]) getObjectBytes(
	ctx context.Context, key string,
) ([]byte, error) {
	resp, err := s.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}
