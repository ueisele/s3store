package s3store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
)

// writeCleanupTimeout bounds best-effort cleanup work (HEAD/DELETE)
// on the ref-PUT failure path, so the caller context being cancelled
// doesn't prevent us from either confirming a lost-ack or removing an
// orphan parquet.
const writeCleanupTimeout = 5 * time.Second

// Write extracts the key from each record via KeyFunc, groups
// by key, and writes one Parquet file + stream ref per key.
func (s *Store[T]) Write(
	ctx context.Context, records []T,
) ([]WriteResult, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf(
			"s3store: records must not be empty")
	}
	if s.cfg.KeyFunc == nil {
		return nil, fmt.Errorf(
			"s3store: KeyFunc is required for Write; " +
				"use WriteWithKey for explicit keys")
	}

	grouped := s.groupByKey(records)

	var results []WriteResult
	for key, group := range grouped {
		result, err := s.WriteWithKey(ctx, key, group)
		if err != nil {
			return results, err
		}
		results = append(results, *result)
	}
	return results, nil
}

// WriteWithKey encodes records as Parquet, uploads to S3, and
// writes an empty ref file with all metadata in the key name.
// Ref timestamp is generated AFTER the data PUT completes.
//
// If the ref PUT fails after the data PUT succeeded, WriteWithKey
// issues a HEAD on the ref key to disambiguate a lost-ack (ref
// actually got written, we just lost the response) from a real
// failure. On a real failure it best-effort deletes the orphan
// parquet; if that cleanup also fails, the returned error
// includes the orphan data path so the operator can clean up.
func (s *Store[T]) WriteWithKey(
	ctx context.Context, key string, records []T,
) (*WriteResult, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf(
			"s3store: records must not be empty")
	}
	if err := s.validateKey(key); err != nil {
		return nil, err
	}

	parquetBytes, err := s.encodeParquet(records)
	if err != nil {
		return nil, fmt.Errorf(
			"s3store: parquet encode: %w", err)
	}

	shortID := uuid.New().String()[:8]

	dataKey := s.buildDataPath(key, shortID)
	if err := s.putObject(
		ctx, dataKey, parquetBytes,
		"application/octet-stream",
	); err != nil {
		return nil, fmt.Errorf(
			"s3store: put data: %w", err)
	}

	tsMicros := time.Now().UnixMicro()
	refKey := s.encodeRefKey(tsMicros, shortID, key)

	result := &WriteResult{
		Offset:   Offset(refKey),
		DataPath: dataKey,
		RefPath:  refKey,
	}

	putErr := s.putObject(
		ctx, refKey, []byte{}, "application/octet-stream")
	if putErr == nil {
		return result, nil
	}

	// Ref PUT failed. Disambiguate lost-ack from a real failure
	// using a bounded, caller-independent context so cleanup
	// still completes if the caller has cancelled.
	cleanupCtx, cancel := context.WithTimeout(
		context.Background(), writeCleanupTimeout)
	defer cancel()

	if exists, headErr := s.objectExists(
		cleanupCtx, refKey,
	); headErr == nil && exists {
		// Ref actually got written — we just lost the ack.
		return result, nil
	}

	if delErr := s.deleteObject(
		cleanupCtx, dataKey,
	); delErr != nil {
		return nil, fmt.Errorf(
			"s3store: put ref: %w (orphan data at %s: %v)",
			putErr, dataKey, delErr)
	}

	return nil, fmt.Errorf("s3store: put ref: %w", putErr)
}

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

func (s *Store[T]) deleteObject(
	ctx context.Context, key string,
) error {
	_, err := s.s3.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(key),
	})
	return err
}

func (s *Store[T]) groupByKey(records []T) map[string][]T {
	grouped := make(map[string][]T)
	for _, r := range records {
		key := s.cfg.KeyFunc(r)
		grouped[key] = append(grouped[key], r)
	}
	return grouped
}

// validateKey enforces that the key is a "/"-delimited sequence
// of exactly len(KeyParts) Hive-style segments, each in the
// form "KeyParts[i]=<non-empty value>", in the configured order.
//
// Values may contain '=' (we split on the first '=' only) but
// cannot contain '/' or be empty. This catches KeyFunc bugs
// that reorder, drop, duplicate, or otherwise malform segments
// before they corrupt the S3 layout.
func (s *Store[T]) validateKey(key string) error {
	segments := strings.Split(key, "/")
	if len(segments) != len(s.cfg.KeyParts) {
		return fmt.Errorf(
			"s3store: key %q has %d segments, "+
				"expected %d (%v)",
			key, len(segments),
			len(s.cfg.KeyParts), s.cfg.KeyParts)
	}
	for i, seg := range segments {
		part := s.cfg.KeyParts[i]
		prefix := part + "="
		if !strings.HasPrefix(seg, prefix) {
			return fmt.Errorf(
				"s3store: key %q segment %d is %q, "+
					"expected %q=...",
				key, i, seg, part)
		}
		if seg == prefix {
			return fmt.Errorf(
				"s3store: key %q segment %d has "+
					"empty value for %q",
				key, i, part)
		}
	}
	return nil
}

func (s *Store[T]) encodeParquet(records []T) ([]byte, error) {
	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[T](&buf)
	if _, err := writer.Write(records); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

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
