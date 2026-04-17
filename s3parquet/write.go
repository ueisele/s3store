package s3parquet

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
	"github.com/ueisele/s3store/internal/core"
)

// writeCleanupTimeout bounds best-effort cleanup work (HEAD /
// DELETE) on the ref-PUT failure path, so the caller's context
// being cancelled doesn't prevent us from either confirming a
// lost ack or removing an orphan parquet.
const writeCleanupTimeout = 5 * time.Second

// Write extracts the key from each record via PartitionKeyOf,
// groups by key, and writes one Parquet file + stream ref per
// key. Returns a WriteResult per group.
//
// An empty records slice is a no-op: (nil, nil) is returned so
// callers don't have to guard against batch-pipeline edge
// cases.
func (s *Store[T]) Write(
	ctx context.Context, records []T,
) ([]core.WriteResult, error) {
	if len(records) == 0 {
		return nil, nil
	}
	if s.cfg.PartitionKeyOf == nil {
		return nil, fmt.Errorf(
			"s3parquet: PartitionKeyOf is required for Write; " +
				"use WriteWithKey for explicit keys")
	}

	grouped := s.groupByKey(records)

	// Sorted key iteration keeps the returned results (and
	// the sequence of S3 PUTs) deterministic across runs,
	// instead of following map iteration order.
	var results []core.WriteResult
	for _, key := range slices.Sorted(maps.Keys(grouped)) {
		result, err := s.WriteWithKey(ctx, key, grouped[key])
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
// issues a HEAD on the ref key to disambiguate a lost ack (ref
// actually got written, we just lost the response) from a real
// failure. On a real failure it best-effort deletes the orphan
// parquet; if that cleanup also fails, the returned error
// includes the orphan data path so the operator can clean up.
func (s *Store[T]) WriteWithKey(
	ctx context.Context, key string, records []T,
) (*core.WriteResult, error) {
	if len(records) == 0 {
		return nil, nil
	}
	if err := s.validateKey(key); err != nil {
		return nil, err
	}

	parquetBytes, err := encodeParquet(records)
	if err != nil {
		return nil, fmt.Errorf(
			"s3parquet: parquet encode: %w", err)
	}

	shortID := uuid.New().String()[:8]

	dataKey := core.BuildDataFilePath(s.dataPath, key, shortID)
	if err := s.putObject(
		ctx, dataKey, parquetBytes,
		"application/octet-stream",
	); err != nil {
		return nil, fmt.Errorf(
			"s3parquet: put data: %w", err)
	}

	tsMicros := time.Now().UnixMicro()
	refKey := core.EncodeRefKey(s.refPath, tsMicros, shortID, key)

	result := &core.WriteResult{
		Offset:   core.Offset(refKey),
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
			"s3parquet: put ref: %w (orphan data at %s: %v)",
			putErr, dataKey, delErr)
	}

	return nil, fmt.Errorf("s3parquet: put ref: %w", putErr)
}

func (s *Store[T]) groupByKey(records []T) map[string][]T {
	grouped := make(map[string][]T)
	for _, r := range records {
		key := s.cfg.PartitionKeyOf(r)
		grouped[key] = append(grouped[key], r)
	}
	return grouped
}

// validateKey enforces that the key is a "/"-delimited sequence
// of exactly len(KeyParts) Hive-style segments, each in the
// form "KeyParts[i]=<non-empty value>", in the configured order.
//
// Values may contain '=' (we split on the first '=' only) but
// cannot contain '/' or be empty. Catches PartitionKeyOf bugs
// before they corrupt the S3 layout.
func (s *Store[T]) validateKey(key string) error {
	segments := strings.Split(key, "/")
	if len(segments) != len(s.cfg.KeyParts) {
		return fmt.Errorf(
			"s3parquet: key %q has %d segments, "+
				"expected %d (%v)",
			key, len(segments),
			len(s.cfg.KeyParts), s.cfg.KeyParts)
	}
	for i, seg := range segments {
		part := s.cfg.KeyParts[i]
		prefix := part + "="
		if !strings.HasPrefix(seg, prefix) {
			return fmt.Errorf(
				"s3parquet: key %q segment %d is %q, "+
					"expected %q=...",
				key, i, seg, part)
		}
		if seg == prefix {
			return fmt.Errorf(
				"s3parquet: key %q segment %d has "+
					"empty value for %q",
				key, i, part)
		}
	}
	return nil
}

func encodeParquet[T any](records []T) ([]byte, error) {
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
