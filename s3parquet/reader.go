package s3parquet

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ueisele/s3store/internal/core"
)

// Reader is the read-side half of a Store. Owns Read / Poll /
// PollRecords / PollRecordsAll / OffsetAt. Construct directly
// via NewReader in read-only services, via Writer.Reader for a
// same-T Reader over a Writer's data, or via NewView /
// NewViewFromStore for a narrower-T view of a writing Store.
type Reader[T any] struct {
	cfg      ReaderConfig[T]
	s3       *s3.Client
	dataPath string
	refPath  string

	// insertedAtFieldIndex is the reflect struct-field path for
	// Config.InsertedAtField, resolved once at New() so the hot
	// path doesn't reparse the type. nil when unset.
	insertedAtFieldIndex []int
}

// getObjectBytes downloads a single object into memory. Used by
// Read and PollRecords; avoids the per-request overhead of
// setting up a ranged io.ReaderAt in exchange for buffering the
// whole file.
func (r *Reader[T]) getObjectBytes(
	ctx context.Context, key string,
) ([]byte, error) {
	resp, err := r.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(r.cfg.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// NewReader constructs a Reader directly from ReaderConfig.
// Intended for read-only services that have no write-side config
// to supply (no PartitionKeyOf / Compression / BloomFilters).
//
// Validates the same read-side invariants New(Config) does:
// required shared fields, InsertedAtField (if set) must resolve
// on T, and a default VersionOf is assigned when EntityKeyOf is
// set but VersionOf is nil (wrote-last-wins).
func NewReader[T any](cfg ReaderConfig[T]) (*Reader[T], error) {
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
	if cfg.EntityKeyOf != nil && cfg.VersionOf == nil {
		cfg.VersionOf = DefaultVersionOf[T]
	}
	insertedAtIdx, err := validateInsertedAtField[T](cfg.InsertedAtField)
	if err != nil {
		return nil, err
	}
	return &Reader[T]{
		cfg:                  cfg,
		s3:                   cfg.S3Client,
		dataPath:             core.DataPath(cfg.Prefix),
		refPath:              core.RefPath(cfg.Prefix),
		insertedAtFieldIndex: insertedAtIdx,
	}, nil
}

// NewView constructs a Reader[T] over the data a Writer[U]
// produces, where T may differ from U — typically a narrower
// struct that omits heavy write-only columns (parquet-go skips
// unlisted columns on decode). Writer's shared wiring (Bucket,
// Prefix, S3Client, PartitionKeyParts) carries over; the user
// supplies read-side knobs via ReaderExtras[T].
//
// Dedup closures (EntityKeyOf / VersionOf) on the Writer are
// typed over U and cannot be auto-transformed to T; the caller
// re-declares them in extras when dedup is needed. For helpers
// that generate these closures from column names, see the
// future EntityKeyByColumns / VersionByColumn helpers (post-v1).
func NewView[T, U any](
	w *Writer[U], extras ReaderExtras[T],
) (*Reader[T], error) {
	if w == nil {
		return nil, fmt.Errorf("s3parquet: NewView: writer is nil")
	}
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

// NewViewFromStore is NewView with a Store[U] instead of a
// Writer[U] — extracts the embedded Writer and forwards. Common
// shape: one Store writes FullRec, a few NewViewFromStore calls
// produce narrow Readers for hot-path reads without respecifying
// the shared config.
func NewViewFromStore[T, U any](
	s *Store[U], extras ReaderExtras[T],
) (*Reader[T], error) {
	if s == nil {
		return nil, fmt.Errorf(
			"s3parquet: NewViewFromStore: store is nil")
	}
	return NewView[T](s.Writer, extras)
}
