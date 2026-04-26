package s3store

import (
	"context"
	"database/sql"
	"errors"
	"iter"
	"time"

	"github.com/ueisele/s3store/s3parquet"
	"github.com/ueisele/s3store/s3sql"
)

// Store is the umbrella entry point: a thin composer over the
// s3parquet writer (in-memory parquet encoding + S3 PUT), the
// s3parquet reader (pure-Go decoding, change-stream tailing, per-
// partition dedup), and the s3sql reader (cgo, DuckDB-powered SQL
// over the same files).
//
// Method forwarding:
//
//   - Write, WriteWithKey, PartitionKey           → s3parquet.Writer
//   - Read, ReadIter, ReadRangeIter, Poll,
//     PollRecords, OffsetAt                       → s3parquet.Reader
//   - Query                                       → s3sql.Reader
//
// Importing this package transitively pulls in DuckDB (cgo). For
// a cgo-free build, import s3store/s3parquet directly — every
// non-SQL surface lives there.
type Store[T any] struct {
	writer        *s3parquet.Writer[T]
	parquetReader *s3parquet.Reader[T]
	sqlReader     *s3sql.Reader
}

// New constructs a Store, building the pure-Go Writer + Reader and
// the DuckDB-backed SQL Reader from a single umbrella Config. All
// three halves share one S3Target so they can't drift on Bucket /
// Prefix / PartitionKeyParts / SettleWindow / DisableRefStream.
//
// Performs no S3 I/O at construction time.
func New[T any](cfg Config[T]) (*Store[T], error) {
	target := s3parquet.NewS3Target(s3parquet.S3TargetConfig{
		Bucket:              cfg.Bucket,
		Prefix:              cfg.Prefix,
		S3Client:            cfg.S3Client,
		PartitionKeyParts:   cfg.PartitionKeyParts,
		SettleWindow:        cfg.SettleWindow,
		DisableRefStream:    cfg.DisableRefStream,
		MaxInflightRequests: cfg.MaxInflightRequests,
		ConsistencyControl:  cfg.ConsistencyControl,
	})
	w, err := s3parquet.NewWriter(s3parquet.WriterConfig[T]{
		Target:          target,
		PartitionKeyOf:  cfg.PartitionKeyOf,
		Compression:     cfg.Compression,
		InsertedAtField: cfg.InsertedAtField,
		DisableCleanup:  cfg.DisableCleanup,
	})
	if err != nil {
		return nil, err
	}
	pr, err := s3parquet.NewReader(s3parquet.ReaderConfig[T]{
		Target:      target,
		EntityKeyOf: cfg.EntityKeyOf,
		VersionOf:   cfg.VersionOf,
	})
	if err != nil {
		return nil, err
	}
	sr, err := s3sql.NewReader(s3sql.ReaderConfig{
		Target:           target,
		TableAlias:       cfg.TableAlias,
		VersionColumn:    cfg.VersionColumn,
		EntityKeyColumns: cfg.EntityKeyColumns,
		ExtraInitSQL:     cfg.ExtraInitSQL,
	})
	if err != nil {
		return nil, err
	}
	return &Store[T]{writer: w, parquetReader: pr, sqlReader: sr}, nil
}

// Writer returns the underlying s3parquet.Writer. Use when a
// feature lives only on the sub-package (e.g. index registration
// via s3parquet.NewIndexWithRegister) without giving up the
// umbrella's ergonomics.
func (s *Store[T]) Writer() *s3parquet.Writer[T] {
	return s.writer
}

// Reader returns the underlying s3parquet.Reader. Symmetric with
// Writer(); useful for passing the Reader into helpers that
// accept it directly.
func (s *Store[T]) Reader() *s3parquet.Reader[T] {
	return s.parquetReader
}

// SQL returns the underlying s3sql.Reader. Use when arbitrary
// DuckDB SQL is needed beyond the umbrella's Query delegation.
func (s *Store[T]) SQL() *s3sql.Reader {
	return s.sqlReader
}

// Target returns the S3Target the umbrella was built with. All
// halves share this value, so it's the canonical handle for
// tooling that operates on the same dataset without carrying T.
func (s *Store[T]) Target() s3parquet.S3Target {
	return s.writer.Target()
}

// Close releases resources. Only the SQL Reader (DuckDB) owns
// anything that needs explicit release; the Writer and parquet
// Reader are stateless on top of the shared S3 client.
func (s *Store[T]) Close() error {
	return errors.Join(s.sqlReader.Close())
}

// Write delegates to the Writer. Accepts WriteOption for
// retry-safe idempotent writes (WithIdempotencyToken).
func (s *Store[T]) Write(
	ctx context.Context, records []T, opts ...WriteOption,
) ([]WriteResult, error) {
	return s.writer.Write(ctx, records, opts...)
}

// WriteWithKey delegates to the Writer. Accepts WriteOption for
// retry-safe idempotent writes (WithIdempotencyToken).
func (s *Store[T]) WriteWithKey(
	ctx context.Context, key string, records []T, opts ...WriteOption,
) (*WriteResult, error) {
	return s.writer.WriteWithKey(ctx, key, records, opts...)
}

// PartitionKey delegates to the Writer. Handy when paired with
// WriteWithKey for single-partition batches:
//
//	_, err := store.WriteWithKey(ctx, store.PartitionKey(recs[0]), recs)
func (s *Store[T]) PartitionKey(rec T) string {
	return s.writer.PartitionKey(rec)
}

// Read delegates to the parquet Reader.
// See s3parquet.Reader.Read for the full contract.
func (s *Store[T]) Read(
	ctx context.Context, patterns []string, opts ...QueryOption,
) ([]T, error) {
	return s.parquetReader.Read(ctx, patterns, opts...)
}

// ReadIter delegates to the parquet Reader.
// See s3parquet.Reader.ReadIter for the full contract.
func (s *Store[T]) ReadIter(
	ctx context.Context, patterns []string, opts ...QueryOption,
) iter.Seq2[T, error] {
	return s.parquetReader.ReadIter(ctx, patterns, opts...)
}

// Query delegates to the SQL Reader.
// See s3sql.Reader.Query for the full contract.
func (s *Store[T]) Query(
	ctx context.Context,
	keyPatterns []string,
	sqlQuery string,
	opts ...QueryOption,
) (*sql.Rows, error) {
	return s.sqlReader.Query(ctx, keyPatterns, sqlQuery, opts...)
}

// Poll delegates to the parquet Reader (pure S3 LIST, no DuckDB).
// See s3parquet.Reader.Poll for the full contract.
func (s *Store[T]) Poll(
	ctx context.Context, since Offset, maxEntries int32,
	opts ...QueryOption,
) ([]StreamEntry, Offset, error) {
	return s.parquetReader.Poll(ctx, since, maxEntries, opts...)
}

// PollRecords delegates to the parquet Reader.
// See s3parquet.Reader.PollRecords for the full contract.
func (s *Store[T]) PollRecords(
	ctx context.Context,
	since Offset,
	maxEntries int32,
	opts ...QueryOption,
) ([]T, Offset, error) {
	return s.parquetReader.PollRecords(ctx, since, maxEntries, opts...)
}

// ReadRangeIter delegates to the parquet Reader.
// See s3parquet.Reader.ReadRangeIter for the full contract.
func (s *Store[T]) ReadRangeIter(
	ctx context.Context,
	since, until time.Time,
	opts ...QueryOption,
) iter.Seq2[T, error] {
	return s.parquetReader.ReadRangeIter(ctx, since, until, opts...)
}

// OffsetAt returns the stream offset corresponding to wall-clock
// time t. Pure computation — no S3 call. Pair with WithUntilOffset
// on Poll / PollRecords to read records within a time window —
// or use ReadRangeIter, which takes time.Time bounds directly.
func (s *Store[T]) OffsetAt(t time.Time) Offset {
	return s.parquetReader.OffsetAt(t)
}
