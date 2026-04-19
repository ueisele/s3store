package s3store

import (
	"context"
	"database/sql"
	"time"

	"github.com/ueisele/s3store/s3parquet"
	"github.com/ueisele/s3store/s3sql"
)

// Store is the umbrella entry point: a thin composer over
// *s3parquet.Writer[T] (pure Go write path, in-memory parquet
// encoding + S3 PUT) and *s3sql.Reader[T] (cgo, DuckDB-powered
// reads) that preserves the single-Store ergonomics callers
// are used to. Reads route through DuckDB so they stay
// consistent with Query / PollRecords on schema evolution and
// dedup semantics.
//
// Method forwarding:
//
//   - Write, WriteWithKey        → s3parquet.Writer
//   - Poll, OffsetAt, PollRecords,
//     PollRecordsAll             → s3sql.Reader (S3 LIST-based
//     stream routes through the shared internal/refstream
//     implementation so parity with s3parquet.Reader is free)
//   - Read, ReadMany, Query,
//     QueryRow, QueryMany,
//     QueryRowMany               → s3sql.Reader
//
// Importing this package transitively pulls in DuckDB (cgo).
// If you want a cgo-free build, import s3store/s3parquet
// directly for write-plus-simple-read workloads.
type Store[T any] struct {
	writer *s3parquet.Writer[T]
	reader *s3sql.Reader[T]
}

// New constructs a Store, building the pure-Go Writer and the
// DuckDB-backed Reader from a single umbrella Config. Both
// halves share one S3Target so they can't drift on Bucket /
// Prefix / PartitionKeyParts / SettleWindow / DisableRefStream.
func New[T any](cfg Config[T]) (*Store[T], error) {
	target := s3parquet.S3Target{
		Bucket:            cfg.Bucket,
		Prefix:            cfg.Prefix,
		S3Client:          cfg.S3Client,
		PartitionKeyParts: cfg.PartitionKeyParts,
		SettleWindow:      cfg.SettleWindow,
		DisableRefStream:  cfg.DisableRefStream,
	}
	w, err := s3parquet.NewWriter(s3parquet.WriterConfig[T]{
		Target:         target,
		PartitionKeyOf: cfg.PartitionKeyOf,
		Compression:    cfg.Compression,
	})
	if err != nil {
		return nil, err
	}
	r, err := s3sql.NewReader(s3sql.ReaderConfig[T]{
		Target:           target,
		TableAlias:       cfg.TableAlias,
		VersionColumn:    cfg.VersionColumn,
		EntityKeyColumns: cfg.EntityKeyColumns,
		ExtraInitSQL:     cfg.ExtraInitSQL,
		InsertedAtField:  cfg.InsertedAtField,
	})
	if err != nil {
		return nil, err
	}
	return &Store[T]{writer: w, reader: r}, nil
}

// Writer returns the underlying s3parquet.Writer. Use when a
// feature lives only on the sub-package (e.g. index registration
// via s3parquet.NewIndexWithRegister) without giving up the
// umbrella's ergonomics.
func (s *Store[T]) Writer() *s3parquet.Writer[T] {
	return s.writer
}

// Reader returns the underlying s3sql.Reader. Symmetric with
// Writer(); useful for passing the Reader into helpers that
// accept it directly.
func (s *Store[T]) Reader() *s3sql.Reader[T] {
	return s.reader
}

// Target returns the S3Target the umbrella was built with. Both
// halves share this value, so it's the canonical handle for
// tooling that operates on the same dataset without carrying T.
func (s *Store[T]) Target() s3parquet.S3Target {
	return s.writer.Target()
}

// Close releases resources. Only the Reader (DuckDB) owns
// anything that needs explicit release; the Writer is purely
// stateless on top of the shared S3 client.
func (s *Store[T]) Close() error {
	return s.reader.Close()
}

// Write delegates to the Writer.
func (s *Store[T]) Write(
	ctx context.Context, records []T,
) ([]WriteResult, error) {
	return s.writer.Write(ctx, records)
}

// WriteWithKey delegates to the Writer.
func (s *Store[T]) WriteWithKey(
	ctx context.Context, key string, records []T,
) (*WriteResult, error) {
	return s.writer.WriteWithKey(ctx, key, records)
}

// PartitionKey delegates to the Writer. Handy when paired with
// WriteWithKey for single-partition batches:
//
//	_, err := store.WriteWithKey(ctx, store.PartitionKey(recs[0]), recs)
func (s *Store[T]) PartitionKey(rec T) string {
	return s.writer.PartitionKey(rec)
}

// Poll delegates to the Reader (pure S3 LIST; no DuckDB
// involvement despite living on the cgo-backed half).
func (s *Store[T]) Poll(
	ctx context.Context, since Offset, maxEntries int32,
	opts ...QueryOption,
) ([]StreamEntry, Offset, error) {
	return s.reader.Poll(ctx, since, maxEntries, opts...)
}

// OffsetAt returns the stream offset corresponding to wall-
// clock time t. Pair with WithUntilOffset on Poll/PollRecords
// to read records within a time window.
func (s *Store[T]) OffsetAt(t time.Time) Offset {
	return s.reader.OffsetAt(t)
}

// Read delegates to the Reader so dedup semantics and the
// reflection-based row binder match Query / PollRecords.
func (s *Store[T]) Read(
	ctx context.Context, keyPattern string, opts ...QueryOption,
) ([]T, error) {
	return s.reader.Read(ctx, keyPattern, opts...)
}

// ReadMany delegates to the Reader. Runs a single DuckDB
// query over the deduplicated union of files matching every
// pattern; see s3sql.Reader.ReadMany for the full contract.
func (s *Store[T]) ReadMany(
	ctx context.Context, patterns []string, opts ...QueryOption,
) ([]T, error) {
	return s.reader.ReadMany(ctx, patterns, opts...)
}

// Query delegates to the Reader.
func (s *Store[T]) Query(
	ctx context.Context,
	keyPattern string,
	sqlQuery string,
	opts ...QueryOption,
) (*sql.Rows, error) {
	return s.reader.Query(ctx, keyPattern, sqlQuery, opts...)
}

// QueryMany delegates to the Reader. Use when a SQL-level
// aggregation or join needs to span a non-Cartesian tuple set;
// see s3sql.Reader.QueryMany for the full contract.
func (s *Store[T]) QueryMany(
	ctx context.Context,
	patterns []string,
	sqlQuery string,
	opts ...QueryOption,
) (*sql.Rows, error) {
	return s.reader.QueryMany(ctx, patterns, sqlQuery, opts...)
}

// QueryRow delegates to the Reader.
func (s *Store[T]) QueryRow(
	ctx context.Context,
	keyPattern string,
	sqlQuery string,
	opts ...QueryOption,
) *sql.Row {
	return s.reader.QueryRow(ctx, keyPattern, sqlQuery, opts...)
}

// QueryRowMany delegates to the Reader. See
// s3sql.Reader.QueryRowMany for the full contract.
func (s *Store[T]) QueryRowMany(
	ctx context.Context,
	patterns []string,
	sqlQuery string,
	opts ...QueryOption,
) *sql.Row {
	return s.reader.QueryRowMany(ctx, patterns, sqlQuery, opts...)
}

// PollRecords delegates to the Reader so dedup and schema-
// evolution transforms are consistent with Read.
func (s *Store[T]) PollRecords(
	ctx context.Context,
	since Offset,
	maxEntries int32,
	opts ...QueryOption,
) ([]T, Offset, error) {
	return s.reader.PollRecords(ctx, since, maxEntries, opts...)
}

// PollRecordsAll reads every record in [since, until) via
// repeated PollRecords calls. Convenience wrapper for bounded
// windows. Pair with OffsetAt for time-based windows.
func (s *Store[T]) PollRecordsAll(
	ctx context.Context,
	since, until Offset,
	opts ...QueryOption,
) ([]T, error) {
	return s.reader.PollRecordsAll(ctx, since, until, opts...)
}
