package s3store

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/ueisele/s3store/s3parquet"
	"github.com/ueisele/s3store/s3sql"
)

// Store is the umbrella entry point: a thin composer over
// *s3parquet.Store[T] (pure Go, write + simple read) and
// *s3sql.Store[T] (cgo, DuckDB-powered queries) that preserves
// the single-Store ergonomics callers are used to.
//
// Method forwarding:
//
//   - Write, WriteWithKey  → s3parquet (encode + S3 PUT + ref)
//   - Poll                 → s3parquet (S3 LIST)
//   - Read, Query,
//     QueryRow, PollRecords → s3sql (DuckDB-powered)
//
// Importing this package transitively pulls in DuckDB (cgo).
// If you want a cgo-free build, import s3store/s3parquet or
// s3store/s3sql directly instead of this umbrella.
type Store[T any] struct {
	parquet *s3parquet.Store[T]
	sql     *s3sql.Store[T]
}

// New constructs a Store, building both the pure-Go write+read
// sub-store and the DuckDB-backed SQL sub-store from a single
// umbrella Config.
func New[T any](cfg Config[T]) (*Store[T], error) {
	pq, err := s3parquet.New[T](s3parquet.Config[T]{
		Bucket:             cfg.Bucket,
		Prefix:             cfg.Prefix,
		PartitionKeyParts:  cfg.PartitionKeyParts,
		S3Client:           cfg.S3Client,
		PartitionKeyOf:     cfg.PartitionKeyOf,
		SettleWindow:       cfg.SettleWindow,
		BloomFilterColumns: cfg.BloomFilterColumns,
		Compression:        cfg.Compression,
		InsertedAtField:    cfg.InsertedAtField,
		// EntityKeyOf / VersionOf deliberately omitted: the
		// umbrella's Read / PollRecords go through s3sql and use
		// SQL-side dedup. Users who want pure-Go dedup should
		// import s3parquet directly.
	})
	if err != nil {
		return nil, err
	}
	sq, err := s3sql.New[T](s3sql.Config[T]{
		Bucket:            cfg.Bucket,
		Prefix:            cfg.Prefix,
		PartitionKeyParts: cfg.PartitionKeyParts,
		S3Client:          cfg.S3Client,
		TableAlias:        cfg.TableAlias,
		SettleWindow:      cfg.SettleWindow,
		VersionColumn:     cfg.VersionColumn,
		EntityKeyColumns:  cfg.EntityKeyColumns,
		ExtraInitSQL:      cfg.ExtraInitSQL,
		InsertedAtField:   cfg.InsertedAtField,
	})
	if err != nil {
		_ = pq.Close()
		return nil, err
	}
	return &Store[T]{parquet: pq, sql: sq}, nil
}

// Close releases resources from both sub-stores. Returns the
// first non-nil error observed; the other sub-store is closed
// regardless.
func (s *Store[T]) Close() error {
	errParquet := s.parquet.Close()
	errSQL := s.sql.Close()
	return errors.Join(errParquet, errSQL)
}

// Write delegates to the parquet sub-store.
func (s *Store[T]) Write(
	ctx context.Context, records []T,
) ([]WriteResult, error) {
	return s.parquet.Write(ctx, records)
}

// WriteWithKey delegates to the parquet sub-store.
func (s *Store[T]) WriteWithKey(
	ctx context.Context, key string, records []T,
) (*WriteResult, error) {
	return s.parquet.WriteWithKey(ctx, key, records)
}

// Poll delegates to the parquet sub-store (pure S3 LIST; no
// DuckDB involvement).
func (s *Store[T]) Poll(
	ctx context.Context, since Offset, maxEntries int32,
	opts ...QueryOption,
) ([]StreamEntry, Offset, error) {
	return s.parquet.Poll(ctx, since, maxEntries, opts...)
}

// OffsetAt returns the stream offset corresponding to wall-
// clock time t. Pair with WithUntilOffset on Poll/PollRecords
// to read records within a time window.
func (s *Store[T]) OffsetAt(t time.Time) Offset {
	return s.parquet.OffsetAt(t)
}

// Read delegates to the SQL sub-store so dedup semantics and
// the reflection-based row binder match Query / PollRecords.
func (s *Store[T]) Read(
	ctx context.Context, keyPattern string, opts ...QueryOption,
) ([]T, error) {
	return s.sql.Read(ctx, keyPattern, opts...)
}

// Query delegates to the SQL sub-store.
func (s *Store[T]) Query(
	ctx context.Context,
	keyPattern string,
	sqlQuery string,
	opts ...QueryOption,
) (*sql.Rows, error) {
	return s.sql.Query(ctx, keyPattern, sqlQuery, opts...)
}

// QueryRow delegates to the SQL sub-store.
func (s *Store[T]) QueryRow(
	ctx context.Context,
	keyPattern string,
	sqlQuery string,
	opts ...QueryOption,
) *sql.Row {
	return s.sql.QueryRow(ctx, keyPattern, sqlQuery, opts...)
}

// PollRecords delegates to the SQL sub-store so dedup and
// schema-evolution transforms are consistent with Read.
func (s *Store[T]) PollRecords(
	ctx context.Context,
	since Offset,
	maxEntries int32,
	opts ...QueryOption,
) ([]T, Offset, error) {
	return s.sql.PollRecords(ctx, since, maxEntries, opts...)
}

// PollRecordsAll reads every record in [since, until) via
// repeated PollRecords calls. Convenience wrapper for bounded
// windows. Pair with OffsetAt for time-based windows.
func (s *Store[T]) PollRecordsAll(
	ctx context.Context,
	since, until Offset,
	opts ...QueryOption,
) ([]T, error) {
	return s.sql.PollRecordsAll(ctx, since, until, opts...)
}
