package s3store

import "github.com/ueisele/s3store/s3parquet"

// IndexDef mirrors s3parquet.IndexDef so users can reference it
// through the umbrella without importing the sub-package.
type IndexDef[T any, K comparable] = s3parquet.IndexDef[T, K]

// Index is the typed query handle returned by NewIndex.
type Index[T any, K comparable] = s3parquet.Index[T, K]

// BackfillStats mirrors s3parquet.BackfillStats for umbrella
// callers of Index.Backfill.
type BackfillStats = s3parquet.BackfillStats

// NewIndex registers a secondary index on the umbrella store.
// The index lives on the pure-Go s3parquet sub-store (no cgo,
// no DuckDB), and its markers are written as part of every
// Write call. Returns a typed handle whose Lookup method
// queries via a single S3 LIST.
//
// Must be called before the first Write to capture every record
// — writes that precede registration produce no markers for this
// index.
//
// See s3parquet.NewIndex for the full contract and validation
// rules.
func NewIndex[T any, K comparable](
	store *Store[T],
	def IndexDef[T, K],
) (*Index[T, K], error) {
	return s3parquet.NewIndex(store.parquet, def)
}
