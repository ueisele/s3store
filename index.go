package s3store

import (
	"context"

	"github.com/ueisele/s3store/s3parquet"
)

// IndexLookupDef mirrors s3parquet.IndexLookupDef so read-only
// callers can reference it through the umbrella without
// importing the sub-package.
type IndexLookupDef[K comparable] = s3parquet.IndexLookupDef[K]

// IndexDef mirrors s3parquet.IndexDef so users can reference it
// through the umbrella without importing the sub-package.
type IndexDef[T any, K comparable] = s3parquet.IndexDef[T, K]

// Index is the typed read-handle for a secondary index.
type Index[K comparable] = s3parquet.Index[K]

// S3Target mirrors s3parquet.S3Target for umbrella callers that
// need to construct a read-only Index or run BackfillIndex.
type S3Target = s3parquet.S3Target

// NewS3Target re-exports s3parquet.NewS3Target so umbrella users
// don't have to import the sub-package to build a Target for a
// read-only Index or a migration-job BackfillIndex call.
var NewS3Target = s3parquet.NewS3Target

// BackfillStats mirrors s3parquet.BackfillStats for umbrella
// callers of BackfillIndex.
type BackfillStats = s3parquet.BackfillStats

// NewIndex registers a secondary index on the umbrella store and
// returns a typed query handle. Markers are emitted as part of
// every Write call; Lookup queries via a single S3 LIST.
//
// Must be called before the first Write to capture every record
// — writes that precede registration produce no markers for this
// index. Use BackfillIndex to retroactively cover records
// written before registration.
func NewIndex[T any, K comparable](
	store *Store[T],
	def IndexDef[T, K],
) (*Index[K], error) {
	return s3parquet.NewIndexFromStoreWithRegister(store.parquet, def)
}

// BackfillIndex scans existing parquet data under pattern (with
// LastModified < until) and writes index markers for every record
// already present. See s3parquet.BackfillIndex for the full
// contract.
func BackfillIndex[T any, K comparable](
	ctx context.Context,
	target S3Target,
	def IndexDef[T, K],
	pattern string,
	until Offset,
	onMissingData func(dataPath string),
) (BackfillStats, error) {
	return s3parquet.BackfillIndex(
		ctx, target, def, pattern, until, onMissingData)
}
