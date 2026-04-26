package s3store

import (
	"context"
	"time"

	"github.com/ueisele/s3store/s3parquet"
)

// IndexLookupDef mirrors s3parquet.IndexLookupDef so read-only
// callers can reference it through the umbrella without
// importing the sub-package.
type IndexLookupDef[K comparable] = s3parquet.IndexLookupDef[K]

// IndexDef mirrors s3parquet.IndexDef so umbrella callers wiring
// indexes into Config.Indexes don't have to import the sub-package.
type IndexDef[T any] = s3parquet.IndexDef[T]

// Layout mirrors s3parquet.Layout for umbrella callers
// configuring auto-projection format options on IndexDef.
type Layout = s3parquet.Layout

// IndexReader is the typed read-handle for a secondary index.
// Build via s3parquet.NewIndexReader(store.Target(), def) — the
// umbrella has no own constructor because IndexReader[K] doesn't
// carry T, so adding a forwarder would only re-introduce a stray
// T type parameter on the call site.
type IndexReader[K comparable] = s3parquet.IndexReader[K]

// S3TargetConfig mirrors s3parquet.S3TargetConfig for umbrella
// callers that need to build a Target for a read-only Index or
// a migration-job BackfillIndex.
type S3TargetConfig = s3parquet.S3TargetConfig

// S3Target mirrors s3parquet.S3Target — the constructed live
// handle returned by NewS3Target.
type S3Target = s3parquet.S3Target

// NewS3Target re-exports s3parquet.NewS3Target so umbrella users
// don't have to import the sub-package to build a Target for a
// read-only Index or a migration-job BackfillIndex call.
var NewS3Target = s3parquet.NewS3Target

// BackfillStats mirrors s3parquet.BackfillStats for umbrella
// callers of BackfillIndex.
type BackfillStats = s3parquet.BackfillStats

// BackfillIndex delegates to s3parquet.BackfillIndex.
// See s3parquet.BackfillIndex for the full contract.
func BackfillIndex[T any](
	ctx context.Context,
	target S3Target,
	def IndexDef[T],
	keyPatterns []string,
	until time.Time,
	onMissingData func(dataPath string),
) (BackfillStats, error) {
	return s3parquet.BackfillIndex(
		ctx, target, def, keyPatterns, until, onMissingData)
}
