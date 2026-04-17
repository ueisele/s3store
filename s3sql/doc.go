// Package s3sql provides SQL-based access to an s3store using
// embedded DuckDB as the query engine. Requires cgo.
//
// Capability sketch:
//
//   - Read: typed records matching a key pattern, with full
//     schema-evolution transforms (ColumnAliases,
//     ColumnDefaults) applied before deduplication.
//   - Query, QueryRow: arbitrary SQL over the parquet files
//     scoped by a key pattern. Use for aggregations, complex
//     filtering, or joins across multiple stores.
//   - Poll, PollRecords: stream refs or typed records after an
//     offset; PollRecords goes through DuckDB so dedup and
//     transforms are consistent with Read.
//
// For cgo-free access to the same store (no SQL, limited glob,
// in-memory dedup), use github.com/ueisele/s3store/s3parquet.
//
// S3 endpoint, region, and URL style are auto-derived from the
// S3Client.Options() — DuckDB's httpfs extension is configured
// once at New() time. Override via ExtraInitSQL if needed
// (e.g. CREATE SECRET for credential rotation).
package s3sql
