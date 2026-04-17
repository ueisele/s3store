// Package s3sql provides SQL-based access to an s3store using
// embedded DuckDB as the query engine. Requires cgo.
//
// The target record type T drives both the SQL result shape and
// the per-row binding: s3sql reflects over T's parquet struct
// tags at New() time and builds a NULL-safe row binder that
// populates T directly from the result set. Columns absent from
// the parquet file land as the field's Go zero value; user
// types implementing sql.Scanner are supported (e.g.
// shopspring/decimal.Decimal).
//
// Capability sketch:
//
//   - Read: typed records matching a key pattern, deduplicated
//     to latest-per-entity by VersionColumn.
//   - Query, QueryRow: arbitrary SQL over the parquet files
//     scoped by a key pattern. Use for aggregations, complex
//     filtering, or joins across multiple stores.
//   - Poll, PollRecords: stream refs or typed records after an
//     offset; PollRecords goes through DuckDB so dedup is
//     consistent with Read.
//
// For cgo-free access to the same store (no SQL, limited glob,
// in-memory dedup), use github.com/ueisele/s3store/s3parquet.
//
// S3 endpoint, region, and URL style are auto-derived from the
// S3Client.Options() — DuckDB's httpfs extension is configured
// once at New() time. Override via ExtraInitSQL if needed
// (e.g. CREATE SECRET for credential rotation).
package s3sql
