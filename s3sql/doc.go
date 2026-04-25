// Package s3sql provides SQL query access to an s3store dataset
// using embedded DuckDB as the query engine. Requires cgo.
//
// The package is read-only and intentionally narrow: it exposes
// just Query and QueryMany, both returning *sql.Rows. Typed-row
// iteration, change-stream tailing, and pure-Go reads live on
// github.com/ueisele/s3store/s3parquet so this package stays a
// thin SQL surface over the parquet files the Writer produced.
//
// Construct via NewReader, passing the same s3parquet.S3Target
// the Writer was built with so the two halves can't drift on
// Bucket / Prefix / SettleWindow / DisableRefStream. The dedup
// CTE references VersionColumn + EntityKeyColumns by name; the
// generic parameter T is only inspected to guard against a
// `parquet:"filename"` field that would collide with DuckDB's
// read_parquet(filename=true) helper.
//
// Capability sketch:
//
//   - Query: arbitrary SQL over the parquet files scoped by a
//     key pattern. Use for aggregations, complex filtering, or
//     joins across multiple stores.
//   - QueryMany: single SQL query over the deduplicated union
//     of files matching every pattern. Aggregations and ORDER
//     BY apply across the full set.
//   - ScanAll[T]: optional helper that materializes a *sql.Rows
//     into []T by parquet tag, NULL-safe, with composite
//     (LIST/STRUCT/MAP) decode via mapstructure. Use when typed
//     records are convenient; skip for raw aggregation pipelines
//     that prefer to drive *sql.Rows themselves.
//
// For typed reads, change-stream polling, and a cgo-free option,
// use github.com/ueisele/s3store/s3parquet.
//
// S3 endpoint, region, and URL style are auto-derived from the
// S3Client.Options() — DuckDB's httpfs extension is configured
// once at NewReader() time. Override via ExtraInitSQL if needed
// (e.g. CREATE SECRET for credential rotation).
package s3sql
