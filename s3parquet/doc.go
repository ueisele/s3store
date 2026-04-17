// Package s3parquet provides pure-Go, cgo-free access to an
// s3store: typed writes, typed batch reads, and a ref stream.
// All reads and writes go through parquet-go directly — no SQL
// engine, no DuckDB, no cgo.
//
// Capability sketch:
//
//   - Write, WriteWithKey: encode []T as Parquet, PUT to S3,
//     write an empty ref file. Pure Go.
//   - Read: list Parquet files matching a key pattern, decode
//     into []T, optionally dedup latest-per-entity in memory.
//   - Poll, PollRecords: stream ref entries or records from
//     the ref stream, starting after a caller-supplied offset.
//
// For SQL queries, arbitrary aggregations, and richer
// schema-evolution transforms (ColumnAliases / ColumnDefaults),
// use github.com/ueisele/s3store/s3sql instead, which requires
// cgo (embedded DuckDB).
//
// Limitations vs. s3sql:
//
//   - Glob patterns are restricted to the shared grammar:
//     whole-segment "*" and a single trailing "*" inside a
//     value (e.g. "period=2026-03-*"). See
//     internal/core.ValidateKeyPattern for the full spec.
//   - Deduplication is in-memory; large key cardinalities can
//     exceed available RAM. Route those workloads to s3sql.
//   - No ColumnAliases / ColumnDefaults (those are SQL-level
//     rewrites).
package s3parquet
