// Package s3store provides append-only, versioned data storage
// on S3 with Parquet data files, a change stream, and embedded
// DuckDB queries.
//
// No server, no broker, no coordinator. S3 is the only
// dependency.
//
// This root package is a thin umbrella that composes two
// sub-packages:
//
//   - s3store/s3parquet — pure Go, cgo-free. Write,
//     WriteWithKey, Read, Poll, PollRecords via parquet-go
//     directly. In-memory latest-per-entity dedup.
//   - s3store/s3sql — requires cgo (embedded DuckDB). Read,
//     Query, QueryRow, PollRecords with arbitrary SQL and
//     streaming dedup. Typed records are produced by a
//     reflection-based row binder driven off T's parquet tags.
//
// Importing this umbrella pulls in DuckDB (cgo). If you only
// need the cgo-free subset (writes, or writes plus simple
// typed reads), import s3store/s3parquet directly. Both
// sub-packages share the S3 layout and ref-stream wire format,
// so the same data is accessible through either.
//
// Access patterns:
//
//   - Write / WriteWithKey: append Parquet + stream ref
//   - Poll: stream of refs (which keys changed)
//   - PollRecords: typed records for the refs Poll would
//     return. Through the umbrella, records go through DuckDB
//     so dedup matches Read.
//   - Read: typed, deduplicated snapshot with glob support
//   - Query / QueryRow: DuckDB SQL with auto-dedup
//
// Every read API defaults to latest-per-key deduplication and
// accepts WithHistory() to opt out.
package s3store
