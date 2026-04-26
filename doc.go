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
//   - s3store/s3parquet — pure Go, cgo-free. Writer (Write,
//     WriteWithKey, ...) plus Reader (Read, ReadIter, Poll,
//     PollRecords, OffsetAt). In-memory per-partition dedup.
//   - s3store/s3sql — requires cgo (embedded DuckDB). SQL-only
//     surface: Query returns *sql.Rows; the caller binds rows
//     themselves.
//
// Importing this umbrella pulls in DuckDB (cgo). If you only
// need the cgo-free subset (writes, typed reads, change-stream
// tailing), import s3store/s3parquet directly. Both sub-packages
// share the S3 layout and ref-stream wire format, so the same
// data is accessible through either.
//
// Access patterns:
//
//   - Write / WriteWithKey: append Parquet + stream ref
//   - Poll: stream of refs (which keys changed)
//   - PollRecords: typed records for the refs Poll would
//     return.
//   - Read / ReadIter: typed, deduplicated snapshot with glob
//     support.
//   - Query: DuckDB SQL with auto-dedup CTE.
//
// Every read API defaults to latest-per-key deduplication and
// accepts WithHistory() to opt out.
package s3store
