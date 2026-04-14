// Package s3store provides append-only, versioned data storage on
// S3 with Parquet data files, a change stream, and embedded DuckDB
// queries.
//
// No server, no broker, no coordinator. S3 is the only dependency.
//
// Write uses parquet-go for encoding. All reads use DuckDB,
// providing consistent schema evolution (union_by_name,
// ColumnAliases, ColumnDefaults) across all access patterns.
//
// Access patterns:
//
//   - Write / WriteWithKey: append Parquet + stream ref
//   - Poll: stream of refs (which keys changed)
//   - PollRecords: flat stream of typed records
//   - Read: typed, deduplicated snapshot with glob support
//   - Query: DuckDB SQL with auto-dedup and schema evolution
//
// Poll = lightweight refs. PollRecords/Read = typed []T via
// ScanFunc. Query = *sql.Rows for aggregations and complex SQL.
package s3store
