// Package s3store provides append-only, versioned data storage
// on S3 with Parquet data files and a change stream. Pure Go,
// cgo-free. No server, no broker, no coordinator — S3 is the
// only dependency.
//
// Capability sketch:
//
//   - Write, WriteWithKey: encode []T as Parquet, PUT to S3,
//     write an empty stream-ref file.
//   - Read, ReadIter, ReadRangeIter: list Parquet files matching
//     a key pattern, decode into []T, optionally dedup latest-
//     per-entity in memory.
//   - Poll, PollRecords: stream ref entries or records from
//     the ref stream, starting after a caller-supplied offset.
//
// Every snapshot read defaults to latest-per-key deduplication
// (configured via EntityKeyOf + VersionOf) and accepts
// WithHistory() to opt out.
//
// Glob patterns are restricted to a deliberately narrow grammar:
// whole-segment "*", a single trailing "*" inside a value (e.g.
// "period=2026-03-*"), and half-open "FROM..TO" ranges. See
// validateKeyPattern for the full spec.
//
// Deduplication is in-memory per partition; large key
// cardinalities can exceed available RAM.
package s3store
