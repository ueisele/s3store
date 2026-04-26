package s3parquet

import "fmt"

// DataPath returns the prefix under which data parquet files are
// stored, relative to the store's top-level Prefix. Exported so
// s3sql callers can compute the same prefix without re-deriving
// the layout convention.
func DataPath(prefix string) string {
	return prefix + "/data"
}

// refPath returns the prefix under which stream-ref files are
// stored, relative to the store's top-level Prefix.
func refPath(prefix string) string {
	return prefix + "/_stream/refs"
}

// buildDataFilePath returns the S3 object key for a data file.
// The filename is `{id}.parquet`. id is opaque to this helper —
// the writer generates it as either `{tsMicros}-{shortID}` (the
// library's default, lex-sortable by time within a partition)
// or the caller's idempotency token verbatim.
//
// Format: `{dataPath}/{hiveKey}/{id}.parquet`.
func buildDataFilePath(dataPath, hiveKey, id string) string {
	return fmt.Sprintf("%s/%s/%s.parquet", dataPath, hiveKey, id)
}

// makeAutoID returns the library's default {tsMicros}-{shortID}
// id used for non-idempotent writes. tsMicros is the writer's
// wall-clock at write-start (so the filename remains lex-
// sortable by time within a partition); shortID is an 8-char
// random fragment so concurrent writes within the same
// microsecond don't collide.
func makeAutoID(tsMicros int64, shortID string) string {
	return fmt.Sprintf("%d-%s", tsMicros, shortID)
}
