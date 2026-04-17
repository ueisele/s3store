// Package core contains pure primitives shared by s3store's
// sub-packages: types, path builders, ref-key codecs, and
// validation. No behavior lives here — just strings, types, and
// validation predicates that every variant of read/write/query
// must agree on.
//
// Nothing in this package uses cgo. It's safe to import from any
// cgo-free package (s3parquet) and from the cgo-requiring one
// (s3sql) alike.
package core

import (
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// RefSeparator splits the fixed-width "{ts}-{id}" header from
// the PathEscape'd Hive key in a ref filename. PathEscape always
// escapes ';' (as %3B), so this character cannot appear in the
// encoded key — the split on it is unambiguous even when the
// original key contains arbitrary bytes.
const RefSeparator = ";"

// Offset represents a position in the stream.
type Offset string

// StreamEntry is a lightweight ref.
type StreamEntry struct {
	Offset   Offset
	Key      string
	DataPath string
}

// WriteResult contains metadata about a completed write.
type WriteResult struct {
	Offset   Offset
	DataPath string
	RefPath  string
}

// BaseConfig holds the fields every sub-package's Config needs.
// Sub-packages flatten these fields rather than embedding this
// struct, so user-construction syntax stays clean. It exists
// mainly as documentation of the shared contract.
type BaseConfig struct {
	Bucket            string
	Prefix            string
	PartitionKeyParts []string
	S3Client          *s3.Client
}
