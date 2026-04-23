package s3parquet

// ConsistencyLevel is the value passed in the Consistency-Control
// HTTP header on S3 requests that require stronger-than-default
// consistency. NetApp StorageGRID defines the header; AWS S3 and
// MinIO ignore it. Declared as a string-alias enum so typos are
// caught at compile time (via the named constants) while leaving
// room for backends that add new levels without a library update
// (ConsistencyLevel("future-level") still compiles and passes
// through verbatim).
//
// The header only needs to be set on operations where the
// library's correctness depends on strong read-after-write /
// list-after-write visibility — today that's:
//
//   - the writer's data PUT when idempotency is on (so
//     overwrite-prevention fires across nodes);
//   - the writer's scoped LIST on the retry path (so the ref
//     dedup scan sees prior refs);
//   - the reader's data-file GET following a LIST (so the GET
//     pairs with the writer's PUT per NetApp's "same consistency
//     for both" rule).
//
// Marker PUTs, ref PUTs, and the Poll/partition LISTs stay at
// the bucket default — they're uniquely keyed or absorb
// propagation skew via SettleWindow.
type ConsistencyLevel string

const (
	// ConsistencyDefault is the zero value: no header sent. The
	// bucket's default consistency applies. Correct on AWS S3 and
	// MinIO (strongly consistent out of the box); the per-op
	// opt-in for StorageGRID is one of the stronger levels below.
	ConsistencyDefault ConsistencyLevel = ""

	// ConsistencyAll requires every storage node to acknowledge.
	// Highest durability / visibility guarantee; highest latency.
	ConsistencyAll ConsistencyLevel = "all"

	// ConsistencyStrongGlobal ensures read-after-write and list-
	// after-write consistency across every site. Safe multi-site
	// choice for idempotency correctness.
	ConsistencyStrongGlobal ConsistencyLevel = "strong-global"

	// ConsistencyStrongSite ensures read-after-write and list-
	// after-write consistency within a single site. Cheaper than
	// ConsistencyStrongGlobal when the deployment is single-site
	// or all readers/writers co-locate.
	ConsistencyStrongSite ConsistencyLevel = "strong-site"

	// ConsistencyReadAfterNewWrite is StorageGRID's default: GET
	// of a newly-PUT object is strongly consistent, but LIST and
	// overwrite-PUT visibility is eventual. Insufficient for
	// Phase 3's scoped-LIST ref dedup — pick a stronger level if
	// idempotency guarantees matter on StorageGRID.
	ConsistencyReadAfterNewWrite ConsistencyLevel = "read-after-new-write"

	// ConsistencyAvailable is the weakest level: any replica may
	// answer. Use only when availability trumps correctness.
	ConsistencyAvailable ConsistencyLevel = "available"
)

// IsKnown reports whether c is one of the named constants. Used
// at NewWriter / NewReader to log a warning on typos while
// letting forward-compatible values (newly-added StorageGRID
// levels) still pass through.
func (c ConsistencyLevel) IsKnown() bool {
	switch c {
	case ConsistencyDefault,
		ConsistencyAll,
		ConsistencyStrongGlobal,
		ConsistencyStrongSite,
		ConsistencyReadAfterNewWrite,
		ConsistencyAvailable:
		return true
	}
	return false
}
