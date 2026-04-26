package s3store

import "log/slog"

// ConsistencyLevel is the value passed in the Consistency-Control
// HTTP header on S3 requests that require stronger-than-default
// consistency. NetApp StorageGRID defines the header; AWS S3 and
// MinIO ignore it. Declared as a string-alias enum so typos are
// caught at compile time (via the named constants) while leaving
// room for backends that add new levels without a library update
// (ConsistencyLevel("future-level") still compiles and passes
// through verbatim).
//
// Configured on S3TargetConfig.ConsistencyControl, applied
// uniformly to every correctness-critical S3 call routed through
// the target — data PUTs (idempotent and unconditional), ref
// PUTs, index marker PUTs, GETs, HEADs, and every LIST
// (partition LIST, marker LIST, ref-stream LIST in Poll, scoped
// retry-LIST in findExistingRef). Setting it on the target rather
// than per-config struct enforces NetApp's "same consistency for
// paired operations" rule by construction. See the README's
// "StorageGRID consistency" section for the full matrix.
//
// A handful of call sites deliberately do not carry the header
// — the cleanup DELETE path is documented in the README.
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

// warnIfUnknownConsistency logs a warning when c is non-empty
// but doesn't match any named ConsistencyLevel constant. configKind
// names the surrounding config struct ("S3TargetConfig" — the only
// caller today) so the message points at the right construction
// site. Forward-compatible levels (e.g. a StorageGRID release
// adding a new value) still pass through — this is just a typo
// guard.
func warnIfUnknownConsistency(c ConsistencyLevel, configKind string) {
	if c == "" || c.IsKnown() {
		return
	}
	slog.Warn(
		"s3store: ConsistencyControl is not one of the known "+
			"levels (all, strong-global, strong-site, "+
			"read-after-new-write, available) — header will be "+
			"sent verbatim; verify the backend accepts it",
		"configKind", configKind, "level", string(c))
}
