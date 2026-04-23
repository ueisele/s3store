package s3parquet

// DuplicateWriteDetection selects how the writer determines that
// an idempotent write is a retry — i.e. that the data object at
// the token-derived path already exists. Three strategies,
// matching three common backend capability profiles:
//
//   - DuplicateWriteDetectionByOverwritePrevention — skip probe;
//     always try the conditional PUT (If-None-Match / bucket
//     policy). Use when the backend's capability is known.
//   - DuplicateWriteDetectionByHEAD — pre-flight HEAD on every
//     write. Use when neither If-None-Match nor a denying bucket
//     policy is available.
//   - DuplicateWriteDetectionByProbe — probe the bucket once at
//     first idempotent write: if the bucket rejects a re-PUT, use
//     the overwrite-prevention path; else fall back to HEAD-
//     before-PUT.
//
// Sealed interface — external packages cannot implement it.
// Callers pick a strategy via one of the factory functions.
type DuplicateWriteDetection interface {
	isDuplicateWriteDetection()
}

// duplicateWriteDetection is the unexported concrete type the
// writer switches on. Public factory functions return this wrapped
// in the sealed interface; nothing else implements the interface
// so the switch in the write path is exhaustive by construction.
type duplicateWriteDetection struct {
	kind          detectionKind
	deleteScratch bool
}

func (duplicateWriteDetection) isDuplicateWriteDetection() {}

type detectionKind int

const (
	detectKindProbe detectionKind = iota
	detectKindOverwritePrevention
	detectKindHEAD
)

// DuplicateWriteDetectionByOverwritePrevention asserts that the
// backend rejects PUTs to existing keys — either by honouring
// If-None-Match: * (AWS, recent MinIO) or by an externally-
// configured bucket policy that denies s3:PutOverwriteObject
// (StorageGRID / STACKIT). The writer always sends
// If-None-Match: * and handles both 412 and 403+HEAD-200
// responses as "already exists".
//
// Caveat: if the backend doesn't actually enforce overwrite
// prevention, retries produce duplicates — the guarantee falls
// back to at-least-once. Use only when you know the backend's
// capability. If unsure, let the default
// DuplicateWriteDetectionByProbe auto-detect.
func DuplicateWriteDetectionByOverwritePrevention() DuplicateWriteDetection {
	return duplicateWriteDetection{kind: detectKindOverwritePrevention}
}

// DuplicateWriteDetectionByHEAD issues a pre-flight HEAD on the
// data key before every idempotent PUT. 200 means the object
// exists (retry), 404 means fresh (proceed to PUT). Use when the
// backend has no overwrite-prevention mechanism. Costs one extra
// HEAD per idempotent write on the happy path; still preserves
// the "no body re-upload on retry" property.
func DuplicateWriteDetectionByHEAD() DuplicateWriteDetection {
	return duplicateWriteDetection{kind: detectKindHEAD}
}

// DuplicateWriteDetectionByProbe auto-detects capability at the
// first idempotent write by PUTting a scratch object twice at a
// stable key ({prefix}/_probe/overwrite-prevention) and checking
// whether the second call is rejected. Rejection → the backend
// has overwrite-prevention (via If-None-Match or bucket policy;
// the writer doesn't need to distinguish); acceptance → fall back
// to HEAD-before-PUT mode. Capability is cached per-Writer after
// the first probe.
//
// deleteScratch controls whether the probe object is removed
// after detection. Requires DELETE permission on the probe key.
// When false, leaves one object at the stable path — subsequent
// restarts overwrite the same key, so storage debt is bounded to
// one object. Use false when DELETE is withheld (e.g. STACKIT
// deployments with a read-only service account for the writer).
func DuplicateWriteDetectionByProbe(deleteScratch bool) DuplicateWriteDetection {
	return duplicateWriteDetection{
		kind:          detectKindProbe,
		deleteScratch: deleteScratch,
	}
}

// asConcrete unwraps the sealed interface to the concrete type.
// Only the in-package factory functions return values, so the
// assertion cannot fail in practice; nil is resolved to the
// default (probe, delete scratch).
func asConcrete(d DuplicateWriteDetection) duplicateWriteDetection {
	if d == nil {
		return duplicateWriteDetection{
			kind:          detectKindProbe,
			deleteScratch: true,
		}
	}
	return d.(duplicateWriteDetection)
}
