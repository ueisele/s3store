package s3store

import "time"

// WriteOption configures write-path behavior. Mirrors the
// QueryOption pattern so the write side has the same mental model:
// one option type, one accumulator, one place to add new knobs.
type WriteOption func(*WriteOpts)

// WriteOpts is the resolved set of write-path options after
// applying a chain of WriteOption values. Exported so each
// sub-package can build its own option handling without a second
// layer of indirection.
type WriteOpts struct {
	// IdempotencyToken, when set, replaces the library-default
	// {tsMicros}-{shortID} id inside the data filename. Retries
	// of the same logical write produce deterministic data paths
	// so overwrite-prevention fires and no parquet body is
	// re-uploaded. Validated via validateIdempotencyToken at the
	// option-application site, not at PUT time, so typos / illegal
	// characters surface immediately.
	IdempotencyToken string

	// MaxRetryAge bounds the scoped LIST issued on the retry path.
	// When a retry is detected (overwrite-prevention fired), the
	// writer scans ref objects published in [now - MaxRetryAge, now]
	// for an entry whose id matches IdempotencyToken — found means
	// a prior attempt already published the ref, skip the write.
	//
	// Required (> 0) when IdempotencyToken is set. Ignored
	// otherwise.
	MaxRetryAge time.Duration
}

// Apply runs every option against the receiver.
func (o *WriteOpts) Apply(opts ...WriteOption) {
	for _, opt := range opts {
		opt(o)
	}
}

// WithIdempotencyToken marks a write as a retry-safe logical unit
// identified by token. On retry with the same token:
//
//   - The data filename is deterministic (token replaces the
//     default {tsMicros}-{shortID} id), so overwrite-prevention
//     on the backend rejects the PUT and the parquet body is
//     not re-uploaded.
//   - Markers and refs are deduplicated best-effort via a scoped
//     LIST bounded by maxRetryAge. A found ref skips the ref PUT;
//     not found (scenario B: data landed but ref didn't) writes
//     the ref to complete the interrupted attempt.
//
// Tokens are unique per (partition key, logical write), not
// globally — the same token may be reused across different
// partition keys without colliding. Orchestrators that batch one
// job-id across many partitions can reuse the job-id verbatim;
// each partition's retry-dedup runs independently. Within one
// partition the token must remain unique per logical write.
//
// token must pass validateIdempotencyToken (non-empty, no "/",
// no "..", printable ASCII, <= 200 chars) — validation runs at
// resolve time so bad tokens fail at the call site rather than
// inside the write path.
//
// maxRetryAge bounds how far back the scoped LIST scans on the
// retry path. Required (> 0); pick based on your retry SLA:
//
//   - 1h for fast-retry streaming workloads.
//   - 6h for same-day recovery.
//   - 24h for cross-day orchestrator recovery.
//
// No library default is imposed — cost is workload-dependent and
// there is no universally-right value.
//
// Returns a no-op option if token is empty (convenience for
// callers whose token might be unset in some code paths) — the
// resulting write runs the non-idempotent path.
func WithIdempotencyToken(
	token string, maxRetryAge time.Duration,
) WriteOption {
	return func(o *WriteOpts) {
		o.IdempotencyToken = token
		o.MaxRetryAge = maxRetryAge
	}
}
