package s3store

// WriteOption configures write-path behavior. Mirrors the
// QueryOption pattern so the write side has the same mental model:
// one option type, one accumulator, one place to add new knobs.
type WriteOption func(*WriteOpts)

// WriteOpts is the resolved set of write-path options after
// applying a chain of WriteOption values. Exported so each
// sub-package can build its own option handling without a second
// layer of indirection.
type WriteOpts struct {
	// IdempotencyToken, when set, anchors the per-attempt id
	// (id = {token}-{attemptID:32hex UUIDv7}) and the per-token
	// commit marker (`<dataPath>/<partition>/<token>.commit`).
	// The writer's upfront HEAD on that commit marker recognises
	// prior attempts of the same logical write and short-circuits
	// the retry. Validated via validateIdempotencyToken at the
	// option-application site, not at PUT time, so typos / illegal
	// characters surface immediately.
	IdempotencyToken string
}

// Apply runs every option against the receiver.
func (o *WriteOpts) Apply(opts ...WriteOption) {
	for _, opt := range opts {
		opt(o)
	}
}

// WithIdempotencyToken marks a write as a retry-safe logical
// unit identified by token. Recovery on retry is automatic and
// works across arbitrarily long S3 outages:
//
//   - Each attempt writes data and ref to fresh per-attempt
//     paths (id = {token}-{attemptID:32hex UUIDv7}). No data
//     or ref PUT in the write path ever overwrites — sidesteps
//     multi-site StorageGRID's eventual-consistency exposure on
//     overwrites.
//   - On retry, the writer issues an upfront HEAD on
//     `<dataPath>/<partition>/<token>.commit`. If a prior
//     attempt's commit is in place, the writer reconstructs the
//     original WriteResult from the marker's metadata (no body
//     re-upload, same DataPath / RefPath / InsertedAt as the
//     prior successful attempt) and returns.
//   - If the commit is missing (no prior attempt landed, or
//     every prior attempt crashed before the commit PUT), the
//     retry proceeds with a fresh attempt-id end-to-end.
//
// Tokens are unique per (partition key, logical write), not
// globally — the same token may be reused across different
// partition keys without colliding. The upfront HEAD runs
// per-partition. Within one partition the token must remain
// unique per logical write.
//
// **Reader-side dedup recommended.** Near-concurrent retry
// overlap (out of contract per the README's Concurrency
// contract, but bounded if it arises by accident) can produce
// two committed attempts whose records share (entity, version).
// Records are bit-identical (parquet encoding is deterministic;
// the writer captures InsertedAt once and the same value drives
// every column / filename / metadata field). Configure
// `EntityKeyOf` + `VersionOf` on the reader to collapse the
// duplicate records on read.
//
// token must pass validateIdempotencyToken (non-empty, no "/",
// no "..", printable ASCII, <= 200 chars) — validation runs at
// resolve time so bad tokens fail at the call site rather than
// inside the write path.
//
// Returns a no-op option if token is empty (convenience for
// callers whose token might be unset in some code paths) — the
// resulting write runs the non-idempotent path (auto-token, no
// upfront HEAD).
func WithIdempotencyToken(token string) WriteOption {
	return func(o *WriteOpts) {
		o.IdempotencyToken = token
	}
}
