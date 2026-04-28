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
	// IdempotencyToken, when set, prefixes the per-attempt id
	// (id = {token}-{tsMicros}-{shortID}) so the writer's
	// upfront-LIST dedup gate under {partition}/{token}- can
	// recognize prior attempts of the same logical write.
	// Validated via validateIdempotencyToken at the
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
//   - Each attempt writes to a fresh per-attempt path (id =
//     {token}-{tsMicros}-{shortID}). No PUT in the write path
//     ever overwrites — sidesteps multi-site StorageGRID's
//     eventual-consistency exposure on overwrites.
//   - On retry, the writer's upfront LIST under
//     {partition}/{token}- pairs .parquet siblings with their
//     .commit siblings by id and returns the first valid commit
//     unchanged (no body re-upload, same DataPath / RefPath /
//     InsertedAt as the prior successful attempt).
//   - If no valid commit exists (no prior attempt landed, or
//     every prior attempt's commit failed the timeliness check),
//     the retry proceeds with a fresh attempt-id and a fresh
//     server-stamped data.LM.
//
// Tokens are unique per (partition key, logical write), not
// globally — the same token may be reused across different
// partition keys without colliding. Each partition's
// upfront-LIST dedup runs independently. Within one partition
// the token must remain unique per logical write.
//
// **Reader-side dedup recommended.** Near-concurrent retry
// overlap can produce two valid commits for the same token
// (slow original + fast retry both succeed). Both contain
// bit-identical records (parquet encoding is deterministic;
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
// resulting write runs the non-idempotent path (auto-id, no
// upfront LIST).
func WithIdempotencyToken(token string) WriteOption {
	return func(o *WriteOpts) {
		o.IdempotencyToken = token
	}
}
