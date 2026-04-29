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
	// IdempotencyToken, when set, anchors the per-attempt id
	// (id = {token}-{attemptID:32hex UUIDv7}) and the per-token
	// commit marker (`<dataPath>/<partition>/<token>.commit`).
	// The writer's upfront HEAD on that commit marker recognises
	// prior attempts of the same logical write and short-circuits
	// the retry. Validated via validateIdempotencyToken when the
	// option chain is resolved so typos / illegal characters
	// surface before any S3 call.
	//
	// Mutually exclusive with IdempotencyTokenFn — pick a static
	// token OR a per-partition function, not both.
	IdempotencyToken string

	// IdempotencyTokenFn, when set, is invoked per partition with
	// that partition's records and returns the token to use. Lets
	// a multi-partition Write derive a different token per
	// partition — useful when each partition's logical write has
	// its own outbox row / external identifier.
	//
	// Type-erased as `any` because WriteOpts is not generic; the
	// concrete shape is `func(partitionRecords []T) (string, error)`
	// for the writer's T. The writer type-asserts at write-time and
	// surfaces a clear error if the closure's T doesn't match.
	//
	// The returned token is validated via validateIdempotencyToken
	// per partition (non-empty, no "/", no "..", printable ASCII,
	// ≤200 chars). A non-nil error from the closure aborts the
	// partition's write. Set via WithIdempotencyTokenOf.
	//
	// Mutually exclusive with IdempotencyToken.
	IdempotencyTokenFn any

	// InsertedAt, when non-zero, overrides the writer's default
	// pre-encode wall-clock capture as the "insertion time" of the
	// batch. The supplied value drives every downstream surface
	// uniformly: the parquet InsertedAtField column (when configured
	// on the schema), the token-commit `insertedat` user-metadata,
	// and WriteResult.InsertedAt. Truncation to microsecond
	// precision happens at use site so a LookupCommit round-trip
	// (UnixMicro / time.UnixMicro) compares Equal to the original
	// value.
	//
	// The zero value (time.Time{}) means "not supplied" — the writer
	// falls back to time.Now() captured immediately before parquet
	// encode. Set via WithInsertedAt.
	InsertedAt time.Time
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
//
// Mutually exclusive with WithIdempotencyTokenOf — combining the
// two surfaces an error at option-resolution time.
func WithIdempotencyToken(token string) WriteOption {
	return func(o *WriteOpts) {
		o.IdempotencyToken = token
	}
}

// WithIdempotencyTokenOf is the per-partition variant of
// WithIdempotencyToken: fn is invoked once per partition with the
// records routed to that partition, and its return value drives
// that partition's idempotency token. Lets a single multi-
// partition Write retry with a different token per partition
// (e.g. one outbox row per partition).
//
// Semantics on each partition match WithIdempotencyToken: the
// returned token anchors the per-attempt id and the
// `<dataPath>/<partition>/<token>.commit` marker, the upfront HEAD
// dedups same-token retries, and the token must pass
// validateIdempotencyToken.
//
// On WriteWithKey (single partition) fn is invoked once with the
// full records slice — symmetric with the multi-partition case.
//
// fn returning an error fails that partition's write; under
// Write's parallel fan-out a single failure propagates as
// "first error wins" with partial-success on already-committed
// partitions (same shape as any other write-path error).
//
// Mutually exclusive with WithIdempotencyToken — passing both
// surfaces an error at option-resolution time. fn=nil is a no-op
// (the partition runs the non-idempotent auto-token path).
func WithIdempotencyTokenOf[T any](
	fn func(partitionRecords []T) (string, error),
) WriteOption {
	return func(o *WriteOpts) {
		if fn == nil {
			return
		}
		o.IdempotencyTokenFn = fn
	}
}

// WithInsertedAt overrides the writer's default "insertion time"
// for this batch. The supplied value flows uniformly to every
// downstream surface — the parquet InsertedAtField column (when
// configured), the token-commit `insertedat` user-metadata, and
// WriteResult.InsertedAt — replacing the default capture of
// time.Now() taken just before parquet encoding.
//
// Use cases:
//
//   - Caller-owned event time. The "real" insertion timestamp
//     lives in an outbox row / external system; passing it here
//     keeps the in-file column and result aligned with that
//     source rather than the writer's wall-clock.
//   - Reproducible writes. Tests and replays that supply the same
//     records + same InsertedAt + same compression codec produce
//     byte-identical parquet bytes (the only non-determinism in
//     the encode path is the InsertedAtField injection).
//
// Truncated to microsecond precision at use site so a LookupCommit
// round-trip yields a time.Time that compares Equal to the value
// in the original WriteResult.
//
// The zero value (time.Time{}) is treated as "not supplied" — the
// writer falls back to time.Now(). Callers who legitimately need
// to stamp the year-1 zero time should pass a value with a
// non-zero monotonic component, but no real workload needs this.
//
// Idempotency-retry interaction: when the upfront HEAD on
// `<token>.commit` finds a prior commit, the returned WriteResult
// is reconstructed from the prior commit's metadata —
// WithInsertedAt on the retry attempt is ignored in favour of the
// original attempt's value. This preserves the contract that a
// same-token retry returns the original WriteResult unchanged.
func WithInsertedAt(t time.Time) WriteOption {
	return func(o *WriteOpts) {
		o.InsertedAt = t
	}
}
