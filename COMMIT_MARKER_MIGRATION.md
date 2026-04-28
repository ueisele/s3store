# Atomic per-file visibility — migration plan

## Status

**Shipped.** Phases 1–3 of the redesign are on `main`:

- **Phase 1** (commit `97a661f`) — write-path redesign: token
  resolution + UUIDv7 attempt-id + per-attempt data and ref
  paths + `<token>.commit` PUT as the atomic-visibility flip
  + writer-local `CommitTimeout` enforcement.
  `ConsistencyControl` defaults to `ConsistencyStrongGlobal`.
- **Phase 2** (commit `dbf413b`) — read-path commit gating:
  snapshot reads (`Read` / `ReadIter` / `BackfillProjection`)
  drop parquets without a paired `<token>.commit` (skip-HEAD-
  when-unambiguous, fan-out HEAD for multi-attempt tokens);
  stream reads (`Poll` / `PollRecords` / `ReadRangeIter`) HEAD
  `<token>.commit` per ref with a per-poll
  `(partition, token)` cache. Public
  `Writer.LookupCommit(ctx, partition, token)` API for direct
  commit existence checks. Observability counters
  `s3store.read.commit_head` /
  `s3store.read.commit_head_cache_hit` for HEAD volume tracking.
- **Phase 3** (this commit) — documentation sweep: README's
  "Concurrency contract" disclaimer, "Atomic per-file
  visibility" guarantee, refreshed S3-layout example,
  `CommitTimeout > 0s` floor (was 1 s); CLAUDE.md "Backend
  assumptions" replaced with the writer-clock + concurrent-
  same-token-out-of-contract + strong-global-required-on-multi-
  site-StorageGRID statement; STORAGEGRID.md appendix's
  `LM ≈ first-observable-time` subsection rewritten as the
  history of why the assumption was dropped.

The earlier Phase 3/4 design (per-attempt commit marker with
LM-stamped timeliness check) is preserved as **Appendix A:
Superseded — earlier design** at the bottom of this file for
historical context.

## Phase 4 — Error-message convention cleanup

Surfaced during Phase 2 review and tracked here so it doesn't
get lost. Independent of the redesign.

The package's error chain currently produces strings of the
shape `"s3store: outer: s3store: inner"` because both leaf
errors and wrap layers prefix the package name. Pick one
convention across the package:

- **(a) Leaves keep `s3store:`, wrap layers drop it.** New
  wrap shape: `fmt.Errorf("head token-commit: %w", err)`.
  Matches the existing `"WithIdempotentRead: %w"` site
  (option-validation wrap in `idempotency.go`) — that one
  already follows the convention, so picking this direction
  means most leaves are already correct and most wrap sites
  need a touch.
- **(b) Wrap layers keep `s3store:`, leaves drop it.** New
  leaf shape: `errors.New("IdempotencyToken must not be
  empty")`. Touches every leaf — `validateIdempotencyToken`,
  `headTokenCommit`, `readTokenCommitMeta`,
  `loadDurationConfig`, `validatePartitionKeyParts`, every
  `validateProjectionDef*`, etc. Larger blast radius.

(a) is cheaper (most leaves are already in shape) and keeps
the package name visible at the deepest point of the chain
(where it identifies the source most usefully). Recommend
(a). ~30–50 sites in source + a similar count in test
assertions. Mechanical edit; no semantics change. Verify with
all four gates.

---

## Appendix A: Superseded — earlier design

Preserved here so the redesign rationale stays in-tree.

### Earlier design (Phases 3–4 as previously shipped on `main`)

- **Per-attempt commit marker.** Every data PUT was followed
  by a sibling `<id>.commit` PUT carrying `dataLM` user
  metadata. Snapshot reads paired `.parquet` with `.commit`
  via partition LIST; stream reads HEADed the marker per
  ref. Both paths applied the timeliness check
  `marker.LM - data.LM < CommitTimeout`.
- **Server-stamped `dataLM` in ref filename.** Refs encoded
  as `<dataLM>-<tsMicros>-<shortID>-<token>;<hive>.ref`. The
  reader's `refCutoff = now - SettleWindow` was therefore a
  reader↔server comparison only; writer wall-clock was not
  in the protocol.
- **Upfront-LIST dedup gate.** A retry under a known
  idempotency token did `LIST <partition>/<token>-`, paired
  `.parquet` and `.commit` siblings, and returned a
  reconstructed `WriteResult` for any pair that passed
  the timeliness check.
- **Post-PUT HEADs (data + marker).** Writer HEADed the data
  file to capture `dataLM` (used in ref filename and marker
  metadata) and HEADed the marker to verify the timeliness
  check before returning success.
- **`CommitTimeoutFloor = 1 s`.** Forced because HTTP-date
  `Last-Modified` is second-precision; sub-second timeliness
  windows would suffer phantom failures.
- **`MaxClockSkew` bounded server↔reader only.**

### Why it didn't survive

The whole timing model rested on `LastModified ≈ first-observable-time`.
On the StorageGRID test cluster (and inferred across S3-compliant
backends in general), `LastModified` is stamped at *request
receipt time*, not at observability. A 100 MB upload taking 43 s
on the wire still gets `LastModified` set at the receive instant.

Implication: the timeliness check
`marker.LM - data.LM < CommitTimeout` cannot detect a phantom
marker write — by the time `marker.LM` would diverge meaningfully
from observability, the server has already accepted the PUT
under its receipt timestamp. The check passes (or fails) for
reasons unrelated to its stated purpose.

Rather than weaken the documented guarantee silently or impose
ever-larger `CommitTimeout` values to absorb upload-duration
variance, the redesign dropped the check and the assumption.
The current design solves the original "atomic visibility"
problem (crash before commit → invisible to both read paths;
crash after → visible to both) using a presence-only
`<token>.commit` marker with no timing dependency on backend
`LastModified`.
