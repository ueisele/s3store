# Atomic per-file visibility — migration plan

## Status

The earlier design (Phases 3–4 already shipped on `main`) added a
per-attempt `.commit` sibling and a server-time timeliness check
(`marker.LM - data.LM < CommitTimeout`) on top of a multi-PUT
write sequence. That design rested on the assumption that
`LastModified ≈ first-observable-time` on every supported
backend.

**Empirical finding (StorageGRID experiment, 2026-04-28):** the
backend stamps `LastModified` at *request receipt time*, not
upload-completion time. A 100 MB upload that takes 43 s on the
wire still gets `LastModified` set ≈ when the PUT first arrived
at the server. This invalidates the assumption — the timeliness
check cannot detect phantom-write scenarios it was designed for,
because by the time `marker.LM` would diverge from
"observability," the server has already accepted the PUT under
its receipt time.

**Pivot.** Rather than weaken the guarantee silently, drop the
timeliness check and the foundational `LM ≈ first-observable-time`
assumption entirely. Replace with a redesign that does not depend
on backend `LastModified` semantics at all. This document is the
plan for that redesign.

The earlier design is preserved as **Appendix A: Superseded —
earlier design** at the bottom of this file, including the LM
saga and why each piece doesn't survive.

---

## Contract changes (user-visible)

These ride with the redesign and need to land in the README's
opening section before any code change is shipped.

### 1. Single in-flight write per token

> Concurrent writes that share the same `WithIdempotencyToken`
> are out of contract. The library guarantees correctness only
> when at most one write is in flight per `(partition, token)`
> at any given moment. Sequential retries (a failed write
> followed by a retry of the same token) remain fully
> supported — that is the design's primary use case.

Was implicit before; now explicit. Buys back enough determinism
that token-commit overwrites can be tolerated under multi-site
eventual consistency without breaking reader semantics
(deterministic encoding makes any two attempts of the same token
byte-equivalent).

### 2. `ConsistencyControl` defaults to `strong-global`

Was `ConsistencyDefault` (empty header → bucket default). On
StorageGRID the bucket default is `read-after-new-write`, which
is *insufficient* for this redesign on multi-site deployments
(token-commit overwrites are eventual otherwise). Single-site
deployments downgrade explicitly via
`ConsistencyControl: ConsistencyStrongSite` if cost matters.

AWS S3 and MinIO ignore the header; no behavioural change there.

### 3. `CommitTimeout` floor: drop the 1 s minimum, but require strictly positive

The 1 s floor existed because HTTP-date `Last-Modified` is
second-precision. With LM out of the protocol, microsecond
precision is honest again — the floor on minimum *units* drops
to zero. `CommitTimeout` itself, however, must be strictly
positive: zero is not "unlimited," and writer elapsed wall-clock
between write-start and token-commit completion is always
strictly positive, so a zero CommitTimeout would cause every
write to surface the new "write committed after CommitTimeout"
error. Operators typically pick something on the order of
seconds (5–30s, tuned to expected upload duration). Negatives
and zero are rejected at `NewS3Target`.

`CommitTimeout` is now contract-enforcing: the writer compares
its end-to-end wall-clock against this value and **returns an
error** if exceeded. The token-commit still lands (data is
durable; snapshot reads see the commit), so a same-token retry
via the upfront-HEAD recovers transparently. Stream readers
whose SettleWindow already moved past the ref's `refMicroTs`
may miss the write — the error is the writer's signal to the
caller that this risk exists.

### 4. `WithIdempotencyToken` retry recovery is unchanged in shape

Same behaviour from the caller's view: a same-token retry
returns the prior `WriteResult`. Implementation shifts from
upfront-LIST to upfront-HEAD on `<token>.commit`. No API
change.

---

## Final design

### Path layout

```
<dataPath>/<partition>/<token>-<UUIDv7>.parquet     — data files
<dataPath>/<partition>/<token>.commit               — commit marker
<refPath>/<refMicroTs>-<token>-<UUIDv7>;<hive>.ref  — refs
```

For non-idempotent writes (no `WithIdempotencyToken`), the writer
generates a fresh UUIDv7 and uses it as **both** `token` and the
attempt-id. Path shape is therefore uniform — always
`<token>-<UUIDv7>` — and parsing has one case.

`UUIDv7` is the canonical 32-hex form (internal dashes stripped
via `strings.ReplaceAll`) for visual cleanliness; we never
round-trip through `uuid.Parse`. `refMicroTs` is the writer's
microsecond wall-clock captured immediately before the ref PUT.
`<hive>` is `url.PathEscape(hiveKey)`, which always escapes `;`
so the separator is unambiguous.

### Write sequence (3 PUTs, 0 HEADs in the happy path)

Inputs: `partition` (Hive key), `records`, optional
`WithIdempotencyToken(token)`.

1. **Token resolution.** If user provided a token, validate it
   (existing rules: non-empty, ≤200 chars, no `/` `;` `..`,
   printable ASCII). If not, generate a fresh UUIDv7 and use
   it as the token.

2. **Upfront commit check (idempotent path only).** When the
   token is user-provided, `HEAD <dataPath>/<partition>/<token>.commit`.
   - 200 → existing commit. Read user-metadata `attempt-id` →
     reconstruct `WriteResult` (DataPath = `<partition>/<token>-<attemptID>.parquet`,
     RefPath = recomputed ref key, InsertedAt = UUIDv7's
     embedded ms time). Return success.
   - 404 → no prior commit; proceed.
   Skipped entirely on auto-token (UUIDv7 just generated, HEAD
   would be 404 by construction).

3. **Generate attempt-id.** Fresh UUIDv7. Compose data file
   key: `<dataPath>/<partition>/<token>-<UUIDv7>.parquet`.

4. **Encode parquet** (deterministic — same records + codec
   produce byte-identical bytes; this is the invariant that
   makes per-attempt-paths interchangeable record-wise).

5. **Projection markers PUT** (existing Phase 3 ordering — any
   data file on S3 implies its R1 markers landed first).

6. **Data PUT** to fresh path. Unconditional; no overwrite by
   construction.

7. **Capture `refMicroTs`** via `time.Now().UnixMicro()`.

8. **Ref PUT** to `<refPath>/<refMicroTs:16>-<token>-<UUIDv7>;<hive>.ref`.

9. **Token-commit PUT** to `<dataPath>/<partition>/<token>.commit`,
   zero-byte body, with user-metadata:
   - `attemptid` = the canonical UUIDv7 (32 lowercase hex chars).
     Combined with the path's `<token>` prefix it identifies the
     canonical `<token>-<UUIDv7>.parquet`.
   - `refmicrots` = the canonical ref's `refMicroTs` (decimal
     microseconds), so `RefPath` round-trips on retry.
   - `insertedat` = the writer's pre-encode wall-clock at
     write-start (decimal microseconds) — the same value
     stamped into the parquet's InsertedAtField column, so a
     same-token retry's reconstructed `WriteResult.InsertedAt`
     matches the column's value byte-for-byte.

10. **Contract enforcement (writer-local).** If
    `time.Since(writeStartTime) > CommitTimeout`, increment
    `s3store.write.commit_after_timeout` metric and **return an
    error**. The token-commit is already in place (data durable,
    snapshot reads see the commit), so a same-token retry via
    the upfront HEAD recovers transparently with the original
    WriteResult. Stream readers whose SettleWindow already
    advanced past `refMicroTs` may have missed the write — the
    error surfaces that risk to the caller.

Returns `WriteResult{Offset, DataPath, RefPath, InsertedAt}`.

**No post-PUT HEADs.** No timeliness check. Writer never reads
its own writes back.

### Read gating

#### Snapshot (`Read` / `ReadIter` / `BackfillProjection`)

One LIST per partition prefix already happens. For each token
discovered in the LIST:

- **0 `<token>.commit` entries** → uncommitted. Skip all data
  files under that token.
- **1 `<token>.commit` entry, 1 `<token>-*.parquet` entry**
  (the common case) → committed; the parquet is canonical by
  uniqueness. **No HEAD.**
- **1 `<token>.commit` entry, ≥2 `<token>-*.parquet` entries**
  (rare — only when a retry left orphans from a failed-mid-write
  attempt) → HEAD `<token>.commit` to read the canonical
  attempt-id from metadata. Read only that one parquet.

Average HEAD count: ≈ 0 in steady state. Worst case: bounded
by `len(distinct tokens with multi-attempt history)`.

#### Stream (`Poll` / `PollRecords` / `ReadRangeIter`)

For each ref returned by the time-windowed LIST:

1. Parse ref → `(refMicroTs, token, attemptID, hive)`.
2. HEAD `<dataPath>/<hive>/<token>.commit`.
   - 404 → not committed (yet, or ever). Skip if past
     SettleWindow; otherwise let it sit and re-check next poll.
   - 200 → check `attempt-id` metadata. If matches the ref's
     attempt-id → emit `StreamEntry`. If mismatches → skip
     (different attempt won the canonical race; this ref is
     orphaned).
3. Per-poll cache keyed by `(partition, token)` collapses
   repeat HEADs across refs sharing a token. Reset between
   poll cycles.

Cost: one HEAD per ref (uncached). Acceptable; cache helps
when refs cluster by token within a window.

#### `LookupCommit(ctx, partition, token) → (WriteResult, bool, error)`

Single HEAD on `<dataPath>/<partition>/<token>.commit`.
- 200 → reconstruct `WriteResult` from metadata + known
  prefixes; return `(wr, true, nil)`.
- 404 → return `(WriteResult{}, false, nil)`.

### SettleWindow math

```
SettleWindow ≥ (max writer→ref-PUT)
             + (max ref-PUT→token-commit-PUT)
             + MaxClockSkew (writer↔reader)
```

Same magnitude as before; different decomposition. The writer's
clock is now in the protocol via `refMicroTs`, so
`MaxClockSkew` bounds writer↔reader skew (was server↔reader
under the old LM-stamped design).

This is worth documenting prominently in CLAUDE.md "Backend
assumptions" — replacing the now-obsolete `LM ≈ first-observable-time`
paragraph with a "writer wall-clock is in the ref-sort key;
MaxClockSkew bounds writer↔reader skew" statement.

---

## Phases

The redesign lands across three phases. Each phase is a single
PR boundary; verification gates run at each.

### Phase 1 — Write path redesign

**Files (touched / new):**
- `paths.go` — `encodeRefKey` / `parseRefKey` / `makeID` /
  `parseID` rewritten for the new shape (`refMicroTs`-anchored
  ref, UUIDv7 attempt-id, no `dataLM` / `tsMicros` / `shortID`
  fields). `refCutoff` re-derived (no LM dependency).
- `commit.go` — drop `isCommitValid`, drop `dataLMMetaKey`
  (replaced by an `attemptIDMetaKey`), drop `truncLMToSecond`,
  drop `commitInfo` / `listCommitsAtPrefix` /
  `listCommitsForToken` / `findValidCommitForToken` /
  `reconstructWriteResult` (replaced by a single
  `headTokenCommit` helper).
- `writer_write.go` — `writeEncodedPayload` becomes the
  3-PUT / 0-HEAD sequence. Auto-token UUIDv7 generation when
  `IdempotencyToken == ""`. Upfront HEAD on `<token>.commit`
  (idempotent path only). Writer-side `commit_after_timeout`
  sanity check + metric.
- `idempotency.go` — `dataFileBasenameMatchesToken` rewritten
  for the new id shape (`<token>-<UUIDv7>`).
- `target.go` — drop `CommitTimeoutFloor` constant; remove
  the 1 s minimum from validation. Flip `S3TargetConfig`
  default `ConsistencyControl` to `ConsistencyStrongGlobal`
  in the constructor.
- `metrics.go` — add `s3store.write.commit_after_timeout`
  counter.
- `go.mod` — bump `github.com/google/uuid` to ≥ v1.6 for
  `uuid.NewV7()`.
- Tests: `paths_test.go`, `commit_test.go`,
  `writer_write_test.go`, `idempotency_test.go`,
  `store_test.go` updated for new signatures and shapes.

**Out of scope (in this phase):** read-side gating still uses
the old logic transiently — i.e., reads continue to see
parquets via the existing partition LIST without commit gating.
That ships in Phase 2. This phase is correct in isolation
because no reader path *relies* on the timeliness check (it
was a writer-side guard that only failed the write; reads
already work on bare parquets today).

**Verification gates:**
```sh
go vet -tags=integration ./...
go test -count=1 ./...
go test -tags=integration -count=1 -timeout=10m ./...
golangci-lint run ./...
```

### Phase 2 — Read path commit gating

**Files (touched):**
- `reader_read.go` / `reader_iter.go` / `reader_dedup.go` —
  snapshot read gating per the spec above (skip-HEAD-when-
  unambiguous, HEAD-only-when-multi-attempt). Single
  `gateByCommit(keys []KeyMeta) []KeyMeta` helper used by
  every snapshot entry point.
- `reader_poll.go` — stream read gating with per-poll
  `(partition, token) → bool` cache. One HEAD per ref
  (uncached); cache collapses repeat tokens within a poll.
- `projection_backfill.go` — same gating helper as snapshot
  reads.
- `store.go` — public `LookupCommit(ctx, partition, token)`
  API: single HEAD on `<token>.commit`, reconstructs
  `WriteResult` from metadata.
- `metrics.go` — add `s3store.read.commit_head` /
  `s3store.read.commit_head_cache_hit` counters for
  observability.
- Tests: `reader_read_test.go`, `reader_iter_test.go`,
  `reader_poll_test.go`, `projection_test.go` updated for
  commit-gated semantics. Integration tests for the
  uncommitted-data-invisible invariant.

**Verification gates:** same four commands.

### Phase 3 — Documentation sweep

**Files:**
- `README.md` — top-level "Concurrency contract" section
  (single-in-flight-per-token disclaimer); update
  "Guarantees" section (drop `LM ≈ first-observable-time`
  paragraph; add writer-clock-in-protocol note); update
  "StorageGRID consistency" matrix (default flipped to
  `strong-global`, rationale); drop `CommitTimeout` floor
  paragraph; update read/write tables with new HEAD/PUT
  counts.
- `CLAUDE.md` — replace "Backend assumptions" section.
  Old: `LM ≈ first-observable-time`. New: "writer
  wall-clock is in the ref sort key; `MaxClockSkew` bounds
  writer↔reader; concurrent same-token writes are out of
  contract; `ConsistencyControl: strong-global` is required
  on multi-site StorageGRID for token-commit overwrite
  convergence." Update "Correctness invariants" — the
  `WithIdempotencyToken` invariant's reference to "upfront
  LIST under `{partition}/{token}-`" becomes "upfront HEAD
  on `<token>.commit`".
- `STORAGEGRID.md` — appendix subsection rewrite. Drop
  the `LM ≈ first-observable-time` discussion; add the
  token-commit-overwrite convergence story (deterministic
  encoding makes eventual-consistent overwrites correct
  because the replaced and replacing values are
  byte-equivalent record-wise).
- `COMMIT_MARKER_MIGRATION.md` — collapse to a "shipped"
  status note; the appendix below stays as historical
  context.

**Verification gates:** none (pure docs change). The four-gate
checklist in CLAUDE.md says docs-only changes don't need them.

### Phase 4 — Error-message convention cleanup

Independent of the redesign, but surfaced during Phase 2 review.
The package's error chain currently produces strings of the shape
`"s3store: outer: s3store: inner"` because both leaf errors and
wrap layers prefix the package name. Two examples in the current
tree:

- Leaf prefixed: `validateIdempotencyToken` returns
  `"s3store: IdempotencyToken must not be empty"`.
- Wrap layer also prefixed:
  `fmt.Errorf("s3store: head token-commit: %w", err)` in
  `writer_write.go`'s upfront-HEAD step.

The chain reads `"s3store: head token-commit: s3store: ..."` —
functional but visually noisy and easy to misread in logs.

**Choose one direction across the package:**

- **(a) Leaves keep `s3store:`, wrap layers drop it.** New wrap
  shape: `fmt.Errorf("head token-commit: %w", err)`. Matches the
  existing `"WithIdempotentRead: %w"` site (option-validation
  wrap in `idempotency.go`) — that one already follows the
  convention, so picking this direction means most leaves are
  already correct and most wrap sites need a touch.
- **(b) Wrap layers keep `s3store:`, leaves drop it.** New leaf
  shape: `errors.New("IdempotencyToken must not be empty")`.
  Touches every leaf — `validateIdempotencyToken`,
  `headTokenCommit`, `readTokenCommitMeta`,
  `loadDurationConfig`, `validatePartitionKeyParts`, every
  `validateProjectionDef*`, etc. Larger blast radius.

(a) is cheaper (most leaves are already in shape) and keeps the
package name visible at the deepest point of the chain (where it
identifies the source most usefully). Recommend (a).

**Files touched (option a):**

- Every `fmt.Errorf("s3store: <op>: %w", err)` site in
  `writer_write.go`, `writer.go`, `reader_*.go`, `target.go`,
  `commit.go`, `paths.go`, `listing.go`, `idempotency.go`,
  `projection_*.go`, `concurrency.go`. Replace with
  `fmt.Errorf("<op>: %w", err)` only when the wrapped error
  already starts with `"s3store: "`. Wraps of foreign errors
  (parquet-go decode, AWS SDK, `time.ParseDuration`) keep their
  `s3store:` prefix because the inner doesn't have it.
- Every test that asserts on an error string (`strings.Contains(err.Error(), "s3store: ...")`).
  Spot check: `target_test.go`, `idempotency_test.go`,
  `commit_test.go`, `paths_test.go`, `integration_test.go`'s
  CommitTimeout / MaxClockSkew rejection tests.

Estimate ~30–50 sites in source + a similar count in test
assertions. Mechanical edit; no semantics change.

**Out of scope:** changing how errors are surfaced (still
`fmt.Errorf("…: %w", err)`); introducing typed error sentinels
beyond `ErrAlreadyExists`; restructuring the contract around
`errors.Is` / `errors.As`. This phase is purely cosmetic
deduplication.

**Verification gates:** all four. Test-string assertions catch
drift; lint stays clean if tests are updated in the same PR.

---

## Risk notes

- **No on-disk format compatibility.** Existing data written
  under the old design (Phase 3/4 shipped on `main`) uses
  `<dataLM>-<tsMicros>-<shortID>-<token>;<hive>.ref` /
  `{token}-{tsMicros}-{shortID}.parquet`. The new design's
  ref/data parsers will not understand them. Pre-1.0 — clean
  break. Operators wipe and re-write or deploy fresh
  buckets.
- **Default flip is breaking on multi-site StorageGRID with
  cost-sensitive deployments.** `strong-global` costs more
  than `strong-site`. Mitigations: README disclaims at the
  top; default change is loud in release notes.
- **`CommitTimeout` semantic narrowing.** Was a server-time
  gap; now a writer-local elapsed bound. Code paths that
  treated it as a correctness invariant (the writer's
  step-8 HEAD-and-check) become observability metrics
  instead. Any user who hand-tuned `CommitTimeout` for
  timeliness-check tightness can remove the override.
- **Phase 1 lands without read-side commit gating.** During
  the gap between Phase 1 and Phase 2 merging, reads see
  uncommitted parquets that the old code would have surfaced
  too (since the old timeliness check was writer-side, not
  read-side, in the data-fetching paths). Not a regression;
  the gating is the *new* guarantee Phase 2 adds.

---

## Verification — full re-run before each phase ships

```sh
go vet -tags=integration ./...
go test -count=1 ./...
go test -tags=integration -count=1 -timeout=10m ./...
golangci-lint run ./...
```

Pre-existing lint issues are fixed in the same PR per
CLAUDE.md's "Lint discipline" rule.

---

## Appendix A: Superseded — earlier design

Preserved here so the redesign rationale stays in-tree.

### Earlier design (Phases 3–4 as shipped on `main`)

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
variance, drop the check and the assumption. The redesign above
solves the original "atomic visibility" problem (crash before
commit → invisible to both read paths; crash after → visible to
both) using a presence-only `<token>.commit` marker with no
timing dependency on backend `LastModified`.
