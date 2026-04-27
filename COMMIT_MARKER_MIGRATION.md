# Atomic per-file visibility — migration plan

## Idea

Today the write path does two PUTs: the data file, then the ref.
Data-fetching reads (`Read` / `ReadIter` / `BackfillIndex`) LIST
the data tree directly, so they observe a data file before its
ref has committed — and they keep observing orphans from a writer
that crashed between the two PUTs. (`IndexReader.Lookup` is
unaffected — it LISTs only the index/projection tree, not data
files; see [index_read.go:16](index_read.go#L16): "Lookup issues
LIST only — no parquet reads".) Delta workloads that depend on a
clean read boundary are a correctness hazard.

Proposed change: an additional PUT after the ref — a zero-byte
**commit marker** — written into the partition tree alongside
the data file (e.g. `partition=X/key=Y/{id}.commit` next to the
corresponding `{id}.parquet`). Both data-read paths gate
visibility on the commit marker:

- **Snapshot reads** keep their partition LIST. They pair each
  `.parquet` with its `.commit` sibling and ignore unpaired data
  files.
- **Stream reads** keep LISTing the time-sorted ref tree. For
  each polled ref they HEAD the corresponding commit marker
  before considering the ref committed.

Write order becomes: index markers PUT → data PUT → ref PUT →
commit marker PUT. The commit marker is the single atomic event
that flips visibility for both paths simultaneously: crash before
the commit marker → invisible to both; crash after → visible to
both. Per-file atomicity is solved; multi-file atomicity (one
`Write` spanning multiple partitions appearing as one event) is
explicitly out of scope.

## Decided design choices

- **`DisableRefStream` is removed; ref stream + marker are always
  written.** Pre-v1 cleanup that comes along with the marker
  work. One mode for everyone — no `DisableRefStream` branching
  anywhere in the writer, snapshot read paths, stream read
  paths, `LookupCommit`, or fencing. The cost (+2 PUTs per
  write for formerly-stream-disabled workloads) is small in
  absolute terms; the simplification compounds across every
  current and future feature.
- **Marker filename `{id}.commit`** — purely id-derived, no time
  component. Under `WithIdempotencyToken`, id = token, so the
  marker path is deterministic from the token alone: a single
  HEAD answers "did this token commit?".
- **Commit marker carries `dataTs` and ref filename in user
  metadata.** Two purposes: the data-PUT-412 fast path and
  `LookupCommit` can both reconstruct the full `WriteResult`
  (`InsertedAt` from `dataTs`, `Offset` / `RefPath` from the
  ref filename) from a single marker HEAD, without a follow-up
  LIST or GET on the ref tree. `WriteResult`'s shape stays
  unchanged from today: `{Offset, DataPath, RefPath,
  InsertedAt}`.
- **`LookupCommit(ctx, partitionKey, token) → (WriteResult,
  bool, error)`** as a public discovery API. Closes the gap
  where a same-token retry computes zero rows and would
  otherwise never trigger the existing data-PUT-412 dedup
  path. Caller usage: probe on retry; if a commit exists,
  record its `WriteResult` and skip recomputation; if not,
  compute and write. Implementation: **HEAD marker + HEAD
  data file in parallel.** HEAD marker returns
  `marker.LastModified`, `dataTs`, and the ref filename
  (everything `WriteResult` needs). HEAD data returns
  `data.LastModified`. The two `LastModified`s feed the
  timeliness check. Two HEADs in parallel beat a prefix LIST
  on latency (~30–80 ms wall time vs ~50–150 ms for LIST),
  cost (~$0.0008/1000 vs ~$0.005/1000), AND give us the
  user metadata that LIST doesn't return.
- **Marker timeliness check (read paths).** A commit marker
  is treated as valid only if `marker.LastModified -
  data.LastModified < SettleWindow`. Both timestamps are
  server-stamped by S3, so the comparison is free of
  client/server clock skew. Prevents the inconsistency where
  a late-arriving marker (timed-out PUT that lands
  server-side after `SettleWindow` has elapsed, or a caller
  retry that comes too late) is seen by snapshot reads but
  missed by stream consumers whose offset has already
  advanced past the unmarkered ref. Where each path gets the
  timestamps:
  - **Snapshot:** LIST data tree returns both
    `data.LastModified` and `marker.LastModified` directly.
    No extra requests.
  - **Stream:** HEAD commit marker → `marker.LastModified`,
    then GET data file → `data.LastModified` from response
    headers. The GET happens anyway to decode parquet. No
    extra requests.
  - **`LookupCommit`:** HEAD marker + HEAD data in parallel
    return both `LastModified`s plus the marker's user
    metadata. ~1 round-trip wall time.

  The writer never stamps `data.LastModified` anywhere — it's
  discoverable from S3 directly on every read path.
- **`SettleWindow` budget and defaults.** Default 10s, hard
  minimum 2s (the library rejects configured values below
  the floor at construction time — below 2s, client/server
  clock skew dominates the timeliness comparison). The
  `SettleWindow / 2` budget is enforced on **ref PUT +
  commit marker PUT collectively**: the writer captures
  `refTime = time.Now()` at ref filename generation and
  passes a context with deadline `refTime + SettleWindow / 2`
  to both PUTs. After the ref PUT returns, the writer checks
  remaining budget and aborts before the commit marker PUT
  if exceeded. A timeout during commit marker PUT returns an
  error — no internal retry, no cleanup. The in-flight PUT
  may eventually land server-side; the timeliness check on
  read paths ensures a late-arriving marker stays invisible
  to both snapshot and stream consumers if it lands beyond
  `SettleWindow` of `data.LastModified`. At-least-once
  boundary; callers retry within `SettleWindow` for the data
  to become visible (this applies to the partial-commit
  retry case, where the data file is already on S3 and
  `data.LastModified` is fixed — for retries where the
  original data PUT never landed, the retry's fresh
  `data.LastModified` and fresh `marker.LastModified` start
  a new clock together and don't have this constraint).
- **`SettleWindow` is persisted in S3, not in
  `S3TargetConfig`.** Two correctness motivations:

  1. **Writer and reader must agree on the same value.**
     The timeliness check on the read paths
     (`marker.LastModified - data.LastModified <
     SettleWindow`) and the writer's `SettleWindow / 2`
     budget are paired primitives. A reader running with
     a smaller `SettleWindow` than the writer will reject
     markers the writer considered valid (data
     disappears); a reader with a larger value may emit
     data a stricter peer wouldn't (snapshot/stream
     divergence). Out-of-band config coordination is
     fragile; persisting in S3 and reading from there on
     every Target init makes agreement the default
     posture, not a deployment discipline.
  2. **Changing the value silently rewrites history.**
     Decreasing it makes previously-valid markers fail
     the timeliness check — already-visible data
     disappears. Increasing it makes previously-invalid
     markers pass — timed-out writes resurface. Either
     direction is a correctness violation. Persisting
     fixes the value at store creation; an operator who
     genuinely needs a different value must re-create the
     store at a new prefix and migrate.

  Mechanics: stored at `<prefix>/_config/settle-window` as
  a plain-text Go-`time.Duration`-parseable body (e.g.
  `"10s"`). `S3Target` initialization GETs the object and
  stamps the value on the Target; construction fails if
  it's missing or unparseable. Removed from
  `S3TargetConfig` entirely — users can't pass it.

  Operations: no public `Create` function in the library;
  operators initialize the config object out-of-band —
  README ships a copy-pasteable Python `boto3` snippet,
  integration tests use a `fixture_test.go` helper.
  `_config/` is reserved for any other immutable settings
  if they emerge; only `settle-window` for now.
- **Public contract phrasing for the README's Guarantees
  section.** Replaces today's "No atomic write visibility"
  bullet:

  > **Atomic per-file visibility on both paths.** A
  > successful `Write` is visible to all readers atomically
  > once its commit marker lands — there's no window where
  > snapshot sees the data and the change stream doesn't, or
  > vice versa.
  >
  > **Time semantics.** `SettleWindow` is a single
  > Target-level setting persisted at
  > `<prefix>/_config/settle-window` and shared by writer and
  > reader by construction. The writer budgets the ref PUT +
  > commit marker PUT collectively at `SettleWindow / 2`.
  > Readers (both paths) gate visibility on
  > `marker.LastModified - data.LastModified < SettleWindow`
  > — both timestamps are S3-server-stamped, so the
  > comparison is free of client/server clock skew. Default
  > 10s, hard minimum 2s. Changing the value rewrites
  > history and is forbidden in place.
  >
  > - **Snapshot reads** (`Read` / `ReadIter` /
  >   `BackfillIndex`) see new records immediately upon
  >   `Write` returning success.
  > - **Change-stream reads** (`PollRecords` /
  >   `ReadRangeIter`) lag the live tip by `SettleWindow` so
  >   the refs they return have already had their commit
  >   markers land server-side.
  > - **`IndexReader.Lookup`** is unaffected by the commit
  >   marker. It returns observed projection (column) values
  >   from the index tree, not records from data files; index
  >   markers are written immediately and remain visible
  >   regardless of commit-marker state. Orphan tolerance is
  >   the existing contract and unchanged.
  >
  > **Failure modes.** A mid-pipeline write failure (e.g.,
  > commit-marker PUT timed out) leaves the data uniformly
  > invisible to both paths — the timeliness check rejects
  > late-arriving markers. Caller retries to converge
  > (at-least-once, above).
  >
  > **Per-file only.** A `Write` spanning multiple
  > partitions becomes visible per-partition independently;
  > there is no atomic event covering all partitions of one
  > `Write`.
- **Write order: markers → data → ref → commit marker.**
  Index markers strictly precede the data PUT so that any
  data file on S3 implies all R1 markers were written. This
  sidesteps the GET-and-decode that would otherwise be needed
  on retry to reconstruct marker paths from the data file's
  records (the retry's R2 records may differ from R1's).
  Cost: orphan markers accumulate on retries with different
  records, but [index_write.go:106](index_write.go#L106)
  already documents "orphan markers are tolerated at Lookup
  time" — this leans into the existing contract.
- **Data-PUT-412 retry shape.** Index markers PUT runs first
  per the order above (always, even on retry). Then the data
  PUT 412s on the existing token-derived path under
  `WithIdempotencyToken`. At that point HEAD the commit
  marker: if it exists, the previous attempt fully committed
  — return success directly without writing ref or commit
  marker. If missing, fall through to `findExistingRef` and
  write whatever's missing (commit marker alone, or fresh ref
  + commit marker). The marker's user metadata carries
  `dataTs` and the ref filename, so the fast-path HEAD
  returns everything `WriteResult` needs without a follow-up
  LIST.
- **`MaxRetryAge` is removed.** The data file's `created-at`
  metadata (already written today as a user-defined header on
  the data PUT) supplies an exact `dataTs` lower bound for the
  ref LIST in `findExistingRef` — no fuzzy budget needed.
  HEAD on the data file is already on the 403 disambiguation
  path for StorageGRID, so no extra request in that case.
  Trade-off: a partial-commit retry resurrected long after
  the original write (crashed between ref and marker,
  orchestrator retries days later) does a LIST from `dataTs`
  to now — wider than today's bounded scan. Acceptable
  because most retries hit the HEAD-marker fast path before
  `findExistingRef` runs at all.
- **Fencing mechanism (under `WithFencedCommit()`): LIST +
  `LastModified` comparison.** The marker filename has no
  time component, so fencing relies on object metadata: at
  commit-marker PUT time, LIST the partition's `.commit`
  files and reject our own commit if any has a `LastModified`
  greater than **our `dataTs`** (the value we already stamp in
  the data file's `created-at` header). dataTs is the right
  reference because it's stable across our retries and tied
  to "when our write logically started" — a slow writer that
  takes 30s between data PUT and commit-marker PUT still
  rejects markers from jobs that started after our data PUT.
  The LIST short-circuits on the first newer marker, so
  rejection is fast; successful fencing pays a full-partition
  scan (typically 1 LIST page; 2–3 on dense partitions).
  Cost: one extra LIST per fenced write (~$0.005/1000 LIST
  requests, +50–100 ms latency). Best-effort under the
  LIST↔PUT race window — acceptable because the sequential-
  writes constraint already rules out true concurrency.
- **Read-path wiring centralized via a shared
  `isCommitValid` helper.** Single function decides validity
  from `dataLastModified`, `markerLastModified`, and
  `settleWindow` — used by every read entry point that
  cares.
  - **`listCommittedDataFiles`** replaces `listDataFiles`
    (rename + modify in place). LIST already returns both
    `.parquet` and `.commit` siblings under the partition
    prefix; the helper pairs them, drops unpaired data
    files, and applies `isCommitValid`. Used by `Read`,
    `ReadIter`, and `BackfillIndex` (which still scans
    partitions, just now committed-only).
  - **Stream side:** the existing public `Poll` becomes
    private (`poll`, used internally only). `PollRecords`
    and `ReadRangeIter` continue to call it; both already
    have `data.LastModified` from their GET response and
    `marker.LastModified` from a per-ref HEAD, so they
    apply `isCommitValid` after the GET. Putting the check
    in `Poll` itself would be duplicate work — it has no
    GET to compare against. Documented inline that "marker
    existence ≠ validity" so future readers don't add a
    skipped check there.
  - **`IndexReader.Lookup` is unchanged.** It LISTs index
    markers and parses keys for column values — never
    fetches data files (see [index_read.go:16](index_read.go#L16):
    "Lookup issues LIST only — no parquet reads"). Index
    markers are written immediately and visible immediately;
    orphan tolerance is the existing contract and is
    unaffected by the commit-marker design.
  - **`LookupCommit`** uses `isCommitValid` directly via the
    two parallel HEADs.
- **Index → Projection rename (pre-v1 cleanup, separate
  from the marker work).** What the codebase calls "index"
  is really a projection of column values — `Lookup`
  returns observed column values, not record locators.
  Rename `IndexDef` → `ProjectionDef`, `IndexReader` →
  `ProjectionReader`, `IndexLookupDef` →
  `ProjectionLookupDef`, `BackfillIndex` →
  `BackfillProjection`, etc. Orthogonal to commit markers
  but worth folding in while we're already breaking the
  pre-v1 surface.
- **Observability — two new metrics.**
  - **`s3store.read.uncommitted_data{reason=missing|stale,
    method=...}`** — counter. Bumped when any read path
    filters out a data file because its commit marker is
    missing or fails the timeliness check. Mirrors the
    existing `s3store.read.malformed_refs` and
    `s3store.read.missing_data` patterns — operators see
    the signal, the library takes no destructive action.
    No library-driven cleanup in this version (see deferred
    follow-ups below).
  - **`s3store.write.fenced_rejections`** — counter. Bumped
    when `WithFencedCommit()` rejects a commit because LIST
    found a newer marker. Operational signal for fencing
    pressure: high values indicate applications racing more
    than expected (or the sequential-writes-per-partition
    constraint is being violated somewhere).

  Both metrics need a matching panel in
  [`dashboards/s3store.json`](dashboards/s3store.json),
  mirroring the existing `read.malformed_refs` /
  `read.missing_data` panels added in the previous
  observability sweep.
- **`CLAUDE.md` updates required.** The marker design
  changes several invariants documented in
  [CLAUDE.md](CLAUDE.md) and they need refreshing as
  part of this work:
  - **At-least-once at the storage layer** — refine to
    "data is durable after the data PUT, but visible only
    after the commit marker lands within `SettleWindow`."
    Successful `Write` now means data + ref + commit marker
    all landed.
  - **Read-after-write on snapshot reads** — still holds:
    once `Write` returns, the commit marker is in place, so
    snapshot LIST sees the paired files and the timeliness
    check passes.
  - **Read stability — no library-driven deletion** — stays
    in this version (cleanup is deferred). Note that the
    underlying premises shift: the library can now
    distinguish committed from crashed-mid-write. Follow-up
    work will revisit; flag in the comment that cleanup is
    a known future relaxation.
  - **Add new invariant entries:** marker timeliness check,
    writer/reader must agree on `SettleWindow` (enforced
    via the persisted `_config/settle-window` object),
    persisted-config object is the source of truth.
  - **Verification checklist** — confirm no new build flags
    needed; integration tests cover the new fixture-helper
    initialisation step.

## Decided constraints

The marker (atomic per-file visibility) is always written.
Commit-ordering enforcement is **opt-in** per write via
`WithFencedCommit()`, mirroring the existing opt-in shape of
`WithIdempotencyToken`.

- **Default (no `WithFencedCommit()`).** Parallel writers on
  the same partition both succeed; stale retries (Token A's
  retry after Token B has already committed) also succeed.
  Failure mode is at-least-once duplicates, absorbed by
  `EntityKeyOf` + `VersionOf` dedup. Suitable for workloads
  where rows are independent — append-only logs, immutable
  event streams, anything where a record's correctness doesn't
  depend on the latest committed state.
- **With `WithFencedCommit()`.** The commit-marker PUT is
  fenced against any newer marker present in the same
  partition (LIST + `LastModified` comparison; reject if any
  sibling marker has `LastModified > our dataTs`). Reach is
  precise:
  - **Catches:** partial-commit retries where the original
    made it past the data PUT. The retry's `dataTs` is
    preserved across attempts via `findExistingRef`, so the
    fence detects newer commits. Token A's resurrected
    half-finished write after Token B has committed is
    refused.
  - **Doesn't catch:** retries where the original failed
    *before* the data PUT landed server-side. The retry
    writes a fresh data file with a *fresh* `dataTs` (= now),
    so `marker.LastModified > dataTs` is never true for B's
    earlier marker — the retry is indistinguishable from a
    normal new write at the library level. Application's
    re-read-on-retry covers correctness here.
  - **Doesn't catch (constraint, not bug):** true concurrent
    writers — both could LIST and find no newer marker, both
    PUT. Concurrent writers are excluded by the
    sequential-writes-per-partition constraint; this fence
    is the in-band backstop, not OCC.

  Required for delta workloads — pair with the application
  pattern of re-reading state on each retry.
- **Same-token retry (zombie writer) remains safe under both
  modes.** Deterministic data path + marker; if the zombie
  hasn't been overtaken, its retry just confirms the original
  commit, regardless of `WithFencedCommit()`.

## Open questions

(All open questions resolved — see "Decided design choices"
and "Decided constraints" above.)

## Deferred follow-ups

- **Library-driven cleanup of uncommitted / stale data.**
  The marker design relaxes the "library can't delete"
  invariant in [CLAUDE.md](CLAUDE.md): committed vs
  crashed-mid-write is now distinguishable (marker present +
  timely vs missing or stale), and no reader can have
  observed an uncommitted file as visible (both paths gate
  on the marker). The library could therefore safely delete:
  - Data files older than `SettleWindow` with no paired
    marker (the marker can no longer land in time → permanent
    orphan).
  - Data + commit pairs where the marker fails the
    timeliness check (already filtered from every read path).

  Race considerations are benign: writer retries under
  `WithIdempotencyToken` simply turn into a fresh commit
  after their orphan is cleaned; late server-side PUTs that
  land after a delete write to a deleted path with no
  visibility consequence; in-flight readers don't GET
  uncommitted files because they filter first. Three
  approaches for *when* (on-read, on-write, background
  sweeper) — to be decided based on real-workload numbers
  from the `uncommitted_data` metric. Deferred so the
  marker work's correctness review isn't tied to the
  cleanup work's correctness review.

