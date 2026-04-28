# Atomic per-file visibility — migration plan

## Idea

Today the write path does two PUTs: the data file, then the ref.
Data-fetching reads (`Read` / `ReadIter` / `BackfillProjection`)
LIST the data tree directly, so they observe a data file before
its ref has committed — and they keep observing orphans from a
writer that crashed between the two PUTs. (`ProjectionReader.Lookup`
is unaffected — it LISTs only the projection tree, not data
files; see [index_read.go:16](index_read.go#L16): "Lookup issues
LIST only — no parquet reads".) Delta workloads that depend on a
clean read boundary are a correctness hazard.

Proposed change: an additional PUT after the ref — a zero-byte
**commit marker** — written into the partition tree alongside
the data file (`{partition}/{id}.commit` next to the
corresponding `{id}.parquet`). Both data-read paths gate
visibility on the commit marker:

- **Snapshot reads** keep their partition LIST. They pair each
  `.parquet` with its `.commit` sibling, drop unpaired data
  files, and apply a server-LM timeliness check
  (`marker.LM - data.LM < CommitTimeout`) to the pairs.
- **Stream reads** keep LISTing the time-sorted ref tree. For
  each polled ref they HEAD the corresponding commit marker
  and apply the same timeliness check before considering the
  ref committed.

The commit marker is the single atomic event that flips
visibility for both paths simultaneously: crash before the
commit marker → invisible to both; crash after → visible to
both. Per-file atomicity is solved; multi-file atomicity (one
`Write` spanning multiple partitions appearing as one event) is
explicitly out of scope.

Two refinements emerged from the design discussion and shape
the rest of this document:

- **Per-attempt-paths under `WithIdempotencyToken`.** Every
  attempt writes to a fresh path
  (`{partition}/{token}-{tsMicros}-{shortID}.parquet` and
  matching `.commit`). No write-path PUT ever overwrites an
  existing key — sidesteps eventual-consistency exposure on
  multi-site StorageGRID overwrites. The library's upfront
  LIST under `{partition}/{token}-` is the dedup gate for
  retries.
- **Server-stamped `refTs`.** The ref filename embeds the
  data file's `data.LastModified` (not the writer's
  wall-clock), so the reader's `refCutoff = now - SettleWindow`
  comparison is reader↔server only — the writer's clock is
  not in the protocol.

Final write sequence: **upfront LIST → fresh attempt-id →
projection markers → data PUT → HEAD data → ref PUT → marker
PUT → HEAD marker.** See "Decided design choices" below for
the full rationale and trade-offs.

## Decided design choices

- **`DisableRefStream` is removed; ref stream + marker are always
  written.** Pre-v1 cleanup that comes along with the marker
  work. One mode for everyone — no `DisableRefStream` branching
  anywhere in the writer, snapshot read paths, stream read
  paths, `LookupCommit`, or fencing. The cost (+2 PUTs per
  write for formerly-stream-disabled workloads) is small in
  absolute terms; the simplification compounds across every
  current and future feature.
- **Marker filename `{id}.commit`** — sibling of `{id}.parquet`
  in the same partition. The id is always
  `{token}-{tsMicros}-{shortID}` (matching the existing
  `makeAutoID` shape used today for token-less writes),
  even under `WithIdempotencyToken`. The token component
  is the caller-supplied logical-write identifier; the
  `{tsMicros}-{shortID}` suffix is the **attempt-id**, a
  library-generated uniquifier that ensures every attempt of
  the same logical write lands at a distinct path. No
  overwrite anywhere — every PUT is to a new key. This is
  what makes the design cross-backend safe (read-after-new-write
  is strongly consistent on AWS S3, MinIO, and StorageGRID at
  any consistency level; only read-after-overwrite has
  eventual-consistency exposure on multi-site StorageGRID).
  "Did this token commit?" is answered by a **LIST under
  `{partition}/{token}-`**, no HEAD required: the LIST
  response carries both `.parquet` and `.commit` siblings
  with their server-stamped `LastModified` headers directly,
  enough to run the timeliness check and reconstruct the
  full `WriteResult` (the data filename's
  `{token}-{tsMicros}-{shortID}` suffix is unambiguously
  parseable — `tsMicros` is fixed-width 16 digits, `shortID`
  is fixed-width 8 hex chars — and the ref filename is
  deterministic from `(data.LM, id, tsMicros, hiveKey)`).
  See "LookupCommit" below.
- **Commit marker carries `dataLM` in user metadata.** One
  field, one consumer: stream reads. `dataLM` is the
  server-stamped `data.LastModified` (microseconds, captured
  by the writer's post-data HEAD) — it lets stream-side
  `PollRecords` / `ReadRangeIter` apply the timeliness check
  with a *single* per-ref HEAD on the marker, without a
  second HEAD on the data file or a wasted GET on stale
  data.

  Snapshot reads, the upfront-LIST dedup gate in `Write`,
  and `LookupCommit` get `data.LastModified` directly from
  their LIST response (LIST returns both `.parquet` and
  `.commit` siblings with `LastModified` for each), so they
  don't need to read marker metadata at all. The marker is
  effectively a zero-byte presence object on those paths.

  `WriteResult` shape stays unchanged from today:
  `{Offset, DataPath, RefPath, InsertedAt}`. Reconstruction
  for `LookupCommit` / upfront-dedup is from LIST + filename
  parsing:
  - `DataPath` = full `.parquet` key from LIST.
  - `InsertedAt` = `time.UnixMicro(tsMicros)` parsed from
    the data filename's `{token}-{tsMicros}-{shortID}`
    suffix.
  - `RefPath` / `Offset` = computed via the existing
    `encodeRefKey(refPath, data.LM, tsMicros, shortID, token, hiveKey)`
    formula — all inputs derivable from LIST + the
    `_ref/` prefix constant.

  No `dataTs` or `ref-filename` field on the marker because
  both are derivable; keeping the metadata trivially small
  also keeps the marker PUT small.
- **`LookupCommit(ctx, partitionKey, token) → (WriteResult,
  bool, error)`** as a public discovery API. Closes the gap
  where a same-token retry computes zero rows and so never
  reaches `Write`'s upfront-LIST dedup gate (no `Write` call
  at all). Caller usage: probe on retry; if a commit exists,
  record its `WriteResult` and skip recomputation; if not,
  compute and write. Implementation: **LIST under
  `{partition}/{token}-`, pair `.parquet` with `.commit` by
  id, apply `isCommitValid(data.LM, marker.LM,
  CommitTimeout)`, return the first valid commit** (or
  `(WriteResult{}, false, nil)` if none). LIST alone yields
  every value the check and `WriteResult` need — both
  server-stamped `LastModified` headers come back in the
  LIST entries, and `WriteResult` reconstructs from the
  LIST keys + filename parsing. **One LIST per call**,
  no HEADs, ~50–150ms on AWS S3. The same internal helper
  backs `Write`'s upfront-LIST dedup gate (see "Write
  sequence" below) — one code path, one set of semantics.
- **Marker timeliness check (read paths).** A commit marker
  is treated as valid only if `marker.LastModified -
  data.LastModified < CommitTimeout`. Both timestamps are
  server-stamped by S3, so the comparison is free of
  client/server clock skew. Prevents the inconsistency where
  a late-arriving marker (timed-out PUT that lands
  server-side after `CommitTimeout` has elapsed, or a caller
  retry that comes too late) is seen by snapshot reads but
  missed by stream consumers whose offset has already
  advanced past the unmarkered ref. Where each path gets the
  timestamps:
  - **Snapshot:** LIST partition prefix returns both
    `data.LastModified` and `marker.LastModified` directly.
    Pair `.parquet` with `.commit` by id. No HEADs, no
    metadata reads.
  - **Stream:** HEAD commit marker (1 round-trip per ref) →
    `marker.LastModified` from response + `dataLM` from
    user metadata. No second HEAD on the data file, no
    wasted GET on stale data — this is why `dataLM` is the
    one piece of marker user metadata we keep. The data GET
    happens later to decode parquet (only on valid markers).
  - **`LookupCommit` and `Write`'s upfront-dedup gate:**
    LIST under `{partition}/{token}-` returns both `.parquet`
    and `.commit` siblings with `LastModified` for each.
    Pair by id, apply the timeliness check from LIST data
    alone. No HEADs, no metadata reads. Same primitive as
    the snapshot path, scoped to a single token's siblings.

  The writer captures `data.LastModified` via a post-PUT
  HEAD on the data file (see "Write-side post-PUT HEADs"),
  then stamps it into the marker's `dataLM` metadata —
  *only* for the stream-read path's benefit (every other
  read path gets it from LIST directly).
- **Write-side post-PUT HEADs (correctness, not optimization).**
  After the data PUT, the writer HEADs the data file to read
  `data.LastModified` and uses that value as `refTsMicros` in
  the ref filename — making `refTs` server-stamped. After the
  marker PUT, the writer HEADs the marker to read
  `marker.LastModified` and verifies `marker.LastModified -
  data.LastModified < CommitTimeout`; on violation it returns
  an explicit error so the caller retries (the data + ref +
  marker stay on S3 as orphans, filtered by every read path's
  timeliness check, reclaimed by the operator-driven sweeper
  in the deferred follow-up). Both HEADs are unconditional
  — the conditional "skip the marker HEAD when local elapsed
  is small" optimization was rejected because network-latency
  asymmetry on the data PUT response makes local elapsed an
  unreliable proxy for the server-time `marker.LM - data.LM`
  gap (a 2–3s receive-side hiccup invisibly inflates the
  server-time gap while leaving the writer's local elapsed
  unchanged). Cost: +2 HEADs per write (~60–160 ms on AWS
  S3, sub-millisecond on localhost MinIO). Buys: a precise
  correctness guarantee instead of a probabilistic one,
  symmetric writer/reader timeliness checks, and a tighter
  skew model — see "Two-knob time budget" below for why this
  shrinks `MaxClockSkew`'s scope to reader↔server only.

  Why server-stamped `refTs`: the reader's `refCutoff = now -
  SettleWindow` compares reader-time to `refTs`. If `refTs`
  is writer-stamped wall-clock, the comparison is reader↔
  writer (with server in between transitively). If `refTs`
  is server-stamped (= `data.LM`), the comparison is
  reader↔server only — a tighter, simpler skew contract.
  Lex ordering of refs by `data.LM` instead of writer's
  wall-clock isn't a regression: concurrent writers' refs
  could already be lex-out-of-order under the old design
  (different wall clocks, same partition), and stream
  consumers tolerate it via `SettleWindow`. Snapshot reads
  don't depend on ref order at all.
- **Two-knob time budget: `CommitTimeout` + `MaxClockSkew`,
  with `SettleWindow` derived.** Replaces the original
  single-knob `SettleWindow` design (see "Phase 2 redesign"
  below for what changed and why).

  The two knobs decompose what the original single knob
  conflated:

  - **`CommitTimeout`** — the maximum time, *server-side*,
    between data PUT and marker PUT. A marker is valid only
    if `marker.LastModified - data.LastModified <
    CommitTimeout`. Both timestamps are S3-server-stamped, so
    the comparison is **completely free of client/server
    clock skew** and produces the same answer for every
    reader. The writer's PUT-sequence budget is enforced
    *post hoc* by HEADing the marker after PUT and verifying
    `marker.LM - data.LM < CommitTimeout` directly — same
    check the reader runs, same values. On violation the
    writer returns an explicit error and the caller retries
    (the orphan stays invisible to every read path; no
    silent rejection).

  - **`MaxClockSkew`** — the operator's assumed bound on
    reader↔server wall-clock divergence. Used by the reader
    only, in `refCutoff`. **The writer's clock leaves the
    protocol entirely** (under "Write-side post-PUT HEADs"
    above, `refTsMicros` is server-stamped `data.LM`, not
    writer wall-clock), so the only skew pair that matters
    is reader vs the S3 cluster the reader is talking to.

  - **`SettleWindow`** is **derived**, not persisted:
    `SettleWindow = CommitTimeout + MaxClockSkew`. Used in
    one place: Poll's `refCutoff = now - SettleWindow`. Pure
    optimization once Phase 6 ships — refCutoff just decides
    which refs to HEAD-check, the marker timeliness check
    (server-LM) is the actual visibility gate.

  **Why both knobs are needed.** The reader's `refCutoff`
  uses the reader's wall clock (`time.Now()`), but `refTs`
  is server-stamped (= `data.LM`). If reader is `D` seconds
  ahead of the server and `D > CommitTimeout`, refCutoff
  would consider a just-published ref as "outside the commit
  window" before the writer has had time to land its marker
  server-side — reader skips, advances offset, marker lands,
  snapshot reads see the data, stream reads have already
  moved past. The atomic-visibility contract breaks.

  Sizing `SettleWindow = CommitTimeout + MaxClockSkew`
  closes the gap: even at the worst-case skew (`D =
  +MaxClockSkew`), the reader's cutoff only crosses
  `refTsMicros` (= `data.LM`) when `α ≥ CommitTimeout` of
  *server-time* has elapsed. The marker either landed within
  `CommitTimeout` (snapshot and stream both see it) or
  didn't (timeliness check rejects on both paths). Both
  paths converge.

  Skew exceeding `MaxClockSkew` is an explicit operator
  contract violation, surfaced in docs, not a hidden trap.

- **Both knobs persisted in S3, not in `S3TargetConfig`.**
  Two correctness motivations:

  1. **Writer and reader must agree on the same values.**
     `CommitTimeout` is the writer's PUT budget AND the
     reader's marker validity threshold — must match.
     `MaxClockSkew` is read-only (writer doesn't use it),
     but a reader running with a too-small value rejects
     valid commits (data disappears); too-large widens the
     window unnecessarily. Out-of-band config coordination
     is fragile; persisting in S3 makes agreement the
     default posture.
  2. **Changing either silently rewrites history.**
     Decreasing `CommitTimeout` makes previously-valid
     markers fail the timeliness check — already-visible
     data disappears. Increasing it makes previously-invalid
     markers pass — timed-out writes resurface. Decreasing
     `MaxClockSkew` may shift refCutoff and make stream
     consumers temporarily skip refs they used to include.
     Persisting fixes the values at store creation; an
     operator who genuinely needs different values must
     re-create the store at a new prefix and migrate.

  Mechanics: stored at `<prefix>/_config/commit-timeout` and
  `<prefix>/_config/max-clock-skew`, each as a plain-text
  Go-`time.Duration`-parseable body. `S3Target`
  initialization GETs both and stamps the resolved values
  (plus the derived `SettleWindow`) on the Target;
  construction fails if either is missing or unparseable.
  Removed from `S3TargetConfig` entirely — users can't pass
  them.

  **Floors:** `CommitTimeout ≥ 1s` (S3 server-stamps
  `LastModified` at second precision — see Phase 4 fix-up
  below; a write straddling a wall-clock second boundary
  shows a 1s gap even when both PUTs completed in
  milliseconds, so any value below 1s would fail the
  timeliness check on those writes). `MaxClockSkew ≥ 0`
  (zero is a valid claim on tightly-clocked deployments).
  No upper bound — operators size for their environment.

  Operations: no public `Create` function in the library;
  operators initialize the config objects out-of-band —
  README ships a copy-pasteable Python `boto3` snippet,
  integration tests use a `fixture_test.go` helper.
  `_config/` is reserved for any other immutable settings
  if they emerge; only `commit-timeout` and `max-clock-skew`
  for now.
- **Public contract phrasing for the README's Guarantees
  section.** Replaces today's "No atomic write visibility"
  bullet:

  > **Atomic per-file visibility on both paths.** A
  > successful `Write` is visible to all readers atomically
  > once its commit marker lands — there's no window where
  > snapshot sees the data and the change stream doesn't, or
  > vice versa.
  >
  > **Time semantics.** Two Target-level settings, persisted
  > and shared by writer and reader by construction:
  > `<prefix>/_config/commit-timeout` (max server-time
  > between data PUT and marker arrival) and
  > `<prefix>/_config/max-clock-skew` (assumed bound on
  > reader↔server wall-clock divergence — the writer's
  > clock is not in the protocol, see below). Readers gate
  > visibility on `marker.LastModified - dataLM <
  > CommitTimeout` — both timestamps S3-server-stamped
  > (`dataLM` carried in the marker's user metadata), so
  > the comparison is **free of client/server clock skew**.
  > The writer enforces the same check itself via a
  > post-marker-PUT HEAD; on violation, an explicit error.
  > Stream consumers' poll cutoff lags by `CommitTimeout +
  > MaxClockSkew` — the extra `MaxClockSkew` covers the
  > reader↔server wall-clock divergence so the cutoff
  > doesn't overtake refs whose markers might still be
  > valid server-side. Changing either value rewrites
  > history and is forbidden in place.
  >
  > **Same-token retries forever.** Under
  > `WithIdempotencyToken`, every retry writes to a fresh
  > per-attempt path (id = `{token}-{tsMicros}-{shortID}`).
  > The library's upfront LIST under
  > `{partition}/{token}-*.commit` finds any prior
  > successful attempt and returns its result; if none, the
  > retry proceeds with a fresh attempt-id and a fresh
  > server-stamped `data.LM`. Recovery from arbitrarily
  > long S3 outages is automatic — no
  > `ErrCommitWindowExpired` surface, no orchestrator-side
  > generation suffix in the token. **Cost:** failed
  > attempts leave per-attempt orphans on S3; reader paths
  > filter them out via the timeliness check; the deferred
  > operator-driven sweeper reclaims them.
  >
  > **Reader-side dedup recommended under
  > `WithIdempotencyToken`.** Near-concurrent retry overlap
  > can produce two valid commits for the same token (slow
  > original + fast retry both succeed). Both contain
  > bit-identical records (deterministic parquet encoding
  > is required under the token contract). Configure
  > `EntityKeyOf + VersionOf` to collapse the duplicates on
  > read.
  >
  > - **Snapshot reads** (`Read` / `ReadIter` /
  >   `BackfillProjection`) see new records immediately upon
  >   `Write` returning success.
  > - **Change-stream reads** (`PollRecords` /
  >   `ReadRangeIter`) lag the live tip by `CommitTimeout +
  >   MaxClockSkew` so the refs they return have already
  >   had their commit markers land server-side, even at
  >   the worst-case reader↔server skew.
  > - **`ProjectionReader.Lookup`** is unaffected by the
  >   commit marker. It returns observed projection (column)
  >   values from the projection tree, not records from data
  >   files; projection markers are written immediately and
  >   remain visible regardless of commit-marker state.
  >   Orphan tolerance is the existing contract and
  >   unchanged.
  >
  > **Failure modes.** A mid-pipeline write failure (e.g.,
  > commit-marker PUT timed out, post-marker-HEAD verified
  > a stale gap) leaves the per-attempt triple
  > (data + ref + possibly marker) uniformly invisible to
  > both paths — the timeliness check rejects the orphan on
  > every read path. Caller retries to converge
  > (at-least-once, above); under `WithIdempotencyToken`
  > the retry writes a fresh attempt path and recovers
  > automatically, no caller bookkeeping required.
  >
  > **Per-file only.** A `Write` spanning multiple
  > partitions becomes visible per-partition independently;
  > there is no atomic event covering all partitions of one
  > `Write`.
- **Write sequence: upfront-LIST → fresh attempt-id →
  projection markers → data PUT → HEAD data → ref PUT →
  marker PUT → HEAD marker.** Every PUT is to a new key
  (no overwrite anywhere). Step-by-step:

  1. **Upfront LIST** under `{partition}/{token}-` (only
     when `WithIdempotencyToken` is set). The LIST returns
     both `.parquet` and `.commit` siblings for every prior
     attempt with their server-stamped `LastModified`
     headers; pair them by id and apply `isCommitValid`. If
     any valid → reconstruct `WriteResult` from the LIST
     keys + filename parsing (`tsMicros` from filename
     suffix → `InsertedAt`; ref path computed from
     `data.LM + id + tsMicros + hiveKey`) and return
     success. **No HEADs at this stage** — same code path
     as `LookupCommit`.
  2. **Generate fresh attempt-id** = `{tsMicros}-{shortID}`
     (matching the existing `makeAutoID` shape: writer
     wall-clock microseconds + 8 random hex chars). The
     full per-attempt id is `{token}-{tsMicros}-{shortID}`
     under `WithIdempotencyToken`, or just
     `{tsMicros}-{shortID}` for token-less writes (auto-id
     case, today's behavior).
  3. **Projection markers PUT** strictly before data so any
     data file on S3 implies all R1 markers were written.
     (No change from prior plan; orphan markers under R1≠R2
     retries are tolerated per
     [index_write.go:106](index_write.go#L106).)
  4. **Data PUT to fresh path** `data/{partition}/{id}.parquet`.
     Unconditional — but the path is unique per attempt,
     so there's nothing to overwrite. Server stamps `data.LM`.
  5. **HEAD data file** to read `data.LastModified`. This
     value becomes the ref filename's `refTsMicros`
     (server-stamped, no writer-clock dependency) and is
     stamped into the marker's user metadata as `dataLM`
     (consumed only by stream reads).
  6. **Ref PUT** at the path computed from
     `(refPath, data.LM, id, dataTsMicros, hiveKey)` —
     also unique per attempt because `id` includes the
     attempt-id.
  7. **Marker PUT** at `{partition}/{id}.commit` with user
     metadata `{dataLM}` — also unique per attempt.
  8. **HEAD marker** post-PUT to read `marker.LastModified`
     and verify `marker.LM - data.LM < CommitTimeout`. On
     violation: return error. The data + ref + marker stay
     on S3 as orphans of this attempt; the reader's
     timeliness check filters them out, and the deferred
     operator-driven sweeper reclaims them later.

  **Why per-attempt-paths (no overwrite anywhere).**
  Read-after-overwrite is eventually consistent on
  multi-site StorageGRID, even at strong-global consistency
  — cross-site replication catches up asynchronously, and
  during the window a reader can observe a stale `data.LM`
  against a fresh marker, incorrectly rejecting on the
  timeliness check. Read-after-**new-write** is strongly
  consistent on every supported backend (AWS S3 since 2020,
  MinIO, StorageGRID at any consistency level). Writing
  every attempt to a fresh key sidesteps the entire class
  of overwrite-consistency races without any backend-specific
  carve-outs. No `If-None-Match: *` is needed (paths are
  unique per attempt by construction); no
  `s3:PutOverwriteObject` deny policy is needed
  (StorageGRID-side); the consistency-control header
  guidance for paired LIST/PUT operations stays.

  **What this removes from the prior plan:**
  - The `Data-PUT-412 retry shape` fast path (no 412 anymore).
  - `findExistingRef` and the scoped LIST (no retry-dedup
    branch — upfront LIST covers it).
  - `MaxRetryAge` (already removed in Phase 1) — but the
    follow-on cleanup of `findExistingRef`'s `created-at`
    bound is also gone, since `findExistingRef` itself is
    gone.
  - The StorageGRID `s3:PutOverwriteObject` deny requirement
    on the data subtree. README's STORAGEGRID.md shrinks.

  **What this enables that the prior plan didn't:**
  - **Same-token retries forever.** A `JobID`-scoped token
    can retry across an arbitrarily long S3 outage. Each
    attempt gets its own fresh path with fresh server-stamped
    `data.LM`; recovery is automatic.
  - **No `ErrCommitWindowExpired` surface.** The "retry
    within `CommitTimeout` of original `data.LM`" constraint
    disappears — each attempt is independent.
  - **No generation-suffix UX.** Callers pass the same
    `JobID` token across all retries; the library
    internally generates the per-attempt suffix.
  - **Uniform behavior across backends.** No carve-outs,
    no conditional logic, no backend-specific flags.

  **Costs accepted:**
  - **+1 LIST per `WithIdempotencyToken` write upfront**
    (no HEADs — LIST returns both `.parquet` and `.commit`
    LMs directly, enough to run the timeliness check and
    reconstruct `WriteResult`). ~50–150 ms on AWS S3,
    sub-millisecond on localhost MinIO. Non-token writes
    skip the upfront LIST entirely (their auto-generated
    paths can't collide).
  - **Orphan accumulation under `WithIdempotencyToken`.**
    Every failed attempt leaves data + ref + (possibly)
    marker as orphans. Previous designs accumulated at most
    one orphan per token (the original failed attempt);
    this design accumulates one per failed attempt. The
    deferred operator-driven sweeper becomes load-bearing
    for token writes, not just token-less ones. Bounded by
    `(retry rate) × (parquet size)`; sane orchestrator
    backoff makes this slow-growing.
  - **Ref-tree LIST cardinality.** Stream-side `Poll`'s
    LIST returns more entries (one per attempt instead of
    one per logical write) until the sweeper reclaims
    orphans. Each entry passes through the marker
    timeliness check; only valid ones make it to consumers.
    No correctness impact, just LIST-page count growth.

  **One subtle race worth naming.** Two valid commits for
  the same token can briefly coexist if a slow attempt's
  marker PUT lands after a faster retry's marker PUT. Both
  attempts wrote bit-identical bytes (deterministic
  encoding under `WithIdempotencyToken`); both pass the
  reader's timeliness check. Snapshot reads pair both data
  files with their respective markers and emit *both*
  (different filenames, both LIST-visible). Stream reads
  emit both refs. Application sees doubled rows. **Reader-side
  dedup with `EntityKeyOf + VersionOf` collapses this**
  (same record → same `(entity, version)` → kept once).
  Without reader dedup, the application sees duplicates on
  near-concurrent retry overlap. README's
  `WithIdempotencyToken` section calls this out: "configure
  reader-side dedup unless you can guarantee retries never
  overlap with the original attempt's in-flight PUTs."
- **Fencing mechanism (under `WithFencedCommit()`): LIST +
  `LastModified` comparison, both sides server-stamped.** At
  commit-marker PUT time, LIST the partition's `.commit`
  files and reject our own commit if any has a `LastModified`
  greater than **our `data.LM`** (the server-stamped value
  we already have from the post-data HEAD). Both sides of
  the comparison are S3-server-stamped, so the comparison
  is skew-free.

  `data.LM` is the right reference because it's a stable
  server-side anchor for our write's logical start: a slow
  writer whose marker PUT lands much later still rejects
  markers from jobs whose data PUT landed after ours. Under
  per-attempt-paths, each retry has its own fresh `data.LM`
  on its own fresh data path; the fence comparison uses the
  current attempt's `data.LM`, so "did anything land
  server-side after our data PUT" is the right semantics.

  The LIST short-circuits on the first newer marker, so
  rejection is fast; successful fencing pays a full-partition
  scan (typically 1 LIST page; 2–3 on dense partitions).
  Cost: one extra LIST per fenced write (~$0.005/1000 LIST
  requests, +50–100 ms latency). Best-effort under the
  LIST↔PUT race window — acceptable because the sequential-
  writes constraint already rules out true concurrency.
- **Read-path wiring centralized via a shared
  `isCommitValid` helper.** Single function decides validity
  from `dataLM`, `markerLastModified`, and `commitTimeout`
  — used by every read entry point that cares. Pure
  server-LM comparison; no clock-skew sensitivity.
  - **`listCommittedDataFiles`** replaces `listDataFiles`
    (rename + modify in place). LIST already returns both
    `.parquet` and `.commit` siblings under the partition
    prefix; the helper pairs them, drops unpaired data
    files, and applies `isCommitValid` using the LIST-page
    `data.LastModified` directly (no need to read the
    marker's metadata-stamped `dataLM` here, since LIST
    gives us the authoritative current value). Used by
    `Read`, `ReadIter`, and `BackfillProjection` (which
    still scans partitions, just now committed-only).
  - **Stream side:** the existing public `Poll` becomes
    private (`poll`, used internally only). `PollRecords`
    and `ReadRangeIter` continue to call it; both HEAD the
    commit marker per ref to get `marker.LastModified` plus
    the metadata-stamped `dataLM`, then apply
    `isCommitValid` *before* the data GET — saves the GET
    on uncommitted data. The data file's actual
    `LastModified` isn't needed for the timeliness check
    on this path (we trust the marker's metadata-stamped
    `dataLM`, which the writer captured via post-data HEAD
    on the original write). Documented inline that "marker
    existence ≠ validity" so future readers don't drop the
    timeliness check.
  - **`ProjectionReader.Lookup` is unchanged.** It LISTs
    projection markers and parses keys for column values —
    never fetches data files (see
    [index_read.go:16](index_read.go#L16): "Lookup issues
    LIST only — no parquet reads"). Projection markers are
    written immediately and visible immediately; orphan
    tolerance is the existing contract and is unaffected by
    the commit-marker design.
  - **`LookupCommit`** is **a LIST under
    `{partition}/{token}-`** — no HEADs. The LIST returns
    both `.parquet` and `.commit` siblings with their
    server-stamped `LastModified` headers; pair by id,
    apply `isCommitValid(data.LM, marker.LM,
    commitTimeout)`, return the first valid commit's
    reconstructed `WriteResult`. Reconstruction comes from
    LIST keys + filename parsing — no marker metadata read
    required. Returns `(WriteResult{}, false, nil)` if no
    valid commit. The same primitive backs `Write`'s
    upfront-LIST dedup gate (see "Write sequence" above) —
    same semantics, same result type, just inlined into the
    write path so callers don't have to do `LookupCommit`
    + `Write` themselves.
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
    after the commit marker lands within `CommitTimeout`."
    Successful `Write` now means data + ref + commit marker
    all landed *and* the post-marker-HEAD verified the
    timeliness check. Under `WithIdempotencyToken`, every
    attempt writes to a fresh per-attempt path; failed
    attempts leave per-attempt orphans for the deferred
    sweeper to reclaim.
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
  - **Add new invariant entries:**
    - Marker timeliness check is server-LM-based (no
      clock-skew sensitivity).
    - Writer/reader agreement on `CommitTimeout` and
      `MaxClockSkew` is enforced via the persisted
      `_config/commit-timeout` and `_config/max-clock-skew`
      objects; persisted-config objects are the source of
      truth.
    - Deployment must satisfy `|reader↔server skew| ≤
      MaxClockSkew` for stream consumers to maintain atomic
      visibility (the writer's clock is not in the protocol
      — `refTs` is server-stamped `data.LM`).
    - **Per-attempt-unique paths under
      `WithIdempotencyToken`.** Every attempt writes to a
      fresh path
      (`{token}-{tsMicros}-{shortID}`); no PUT in the write
      path overwrites an existing key. Upfront LIST under
      `{partition}/{token}-*.commit` is the dedup gate.
      Sidesteps eventual-consistency exposure on overwrites
      (multi-site StorageGRID).
    - **Reader-side dedup recommended under
      `WithIdempotencyToken`.** Near-concurrent retry
      overlap can produce two valid commits; configure
      `EntityKeyOf + VersionOf` to collapse them.
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
  sibling marker has `LastModified > our data.LM`). Reach
  is precise:
  - **Catches:** retries (or stale fast-path resurrections)
    where another writer's commit landed in the meantime.
    Our `data.LM` is the server-stamped time of our current
    attempt's data PUT (each retry under
    `WithIdempotencyToken` writes its own per-attempt data
    file with its own server-stamped `data.LM`); any sibling
    marker with `LastModified > data.LM` therefore reflects
    a write that happened after our current attempt's data
    landed, and we yield to it. Token A's resurrected
    half-finished write after Token B has committed is
    refused.
  - **Doesn't catch:** the case where Token A's data PUT
    landed *after* Token B's commit, then A continues
    writing its ref + marker. Both A and B now have
    `data.LM > B's marker.LM`, so A's fence sees no newer
    sibling and proceeds — the application sees A's write
    overlay B's, even though A logically started before B.
    Application's re-read-on-retry pattern covers
    correctness here.
  - **Doesn't catch (constraint, not bug):** true concurrent
    writers — both could LIST and find no newer marker, both
    PUT. Concurrent writers are excluded by the
    sequential-writes-per-partition constraint; this fence
    is the in-band backstop, not OCC.

  Required for delta workloads — pair with the application
  pattern of re-reading state on each retry.
- **Same-token retry remains safe under both modes.** The
  upfront LIST under `{partition}/{token}-*.commit` is the
  dedup gate: if a valid commit exists for the token, the
  retry returns success without writing anything new,
  regardless of how long ago the original landed. If no
  valid commit exists, the retry generates a fresh attempt-id
  and writes its own per-attempt data + ref + marker triple
  with fresh server-stamped `data.LM` — independent of any
  earlier attempt's state. `WithFencedCommit()` is layered
  on top: even on a fresh retry's marker PUT, the fence
  rejects if any newer sibling marker has landed since our
  current attempt's `data.LM`.

## Open questions

(All open questions resolved — see "Decided design choices"
and "Decided constraints" above.)

## Implementation order

Each phase is one coherent change that compiles, passes the
four-gate verification from [CLAUDE.md](CLAUDE.md) (`go vet
-tags=integration ./...`, `go test -count=1 ./...`, `go test
-tags=integration -count=1`, `golangci-lint run`), and leaves
behaviour usable end-to-end. Breaking changes are taken at
phase boundaries — no backwards-compat shims, no deprecation
shadows, no flags toggling old vs new behaviour. Order is
chosen so that user-visible correctness lands as early as
possible and later phases never need to revisit earlier ones.

### Phase 1 — Pre-v1 cleanups (independent of marker work)

Three breaking removals/renames that shrink the surface every
later phase has to touch. All three are independent and could
ship in any sub-order; folding them into one phase signals
"clear the underbrush before starting". (Already shipped at
`b4ffecf` and `9ce350a`; documented here for completeness.)

- **Remove `DisableRefStream`** from `Config[T]` and
  `S3TargetConfig`; delete every conditional in writer, snapshot
  reader, stream reader, and S3Target plumbing. Refs are always
  written.
- **Remove `MaxRetryAge`** and its handling. Refactored
  `findExistingRef` to use the data file's `created-at` user
  metadata as the lower bound on the ref LIST. *Note:*
  `findExistingRef` itself is removed in Phase 4 (the upfront
  LIST replaces the entire retry-dedup branch); this Phase 1
  cleanup is the intermediate state.
- **Rename Index → Projection** across the codebase: `IndexDef`
  → `ProjectionDef`, `IndexReader` → `ProjectionReader`,
  `IndexLookupDef` → `ProjectionLookupDef`, `BackfillIndex` →
  `BackfillProjection`, the `_index/` S3 prefix → `_projection/`,
  internal helpers, doc strings, `dashboards/s3store.json`
  labels. Every later phase uses the new names from the start.

### Phase 2 — Persisted `CommitTimeout` + `MaxClockSkew`

Make writer/reader agreement load-bearing before any code
starts depending on the values. Two persisted knobs (see
"Decided design choices" → "Two-knob time budget…" for the
full rationale and the math behind the decomposition).

- Define `<prefix>/_config/commit-timeout` and
  `<prefix>/_config/max-clock-skew` objects: each a plain-text
  body parsed by `time.ParseDuration`. Floor `CommitTimeout
  ≥ 50ms`, `MaxClockSkew ≥ 0`. Reject below the floor at
  parse time.
- `S3Target` initialization GETs both objects once and stamps
  the resolved values on the Target along with the derived
  `SettleWindow = CommitTimeout + MaxClockSkew`. Construction
  fails on missing or unparseable.
- Accessors: `S3Target.CommitTimeout()`, `S3Target.MaxClockSkew()`,
  `S3Target.SettleWindow()` (the last is just a convenience
  for the call sites that want the sum). No `SettleWindow`
  field on `S3TargetConfig` or `Config[T]`.
- Add a `fixture_test.go` helper that PUTs both config objects
  on bucket setup; integration tests call it before
  constructing any Target.
- README: copy-pasteable Python `boto3` snippet operators run
  once when initializing a new prefix; ships sensible starting
  values so the copy-paste path is correct out-of-the-box.

Required before Phases 3+ — the writer's `CommitTimeout`
budget and the reader's timeliness check both consume
`CommitTimeout` off the Target; Poll's refCutoff consumes
`SettleWindow`.

### Phase 2 redesign — single-knob `SettleWindow` → two-knob `CommitTimeout` + `MaxClockSkew`

Phase 2 originally landed a single persisted knob:
`<prefix>/_config/settle-window`, used by both the writer's
`SettleWindow / 2` PUT budget and the reader's marker
timeliness check (`marker.LastModified - data.LastModified <
SettleWindow`), with a hard 2s floor justified as
"client/server clock skew dominates the timeliness comparison
below 2s". A review uncovered two problems with that shape.

**Problem 1: the floor's stated rationale is wrong.** The
timeliness check compares two S3-server-stamped timestamps —
clock skew literally cannot affect it. The 2s floor is doing
something, but it isn't protecting the timeliness check.

**Problem 2: a real clock-skew vulnerability sits elsewhere.**
Poll's `refCutoff = now - SettleWindow` uses the *reader's*
wall clock against `refTsMicros`. Under the current Phase 4
design, `refTsMicros` is server-stamped (`= data.LM`), so
the comparison is reader↔server. If reader's clock is `D`
ahead of the server and `D > SettleWindow`, the cutoff
overtakes a just-published ref before the writer has had
time to land its marker server-side — reader skips the ref
(HEAD finds no marker), advances offset past it, marker
lands on time, snapshot reads see the data, stream reads
have already moved on. Atomic-visibility broken, silent
data loss on the stream side. (Under the *original* Phase 4
proposal — writer-stamped `refTsMicros` — the same
vulnerability existed but the relevant skew bound was
writer↔reader; switching to server-stamped `refTs`
shrinks it to reader↔server.)

The single `SettleWindow` knob conflates two distinct
budgets — server-side commit latency and client-side clock
skew — and applies them to the wrong checks.

**The fix: split the knob.**

| Knob | Semantic | Used by |
|---|---|---|
| `CommitTimeout` | Max server-time gap between data PUT and marker arrival | Reader's timeliness check `marker.LM - data.LM < CommitTimeout`; writer's same-check post-marker-HEAD verification |
| `MaxClockSkew` | Operator's assumed bound on **reader↔server** wall-clock divergence | Reader's `refCutoff` only |

`SettleWindow` is **derived**: `CommitTimeout +
MaxClockSkew`. Used in exactly one place: Poll's
`refCutoff`.

**The math the split enables.** Reader includes a ref when
`α ≥ SettleWindow - D` (where `α` is server-time elapsed
since publication and `D` is the reader↔server skew).
Sizing `SettleWindow = CommitTimeout + MaxClockSkew` means
at worst-case skew (`D = +MaxClockSkew`), `α ≥ CommitTimeout`
— enough server-time has elapsed that the marker either
landed (visible, both paths agree) or didn't (timeliness
check rejects on both paths). **The contract holds for any
`|reader↔server skew| ≤ MaxClockSkew`.** Skew exceeding
`MaxClockSkew` is an explicit operator-contract violation,
not a hidden trap.

**Knock-on effects:**

- **Floors relax dramatically.** `CommitTimeout` only needs
  to cover real PUT latency (50ms is a sane floor — a few ×
  typical localhost-MinIO PUT). `MaxClockSkew ≥ 0` (zero is
  valid on tightly-clocked deployments). `SettleWindow` no
  longer has a hard floor — it's whatever the sum produces.
- **Tests get fast.** Integration tests can seed
  `CommitTimeout=100ms, MaxClockSkew=50ms` →
  `SettleWindow=150ms`. The 2.1s sleeps shrink to ~250ms.
  No correctness loss because localhost MinIO has microsecond
  clock skew and millisecond PUT latency.
- **Phase 4's writer enforcement changes shape.** Was
  `SettleWindow / 2` deadline for ref + marker collectively;
  becomes a post-marker-HEAD check that `marker.LM - data.LM
  < CommitTimeout` (same check the reader runs). On
  violation: explicit error. No deadline math, no `/2`.
- **Phase 6's timeliness check tightens.** Was `marker.LM -
  data.LM < SettleWindow`; becomes `< CommitTimeout`. More
  accurate — `MaxClockSkew` was never relevant to a
  server-LM comparison.
- **The CommitTimeout floor doesn't really need to exist
  for correctness.** It's a sanity check (1ms `CommitTimeout`
  would just make every write fail its budget). 50ms is a
  reasonable "no realistic backend can finish a PUT this
  fast, so anything below is operator confusion".

**What changes mechanically in Phase 2:**

- `_config/settle-window` → `_config/commit-timeout` +
  `_config/max-clock-skew` (drop the old object's path
  entirely, no migration shim — pre-v1).
- `SettleWindowFloor` (= 2s) → `CommitTimeoutFloor` (= 50ms)
  + `MaxClockSkewFloor` (= 0). Old constant removed.
- `S3Target.SettleWindow()` accessor stays (returns the
  derived sum); add `S3Target.CommitTimeout()` and
  `S3Target.MaxClockSkew()`.
- Fixture helper `SeedSettleWindow(prefix, value)` →
  `SeedTimingConfig(prefix, commitTimeout, maxClockSkew)`.
- Integration tests' `testSettleWindow` constant → split into
  `testCommitTimeout` and `testMaxClockSkew`, with
  `testSettleWindow` derived (kept for the sleep-timeout
  call sites).
- README's "Initializing a new dataset" snippet PUTs both
  objects; explains the math relating them to `SettleWindow`.

### Phase 3 — Write path: markers-first ordering

Move existing projection-marker PUTs to **before** the data
PUT (current order is data → markers → ref). Isolated reorder
plus a test that asserts: after a forced failure between the
markers PUT and the data PUT, the data file is absent. Any
data file on S3 now implies all R1 markers landed.

Precondition for Phase 4: orphan markers from R1≠R2 retries
are tolerated per the existing `index_write.go:106` contract,
so we can rely on "data file present implies its R1 markers
landed" without any post-retry cleanup work.

### Phase 4 — Write path: upfront-LIST + per-attempt-paths + post-PUT HEADs

The core write-side change. After this phase the new
write sequence is in place end-to-end: upfront-LIST dedup,
fresh per-attempt-id every write, never-overwrite PUTs,
post-PUT HEADs for correctness verification. Readers still
don't gate on the marker yet (Phases 5–6), so behaviour
from the user's perspective is unchanged in this phase
apart from the new extra round-trips.

**Sequence under `WithIdempotencyToken`:**

1. **Upfront LIST** under `{partition}/{token}-`. The
   response carries both `.parquet` and `.commit` siblings
   for every prior attempt with their server-stamped
   `LastModified` headers. Pair by id, apply
   `isCommitValid(data.LM, marker.LM, CommitTimeout)`. If
   any valid → reconstruct `WriteResult` from LIST keys +
   filename parsing (`tsMicros` from filename suffix →
   `InsertedAt`; ref path computed from `data.LM + id +
   tsMicros + hiveKey`) and return success. **No HEADs at
   this stage** — same primitive as Phase 7's
   `LookupCommit`.
2. **Generate fresh attempt-id** =
   `{tsMicros}-{shortID}` via the existing `makeAutoID`
   helper. Full per-attempt id =
   `{token}-{tsMicros}-{shortID}`.
3. **Projection markers PUT** (unchanged from Phase 3).
4. **Data PUT** to `data/{partition}/{id}.parquet`.
   Unconditional, but the path is unique per attempt by
   construction — nothing to overwrite. Server stamps
   `data.LM`.
5. **HEAD data file** to read `data.LastModified`. Use
   this value as `refTsMicros` in the ref filename and as
   the `dataLM` field in the marker metadata (consumed by
   stream reads only).
6. **Ref PUT** at the path computed from
   `(refPath, data.LM, id, dataTsMicros, hiveKey)` —
   unique per attempt because `id` includes the
   attempt-id.
7. **Marker PUT** at `{partition}/{id}.commit` with user
   metadata `{dataLM}` — unique per attempt.
8. **HEAD marker** to read `marker.LastModified`. Verify
   `marker.LM - data.LM < CommitTimeout`. On violation:
   return error. The orphan triple stays on S3, filtered
   by every read path's timeliness check; sweeper
   reclaims later.

**Sequence without `WithIdempotencyToken`:** skip step 1
(no token to LIST against; auto-generated attempt-ids
can't collide). Skip the token prefix in step 2 (id =
`{tsMicros}-{shortID}`, today's `makeAutoID` shape
unchanged). Steps 3–8 are identical.

**Removals from prior code:**

- `If-None-Match: *` on the data PUT → no longer needed
  (paths are unique per attempt). The `putIfAbsent`
  helper stays in `target.go` for any other call sites
  but isn't called for data writes.
- `findExistingRef` and the ref-stream scoped LIST →
  entire function removed; the upfront LIST replaces it.
- Data-PUT-412 retry-dedup branch → gone with the
  conditional PUT.
- StorageGRID `s3:PutOverwriteObject` deny requirement on
  the data subtree → README's STORAGEGRID.md drops this
  policy.

**Tests added in this phase:**

- Upfront-LIST hits: same-token retry returns success
  without re-issuing data/ref/marker PUTs.
- Per-attempt-path uniqueness: assert two retries with
  the same token write to *different* data paths (verify
  via LIST after both complete).
- Stale-marker rejection: a synthetic marker with
  `marker.LM - dataLM ≥ CommitTimeout` triggers the
  post-PUT-HEAD verify and returns error.
- Long-delayed retry: write a record, sleep > CommitTimeout,
  retry with same token — upfront LIST finds no valid
  commit (original is stale by timeliness), retry writes
  to a fresh attempt path with fresh `data.LM` and
  succeeds.
- Near-concurrent retry overlap: two retries with the
  same token both succeed (each at its own attempt
  path); both pass the upfront-LIST gate (LIST sees no
  valid commit yet at that moment); both write valid
  commits. Assert reader-side dedup with
  `EntityKeyOf+VersionOf` collapses the duplicate
  records.

**`WriteResult` shape stays exactly as today.** Its
`Offset`, `DataPath`, `RefPath` now embed the per-attempt
id (`{token}-{tsMicros}-{shortID}` instead of `{token}`)
but the API surface is unchanged — these are opaque
strings.

**Docstring updates owned by this phase:** `Write`
(at-least-once boundary, upfront-LIST dedup gate,
per-attempt-paths semantics, post-PUT verification,
same-token-retries-forever guarantee, reader-side dedup
recommendation under `WithIdempotencyToken`).

### Phase 4 fix-up — server-LM precision + ref-format restructure

Three issues surfaced when Phase 4's integration suite ran on
MinIO. Root causes were always present in the design; unit tests
didn't catch them. All resolved in-place after Phase 4 shipped.

- **HEAD vs LIST `LastModified` precision mismatch.** S3's HEAD
  carries `Last-Modified` as an HTTP-date (RFC 1123, second
  precision); MinIO's LIST returns it at millisecond precision
  in the XML body. Same object, different values via different
  APIs. The upfront-LIST dedup gate's reconstructed
  `refTsMicros` disagreed with the original write's HEAD-derived
  `refTsMicros` — same logical commit, different ref-key bytes,
  surfaced as "drifted RefPath" on retry.
  
  **Fix:** new `truncLMToSecond` helper, applied where
  `listCommitsAtPrefix` reads LIST entries, normalizes
  everything to second precision (which is what HEAD already
  gives us *and* what AWS S3 stores natively — MinIO's
  millisecond LIST output is non-standard). Cross-source
  agreement restored.

- **`CommitTimeoutFloor` bumped from 50ms to 1s.** With
  second-precision LMs everywhere, two PUTs straddling a
  wall-clock second boundary appear 1s apart even when both
  completed in milliseconds. The post-marker timeliness check
  (`marker.LM - data.LM < CommitTimeout`) would reject those
  writes outright at any value below 1s. README and the
  per-CommitTimeout-knob `_config/commit-timeout` documentation
  recommend 2s or higher to leave headroom for slow PUTs on top
  of the second-boundary effect.

- **Ref filename restructured** from
  `{refTsMicros}-{id}-{dataTsMicros};{hive}.ref` to
  `{dataLM}-{tsMicros}-{shortID}-{token};{hive}.ref` (token
  omitted when empty). Two motivations:

  1. **Stable sub-second ordering for same-second writes.**
     `dataLM` is the lex-primary key (Poll's `refCutoff` only
     inspects the first 16 chars — second-precision is fine
     there); `tsMicros` (writer wall-clock at write-start,
     microsecond precision) becomes the within-second
     tiebreaker so refs from same-second writes stream in
     stable sub-second order. Stream consumers don't need
     server-completion order within a second — atomic
     visibility comes from the marker, not the ref's lex
     position.
  2. **Position-parseable layout.** Three fixed-width fields
     (16-digit dataLM + 16-digit tsMicros + 8-hex shortID = 42
     chars + dashes) followed by an optional `-{token}` tail.
     The `validateIdempotencyToken` rule "no `;`" closes the
     last way a token could break the parse.

  Data file format **unchanged** at
  `{token}-{tsMicros}-{shortID}.parquet` so the upfront LIST
  under `{partition}/{token}-` keeps its prefix-scope.

- **`validateIdempotencyToken` rejects `;`** (in addition to
  the existing `/` and `..` rejections). The ref filename's
  `;` separator splits the header from the PathEscape'd Hive
  key; a token containing `;` would split the ref filename at
  the wrong position.

**What this fix-up doesn't touch:** the writer's clock stays
out of the protocol's *correctness* contract — `dataLM` (server-
stamped, second-precision) is still the timeliness anchor and
the lex-primary sort key. `tsMicros` (writer wall-clock) only
affects within-second sort order, which has no correctness
implications.

### Phase 5 — Read path: snapshot gating

Wire snapshot reads through the marker. After this phase, the
snapshot side enforces atomic per-file visibility end-to-end.

- Add `isCommitValid(dataLM, markerLM time.Time, commitTimeout
  time.Duration) bool` shared helper. Implements `markerLM -
  dataLM < commitTimeout`; both timestamps server-stamped, no
  clock-skew sensitivity.
- Rename `listDataFiles` → `listCommittedDataFiles` (modify in
  place). LIST on the partition prefix returns both `.parquet`
  and `.commit` siblings; pair them, drop unpaired data files,
  apply `isCommitValid` to the pairs.
- Switch `Read`, `ReadIter`, and `BackfillProjection` to the
  new helper.
- Emit `s3store.read.uncommitted_data{reason=missing|stale,
  method=...}` from the filter.
- Docstring updates owned by this phase: `Read`, `ReadIter`,
  `BackfillProjection` (commit-marker gating; explanation that
  uncommitted data is filtered, not surfaced as an error).

### Phase 6 — Read path: stream gating

Mirror the gating on the change-stream side. After this phase
the upgraded README Guarantees block (landing in Phase 10) is
upholdable end-to-end.

- Demote public `Poll` to package-private `poll`. `PollRecords`
  and `ReadRangeIter` become the public stream entry points.
- `poll` continues to use `refCutoff = now - SettleWindow`
  (where `SettleWindow = CommitTimeout + MaxClockSkew`). With
  the marker now load-bearing, refCutoff becomes purely an
  optimization — it bounds which refs we HEAD-check; the actual
  visibility gate is the marker timeliness check on server-LM.
  The comparison is reader↔server (refTs is server-stamped =
  data.LM, reader's `now()` is reader-clock), so `MaxClockSkew`
  bounds the reader↔server pair only.
- For each polled ref: HEAD the commit marker → both
  `marker.LastModified` (response header) and `dataLM` (user
  metadata). If the marker is missing, emit
  `uncommitted_data{reason=missing}` and skip — no GET on the
  data file (saves the round-trip on uncommitted data). If
  present, apply `isCommitValid(dataLM, marker.LastModified,
  commitTimeout)` *before* the data GET; on stale, emit
  `uncommitted_data{reason=stale}` and skip — again no GET.
  On valid, GET the data file to decode parquet.
- Inline comment on `poll`: "marker existence ≠ validity; the
  timeliness check runs against the marker's metadata-stamped
  `dataLM` in the public callers. Skew-safety on the
  `refCutoff` side is delivered by the cutoff using
  `CommitTimeout + MaxClockSkew` (reader↔server skew bound)."
- Docstring updates owned by this phase: `PollRecords`,
  `ReadRangeIter` (commit-marker gating, the `CommitTimeout +
  MaxClockSkew` lag, pre-GET timeliness filter).

### Phase 7 — Public `LookupCommit` API

Closes the same-token-zero-rows gap: a retry that computes
empty input never reaches the upfront-LIST-in-Write path
because there's no `Write` call. Also useful as a
caller-facing "probe before recomputing" primitive.

- `LookupCommit(ctx, partitionKey, token) → (WriteResult, bool,
  error)`.
- Implementation: **single LIST** under
  `{partition}/{token}-`. Pair `.parquet` with `.commit`
  by id, apply `isCommitValid(data.LM, marker.LM,
  commitTimeout)` from the LIST entries' `LastModified`
  headers, return the first valid commit's reconstructed
  `WriteResult` (LIST keys + filename parsing produce
  `DataPath`, `InsertedAt`, `RefPath`, `Offset`). No HEADs
  at all. Typical case: ~50–150ms on AWS S3.
- The same primitive backs Phase 4's upfront-LIST dedup
  inside `Write`: one code path, one set of semantics.
- Public docstring: "probe before recomputing on retry".

### Phase 8 — Opt-in fencing: `WithFencedCommit`

The OCC-adjacent backstop for delta workloads.

- Add `WithFencedCommit()` write option.
- Just before the commit-marker PUT under the option: LIST the
  partition's `.commit` siblings and reject (return a sentinel
  error) if any has `LastModified > our data.LM`. Both sides
  server-stamped — skew-free comparison.
- Post-rejection state: data PUT and ref PUT have already
  succeeded; only the commit-marker PUT was refused. Both read
  paths gate on the marker, so the orphan stays invisible. No
  cleanup; the operator-driven sweeper from the deferred
  follow-up reclaims later.
- Add `s3store.write.fenced_rejections` counter.
- Docstring updates owned by this phase: `WithFencedCommit`
  (what it catches and doesn't catch — echo the bullets from
  "Decided constraints"; the application's re-read-on-retry
  pattern).

### Phase 9 — Dashboards

Add panels to `dashboards/s3store.json` for
`s3store.read.uncommitted_data` (split by `reason`) and
`s3store.write.fenced_rejections`, mirroring the existing
`read.malformed_refs` / `read.missing_data` panels. No code
change.

### Phase 10 — Cross-cutting documentation

Method-level docstrings are owned by the phases that change
each method's behaviour (Phases 4–8, called out per phase).
This phase covers the cross-cutting docs that depend on the
final state of every prior phase.

- README's Guarantees section: replace the existing "No atomic
  write visibility" bullet with the "Atomic per-file visibility
  on both paths" + Time semantics + Same-token retries forever
  + Failure modes + Per-file only block from "Decided design
  choices".
- README's STORAGEGRID.md: drop the
  `s3:PutOverwriteObject` deny policy from the required setup
  (Phase 4 made it unnecessary). The
  `Consistency-Control` header guidance stays — that's still
  needed for paired LIST/PUT operations.
- **README — new "Write path / Read path" section** with the
  S3 round-trip tables for each path (write, snapshot read,
  stream read, `LookupCommit`, `ProjectionReader.Lookup`),
  showing the S3 op + key + per-step rationale. Same shape
  as the overview table in this plan's discussion. Each
  step's "why" cell ties back to a guarantee — atomic
  visibility, server-LM skew safety, same-token-retries-forever,
  cross-backend uniformity. A reader scanning this section
  should be able to predict the next round-trip and explain
  *why* it's there without consulting the code.
- **README — new "Settle window math" subsection** under
  the "Settle window" section. Spell out:
  - Why `SettleWindow = CommitTimeout + MaxClockSkew` and
    not just `CommitTimeout`: the reader's `refCutoff = now -
    SettleWindow` compares reader-time to server-stamped
    `refTs`. Worst-case `D = +MaxClockSkew` reader↔server
    skew means the cutoff only crosses `refTsMicros` after
    `α ≥ CommitTimeout` of server-time has elapsed — exactly
    the marker-landing budget. Both paths converge on the
    same boundary.
  - Why server-time everywhere we can: the reader's
    timeliness check (`marker.LM - data.LM < CommitTimeout`)
    uses two server-stamped values, so it's free of
    client/server clock skew and produces the same answer
    for every reader regardless of clock state. The writer
    enforces the same check via post-marker-HEAD with the
    same comparison — symmetric, no boundary cases. The
    only place wall-clock matters is `refCutoff`, where
    it's bounded by `MaxClockSkew`.
  - Why the writer's clock is not in the protocol: under
    server-stamped `refTs` (= `data.LM`), the only skew
    pair the protocol depends on is reader↔server. NTP-syncing
    against the same source as the S3 cluster gives the
    operator a tight `MaxClockSkew` claim with no
    writer-side calibration needed.
  - A worked example: `CommitTimeout=5s, MaxClockSkew=1s →
    SettleWindow=6s`. A `Poll` at `now=T` returns refs
    with `refTsMicros ≤ T − 6s`. Even if the reader's
    clock is 1s ahead of the server, those refs were
    published ≥ 5s ago in server-time, so any marker that
    was going to land has landed.
- [CLAUDE.md](CLAUDE.md) invariants:
  - Refine "At-least-once at the storage layer" — data
    durable after data PUT, visible after marker. Note:
    under `WithIdempotencyToken`, every attempt writes to
    a fresh per-attempt path; retries always succeed on
    fresh state regardless of how long ago the original
    landed. Failed attempts leave per-attempt orphans
    (data + ref + possibly marker) that the deferred
    sweeper reclaims.
  - Refine "Read stability — no library-driven deletion" —
    flag that marker presence makes future cleanup safe (the
    deferred follow-up below).
  - Add: marker timeliness check (server-LM-based, no
    clock-skew sensitivity); writer/reader agreement on
    `CommitTimeout` and `MaxClockSkew` enforced via the
    persisted `_config/commit-timeout` and
    `_config/max-clock-skew` objects; persisted-config
    objects are the source of truth; deployment must
    satisfy `|reader↔server skew| ≤ MaxClockSkew` for
    stream consumers to maintain atomic visibility (the
    writer's clock is not in the protocol — `refTs` is
    server-stamped `data.LM`).
  - Add: under `WithIdempotencyToken`, every attempt writes
    to a per-attempt-unique path
    (`{token}-{tsMicros}-{shortID}`); the upfront LIST
    under `{partition}/{token}-*.commit` is the dedup gate;
    no PUT in the write path overwrites an existing key,
    sidestepping multi-site eventual-consistency exposure
    on overwrites.
  - Add: under `WithIdempotencyToken`, near-concurrent
    retry overlap can produce two valid commits with
    bit-identical records; reader-side dedup with
    `EntityKeyOf + VersionOf` is **recommended** to
    collapse them on read.

## Deferred follow-ups

- **Library-driven cleanup of uncommitted / stale data.**
  The marker design relaxes the "library can't delete"
  invariant in [CLAUDE.md](CLAUDE.md): committed vs
  crashed-mid-write is now distinguishable (marker present +
  timely vs missing or stale), and no reader can have
  observed an uncommitted file as visible (both paths gate
  on the marker). The library could therefore safely delete:
  - Data files older than `CommitTimeout` with no paired
    marker (the marker can no longer land in time → permanent
    orphan).
  - Data + commit pairs where the marker fails the
    timeliness check (already filtered from every read path,
    no value in keeping them).

  Cleanup matters more under per-attempt-paths than under
  prior designs: every failed attempt under
  `WithIdempotencyToken` leaves a fresh per-attempt orphan
  triple (data + ref + possibly marker). Bounded by
  `(retry rate) × (parquet size)` per token; sane
  orchestrator backoff makes this slow-growing, but in a
  retry storm orphans accumulate quickly. Token-less writes
  also produce orphans on partial failures, but typically
  at lower volume.

  Race considerations are benign: writer retries under
  `WithIdempotencyToken` produce fresh commits at fresh
  paths regardless of whether the orphan was cleaned; late
  server-side PUTs that land after a delete write to a
  deleted path with no visibility consequence; in-flight
  readers don't GET uncommitted files because they filter
  first. Three approaches for *when* (on-read, on-write,
  background sweeper) — to be decided based on real-workload
  numbers from the `uncommitted_data` metric. Deferred so
  the marker work's correctness review isn't tied to the
  cleanup work's correctness review.

