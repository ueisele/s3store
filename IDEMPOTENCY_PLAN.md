# Idempotent writes — implementation plan

Goal: reduce duplicate data on retry by making the write path idempotent end-to-end, and give readers deterministic emission order for correctness under retry. **Six phases** — two prerequisites (Phase 1 + Phase 1.5), one standalone (Phase 2), the main feature (Phase 3), its read-side companion (Phase 3b), and one optional follow-up (Phase 4). Each can land as a separate commit.

## Background

Today's write order is `data → marker → refs`. At-least-once is the contract: a retry after a partial failure re-runs the whole write and creates duplicate data + markers + refs, all with fresh keys because every key component derives from `time.Now()` + random UUID.

Three independent improvements combine to give exactly-once record semantics:

1. **Replica dedup** (Phase 1.5) collapses records that share `(entityKey, version)` regardless of `WithHistory`. Correctness primitive — needed whenever retries are possible.
2. **Transparent internal retries** (Phase 2) for transient S3 errors inside one call. No API change.
3. **Caller-driven idempotency** (Phase 3) via `WithIdempotencyToken(token, maxRetryAge)`. Retries of the same logical write produce deterministic data paths so overwrite-prevention triggers and no body is re-uploaded. Reduces physical duplicates at the storage layer but does not on its own guarantee exactly-once at the consumer (that's Phase 1.5's job).

The filename-embedded timestamp (`{tsMicros}-{shortID}.parquet`) currently serves two purposes: populating the optional `InsertedAtField` on records at read time, and acting as the dedup tiebreaker in `VersionOf(T, time.Time)` when no version column is set. Phase 1 repoints both to the writer-populated `InsertedAtField` parquet column (with S3 `LastModified` as the fallback), drops filename parsing for timestamps, and makes emission order explicit via a record/metadata-content sort. On-disk filename shape is unchanged. Phase 3 then adds `WithIdempotencyToken` — when set, the caller's token replaces the library's default `{tsMicros}-{shortID}` id, giving callers deterministic filenames for retry-safe writes without forcing any format migration.

---

## Phase 1 — Column-based `InsertedAtField` + separate ref-publish `tsMicros` + explicit sort order

**Outcome**: `InsertedAtField` becomes a real, writer-populated parquet column — identical value across every read path (s3parquet Read/ReadIter/PollRecords, s3sql Read/PollRecords). Ref filenames get their `tsMicros` captured *just before* the ref PUT instead of at write-start, which tightens `SettleWindow` correctness. Emission order becomes explicit via a two-tier sort. `DefaultVersionOf` stays, repointed to use the column's value (or `LastModified` as fallback when the column isn't configured).

### Why column, not `LastModified` or `x-amz-meta` alone

Earlier iterations considered:

- **Filename `tsMicros`** (pre-Phase-1): works today but Phase 3 removes `tsMicros` from the data filename for idempotency.
- **LIST `LastModified`**: free, no writer work, but `Read` reads data's `LastModified` while `PollRecords` reads the ref's — ~ms divergence across read paths. s3sql has the same divergence. Visible to callers via `InsertedAtField`.
- **`x-amz-meta-created-at` only**: zero overhead, but `s3sql` (DuckDB-backed) can't access response headers — full divergence persists there.
- **Parquet column**: +8 bytes/record storage, one writer-side reflection call; **consistent across every read path**. The only solution that closes the gap for s3sql.

Column is the correctness-for-all-paths answer. It's also the most explicit — the value is in the file, not in opaque backend metadata.

### Belt-and-suspenders: column AND metadata

In addition to the column, writes set an `x-amz-meta-created-at` header on the data PUT carrying the same `writeStartTime` value. Not read by the library today. Guarantees that any future code path (or external tool inspecting S3 directly) sees the same value at all three locations: parquet column, S3 metadata, caller's captured write time. Trivial cost (one header); avoids a subtle "why does metadata differ from column" question.

### Separate `refTsMicros` captured just before ref PUT

Today the writer captures `tsMicros := time.Now()` at the top of `WriteWithKey` and uses it for both the data filename AND the ref filename. Problem: the ref isn't *visible* in LIST until after the data + marker + ref PUTs all complete. If total write latency exceeds `SettleWindow`, a consumer can advance its offset past `tsMicros` before the ref lands → **silent data loss in the stream**. SettleWindow today is sized to cover worst-case full write latency, which depends on marker count and is hard to tune.

Phase 1 fixes this by capturing a **second** timestamp `refTsMicros` right before the ref PUT. The ref filename uses this. `SettleWindow` now only needs to cover ref-PUT latency + LIST propagation — a tight, marker-count-independent bound.

The data filename and column still use `writeStartTime` (what the caller thinks of as "when this was written"). Ref stream ordering reflects publication time (what consumers think of as "when this became visible"). Two meaningful times, two places to use them.

Behavioural change for `InsertedAtField`:

- Today: `InsertedAtField` = filename `tsMicros` = writer's wall clock at write-start.
- After Phase 1: `InsertedAtField` = parquet column = writer's wall clock at write-start (the same value, now sourced from the file itself rather than the filename). **Semantically identical**, just robust to Phase 3's filename change.

### Changes

- **`internal/core/paths.go`**: no changes yet. `ParseDataFileName` still exists; it's just no longer called from any hot path after this phase. (Phase 3 removes the `tsMicros` field from the filename format and simplifies this helper.)

- **`s3parquet/writer.go`** (`NewWriter` validation): if `WriterConfig.InsertedAtField` is set, validate T has that reflect field with type `time.Time`. Stash the field index on the Writer so the write hot path doesn't re-resolve it per record.

- **`s3parquet/write.go`**:
  - Capture `writeStartTime := time.Now()` at the top of `writeEncodedPayload` (today's `tsMicros`; now used for the data filename and the column).
  - New helper `populateInsertedAtField[T](recs []T, fieldIdx []int, t time.Time)` that reflectively sets `fieldIdx` on every record before parquet encoding. Called when the writer has `InsertedAtField` configured.
  - Data PUT: add HTTP header `x-amz-meta-created-at: <RFC3339Nano ISO>` carrying `writeStartTime`. Not read by the library today; belt-and-suspenders so the value is present in S3 metadata for future tooling or external inspection.
  - After markers complete, capture `refTsMicros := time.Now().UnixMicro()`. Ref filename uses this (not `writeStartTime`). `SettleWindow` now only covers ref-PUT latency + LIST propagation instead of the full write-latency chain.
  - Data filename keeps using `writeStartTime` (today's `tsMicros`) — Phase 3 will rename this away.

- **`s3parquet/write_rowgroups.go`**: same writer-side changes (populate column, metadata header, separate `refTsMicros`).

- **`s3parquet/target.go`**:
  - `put` helper grows an optional metadata parameter. Either a new signature with a `map[string]string` (simplest) or a variadic options pattern. Call sites that don't pass metadata work as today. The ref PUT and marker PUTs don't need metadata; only the data PUT does.
  - LIST helpers: keep the raw `*s3.ListObjectsV2Paginator` return. `s3types.Object.LastModified` is already accessible; callers just read it.

- **`s3parquet/read.go`**:
  - `listMatchingParquet` / `listAllMatchingParquet`: return `[]core.KeyMeta{Key string; InsertedAt time.Time}` instead of `[]string` (already the case in the current implementation — stays).
  - `groupKeysByPartition`: return `map[string][]core.KeyMeta` (already the case — stays).
  - `versionedRecord[T]` keeps `insertedAt time.Time` and `fileName string`. **Populate `insertedAt` from the decoded parquet column** when `InsertedAtField` is configured; fall back to `KeyMeta.InsertedAt` when it isn't (for `DefaultVersionOf`'s internal use). The column value is authoritative when available.
  - `downloadAndDecodeOne`: after decoding records, if the reader has `insertedAtFieldIndex` set, read the column's value off each record into `versionedRecord.insertedAt`. If the column is unset on T, fall back to `KeyMeta.InsertedAt`.
  - `emitPartition` and the materialised paths: sort logic unchanged from current Phase 1 work — the resolved `sortCmp` runs before dedup/stripVersions.

- **`s3parquet/read_rowgroups.go`**: parallel update to `downloadFilteredOne` — populate `versionedRecord.insertedAt` from the decoded column when configured.

- **`s3parquet/reader.go`**:
  - Keep `DefaultVersionOf[T]` — returns `insertedAt.UnixMicro()`. With the column-based approach this is the writer's `writeStartTime`. With the LastModified fallback (no column configured) this is S3's `LastModified`. Same semantics in both cases: "newer wins".
  - Keep `insertedAtFieldIndex` — now used on the READ side to pull the value OUT of the decoded record, not (as in the earlier revised plan) to write it in.

- **`s3sql/reader.go`** / **`s3sql/transforms.go`**:
  - `InsertedAtField` becomes a real parquet column. Remove the `parquet:"-"` validation — the field is now part of the parquet schema, so `parquet:"<column-name>"` applies. DuckDB decodes it naturally via `SELECT *`; no `filename=true` routing needed for InsertedAtField.
  - The filename/LastModified map path currently in the implementation becomes unnecessary for InsertedAtField — DuckDB reads the column directly. Keep the filename-routing only for the dedup CTE's tie-breaker (which is a separate concern).
  - Remove `InsertedAtField`'s special-case in `needsFilename` / `wrapScanExpr` / `rowBinder`.
  - Remove the `filenameLastMod` map built during LIST — not needed once the column is the source. (LIST itself still returns `KeyMeta` for s3parquet sort-fallback purposes.)

- **`internal/refstream/poll.go`**: `StreamEntry.LastModified` population stays (already done) but is no longer needed for `InsertedAtField` — s3sql `PollRecords` reads the value from the column. Keep the field on `StreamEntry` for external tooling / debugging.

### Sort-key cascade (simplified)

Resolved once at `NewReader`:

| Configuration | Sort key | Granularity | Emission order |
| --- | --- | --- | --- |
| `EntityKeyOf` set (with or without explicit `VersionOf`; default uses `LastModified`) | `(entityKey, versionOf(rec, lastMod))` ascending | Per-record | Grouped per entity; within each entity oldest → newest (newest last) |
| `EntityKeyOf` nil | `(lastMod, fileName)` ascending | Per-file | Per-file chronological; tiebreak by filename for determinism |

With `DefaultVersionOf` naturally handling the "EntityKey set, VersionOf nil" case (returns LastModified), there's no ambiguity to resolve at `NewReader`.

Semantic notes on `LastModified` as the sort / version source:

- On overwrites (idempotent retries where `LastModified` updates), the sort reflects the most recent physical write — consistent with "what S3 currently holds".
- On StorageGRID at weak consistency, `LastModified` can drift between replicas. Callers who care about exact ordering configure an explicit `VersionOf` over a record field.

### Tests

- New `TestInsertedAtField_PopulatedFromLastModified` — reader populates the configured field with the S3 object's `LastModified`, not filename parse.
- New `TestDefaultVersionOf_UsesLastModified` — dedup picks newest by `LastModified` when `VersionOf` is unset.
- New `TestSort_ByEntityKeyAndExplicitVersion` — `EntityKeyOf` + explicit `VersionOf` → grouped per entity, ascending version.
- New `TestSort_ByEntityKeyAndDefaultVersion` — `EntityKeyOf` without `VersionOf` → grouped per entity, ascending `LastModified`.
- New `TestSort_LastModifiedFallback` — no `EntityKeyOf` → per-file chronological, tiebreak by filename.
- New `TestSort_AppliesToAllReadPaths` — same dataset through `Read`, `ReadIter`, `ReadIterWhere`, `ReadMany`, `ReadManyIter`, `PollRecords` yields the same sorted order.
- Update existing tests that asserted filename-derived `insertedAt` → assert LastModified-derived (values will be equivalent in practice, just the source of truth differs).

### Risk

**Behavioural change**: `InsertedAtField` value shifts from "client-side write time" to "S3 LastModified time". Millisecond skew for non-retry writes; larger skew (minutes) for idempotent retry overwrites in Phase 3. **No on-disk breaking change** — this is a pure read-path repointing. Emission order becomes deterministic in a new way. Callers who relied on the implicit LIST-lex-order-equals-chronological contract see a behavioural change but one that's stronger (explicit sort) rather than weaker.

**Estimate**: ~1 day (no writer-side work; LIST plumbing is a few lines; sort is one `sort.SliceStable` call per entry point).

---

## Phase 1.5 — Replica dedup (independent of version dedup) ✅ implemented

**Outcome**: when `EntityKeyOf` + `VersionOf` are configured, records with the same `(entity, version)` are collapsed to one regardless of `WithHistory`. This catches retry / zombie replicas that produce byte-identical writes, while `WithHistory` continues to control whether *distinct versions* are preserved or reduced to "latest wins".

### Motivation

Today's dedup conflates two orthogonal concepts:

- **Version dedup** (controlled by `WithHistory`): caller's semantic choice about whether to see all versions of an entity or only the latest.
- **Replica dedup** (never what the caller wanted): two physical copies of the *same write* — same entity, same version, byte-identical payload — produced by retries, zombie writers, or cross-node races.

`WithHistory` today means "no dedup at all", which surfaces replica duplicates as if they were distinct versions. After this phase, `WithHistory` means "keep all *distinct* versions", and identical replicas are always collapsed.

### Changes

- **`s3parquet/reader.go`** (dedup pass) / **`internal/core`** (wherever the per-partition dedup fold lives): add a pre-step that collapses records with identical `(EntityKeyOf(r), VersionOf(r, t))` to a single representative (deterministic: first-seen in LIST order).
- After this pre-step, the existing `WithHistory` branch runs unchanged:
  - Without `WithHistory`: reduce each entity's version set to `max(version)`.
  - With `WithHistory`: emit all surviving records (now one per distinct version per entity).
- **`s3sql/transforms.go`**: the DuckDB CTE path needs the same semantic — add `GROUP BY entity, version` (with a deterministic pick from the group) in the query before the version-ordering step.
- Applies to all read paths: `Read`, `ReadIter`, `ReadIterWhere`, `ReadMany`, `ReadManyIter`, `PollRecords` (within a batch).

### Tests

- `TestReplicaDedup_CollapsesIdenticalPairs` — two files containing the same `(entity, version)` produce one record with or without `WithHistory`.
- `TestReplicaDedup_WithHistoryKeepsDistinctVersions` — with `WithHistory`, distinct versions of the same entity are all returned (unchanged from today).
- `TestReplicaDedup_DuckDBPath` — same semantics through the `s3sql` CTE.
- Update existing `WithHistory` tests to reflect that pure replicas collapse but distinct versions still flow through.

### Scope limits

- **No content hashing**. If the caller hasn't configured `EntityKeyOf`+`VersionOf`, replicas are not detected. Callers without dedup are explicitly opting into "show me everything". Deferred to a future phase if a real use case emerges.
- **No cross-session dedup for `PollRecords`**. Within a single `Poll` batch, replica dedup via `(entity, version)` works. Across sessions (consumer checkpoints offset, restarts, later sees a duplicate ref the writer-side LIST missed beyond `maxRetryAge`), the library can't detect the replica without persistent consumer state. Documented tradeoff: at-least-once across sessions in this edge case. For exactly-once across sessions, callers route ref-stream consumption through a Postgres outbox (see Phase 3 README notes) or maintain their own offset+seen-tokens state.

### Contract summary after Phase 1.5 + Phase 3

Record-layer **exactly-once** at the consumer requires:

1. `EntityKeyOf` + `VersionOf` configured on the reader (Phase 1.5's replica dedup is the correctness primitive).
2. Reader reads stay within a single session, **or** retries stay within the writer's `maxRetryAge` window **or** the orchestrator reuses the same idempotency token across zombie/replacement writers.

Storage-layer duplication is further reduced (but not eliminated) by:

3. `WithIdempotencyToken` set on every write (deterministic paths; overwrite-prevention fires).
4. Backend with strong read-after-write **and** strong LIST-after-write consistency — for Phase 3's scoped-LIST ref dedup to be reliable. AWS S3 and recent MinIO deliver this by default; StorageGRID requires at least `strong-site` (single-site) or `strong-global` (multi-site). StorageGRID's default `read-after-new-write` is insufficient — LIST is eventually consistent, so scoped-LIST can miss prior writes on cross-node retries. Phase 1.5 absorbs the residual duplicates at the record layer.

Miss any of these and guarantees degrade to at-least-once (but never below).

| Configuration | Storage layer | Record layer |
| --- | --- | --- |
| No token, no dedup | at-least-once | at-least-once |
| No token, `EntityKeyOf`+`VersionOf` | at-least-once | **exactly-once** (per entity, latest or distinct-versions depending on `WithHistory`) |
| Token + dedup, strong consistency, no zombies (or token-sharing zombies) | at-least-once (minimal storage duplication; ref dedup effective) | **exactly-once** at the reader; for PollRecords exactly-once holds within a session; across sessions exactly-once holds within `maxRetryAge` |
| Token + dedup, weak consistency (StorageGRID `read-after-new-write`) | at-least-once (residual cross-node races leave storage duplicates) | **exactly-once** at the reader — Phase 1.5 collapses storage replicas that share `(entity, version)` |
| Token + dedup, strong consistency, but zombies use different tokens | at-least-once (different hashes → different storage paths) | **exactly-once** if replicas share `(entity, version)`; otherwise they surface as distinct "versions" per entity (which is accurate — they *are* different writes from the reader's POV) |

**Estimate**: ~0.5 day.

---

## Phase 2 — Internal retries (transparent) ✅ implemented

**Outcome**: transient S3 errors inside one `Write` call don't bubble up or produce orphans. No API surface change.

### Changes

- **`s3parquet/target.go`**: wrap `put`, `get`, `getRange`, `list` with a retry loop on 5xx, 429 (SlowDown), network errors. Exponential backoff, small cap (e.g. 3 attempts, 200ms/400ms/800ms). Do **not** retry 4xx other than 429.
- Retry is per-S3-call, not per-write — every individual PUT (data / marker / ref) gets its own retry loop. `writeTime` is captured once at the top of `Write` / `WriteWithKey` and reused across all retries.

### Tests

- `TestTarget_RetryOn5xx`, `TestTarget_RetryOn429`, `TestTarget_NoRetryOn404` — mock S3 client returning transient errors on first N calls.
- Integration test unaffected; MinIO doesn't naturally produce 5xx.

### Risk

Low. Pure retry loop; no behaviour change on happy path.

**Estimate**: ~0.5 day.

---

## Phase 3 — External idempotency via `WithIdempotencyToken(token)` ✅ implemented

**Outcome**: caller retries of a whole `Write` call with the same token produce deterministic data file paths, so overwrite-prevention triggers and the parquet body is not re-uploaded. Markers and refs are best-effort deduplicated via scoped LIST. **Consumer-side exactly-once is delivered jointly with Phase 1.5's replica dedup** — tokens reduce the number of physical replicas; replica dedup collapses any that remain (residual duplicates from weak consistency, zombie writers, or retries beyond `maxRetryAge`).

The design rests on two write-side primitives:

1. **Retry detection on the data PUT** — the writer determines whether the data object already exists before uploading its body. Three backend-specific mechanisms produce equivalent semantics:
   - AWS S3 and recent MinIO: HTTP header `If-None-Match: *` on `PutObject` → `412 PreconditionFailed` on retry. No body is uploaded.
   - NetApp StorageGRID (STACKIT): bucket policy denies `s3:PutOverwriteObject` → `403 AccessDenied` on retry. Writer disambiguates with a HEAD (200 = retry, 404/403 = real permission error). No body is uploaded.
   - Backends that do neither: `HEAD` the data key before PUT. 200 → retry, 404 → fresh. Extra HEAD on every write, but preserves the "no body re-upload on retry" property that the other two paths get from the PUT itself.

2. **Scoped LIST on retry** to determine whether a ref for this token already exists. Only paid when the data PUT is rejected by overwrite-prevention. Bounded by the `maxRetryAge` parameter of `WithIdempotencyToken` so it doesn't scan the full stream.

### Design recap

- **Data file path**: `{dataPath}/{hiveKey}/{id}.parquet` — filename format **unchanged from Phase 1**. There's always an `id`; only its source changes:
  - Without token: library auto-generates `{insertedAt}-{shortID}` (today's `{tsMicros}-{shortID}` shape, just conceptually relabeled). `insertedAt` is the writer's captured wall clock; `shortID` is a random 8-char UUID fragment. Fresh per write, preserves time-sortable filenames within a partition.
  - With `WithIdempotencyToken(token, ...)`: `id = token` verbatim. Caller-chosen formats like `"2026-04-22T10:15:00Z-batch42"` land as meaningful, debuggable filenames that remain time-sortable if the caller follows the `{time}-{suffix}` convention. The partition is already in the S3 path, so the token alone uniquely identifies the file without per-partition munging. Retry with the same token produces the same path, so overwrite-prevention triggers and no body is re-uploaded.
  
  Conceptually, there's *always* a token — either library-generated (default shape) or caller-provided. The filename format is uniform; only the source of the `id` differs.

  **No on-disk format change from Phase 1.** Phase 1's `{tsMicros}-{shortID}` filenames remain parseable with the same helpers; Phase 3 just adds a code path that lets callers override the `id` via their own token.

- **Marker paths**: already derived from data file identity (which uses the same `id`). Byte-identical overwrite on retry is idempotent transitively.

- **Ref key**: `{refPath}/{tsMicros}-{id}{RefSep}{escapedHive}.ref` — **keeps `tsMicros`** because global time ordering is needed for `OffsetAt`, Poll iteration, and SettleWindow cutoff logic. Retries produce a new ref at a new `tsMicros` but the same `id`; the scoped LIST finds the original ref by `id` and skips the duplicate write. So refs are not idempotent at the path level (different `tsMicros` per attempt) but are dedupped at write time via LIST.

- **Retry detection and ref dedup**: when the data PUT is rejected (see [Retry detection](#retry-detection) below), the writer LISTs refs within `[now − maxRetryAge, now]` and scans for a ref whose `id` field equals the `id` computed from `(token, partitionKey)`.
  - Found → skip ref write (full-success retry).
  - Not found → PUT ref (scenario B: original attempt wrote data but not ref).

- **Write-side ref dedup is best-effort, not absolute**. The scoped LIST catches duplicates within `maxRetryAge` on strongly-consistent backends (AWS, recent MinIO, StorageGRID at `strong-*` consistency). On weaker consistency or retries beyond the window, duplicate refs can still reach consumers — those are absorbed at the reader layer by Phase 1.5's replica dedup (records sharing `(entity, version)` collapse to one). **Idempotency and reader-side replica dedup are complementary**: tokens reduce physical replicas; replica dedup guarantees exactly-once records at the consumer.

### Retry detection

The writer attempts a PUT of the data object that should fail if the object already exists. The specific mechanism depends on the backend's capability (probed at `NewWriter`):

| Backend | Mechanism | Retry signal | Body uploaded on retry? |
| --- | --- | --- | --- |
| AWS S3, recent MinIO | `If-None-Match: *` on `PutObject` | `412 PreconditionFailed` — unambiguous | No |
| STACKIT / StorageGRID | Bucket policy denies `s3:PutOverwriteObject` (ops-level setup) | `403 AccessDenied` — ambiguous, HEAD to disambiguate | No |
| Neither | Pre-flight `HEAD` on the data key before PUT | HEAD 200 = retry, 404 = fresh | No |

**Disambiguating 403**: a 403 response can mean either "overwrite denied" (our retry signal) or "no permission to write at all" (real config error). When the writer receives 403 on a data PUT, it issues a `HEAD` on the data path:

- HEAD returns 200 → object exists → this is a retry. Proceed to scoped LIST.
- HEAD returns 404 → object doesn't exist → 403 is a real permission error. Surface to caller.
- HEAD returns 403 → writer has no read permission either → surface as config error.

This extra HEAD is only paid on StorageGRID-style backends and only on retry — the happy path is still a single PUT.

### Write flow

```
Determine fresh vs retry (per detected capability):
  • If-None-Match mode: PUT with If-None-Match: *
        201 → fresh;  412 → retry
  • Bucket-policy mode: PUT; on 403 → HEAD
        200 → fresh;  403-then-HEAD-200 → retry;
        403-then-HEAD-404/403 → real permission error (surface)
  • Fallback mode: HEAD data
        404 → fresh, then PUT data;  200 → retry, skip data PUT

Fresh:
  PUT markers (unconditional) + PUT ref (unconditional) — done.
  No LIST needed (we know no ref exists yet under the single-writer-
  per-partition invariant).

Retry:
  LIST refs in [now − maxRetryAge, now] for id
  ├─ found → skip markers, skip ref — done
  └─ not found → PUT markers (byte-identical overwrite) + PUT ref
```

### Request-count comparison

AWS / MinIO (`If-None-Match` mode):

| Scenario | Today | Phase 3 |
| --- | --- | --- |
| Fresh write | 1 data + N markers + 1 ref | 1 conditional data + N markers + 1 ref (same) |
| Retry, full success | 1 data + N markers + 1 ref (all duplicates) | 1 conditional data (412, no body) + 1 LIST |
| Retry, scenario B (data written, ref not) | 1 data + N markers + 1 ref (data duplicate) | 1 conditional data (412) + 1 LIST + N markers + 1 ref |

STACKIT / StorageGRID (bucket-policy mode):

| Scenario | Today | Phase 3 |
| --- | --- | --- |
| Fresh write | 1 data + N markers + 1 ref | 1 data + N markers + 1 ref (same) |
| Retry, full success | 1 data + N markers + 1 ref (all duplicates) | 1 data (403) + 1 HEAD (200) + 1 LIST |
| Retry, scenario B | 1 data + N markers + 1 ref (data duplicate) | 1 data (403) + 1 HEAD (200) + 1 LIST + N markers + 1 ref |

Fallback mode (HEAD-before-PUT):

| Scenario | Today | Phase 3 |
| --- | --- | --- |
| Fresh write | 1 data + N markers + 1 ref | 1 HEAD (404) + 1 data + N markers + 1 ref (+1 HEAD) |
| Retry, full success | 1 data + N markers + 1 ref (all duplicates) | 1 HEAD (200) + 1 LIST |
| Retry, scenario B | 1 data + N markers + 1 ref (data duplicate) | 1 HEAD (200) + 1 LIST + N markers + 1 ref |

All three modes avoid the parquet body upload on retry (the dominant cost). Fallback mode pays one HEAD on every write as the cost of not having a PUT-level retry signal.

### Changes

- **`internal/core/paths.go`**:
  - `BuildDataFilePath` / `ParseDataFileName`: **no signature change**. Still build/parse `{dataPath}/{hiveKey}/{id}.parquet`. The `id` is produced by the writer as either the library's default `{tsMicros}-{shortID}` or the caller's raw token — the helpers don't care which.
  - `EncodeRefKey` / `ParseRefKey` / `RefCutoff` all **unchanged**. Refs still encode the publication `tsMicros` for global time ordering, and the in-flight 3-field layout from Phase 1 stays.
  - New helper `RefRangeForRetry(refPath, now, maxRetryAge)` returning the `[lo, hi]` pair used for the scoped LIST on `ErrAlreadyExists`.
  - New helper `ValidateIdempotencyToken(token string) error` — mirrors the existing `ValidateHivePartitionValue` pattern. Rejects empty, `/`-containing, `..`-containing, non-printable-ASCII, or overlong (>200 char) tokens. Runs at `WithIdempotencyToken` option application time so bad tokens surface immediately, not at PUT time.
  - **`TestBuildDataFilePathLexicalOrdering` in `paths_test.go` stays**: the auto-generated id still has the `{tsMicros}-{shortID}` prefix shape, so auto-gen filenames remain lex-sortable by time within a partition. Idempotent writes can opt into or out of this shape via their token format.
  - **Note on `InsertedAtField`**: after Phase 1, `InsertedAtField` is populated from the writer's column, with `LastModified` as the fallback. Phase 3 doesn't touch this path.

- **`internal/core/refkey.go`** (or existing place): helper `ExtractRefID(refKey) string` that pulls the id out of a ref key for retry-scan comparison. (Names it "id" rather than "hash" because the field is generic — sometimes a random shortID, sometimes a token hash.)

- **`internal/core/writeopt.go`** (new): introduce the `WriteOption` / `WriteOpts` pair mirroring the existing `QueryOption` / `QueryOpts` pattern in [internal/core/queryopt.go](internal/core/queryopt.go). No `WriteOption` exists today — writes currently take no options. This file declares the type, the `Apply(...WriteOption)` accumulator, and the `WriteOpts` struct holding resolved per-call state.
- **`s3parquet/options.go`**: new option `WithIdempotencyToken(token string, maxRetryAge time.Duration) WriteOption`. Both parameters required — no library default for `maxRetryAge` since cost is workload-dependent and there is no universally-right value. `maxRetryAge == 0` explicitly disables ref dedup (retry always writes a duplicate ref; cheapest retry path, caller handles downstream). Stored on the per-call write context; no change to `Store` / `Writer` constructors.
- **`s3parquet/write.go` / `s3parquet/write_rowgroups.go`** (existing methods): update signatures of `Write`, `WriteWithKey`, `WriteRowGroupsBy`, `WriteWithKeyRowGroupsBy` to accept `opts ...WriteOption`. Same pattern as the existing read-side methods accepting `QueryOption`.

- **`s3parquet/consistency.go`** (new, small): typed `ConsistencyLevel` string alias with named constants for the StorageGRID-defined levels. Shared by writer and reader config.

  ```go
  type ConsistencyLevel string

  const (
      ConsistencyDefault           ConsistencyLevel = "" // no header; bucket default applies
      ConsistencyAll               ConsistencyLevel = "all"
      ConsistencyStrongGlobal      ConsistencyLevel = "strong-global"
      ConsistencyStrongSite        ConsistencyLevel = "strong-site"
      ConsistencyReadAfterNewWrite ConsistencyLevel = "read-after-new-write"
      ConsistencyAvailable         ConsistencyLevel = "available"
  )
  ```

  Rationale: typed constants catch typos at compile time (IDE autocomplete), while the underlying string alias still permits `ConsistencyLevel("future-level")` if NetApp adds new levels without a library update. Matches Go idiom (like `http.MethodGet`).

- **`s3parquet/detection.go`** (new): duplicate-write detection strategy as a sealed interface with three factory functions. Lets callers either pick a strategy explicitly or let the library auto-detect.

  ```go
  // DuplicateWriteDetection controls how the writer detects that an
  // idempotent write is a retry (i.e. the data object already exists).
  //
  // Sealed interface — external packages cannot implement it; they must
  // use one of the provided factory functions.
  type DuplicateWriteDetection interface {
      isDuplicateWriteDetection()
  }

  // DuplicateWriteDetectionByOverwritePrevention asserts that the backend
  // rejects PUTs to existing keys — either by honouring If-None-Match: *
  // (AWS, recent MinIO) or by an externally-configured bucket policy that
  // denies s3:PutOverwriteObject (StorageGRID / STACKIT). No probe
  // scratch object is written. The writer always sends If-None-Match and
  // handles both 412 and 403+HEAD responses.
  //
  // Caveat: if the caller picks this strategy but the backend doesn't
  // actually enforce overwrite prevention, retries produce duplicates.
  // The guarantee falls back to at-least-once.
  func DuplicateWriteDetectionByOverwritePrevention() DuplicateWriteDetection

  // DuplicateWriteDetectionByHEAD issues a pre-flight HEAD on the data
  // key before every PUT. 200 → retry, 404 → fresh. Use when the backend
  // has no overwrite-prevention mechanism. Costs one extra HEAD per write
  // on the happy path.
  func DuplicateWriteDetectionByHEAD() DuplicateWriteDetection

  // DuplicateWriteDetectionByProbe auto-detects at NewWriter by writing
  // a scratch object at a stable key and attempting a second PUT. If the
  // second call is rejected, overwrite-prevention is active; otherwise
  // falls back to HEAD-before-PUT mode.
  //
  // deleteScratch controls whether the probe object is removed after
  // detection. Requires DELETE permission on the probe key. When false,
  // leaves one object at a stable path — subsequent restarts overwrite
  // the same key, so storage debt is bounded to one object.
  func DuplicateWriteDetectionByProbe(deleteScratch bool) DuplicateWriteDetection
  ```

- **`s3parquet/writer.go`**: new config fields
  - `DuplicateWriteDetection DuplicateWriteDetection` — strategy for detecting retries. Default: `DuplicateWriteDetectionByProbe(true)` (auto-detect, clean up scratch). Set explicitly when you know the backend's capability, or when you lack DELETE permission.
  - `DisableCleanup bool` — prevents `DeleteObject` on partial-write failure paths. When `true`, orphan data/marker objects remain in place for lifecycle policies to garbage-collect. Independent of `DuplicateWriteDetection`'s `deleteScratch` flag — the two cover different DELETE-permission concerns.
  - `ConsistencyControl ConsistencyLevel` — value of the `Consistency-Control` header the writer sets on operations that require strong read-after-write / list-after-write consistency. Default `ConsistencyDefault` (empty string; no header sent, backend applies its bucket default). Set to `ConsistencyStrongGlobal` or `ConsistencyStrongSite` to opt into stronger consistency for the critical operations. NetApp StorageGRID extension; ignored by AWS S3 and MinIO as an unknown header. See the **Consistency-Control handling** note below.
  - Validation at `NewWriter`: if `ConsistencyControl` is non-empty and not one of the known constants, log a warning (forward-compatibility: still pass the header, but flag the typo risk to the caller).
  - No other constructor changes. (`MaxRetryAge` is **not** a `WriterConfig` field — it's a per-call parameter of `WithIdempotencyToken`, paired with the token it's required for.)

- **`s3parquet/reader.go`** and **`s3sql/reader.go`** both get the matching field:
  - `ConsistencyControl ConsistencyLevel` on their respective `ReaderConfig` — mirrors `WriterConfig.ConsistencyControl`. Default `ConsistencyDefault`. Per NetApp's guidance, PUT and GET must use matching consistency — the reader needs this for the GET-after-LIST path to pair correctly with the writer. Library logs a warning at `NewReader` if writer and reader disagree on the value.
  - **`s3parquet.Reader`** applies the header on its own data-file GETs (via the shared `S3Target`), so the header reaches S3 on every `Read` / `ReadIter` / `ReadIterWhere` data fetch.
  - **`s3sql.Reader`** fetches parquet files through DuckDB's HTTP client, which is opaque to the library — **our `Consistency-Control` header does not reach those requests**. The ref-stream LIST path in `s3sql.Reader` (driven via the shared `S3Target`) does set the header. See the risks section for the implication.

- **`s3parquet/target.go`**:
  - Add `putIfAbsent(ctx, key, body, opts ...putOpt)` — PUTs the object. If the `ConditionalPutSupported` capability was detected at probe time, uses `If-None-Match: *`; otherwise issues a plain PUT (relying on an externally-configured overwrite-prevention bucket policy). Returns a sentinel `ErrAlreadyExists` on:
    - `412 PreconditionFailed`, or
    - `403 AccessDenied` after a follow-up HEAD confirms the object exists.
    A 403 whose follow-up HEAD returns 404 or 403 is surfaced as a real permission error.
  - `list(ctx, start, end, opts ...listOpt)` — already supported; if not, add a range-LIST helper.
  - `get(ctx, key, opts ...getOpt)` / existing reads — accept optional `withConsistencyControl(value string)` option.
  - `probeOverwritePrevention(ctx, deleteScratch bool)` — only invoked when `DuplicateWriteDetectionByProbe(...)` strategy is active. Writes a scratch object at a stable key (`{prefix}/_probe/overwrite-prevention`), then attempts a second PUT via `putIfAbsent`. If the second call returns `ErrAlreadyExists`, the backend has overwrite-prevention (via `If-None-Match` *or* bucket policy — the writer doesn't need to distinguish). Otherwise, falls back to HEAD-before-PUT mode on all writes. If `deleteScratch` is true, deletes the scratch object after probing; otherwise leaves it at the stable key (subsequent restarts overwrite, storage debt bounded to one object).
  - `headThenPut(ctx, key, body, opts)` — the HEAD-before-PUT path. Always issues HEAD first; on 404 proceeds to PUT; on 200 returns `ErrAlreadyExists`. Used when the strategy is `DuplicateWriteDetectionByHEAD()` or when probe mode detected no capability.
  - **Consistency-Control handling**: `S3Target` is stateless with respect to consistency. Callers (Writer / Reader) pass the header value **per call** via a per-method option (`withConsistencyControl(value)`). When the option is empty or absent, no header is sent. Empty-string is harmless on AWS / MinIO (they ignore unknown headers); StorageGRID applies it when present, falls back to bucket default otherwise. This keeps `S3Target` backend-agnostic and lets each caller decide per-operation which calls need strong consistency.

- **`s3parquet/write.go`**:
  - Compute `id`: when the token is set, `id := token` (validated at option-application time). When unset, keep today's library default: `id := fmt.Sprintf("%d-%s", writeStartTime.UnixMicro(), shortID)`. Same downstream codepath afterwards — the library-default shape is lex-sortable by time within a partition.
  - Build data path and marker paths using this `id` — same builders across both idempotent and non-idempotent paths.
  - Remove the `tsMicros` argument to `BuildDataFilePath` call sites (comment at [s3parquet/write.go:206](s3parquet/write.go#L206) about "chronologically sortable in S3 LIST" is deleted — the property no longer holds; record-level sort replaces it).
- **`s3parquet/read.go`** / **`s3parquet/read_rowgroups.go`**: remove the `ParseDataFileName` calls in `downloadAndDecodeOne` / `downloadFilteredOne` (these parsed `tsMicros` for `insertedAt`; already removed by Phase 1's column-based `InsertedAtField`, just update the signatures when `ParseDataFileName`'s return type changes).
- **Continuing the write steps above:**
  3. `putIfAbsent(dataPath, parquetBytes, withConsistencyControl(cfg.ConsistencyControl))` — strong consistency needed for the overwrite-deny / LIST-dedup correctness on StorageGRID.
  4. On success: PUT markers (no consistency header — byte-identical idempotent overwrite is safe under any consistency level), PUT ref (no consistency header — each ref is uniquely-keyed, eventual propagation is fine). Done.
  5. On `ErrAlreadyExists`: `LIST refs in [now − maxRetryAge, now]` **with `Consistency-Control` header set** (the LIST must see prior refs to prevent duplicates; this is the scoped-LIST dedup correctness condition), scan for `id`.
     - Found → done.
     - Not found → PUT markers + PUT ref.

  When the token is unset, current path unchanged (no consistency header needed — writes are already unique-keyed).

- **`s3parquet/read.go`** (reader paths): apply `Consistency-Control` header on GETs that follow a LIST — specifically the `downloadAndDecodeOne` data-file GETs, per NetApp's "same consistency for PUT and GET" requirement. LIST operations themselves use default consistency (SettleWindow absorbs the skew on the ref stream; partition LIST is likewise tolerant).

- **`store.go` / `types.go`**: re-export `WithIdempotencyToken`. No new method on `Store`; existing `Write`, `WriteWithKey`, `WriteRowGroupsBy`, `WriteWithKeyRowGroupsBy` all accept `WriteOption`s.

- **`README.md`**: new section "Idempotent writes" covering:
  - The `WithIdempotencyToken(token, maxRetryAge)` contract. No default for `maxRetryAge` — callers pick based on their retry SLA. Include a tuning-guide table: 1h / 6h / 24h with the LIST-cost implications and target use case for each.
  - **Detection-strategy selection table** — map common deployments to the right `DuplicateWriteDetection` and `DisableCleanup` settings:

    | Scenario | `DuplicateWriteDetection` | `DisableCleanup` |
    | --- | --- | --- |
    | AWS S3, full permissions (typical) | default (`Probe(true)`) | `false` |
    | STACKIT / StorageGRID with bucket policy, full permissions | default (`Probe(true)`) | `false` |
    | STACKIT / StorageGRID with bucket policy, no DELETE permission | `OverwritePrevention()` or `Probe(false)` | `true` |
    | Old MinIO (no overwrite-prevention), full permissions | default (`Probe(true)` auto-falls-back to HEAD) | `false` |
    | Old MinIO, no DELETE permission | `HEAD()` | `true` |
    | Testing / forcing a specific path | `OverwritePrevention()` or `HEAD()` | either |

  - **Trust contract for `OverwritePrevention()`**: picking this strategy is an assertion that the backend enforces overwrite prevention. If the assertion is wrong, retries silently produce duplicates (at-least-once fallback). Document clearly.
  - **Idempotency and reader dedup are complementary, not alternatives.** Call this out up-front in the docs:
    - Reader dedup (`EntityKeyOf` + `VersionOf`) is the correctness primitive. Needed for exactly-once under any backend consistency model, any retry scenario, any zombie-writer configuration.
    - Idempotency tokens are a **storage and bandwidth optimization** — they reduce the number of physical replicas that retries produce (fewer data files, no body re-upload on retry, fewer duplicate refs). They only deliver the "exactly-once within `maxRetryAge`" guarantee at the storage layer when the backend is strongly consistent. On weaker backends (e.g. StorageGRID at `read-after-new-write`), residual duplicates can still leak through — reader dedup catches those.
    - **For any workload where correctness matters**: enable reader dedup unconditionally. Tokens are additive.
  - **Guarantee layers** — be explicit about what idempotency actually promises:
    - *Storage layer*: at-least-once. The idempotency token makes retries best-effort idempotent at the data path, but zombie writers / concurrent races can still leave duplicate refs or (rarely) byte-identical data overwrites.
    - *Record layer, with `EntityKeyOf` + `VersionOf` configured*: **exactly-once**. Readers deterministically pick one version per entity key regardless of how many physical replicas exist.
    - *Record layer, without dedup*: at-least-once (matches today's contract).
    - **Recommendation**: enable dedup for any workload where duplicates are not acceptable. The idempotency token reduces duplicates at the storage layer but is not a substitute for record-level dedup.
  - **Zombie-writer / orchestrator guidance**:
    - For best-case idempotency under orchestrator failover, have the orchestrator re-use the **same** idempotency token when restarting a failed writer. Regenerating a fresh token on restart is valid but creates real storage duplication (different hashes → different paths); record-level dedup is the catch-all.
    - The library does not enforce single-writer-per-partition — it's a caller invariant. Violations are absorbed by the at-least-once contract plus reader-side dedup.
  - Backend-specific setup:
    - AWS / recent MinIO: no setup — `If-None-Match: *` works out of the box.
    - STACKIT / StorageGRID:
      1. Apply a bucket policy denying `s3:PutOverwriteObject` (ops setup).
      2. Set `WriterConfig.ConsistencyControl` and `ReaderConfig.ConsistencyControl` to `"strong-global"` (or `"strong-site"` for single-site deployments). The library then injects `Consistency-Control: <value>` **per operation** only on the calls that need strong consistency (writer data PUT, writer scoped LIST, reader GET-after-LIST). Marker PUTs, ref PUTs, and LISTs for Poll/partition scans go at the bucket default. This follows NetApp's own guidance that per-request control is preferred over bucket-level defaults.

      Document that:
      - The `ConsistencyControl` config defaults to **empty** — no header sent, bucket default applies. This is the right choice on AWS / MinIO (they're strongly consistent by default). On StorageGRID, setting it explicitly is the opt-in for stronger idempotency guarantees.
      - Tune by workload: `"strong-site"` for single-site saves cross-site coordination cost; `"strong-global"` is the safe choice for multi-site.
      - `WriterConfig.ConsistencyControl` and `ReaderConfig.ConsistencyControl` must match — NetApp requires the same consistency for paired PUT and GET operations. Library logs a warning at construction if they disagree.
      - The header is harmless on AWS / MinIO (unknown header, ignored).
      - Data durability is governed by ILM, not by consistency level — acknowledged writes aren't lost even at `read-after-new-write`. Consistency controls visibility only.

      **Include the full bucket-policy JSON example in the README** so callers can copy-paste:

      ```json
      {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Sid": "DenyOverwrite",
            "Effect": "Deny",
            "Principal": "*",
            "Action": ["s3:PutOverwriteObject"],
            "Resource": "arn:aws:s3:::YOUR-BUCKET/*"
          }
        ]
      }
      ```

      Plus the Python snippet (`s3.put_bucket_policy(Bucket=..., Policy=json.dumps(policy))`) as a ready-to-run example.
    - Backends without either: writer falls back to HEAD-before-PUT mode (adds one HEAD per write, no body re-upload on retry).
  - **Alternative streaming architecture (external coordinator)**: callers who already run a transactional database alongside their writers may find the outbox pattern simpler than the built-in ref stream. Set `DisableRefStream: true`, and record `(token, partition_key, data_path, created_at)` in a Postgres (or similar) table on every successful write. A monotonic `id` column gives global ordering; consumers read by `id`. Composes cleanly with `WithIdempotencyToken`: the unique constraint on `token` makes zombie/retry writes visible as constraint violations. Call this out as a valid pattern, but explicitly state it's outside s3store's scope — the library doesn't implement it.

    **Consistency-level relationship with the outbox pattern**: the dedup primitive that Phase 3 needs from S3 (strong consistency for overwrite-deny and scoped LIST) moves to Postgres in this architecture. Postgres's unique constraint on `token` dedups the stream strongly regardless of S3's consistency level. The remaining S3 risk — a rare duplicate data file from a consistency-race overwrite — produces a "ghost" file that isn't referenced by any stream entry; it's storage waste, not a duplicate from the consumer's perspective. **Users on STACKIT (or any weakly-consistent backend) with Postgres already in their stack can therefore leave `ConsistencyControl` at the default empty value** and still get exactly-once at the consumer layer. The outbox pattern lets them skip the cross-site coordination tax on every S3 write.

### Tests

- `TestWriteWithToken_FreshWrite` — PUT succeeds, all three PUTs happen.
- `TestWriteWithToken_RetryFullSuccess` — second call with same token, `putIfAbsent` returns `ErrAlreadyExists`, LIST finds existing ref, no ref or marker writes.
- `TestWriteWithToken_RetryScenarioB` — simulate data-present, ref-absent; retry writes ref only.
- `TestWriteWithToken_RetryAfterMaxRetryAge` — retry beyond `maxRetryAge` parameter; assert duplicate ref is written (documented tradeoff, relies on Phase 1.5 for record-layer dedup).
- `TestWriteWithToken_PerPartitionHash` — same token across different partitions produces distinct hashes / paths.
- `TestPutIfAbsent_412Detection` — backend returning 412 surfaces `ErrAlreadyExists`.
- `TestPutIfAbsent_403ThenHeadFound` — backend returning 403 + HEAD 200 surfaces `ErrAlreadyExists` (StorageGRID path).
- `TestPutIfAbsent_403ThenHeadMissing` — 403 + HEAD 404 surfaces a real permission error (not `ErrAlreadyExists`).
- `TestDuplicateWriteDetection_OverwritePreventionStrategy` — strategy set to `OverwritePrevention()`, writer never probes, always sends `If-None-Match`.
- `TestDuplicateWriteDetection_HEADStrategy` — strategy set to `HEAD()`, writer always issues pre-flight HEAD.
- `TestDuplicateWriteDetection_ProbeStrategy_Capable` — strategy set to `Probe(true)`, capability detected, scratch deleted after probe.
- `TestDuplicateWriteDetection_ProbeStrategy_NotCapable` — strategy set to `Probe(true)`, no capability → falls back to HEAD-before-PUT mode.
- `TestDuplicateWriteDetection_ProbeStrategy_NoDelete` — strategy set to `Probe(false)`, scratch object remains at stable key after probe.
- `TestDuplicateWriteDetection_ProbeStrategy_StableKeyOverwrite` — two probe runs use the same key; second run overwrites rather than accumulating.
- `TestDisableCleanup_LeavesOrphansOnFailure` — `DisableCleanup: true` + induced partial-write failure → orphan data/marker objects remain in S3.
- `TestDisableCleanup_Independent` — `DisableCleanup: true` + `Probe(true)` still deletes probe scratch; `DisableCleanup: false` + `Probe(false)` still leaves probe scratch. Confirms the two flags are independent.
- `TestConsistencyControl_HeaderSentOnIdempotentPUT` — idempotent-mode data PUT carries `Consistency-Control` header with configured value.
- `TestConsistencyControl_HeaderSentOnScopedLIST` — scoped LIST for ref dedup carries the header.
- `TestConsistencyControl_HeaderOmittedOnMarkerPUT` — marker and ref PUTs don't carry the header (confirms the per-op policy).
- `TestConsistencyControl_HeaderSentOnReaderGET` — reader's data-file GET carries the header (matches writer).
- `TestConsistencyControl_WarningOnMismatch` — `WriterConfig.ConsistencyControl != ReaderConfig.ConsistencyControl` surfaces a warning at construction.
- `TestConsistencyControl_EmptyDisables` — empty string means no header sent on any operation.
- `TestConsistencyControl_UnknownValueWarnsButPasses` — `ConsistencyLevel("future-level")` logs a warning at construction but still sends the header verbatim.
- Integration test against MinIO to verify `If-None-Match: *` behaviour.
- Integration test against a STACKIT-style setup (or a LocalStack/MinIO proxy that simulates 403 on overwrite) to verify the HEAD-disambiguation path.
- `TestPhase1_5xPhase3Interaction_DuplicateRefsCollapsed` — simulate a retry beyond `maxRetryAge` (or under weak consistency) that produces two refs pointing at the same idempotent data file, plus two data files from a different-token zombie that share the same `(entity, version)`. With `EntityKeyOf`+`VersionOf` configured, asserts that the consumer sees exactly one record per entity (Phase 1.5 collapses the remaining replicas that Phase 3 couldn't prevent).

### Risks / open questions

1. **Backend capability variation**:
   - AWS S3 / MinIO ≥ RELEASE.2024-10-29 support `If-None-Match: *` natively.
   - NetApp StorageGRID (STACKIT) supports overwrite-prevention via bucket policy (`s3:PutOverwriteObject`). Setup is ops-level — document it.
   - Older MinIO and any other backend without either mechanism falls back to HEAD-before-PUT mode. Pays one HEAD per write on the happy path; still no body re-upload on retry, still no duplicates.
   - The writer auto-detects at `NewWriter`; no per-backend config required.

2. **`maxRetryAge` sizing**: cost on the retry path is `writes × maxRetryAge / 1000` LIST pages. No library default — callers must pass a value via `WithIdempotencyToken(token, maxRetryAge)` that reflects their retry SLA and write-rate tolerance. Document a tuning guide (e.g. 1h for fast-retry streaming, 6h for same-day recovery, 24h for cross-day orchestrator recovery) with the LIST-cost implication at each point.

3. **Retries after `maxRetryAge`**: documented tradeoff. The ref gets written twice, consumer sees one duplicate. Passing `maxRetryAge == 0` disables scoped LIST entirely and always writes the ref (useful when the caller has some other dedup guarantee and wants to minimise retry latency).

4. **Concurrent partition writes forbidden**: the design relies on the library's single-writer-per-partition invariant. Two writers on the same partition with different tokens are fine (different filenames, different paths). Two writers on the same partition with the same token is explicitly unsupported — caller contract.

5. **No filename format change, no reader branching**: idempotent and non-idempotent writes produce the same `{id}.parquet` shape. The `id` is either the library-default `{tsMicros}-{shortID}` (fresh per write, lex-sortable) or the caller's token verbatim. Reader parse logic is unchanged from Phase 1.

6. **STACKIT HEAD cost**: on StorageGRID-style backends, retry detection adds one HEAD per rejected PUT. Only paid on retry (rare). The HEAD also serves as the "is this a real permission error?" check, so it's not pure overhead.

7. **`Consistency-Control` header doesn't reach DuckDB-driven reads**: `s3sql.Reader`'s data-file fetches go through DuckDB's opaque HTTP client. The library can't attach headers to those requests. Implication: on weakly-consistent backends (StorageGRID `read-after-new-write`), callers who need strong-consistent GETs must use the `s3parquet.Reader` paths (`Read`, `ReadIter`, `ReadIterWhere`), not the `s3sql.Reader` SQL paths. Document this explicitly in the README. For most use cases this is fine — Phase 1.5's replica dedup absorbs any residual inconsistency at the record layer.

**Estimate**: ~2 days for Phase 3 itself, plus:
- ~0.5 day if the StorageGRID integration path needs a non-MinIO test fixture.
- ~0.5 day of cross-phase coordination work if Phase 1.5 lands before Phase 3 (test updates where Phase 1.5's replica-dedup tests now see fewer duplicates due to Phase 3's token-based deduplication). Minor but noticeable.

---

## Phase 3b — Idempotent reads via `WithIdempotentRead(token)` ✅ implemented

**Outcome**: callers doing read-modify-write with idempotency can get retry-safe reads in one option. A Read / ReadIter / ReadMany / PollRecords call takes `WithIdempotentRead(token)` and returns state as of the first attempt's write time, excluding both the caller's own prior attempts AND any data written at or after them. Under the single-writer-per-partition caller invariant this is an exact snapshot of attempt-1's view; under a violated invariant self-exclusion still holds and record-layer dedup absorbs the rest.

### Motivation

Typical read-modify-write cycle:

```go
const token = "job-2026-04-22T10:15:00Z-batch42"
existing, _ := store.Read(ctx, pattern,
    s3parquet.WithIdempotentRead(token))
// compute diff against `existing`
store.Write(ctx, changes,
    s3parquet.WithIdempotencyToken(token, 6*time.Hour))
```

On retry, the Read sees the same state attempt 1 saw, computes the same diff, writes the same bytes. Idempotent read-modify-write without callers having to thread a separate timestamp through their retry state.

### Design

Depends on Phase 3 (for the token-in-filename encoding). Both filters are LIST-time, no extra S3 calls:

1. **Self-exclusion**: filter out files where `path.Base(key) == token + ".parquet"` (the caller's own prior attempts).
2. **Later-write exclusion**: among files matching the token, record `barrier[partition] = min(LastModified)`. For every other file in that partition, filter out if `LastModified >= barrier[partition]`.

Per-partition barriers because each partition's retry window is independent. On the first attempt (no matching files yet), no barrier applies — the Read returns the current state.

### Correctness under the single-writer-per-partition contract

The library's invariant means there are no concurrent writers to the same partition. Between attempt-1's read-start and attempt-1's first write, no other data lands in that partition. So `min(LastModified of own files)` is a sufficient barrier — state "between my read and my first write" is empty by construction.

If the caller violates single-writer-per-partition, the barrier still catches their own attempts but may include concurrent writers' data that attempt 1 didn't see. The record-layer dedup (Phase 1.5) absorbs any resulting overlap.

### Why the token, not a timestamp

Earlier iteration considered `WithIdempotentRead(t time.Time, token string)`. Dropped because:

- The token's files' LastModified is *more* accurate than any caller-supplied time for reconstructing "when did attempt 1 run". A caller-supplied `time.Now()` on retry could be hours past the original attempt.
- Passing both forces callers to persist a `(time, token)` pair between attempts. Passing only the token means the caller persists just the token (which they already do for `WithIdempotencyToken`).
- The snapshot marker file variant (PUT a marker before the data PUT so we get an earlier barrier) adds complexity without benefit under the single-writer contract.

### Changes

- **`internal/core/queryopt.go`** (or wherever read options live): new option `WithIdempotentRead(token string) QueryOption`. Adds `IdempotentReadToken string` to `QueryOpts`.
- **`s3parquet/read.go` `listMatchingParquet`**: when `IdempotentReadToken` is set, apply the two-pass filter:
  1. First pass builds `barrier[partition]` from token-matching files.
  2. Second pass excludes token-matching files and files with `LastModified >= barrier[partition]`.
  
  Can be a single pass with delayed emission if we buffer per-partition, but two-pass is simpler.
- **`s3sql/listing.go` `listMatchingParquet`**: same filter. Affects `s3sql.Read` / `ReadIter` / `PollRecords` (for the SELECT-over-specific-URIs path).
- **`internal/core/queryopt.go`**: if tokens are validated at `WithIdempotencyToken` time (per Phase 3's spec), `WithIdempotentRead` reuses the same `ValidateIdempotencyToken` helper.

### Tests

- `TestIdempotentRead_NoFirstAttempt` — no file matches token yet → no barrier → returns full current state.
- `TestIdempotentRead_RetryReturnsSameState` — write with token, write with a second token (simulating a subsequent job), read with the *first* token → excludes own writes AND the second token's later writes.
- `TestIdempotentRead_PerPartitionIsolation` — multi-partition pattern, different partitions have different barrier times; each partition is filtered independently.
- `TestIdempotentRead_SingleWriterInvariant` — documents the correctness contract (under single-writer, the barrier is exact).
- Integration test over MinIO exercising the full read-modify-write cycle twice with the same token, asserting the second attempt produces the same output.

### Risks / open questions

1. **Two-pass LIST**: adds in-memory buffering of all matching files before filtering. For patterns spanning hundreds of thousands of files, this is a memory bump. Mitigation: streaming two-pass (buffer per-partition only, emit as each partition completes). Not urgent for v1.
2. **s3sql fast path**: today the single-pattern Read uses DuckDB's glob directly, skipping the Go-side LIST. When `WithIdempotentRead` is set, we need the Go-side LIST to build the barrier — same fallthrough pattern as `s3sql.Read`'s multi-pattern branch. Small branching at `s3sql/read.go:ReadMany`.
3. **Read-modify-write contract must be documented**: callers need to understand that retries with the *same* token produce deterministic reads. Persisting the token between attempts is the caller's job.

**Estimate**: ~1 day once Phase 3 has landed.

### Dependencies

- Depends on Phase 3 (token-in-filename encoding).
- Phase 1.5 complements it (record-layer dedup handles any residual overlap from contract violations).

---

## Phase 4 (optional) — Benchmark harness

**Outcome**: a standalone binary at `cmd/s3store-bench/` that measures the latency of the write, read, and Poll paths under configurable load, so callers can evaluate the configurable knobs (`SettleWindow`, the per-call `maxRetryAge` parameter on `WithIdempotencyToken`, backend consistency level) against their own infrastructure.

### Why a separate binary instead of `testing.B`

- Real-world measurements need controlled concurrency, warmup, and ramp-up — awkward in `testing.B`.
- Side-by-side comparison across consistency levels is a scripted ops workflow, not a test.
- `testing.B` runs per-package and interleaves with unit tests; a separate binary is cleaner for benchmark sessions.
- Benchmark results are environment-specific; separating from CI tests makes this structural.

### Scope

- Standalone binary at `cmd/s3store-bench/main.go`. Single file, under ~500 lines.
- Uses only the **public library surface** (`Store`, `Writer`, `Reader`, options) so API drift is caught at compile time.
- Flags:
  - `--bucket`, `--prefix`, `--endpoint-url` (for MinIO / STACKIT / AWS)
  - `--writes N`, `--concurrency C`
  - `--record-size` (bytes per record)
  - `--idempotency` (on/off)
  - `--dedup` (on/off, i.e. set `EntityKeyOf` + `VersionOf`)
  - `--scenario` — `write`, `read`, `poll`, `all`
- Output: p50 / p95 / p99 latency per operation, total throughput, error counts. Human-readable table by default; `--json` for scripted parsing.

### Measurements worth including

1. Write latency: fresh write vs. idempotent retry (412 / 403 / HEAD-404 paths).
2. Read latency: single file, full partition, with and without dedup.
3. Poll latency: ref-stream length vs. Poll batch duration.
4. LIST latency: how it scales with ref-stream size (relevant for `maxRetryAge` tuning).
5. Consistency-level A/B: same workload run against buckets with `read-after-new-write` vs `strong-site` vs `strong-global`; table the deltas.

### Documentation

- Short section in `README.md` explaining how to run the harness.
- An `examples/benchmark-output.md` (optional) showing representative runs against each of: AWS S3, MinIO, STACKIT — with the explicit caveat that numbers are illustrative and users should run their own.
- Call out that results are environment-specific and not CI-gated.

### Tests

- `TestBench_RunsCleanly` — smoke test that the binary builds and completes a tiny run against MinIO in integration mode. No assertions on timing.

### Risks / open questions

1. **Environment variance**: numbers are useless without context. Documentation must be clear that this is a measurement tool, not a benchmark suite with reference results.
2. **Maintenance drift**: mitigated by using the public API surface only. If the public API breaks, the benchmark binary fails to compile and forces an update.
3. **Consistency-level testing on AWS S3**: AWS doesn't expose consistency level configuration (it's always strong). Document that this axis only applies to StorageGRID-family backends.

**Estimate**: ~1 day once Phase 3 has landed.

### Dependencies

- Depends on Phase 3 (so the benchmark can measure idempotent vs. non-idempotent paths). Phases 1, 1.5, and 2 don't block it but add axes worth measuring.

---

## Ordering summary

| Order | Phase | Blocks | Independent of |
| --- | --- | --- | --- |
| 1 | Phase 1 (column-based `InsertedAtField`) | Phase 3 | Phase 1.5, Phase 2 |
| 2 | Phase 1.5 (replica dedup) | — | Phase 1, Phase 2, Phase 3 |
| 3 | Phase 2 (internal retries) | — | Phase 1, Phase 1.5, Phase 3 |
| 4 | Phase 3 (idempotency token) | Phase 3b, Phase 4 | — (but depends on Phase 1) |
| 5 | Phase 3b (idempotent reads) | — | — (depends on Phase 3) |
| 6 (optional) | Phase 4 (benchmark harness) | — | — (depends on Phase 3) |

Phase 3 code-depends on Phase 1 (Phase 3 removes `tsMicros` from the data filename, which only works after Phase 1 moves `insertedAt` off filename parsing). Phase 1.5 does **not** code-depend on Phase 3 and can ship standalone, but the full record-level exactly-once guarantee requires both: Phase 3 reduces physical replicas, Phase 1.5 collapses any that remain. Phase 3b code-depends on Phase 3 (the token-in-filename encoding). Phase 2 is orthogonal and can land in any order. Phase 4 is optional follow-up, best done after Phase 3 stabilises so the benchmark isn't measuring about-to-change code.
