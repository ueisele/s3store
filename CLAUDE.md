# Correctness invariants

These are contracts the library makes to its users. Refactors
must preserve them — even when the change appears unrelated.

- **At-least-once at the storage layer** — successful Write is
  durable; a retry after partial failure may produce duplicate
  data files / refs. A failed Write between data PUT and ref
  PUT leaves an orphan data file. The library never deletes
  data, refs, or markers it has written.
- **Read-after-write on snapshot reads** — once Write returns
  success, Read / ReadIter / ReadPartitionIter /
  ProjectionReader.Lookup / BackfillProjection see the new
  records immediately. The settle window applies only to the
  change stream (Poll / PollRecords / ReadRangeIter /
  ReadPartitionRangeIter).
- **Read stability — no library-driven deletion** — two
  consecutive snapshot reads with no intervening writes return
  the same records. Without transactional metadata, the library
  can't tell "committed" from "crashed-mid-write," and can't
  know if a reader has already observed a file. Refactors must
  not introduce automatic GC, age-based pruning, rewrite-in-
  place compaction, or in-Write cleanup of failed-Write
  orphans. Cleanup is operator-driven only.
- **Stream replay stability** — once a record has been observed
  at offset N by a stream reader (Poll / PollRecords /
  ReadRangeIter), that record at offset N never changes;
  replay from offset 0 produces the same sequence every time.
  Three properties combine to back this:

  1. Refs are written to per-attempt paths and never
     overwritten or deleted by the library.
  2. The ref filename encodes a writer-stamped refMicroTs with
     fixed-width lex-numeric ordering (16 decimal digits via
     `%016d`), so the LIST order is stable across calls.
  3. SettleWindow (= CommitTimeout + MaxClockSkew) bounds how
     late a writer's ref can become observable relative to its
     stamped time; by the time a ref clears the cutoff, all
     earlier writers' refs (within MaxClockSkew of reality) are
     already visible — no retroactive insertion at or before an
     observed offset.

  Refactors must not introduce ref rewrites, ref deletion in
  the write path, or non-monotonic refMicroTs (e.g., deriving
  it from LastModified instead of writer wall-clock).
- **Exactly-once at the consumer layer is opt-in via reader
  dedup** — `EntityKeyOf` + `VersionOf` collapse duplicates by
  (entity, version). `WithIdempotencyToken` makes retries
  recover automatically: every attempt writes its data and ref
  to per-attempt paths (id = `{token}-{UUIDv7}`), and the
  writer's upfront HEAD on `<dataPath>/<partition>/<token>.commit`
  finds any prior commit and returns its `WriteResult` unchanged.
  Refactors must not reintroduce overwrite-based retry detection
  on data or refs — per-attempt-paths are what keep the design
  correct on multi-site StorageGRID, where read-after-overwrite
  on those keys is eventually consistent. The `<token>.commit`
  marker itself overwrites only under declared-out-of-contract
  concurrent same-token writes (see "One in-flight write per
  token" below); under sequential retries the upfront HEAD
  short-circuits before any second commit PUT lands.
- **Atomic per-file visibility via `<token>.commit`** — both
  read paths gate on the marker. Snapshot reads
  (`Read` / `ReadIter` / `ReadPartitionIter` /
  `ProjectionReader.Lookup` / `BackfillProjection`) drop parquets
  without a paired commit; the change-stream APIs (`Poll` /
  `PollRecords` / `ReadRangeIter` / `ReadPartitionRangeIter`)
  HEAD `<token>.commit` per ref before yielding (per-poll cache
  collapses repeat tokens). A writer that crashes mid-sequence
  leaves an orphan parquet/ref pair invisible to every read
  path. Refactors must not introduce read paths that bypass the
  gate.
- **Deterministic parquet encoding** — same records + same codec
  produce byte-identical bytes. `WithIdempotencyToken` retries
  depend on this; refactors must not introduce non-determinism
  (random ordering, timestamp injection beyond `InsertedAtField`).
- **One ConsistencyControl per S3Target** — all paired
  operations routed through one Target carry the same level
  (NetApp's "same consistency for paired operations" rule,
  enforced by construction). Refactors must not introduce
  per-call overrides.
- **Per-partition dedup on every read path** — dedup runs within
  one Hive partition at a time across `Read` / `ReadIter` /
  `ReadPartitionIter` / `ReadRangeIter` / `ReadPartitionRangeIter`
  / `PollRecords`. `EntityKeyOf` must be fully determined by the
  partition key so no entity ever spans partitions; otherwise an
  entity surfaces with a separate "latest" pick per partition.
  No global-dedup escape hatch — refactors must not reintroduce
  one (the union-slice memory cost was real, and per-partition
  dedup is correctness-equivalent under the precondition).
- **Deterministic emission order across read and write paths**
  — every read path (`Read` / `ReadIter` / `ReadPartitionIter` /
  `ReadRangeIter` / `ReadPartitionRangeIter` / `ReadEntriesIter`
  / `ReadPartitionEntriesIter` / `PollRecords`) AND the
  write-side `GroupByPartition` emit partitions in **lex order
  of the Hive partition-key string**, with per-partition records
  in **`(entity, version)` ascending order** when dedup is
  configured (decode/insertion order without it, file-lex then
  parquet-row on reads; insertion order on `GroupByPartition`
  since the writer has no EntityKeyOf to sort on). Same input
  on the same data produces byte-identical output every time.

  Four load-bearing pieces back this:

  1. `preparePartitions` collects the `byPartition` map keys
     and `slices.Sort`s them before constructing the per-
     partition state — collapses Go's randomized map iteration
     to deterministic lex order on the read side.
  2. `sortKeyMetasByKey` sorts each partition's files by S3
     key — deterministic decode order within a partition.
  3. `decodePartition` runs `sortAndDedup`, a stable
     `(entity, version)` sort followed by in-place dedup —
     deterministic record order within a partition's output.
  4. `GroupByPartition` collects records into a `byKey` map and
     `slices.Sorted(maps.Keys(...))`s them before building the
     `[]HivePartition[T]` slice — same map-iteration-collapse
     pattern as the read side. `writeGroupedFanOut` consumes
     this slice directly.

  Refactors must not introduce non-deterministic partition
  iteration (e.g., emitting from the `byPartition` / `byKey`
  map directly without sorting), parallel-decode pipelines
  that race `decodedBatch` sends to the channel out of order,
  or non-stable record sorts inside a partition. Consumers may
  rely on byte-for-byte stable output across calls — diffing,
  hashing, replay-equality, and golden-file tests all depend
  on it.

  `PollRecords` consumers needing wall-clock ordering across
  partitions must re-sort by their own timestamp field — its
  `nextOffset` advancement (the "don't miss records"
  property) is unaffected.

# Backend assumptions

Properties of the underlying object-storage backend that the
library's correctness reasoning depends on. The library *assumes*
them — it does not detect or test for them at runtime. Backends
where these don't hold are out of scope for the library's
correctness guarantees, even when the wire-level S3 API is
honoured.

- **One in-flight write per `(partition, token)`.** Concurrent
  writes that share the same `WithIdempotencyToken` are
  out of contract. The library guarantees correctness only
  when at most one write is in flight per `(partition, token)`
  at any given moment. Sequential retries (a failed write
  followed by a same-token retry) remain fully supported —
  that is the design's primary use case. The constraint buys
  back enough determinism that the writer's `<token>.commit`
  PUT can be tolerated as a no-overwrite write under
  sequential retries: the upfront HEAD short-circuits before
  any second commit lands. Refactors must not weaken this
  contract or remove the upfront-HEAD short-circuit.

- **Read-after-new-write on every PUT key.** Data files, refs,
  projection markers, and `<token>.commit` markers all rely on
  read-after-new-write at the configured consistency level.
  AWS S3 (since 2020), MinIO, and StorageGRID at `strong-*`
  all guarantee this for new-key writes. Data and ref PUTs use
  per-attempt paths (`<token>-<UUIDv7>`) and never overwrite,
  so they always live inside the new-key guarantee.
  `<token>.commit` PUTs may overwrite only under declared-out-
  of-contract concurrent same-token writes; record-wise
  byte-equivalence (deterministic parquet encoding) bounds the
  blast radius even then.

- **`ConsistencyControl: strong-global` is required on
  multi-site StorageGRID for the token-commit-overwrite
  convergence path.** When two attempts of the same token race
  through the upfront HEAD (out of contract, but possible) and
  both PUT `<token>.commit`, multi-site grids at `strong-site`
  or `read-after-new-write` propagate the overwrite eventually.
  At `strong-global` the overwrite resolves under the same
  EACH_QUORUM mechanism that backs every-site PUT
  acknowledgement, so subsequent HEADs converge on the
  latest-wins metadata. Single-site grids where every
  reader/writer pair lives in one site can downgrade to
  `strong-site` — there's no cross-site overwrite-propagation
  exposure to absorb. AWS S3 and MinIO ignore the header
  entirely.

- **Writer wall-clock is in the protocol.** Refs encode
  `refMicroTs` — the writer's microsecond wall-clock captured
  immediately before the ref PUT. The reader's
  `refCutoff = now - SettleWindow` therefore compares writer-
  stamped time against reader wall-clock; `MaxClockSkew` bounds
  writer↔reader skew. There is no `LastModified` dependency in
  the cutoff math (the earlier design's
  `LM ≈ first-observable-time` assumption was dropped on the
  empirical finding that StorageGRID stamps `LastModified` at
  request-receipt time, not observability). Refactors that
  reintroduce a server-`LastModified` dependency on the
  ordering path break this contract and must not be introduced.

  `BackfillProjection`'s `until time.Time` bound still relies
  on `LastModified`, but only as a *relative* ordering signal
  across files written through the library — not as an
  observability proxy. It compares LMs that the same backend
  stamped at PUT time on new keys, which is monotonic enough
  for that use case on every supported backend.

# Concurrency invariants

These bound how the iter pipeline (and any future multi-goroutine
pipeline) must coordinate. Properties recent regressions taught
us — preserve them on every refactor.

- **No `cond.Broadcast` with per-actor predicates.** When N
  actors wait on a condition that depends on each actor's
  individual progress (e.g., "did *my* slot land"), use a
  buffered channel as a FIFO semaphore — Go's runtime drains a
  channel's sendq strictly FIFO on every receive.
  `streamState.slotCh` in `reader_iter.go` is the canonical
  shape. The earlier cond+counter design allowed scheduler-
  biased starvation: `Broadcast` wakes everyone, all waiters
  race for the mutex, and the scheduler can consistently pick
  the same winner, leaving one specific actor's predicate
  permanently unsatisfied and deadlocking the pipeline. This
  is not theoretical — it was reproduced deterministically on
  a 1417-partition cost read (commit `da75ca9`). `sync.Cond` is
  acceptable only when there is exactly one waiter AND the
  predicate is global (not per-actor) AND a chan-based wake
  bell would be measurably worse; in practice that combination
  is rare enough that channels should be the default.

- **Every blocking primitive must observe `ctx.Done()`
  natively.** A primitive that requires a sibling broadcast-on-
  cancel goroutine to unblock is a code smell — the workaround
  adds goroutine count, lifecycle complexity, and a separate
  synchronisation site that can drift out of sync with the
  primitive it protects. Channel-based primitives (`<-ch`,
  buffered semaphores, wake bells) `select` on `ctx.Done()` for
  free. Refactors must not reintroduce primitives that need a
  cancel-broadcast helper goroutine.

- **Stall watchdogs are observers, never cancellers.** The
  iter pipeline runs `runDeadlockObserver` to surface stalls
  via `slog.Warn` + `s3store.read.iter.stall.count` without
  aborting. Auto-cancelling on stall would mask the goroutine
  state needed for SIGQUIT diagnosis and risk false-positive
  aborts of legitimately slow consumers. Hard ceilings belong
  at the call site via `ctx.WithTimeout`. Refactors must not
  promote the observer into a circuit breaker.

# Verification

Before considering a change done, run all four:

```sh
go vet -tags=integration ./...
go test -count=1 ./...
go test -tags=integration -count=1 -timeout=10m ./...
golangci-lint run ./...
```

Only required when the change touches Go code (`.go` files,
`go.mod`, `go.sum`, build tags, generated code). Pure
documentation, comment, or asset changes (e.g. `README.md`,
`CLAUDE.md`, image files) don't need them — none of these gates
exercise non-Go content.

`go vet -tags=integration` is the cheapest way to catch
non-compiling integration test files — `go test` without the tag
silently skips them.

## Build tags

Integration tests live behind `//go:build integration` and need a
Docker daemon (`fixture_test.go` spins up a pinned MinIO
container — see [README §Testing](README.md#testing)). Always pass
`-tags=integration` when verifying any read- or write-path change;
plain `go test ./...` quietly omits them.

## Lint discipline

`golangci-lint run` covers gofmt, govet, and the project's
configured linters in one shot. Pre-existing lint issues count —
fix them in the same PR rather than carrying them forward.
