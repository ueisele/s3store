# Correctness invariants

These are contracts the library makes to its users. Refactors
must preserve them — even when the change appears unrelated.

- **At-least-once at the storage layer** — successful Write is
  durable; a retry after partial failure may produce duplicate
  data files / refs. A failed Write between data PUT and ref
  PUT leaves an orphan data file. The library never deletes
  data, refs, or markers it has written.
- **Read-after-write on snapshot reads** — once Write returns
  success, Read / ReadIter / ProjectionReader.Lookup / BackfillProjection
  see the new records immediately. The settle window applies
  only to the change stream (Poll / PollRecords /
  ReadRangeIter).
- **Read stability — no library-driven deletion** — two
  consecutive snapshot reads with no intervening writes return
  the same records. Without transactional metadata, the library
  can't tell "committed" from "crashed-mid-write," and can't
  know if a reader has already observed a file. Refactors must
  not introduce automatic GC, age-based pruning, rewrite-in-
  place compaction, or in-Write cleanup of failed-Write
  orphans. Cleanup is operator-driven only.
- **Exactly-once at the consumer layer is opt-in via reader
  dedup** — `EntityKeyOf` + `VersionOf` collapse duplicates by
  (entity, version). `WithIdempotencyToken` makes retries
  recover automatically: every attempt writes to a per-attempt
  path (id = `{token}-{tsMicros}-{shortID}`) and the writer's
  upfront LIST under `{partition}/{token}-` finds any prior
  valid commit and returns its `WriteResult` unchanged.
  Refactors must not reintroduce overwrite-based retry
  detection — per-attempt-paths are what makes the design
  correct on multi-site StorageGRID, where read-after-overwrite
  is eventually consistent.
- **Deterministic parquet encoding** — same records + same codec
  produce byte-identical bytes. `WithIdempotencyToken` retries
  depend on this; refactors must not introduce non-determinism
  (random ordering, timestamp injection beyond `InsertedAtField`).
- **One ConsistencyControl per S3Target** — all paired
  operations routed through one Target carry the same level
  (NetApp's "same consistency for paired operations" rule,
  enforced by construction). Refactors must not introduce
  per-call overrides.
- **Per-partition dedup on ReadIter** — dedup runs within one
  partition at a time, so `EntityKeyOf` must be fully determined
  by the partition key. `Read` does global dedup and pays the
  memory cost.

# Backend assumptions

Properties of the underlying object-storage backend that the
library's correctness reasoning depends on. The library *assumes*
them — it does not detect or test for them at runtime. Backends
where these don't hold are out of scope for the library's
correctness guarantees, even when the wire-level S3 API is
honoured.

- **`LastModified` ≈ first-observable-time, for every reader.**
  A server-stamped `LastModified` value `L` on an object
  reflects approximately when the object first became
  *observable* (readable via subsequent HEAD / GET / LIST) by
  any reader. Equivalently: when a reader's HEAD or LIST
  returns an object with `LastModified = L`, that object became
  visible to all readers somewhere around time `L`. This is the
  operational shape of the **read-after-new-write** guarantee
  that AWS S3 (since 2020), MinIO, and StorageGRID at
  `strong-*` all provide for new-key writes; the library never
  overwrites under `WithIdempotencyToken` (per-attempt-paths),
  so every PUT in the write path is a new-key write — well
  inside that guarantee on every supported backend.

  Where this assumption is load-bearing:

  - The **post-marker timeliness check**
    (`marker.LM - data.LM < CommitTimeout`) treats the gap
    between two server-stamped LMs as the gap between when the
    data became visible and when the marker became visible. If
    LM were divorced from observability (e.g., LM is set at
    request arrival but cross-replica propagation takes
    seconds), the check could pass while the data is still
    invisible to a reader who can already see the marker —
    breaking atomic visibility.
  - **`Poll`'s `refCutoff = now - SettleWindow`** assumes that
    any ref whose `dataLM` ≤ `now - SettleWindow` is
    LIST-visible to the reader. If LM were set significantly
    before the ref became LIST-visible, refs would silently
    drop out of the reader's view and never reappear — the
    cutoff advances past them. `MaxClockSkew` bounds
    reader↔server clock skew, not server-LM-stamp ↔
    server-visibility skew, which is what this assumption
    constrains.

  Refactors that swap LM for a different timestamp source
  (e.g., `x-amz-date` request time, a client-stamped value, or
  a header that some backends emit ahead of replication) break
  this assumption and must not be introduced.

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
