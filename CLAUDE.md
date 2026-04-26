# Correctness invariants

These are contracts the library makes to its users. Refactors
must preserve them — even when the change appears unrelated.

- **At-least-once at the storage layer** — successful Write is
  durable; a retry after partial failure may produce duplicate
  data files / refs. Read paths surface what's in S3, so
  duplicates flow through unless dedup is configured.
- **Read-after-write on snapshot reads** — once Write returns
  success, Read / ReadIter / IndexReader.Lookup / BackfillIndex
  see the new records immediately. The settle window is the
  *only* deliberate exception, and it applies only to the change
  stream (Poll / PollRecords / ReadRangeIter).
- **Exactly-once at the consumer layer is opt-in via reader
  dedup** — `EntityKeyOf` + `VersionOf` collapse duplicates by
  (entity, version). `WithIdempotencyToken` + a conditional-PUT
  backend additionally minimize storage-layer duplication on
  retries; correctness lives on reader dedup either way.
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
