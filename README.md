# s3store

[![Go Reference](https://pkg.go.dev/badge/github.com/ueisele/s3store.svg)](https://pkg.go.dev/github.com/ueisele/s3store)
[![Release](https://img.shields.io/github/v/tag/ueisele/s3store?sort=semver&label=release)](https://github.com/ueisele/s3store/releases)

Append-only, versioned data storage on S3 with a change stream. Pure Go,
cgo-free. No server. No broker. No coordinator. Just a Go library and an
S3 bucket.

## What it does

- **Write** typed records as Parquet, grouped into Hive-style partitions.
- **Stream** changes via lightweight "ref" files — one empty S3 object per
  write, all metadata in the object key.
- **Read** point-in-time deduplicated snapshots with glob support.
- **Iterate** large reads with bounded-memory streaming.
- **Project** secondary lookups with empty marker files (LIST-only Lookup).

## Guarantees

No transactional coordinator — S3 is the only source of truth. The
contract follows from that:

- **At-least-once on storage.** A successful `Write` is durable; a
  retry after partial failure may produce duplicate data files and
  refs. The library never deletes data it has written.
- **Exactly-once at the consumer is opt-in.** Two complementary
  mechanisms collapse the at-least-once duplicates:
  - `EntityKeyOf` + `VersionOf` on the reader collapses duplicates
    with the same `(entity, version)` to one record — the
    universal solution that works for any duplicate source.
  - `WithIdempotencyToken` on the writer makes sequential retries
    recover the prior commit unchanged via an upfront HEAD on
    `<token>.commit` — no re-upload, byte-identical `WriteResult`.

  Combine both for the strongest guarantee: tokens shrink the
  duplicate window at storage; reader dedup catches whatever
  slips through.
- **Read-after-write on snapshot reads.** `Read` / `ReadIter` /
  `ReadPartitionIter` / `ProjectionReader.Lookup` /
  `BackfillProjection` see new records the moment `Write`
  returns. The change-stream APIs (`Poll`, `PollRecords`,
  `ReadRangeIter`, `ReadPartitionRangeIter`) intentionally lag
  the tip by `SettleWindow` to tolerate S3 LIST propagation skew.
- **Read stability.** Two consecutive snapshot reads with no
  intervening writes return the same records — the library never
  deletes or rewrites data on its own.
- **Stream replay stability.** Refs (the change-stream offsets)
  are immutable: once a record is observed at offset N by `Poll`
  / `PollRecords` / `ReadRangeIter` / `ReadPartitionRangeIter`,
  replaying from offset 0 sees
  that same record at the same offset N, every time. Load-bearing
  for checkpointed pipelines — a consumer that processed up to
  offset 100 can crash, restart from offset 100, and resume
  without missing or duplicating. Holds because the ref filename
  embeds a writer-stamped `refMicroTs` (fixed-width lex-numeric),
  refs are per-attempt paths the library never rewrites or
  deletes, and `MaxClockSkew` + `SettleWindow` bound how late a
  ref can become visible relative to its stamped time — no new
  ref can be retroactively inserted at or before an
  already-observed offset.
- **Atomic per-file visibility via the token-commit marker.**
  Every `Write` lands a single `<token>.commit` zero-byte object
  *after* the data and ref PUTs. Both read paths gate on its
  presence: snapshot reads (`Read` / `ReadIter` /
  `ReadPartitionIter` / `ProjectionReader.Lookup`) drop parquets
  without a sibling commit; the change-stream APIs (`Poll` /
  `PollRecords` / `ReadRangeIter` / `ReadPartitionRangeIter` /
  `ReadEntriesIter` / `ReadPartitionEntriesIter`) HEAD
  `<token>.commit` for each ref before yielding. A writer that
  crashes mid-sequence leaves an orphan
  parquet/ref pair that stays invisible to every read path — no
  in-flight states leak. A multi-partition `Write` is still not
  atomic *across* partitions; partition commits become visible one
  at a time. For per-partition workloads (typical CDC,
  read-modify-write under `WithIdempotencyToken`) that's
  sufficient. **For workloads that compute deltas across
  partitions in one logical step**, treat each partition's commit
  independently — read each via `PollRecords` / `ReadRangeIter` /
  `ReadPartitionRangeIter` so the read boundary lines up with
  committed refs, and checkpoint by offset rather than wall-clock.

**Corollary: once a data file is in S3, it stays forever — even if
the `Write` call returned an error.** A crashed write can leave an
orphan data file with no matching ref; it's still visible to
snapshot reads, which is consistent with at-least-once. The library
can't tell "committed" from "crashed-mid-write" without external
transactional metadata, and can't know whether a reader has already
observed a file — so **automatic garbage collection isn't possible
without breaking read stability**. Cleanup of orphans is an
operator decision (S3 lifecycle rules, or a manual prune with
readers quiesced), not a library feature.

**Concurrency contract: one in-flight write per token.**
Concurrent writes that share the same `WithIdempotencyToken` —
or auto-token writes that happen to share an attempt-id, a
non-occurrence under UUIDv7 — are **out of contract**.
Sequential retries (a failed write followed by a same-token
retry) are the design's primary use case and remain fully
supported. The constraint buys back enough determinism that
the writer's `<token>.commit` PUT can be tolerated as a
no-overwrite write under sequential retries: the upfront HEAD
short-circuits a same-token retry before any second commit
PUT lands.

**Writer wall-clock is in the protocol.** Refs encode
`refMicroTs` — the writer's microsecond wall-clock captured
immediately before the ref PUT. The reader's `refCutoff = now
- SettleWindow` therefore compares writer-stamped time against
reader wall-clock; `MaxClockSkew` bounds writer↔reader skew.
No backend-`LastModified` dependency. See
[CLAUDE.md "Backend assumptions"](CLAUDE.md#backend-assumptions)
for the full treatment.

Detailed contract and configuration in
[Durability guarantees](#durability-guarantees).

## Install

```bash
go get github.com/ueisele/s3store@latest
```

s3store is pre-v1 — **minor version bumps (`v0.x.0`) may carry breaking
API changes**. Pin an exact version in your `go.mod` (or commit your
`go.sum`) to control when you pick them up. Requires Go 1.26.2+ (declared
in [go.mod](go.mod)).

## Initializing a new dataset

Each dataset's timing config is persisted as **two** objects so
writer and reader agree on the values by construction:

- `<Prefix>/_config/commit-timeout` — the writer's budget for
  the contract-relevant tail of the write: the elapsed wall-clock
  from `refMicroTs` (captured just before the ref PUT) to
  token-commit-PUT completion. Pre-ref work (parquet encoding,
  marker PUTs, data PUT — the last of which scales with payload
  size) is **not** in the budget. A write whose ref→commit
  elapsed exceeds this budget returns an error to the caller —
  the token-commit still lands, so a same-token retry recovers
  via the upfront HEAD; the error surfaces that the stream
  reader's `SettleWindow` (derived from this same value) may
  already have advanced past the write's `refMicroTs`. See
  *Tuning CommitTimeout* below for the recommended range.
- `<Prefix>/_config/max-clock-skew` — the operator's assumed
  bound on writer↔reader wall-clock divergence. Refs encode
  the writer's `refMicroTs` directly, so this skew is what the
  reader's `refCutoff` has to absorb. Read by the reader's poll
  cutoff; the writer doesn't read it.

`SettleWindow = CommitTimeout + MaxClockSkew` is **derived**, not
persisted; it's the cutoff `Poll` / `PollRecords` /
`ReadRangeIter` / `ReadPartitionRangeIter` apply to the live tip.

Operators seed both objects once when provisioning a new prefix;
`NewS3Target` (and `New(StoreConfig)`) GET them at construction time
and stamp the resolved values (plus the derived `SettleWindow`)
on the Target. Construction fails with a hint when either object
is missing.

Each body is a Go `time.Duration` string. The library has no
fallback — a missing or unparseable object fails construction
with a hint at the seeding step. Floors:
`commit-timeout ≥ 1ms` (`CommitTimeoutFloor` — strictly positive;
zero would cause every write to exceed the timeout); construction
additionally emits a `slog.Warn` at level WARN when the configured
value is below `CommitTimeoutAdvisory` (6s — see *Tuning
CommitTimeout* below). `max-clock-skew ≥ 0` (zero is valid on
tightly-clocked deployments, negative is incoherent). The boto3
snippet below ships `10s` / `1s` as sensible starting values for a
typical deployment with NTP-synced nodes; tune higher when you
can't rule out larger skew, lower (down to ~6s) when you want a
tighter stream-reader cutoff.

Once seeded, both values are **immutable** — changing either
silently rewrites history (decreasing `commit-timeout` makes valid
markers fail; increasing it resurrects timed-out writes;
decreasing `max-clock-skew` may shift the cutoff so stream
consumers temporarily skip refs they used to include). An operator
who genuinely needs different values re-creates the store at a
new prefix and migrates.

Seed both with the Python `boto3` snippet below (one-time, before
any process constructs a Target):

```python
import boto3

s3 = boto3.client("s3", endpoint_url="https://s3.example.com")
s3.put_object(
    Bucket="my-bucket",
    Key="my-prefix/_config/commit-timeout",
    Body=b"10s",
    ContentType="text/plain",
)
s3.put_object(
    Bucket="my-bucket",
    Key="my-prefix/_config/max-clock-skew",
    Body=b"1s",
    ContentType="text/plain",
)
```

The integration-test fixture (`fixture_test.go`) provides a
`SeedTimingConfig` helper that does the same thing, called once
per test.

### Tuning `CommitTimeout`

`CommitTimeout` bounds the elapsed wall-clock from `refMicroTs`
(captured just before the ref PUT) to token-commit-PUT
completion. Two PUTs participate in that window. Each S3 call is
wrapped by the library's transient-error retry policy:
`retryMaxAttempts = 4` (1 initial + 3 retries) with
`retryBackoff = [200ms, 400ms, 800ms]` — total **1.4s of backoff
sleep** per call in the worst case, plus the per-attempt request
time. Across the two PUTs, the worst-case retry-sleep envelope is
**~2.8s**, again before counting actual request time.

`CommitTimeoutAdvisory` is set to **6s** — roughly 2× the worst-
case retry envelope, with headroom for actual request latency.
Values below this still pass construction (the hard floor is
`CommitTimeoutFloor = 1ms`), but `NewS3Target` emits a
`slog.Warn` at level WARN naming the configured value and the
advisory floor:

| Configured value | Behaviour                                                |
|------------------|----------------------------------------------------------|
| `< 1ms` or `≤ 0` | Construction fails — "below the floor".                  |
| `1ms` … `< 6s`   | Construction succeeds with a `slog.Warn`. Acceptable on tightly-clocked dev/test deployments where you control retry behaviour and want a tight `SettleWindow`. |
| `≥ 6s`           | No warning. Recommended for production.                  |

Sizing guidance:

- **Production on AWS S3 / MinIO / single-site StorageGRID with
  NTP-synced nodes**: 10–30s. Leaves headroom on transient retries
  and keeps `SettleWindow = CommitTimeout + MaxClockSkew` at a
  modest tens of seconds.
- **Multi-site StorageGRID**: tune higher (30–120s) if your
  cross-site replication adds visible latency to PUT
  acknowledgement at `strong-global`.
- **Dev/test loops where you want sub-second `SettleWindow`**:
  pick `< 6s`, accept the warning, run on a backend where retries
  are unlikely (local MinIO).

The advisory floor is a recommendation, not a contract — the
library's correctness invariants don't depend on it. It exists so
operators see a clear signal when their configured budget is
below the worst-case retry envelope of the two SettleWindow-
relevant PUTs.

## Quick start

```go
type CostRecord struct {
    CustomerID   string    `parquet:"customer_id"`
    ChargePeriod string    `parquet:"charge_period"`
    SKU          string    `parquet:"sku"`
    NetCost      float64   `parquet:"net_cost"`
    Currency     string    `parquet:"currency"`
    CalculatedAt time.Time `parquet:"calculated_at,timestamp(millisecond)"`
}

// New does two S3 GETs on the persisted timing-config objects at
// <Prefix>/_config/commit-timeout and <Prefix>/_config/max-clock-skew
// — seed both once via the snippet in "Initializing a new dataset"
// before this call.
store, err := s3store.New[CostRecord](ctx, s3store.StoreConfig[CostRecord]{
    S3TargetConfig: s3store.S3TargetConfig{
        Bucket:            "warehouse",
        Prefix:            "billing",
        S3Client:          s3Client,
        PartitionKeyParts: []string{"charge_period", "customer"},
    },
    PartitionKeyOf: func(r CostRecord) string {
        return fmt.Sprintf("charge_period=%s/customer=%s",
            r.ChargePeriod, r.CustomerID)
    },
    // Optional: enable latest-per-entity dedup on Read / PollRecords.
    // EntityKeyOf and VersionOf are required together — both or
    // neither. New rejects partial config.
    EntityKeyOf: func(r CostRecord) string {
        return r.CustomerID + "|" + r.SKU
    },
    VersionOf: func(r CostRecord) int64 {
        return r.CalculatedAt.UnixMicro()
    },
})
if err != nil {
    log.Fatal(err)
}

// Write — groups records by PartitionKeyOf, one Parquet file per group
_, err = store.Write(ctx, records)

// Snapshot read — deduplicated by VersionOf when EntityKeyOf is set
latest, err := store.Read(ctx, []string{"charge_period=2026-03-17/customer=abc"})

// Iterate over a range without buffering everything in memory
for r, err := range store.ReadIter(ctx, []string{"charge_period=2026-03-*/*"}) {
    if err != nil { return err }
    process(r)
}

// Stream changes since an offset (one S3 LIST, no GETs)
entries, next, err := store.Poll(ctx, since, 100)
```

`s3store` decodes directly into `[]CostRecord` via the parquet struct
tags on `T` (parquet-go's `GenericReader[T]`) — no `ScanFunc` or manual
column-order bookkeeping. A field whose column is missing from a given
file lands as Go's zero value.

## Writer / Reader / Narrow-T reads

`s3store.Store[T]` is a composition of two public halves: a `Writer[T]`
for the write path and a `Reader[T]` for the read path. Most users keep
using `New(StoreConfig)` and get both through `Store`. Services that only
write or only read can construct a single half with a narrower config:

```go
// Build the shared S3 wiring once — Target is the untyped handle
// Writer, Reader, ProjectionReader, and BackfillProjection all speak.
// NewS3Target does two S3 GETs (commit-timeout + max-clock-skew) at
// construction time; seed both via the snippet in "Initializing a new
// dataset" before this call.
target, err := s3store.NewS3Target(ctx, s3store.S3TargetConfig{
    Bucket:            "warehouse",
    Prefix:            "billing",
    S3Client:          s3Client,
    PartitionKeyParts: []string{"charge_period", "customer"},
})
if err != nil { log.Fatal(err) }

// Write-only service: no read-side knobs in config.
w, err := s3store.NewWriter[CostRecord](s3store.WriterConfig[CostRecord]{
    Target:         target,
    PartitionKeyOf: func(r CostRecord) string { /* ... */ },
})

// Read-only service: no PartitionKeyOf / Compression.
r, err := s3store.NewReader[CostRecord](s3store.ReaderConfig[CostRecord]{
    Target: target,
})
```

### Narrow-T reads

When the on-disk record has a heavy write-only column you never read
(a JSON blob, an audit log, an embedding vector), declare a narrower
`T'` that omits it and open a `Reader[T']` over the Writer's data.
parquet-go skips unlisted columns on decode — the heavy bytes stay in
S3.

```go
type FullRec struct {
    Customer   string `parquet:"customer"`
    Amount     float64 `parquet:"amount"`
    ProcessLog string `parquet:"process_log"` // huge, write-only
}
type NarrowRec struct {
    Customer string  `parquet:"customer"`
    Amount   float64 `parquet:"amount"`
    // ProcessLog deliberately absent.
}

store, _ := s3store.New[FullRec](cfg)
view, _ := s3store.NewReaderFromStore[NarrowRec, FullRec](store,
    s3store.ReaderConfig[NarrowRec]{})

recs, _ := view.Read(ctx, []string{"*"}) // []NarrowRec — ProcessLog not fetched
```

The narrow `ReaderConfig[T']` carries the read-side knobs
(`EntityKeyOf`, `VersionOf`, `ConsistencyControl`); the
constructor overwrites the `Target`
field from the source Writer/Store so `SettleWindow` and the
S3 wiring are inherited automatically. Dedup closures are typed
over `T'`, so you supply them explicitly for the narrow shape
when needed.

The relationship between constructors:

| Want | Call |
|---|---|
| Both write and read, same `T` | `New(StoreConfig[T])` → `*Store[T]` |
| Write only, narrow config | `NewWriter(WriterConfig[T])` → `*Writer[T]` |
| Read only, narrow config | `NewReader(ReaderConfig[T])` → `*Reader[T]` |
| Same-/narrower-`T'` Reader over a Writer | `NewReaderFromWriter[T', U](writer, cfg)` |
| Same-/narrower-`T'` Reader over a Store | `NewReaderFromStore[T', U](store, cfg)` |

## Non-trivial Go types (`decimal.Decimal`, UUID wrappers, …)

parquet-go can't encode types like `shopspring/decimal.Decimal` or wrapper
types with custom marshaling. The library takes no opinion on the
translation — define a parquet-friendly shadow struct in your package and
translate at the boundary:

```go
// Domain type — used throughout your app
type Usage struct {
    InstanceID     string
    SkuID          string
    ProjectID      uuid.UUID
    Amount         decimal.Decimal
    CalculatedAt   time.Time
}

// File layout — parquet-friendly primitives only
type UsageFile struct {
    InstanceID   string    `parquet:"instance_id"`
    SkuID        string    `parquet:"sku_id"`
    ProjectID    string    `parquet:"project_id"`
    Amount       int64     `parquet:"amount,decimal(18,6)"` // scaled integer
    CalculatedAt time.Time `parquet:"calculated_at,timestamp(millisecond)"`
}

func toFile(u Usage) (UsageFile, error) {
    scaled := u.Amount.Shift(6)
    if !scaled.IsInteger() {
        return UsageFile{}, fmt.Errorf(
            "amount %s has more than 6 decimal places", u.Amount)
    }
    return UsageFile{
        InstanceID:   u.InstanceID,
        SkuID:        u.SkuID,
        ProjectID:    u.ProjectID.String(),
        Amount:       scaled.BigInt().Int64(),
        CalculatedAt: u.CalculatedAt,
    }, nil
}

func fromFile(f UsageFile) (Usage, error) {
    pid, err := uuid.Parse(f.ProjectID)
    if err != nil { return Usage{}, err }
    return Usage{
        InstanceID:   f.InstanceID,
        SkuID:        f.SkuID,
        ProjectID:    pid,
        Amount:       decimal.New(f.Amount, -6),
        CalculatedAt: f.CalculatedAt,
    }, nil
}

// Hand the library UsageFile, not Usage.
store, _ := s3store.New[UsageFile](s3store.StoreConfig[UsageFile]{ /* ... */ })

// Writes:
files := make([]UsageFile, len(usages))
for i, u := range usages {
    f, err := toFile(u); if err != nil { return err }
    files[i] = f
}
_, err := store.Write(ctx, files)

// Reads:
files, err := store.Read(ctx, []string{"..."})
usages := make([]Usage, len(files))
for i, f := range files {
    u, err := fromFile(f); if err != nil { return err }
    usages[i] = u
}
```

Parquet's `DECIMAL(p, s)` logical type is the preferred encoding for
monetary values — most parquet readers (Spark, Trino, DuckDB) decode
it as a real decimal so `SUM(amount)` just works. Use `int64`
backing for precision ≤ 18, `[N]byte` for more.

## S3 layout

```
s3://warehouse/billing/
  data/
    charge_period=2026-03-17/
      customer=abc/
        job-2026-03-17-batch01-0190a8b4d3a87f4ab1c2d3e4f5a6b7c8.parquet
        job-2026-03-17-batch01.commit                                              ← atomic-visibility marker
  _ref/
    1710684000123456-job-2026-03-17-batch01-0190a8b4d3a87f4ab1c2d3e4f5a6b7c8;charge_period=2026-03-17%2Fcustomer=abc.ref
```

- `data/` holds Parquet files, partitioned Hive-style. Filenames
  are `<token>-<UUIDv7>.parquet` — under
  `WithIdempotencyToken("job-…batch01")` the token prefixes the
  basename; without a token, an auto-generated UUIDv7 stands in
  as the token (so the basename becomes `<UUIDv7>-<UUIDv7>`).
  Every attempt of a write lands on its own per-attempt path; no
  PUT in the write path overwrites.
- `<token>.commit` siblings are zero-byte commit markers. A
  parquet without a paired commit is invisible to every read path.
- `_ref/` holds one **empty** file per ref. The filename encodes
  `refMicroTs` (writer wall-clock), the data file's
  `<token>-<UUIDv7>` id, and the URL-escaped partition key. `Poll`
  is a single S3 LIST over this prefix; per-ref HEAD on
  `<token>.commit` gates visibility.

### Partition naming: column or path-only

`PartitionKeyParts` names can either match Parquet column names or be entirely
separate — both patterns are supported.

**Pattern A — partition name == Parquet column name** (all the examples above).
The partition key is a real record attribute, and `PartitionKeyOf` reads it
directly from the struct. Because `Write` derives the path from the same
record the Parquet file holds, the Hive value and the column value always
agree, and external SQL engines pointed at the layout (DuckDB, Trino,
Athena, …) see one consistent value either way. The one way to surface a
mismatch is to call `WriteWithKey` with a key inconsistent with the
record's fields — don't do that.

**Pattern B — partition name separate from every Parquet column** (classical
Hive). The partition key is *derived*, not stored as a field:

```go
// Parquet columns: ts, customer_id, amount — no "year"/"month".
PartitionKeyParts: []string{"year", "month"},
PartitionKeyOf: func(r Record) string {
    y, m, _ := r.Ts.Date()
    return fmt.Sprintf("year=%d/month=%02d", y, int(m))
},
```

Partition values live only in the path, saving storage. `s3store`
ignores Hive paths on the typed read path — if you need
`year`/`month` on records, reconstruct them from `Ts`. External
SQL engines (DuckDB, Trino, Athena, …) still surface the Hive
columns when pointed at the data path, so you can `SELECT` /
`WHERE` on them from outside the library.

Pick A when the partition is a first-class attribute (customer, tenant);
pick B when it's a derived time bucket. Mixing within one store is fine.

## Glob grammar

The same grammar is used by every snapshot read entry point and
validated once:

| Pattern | Accepted? |
|---|---|
| `*` (literal) — match everything | ✓ |
| `charge_period=2026-03-17/customer=abc` — exact | ✓ |
| `charge_period=2026-03-*/customer=abc` — trailing `*` in value | ✓ |
| `*/customer=abc` — whole-segment `*` | ✓ |
| `charge_period=2026-03-01..2026-04-01/customer=abc` — range `FROM..TO` | ✓ |
| `charge_period=2026-03-01../customer=abc` — range, unbounded upper | ✓ |
| `charge_period=..2026-04-01/customer=abc` — range, unbounded lower | ✓ |
| `charge_period=*-17/customer=abc` — leading `*` | ✗ |
| `charge_period=2026-*-17/customer=abc` — middle `*` | ✗ |
| `charge_period=[0-9]/customer=abc` — char class | ✗ |
| `charge_period={2026,2027}/customer=abc` — alternation | ✗ |

Truncated patterns (fewer segments than `PartitionKeyParts`) and mislabelled
segments (part name in the wrong position) are also rejected.

### Partition ranges

`keyPart=FROM..TO` matches any value `v` with `FROM <= v < TO`, lex order
(half-open, mirroring `WithUntilOffset`). Either side may be empty for an
unbounded end. Both sides are plain literals — no `*`, no `..`. `..` alone
is rejected; use `*` to match everything.

Ranges enable partition pruning: the read path extracts the common
prefix of `FROM` and `TO` as an S3 `LIST` prefix so only
potentially-matching keys are enumerated.

**Bounds are compared lexicographically** — byte-wise Go string
compare. The range matches *characters*, not numbers or dates.
**Partition values must be chosen so lex order matches intent:**

- ISO-8601 timestamps (`2026-03-01T00`, `2026-03-01`) — correct by design.
- Zero-padded fixed-width numbers (`00042`, not `42`) — correct.
- Unpadded numbers are a trap: `customer=10..100` validates cleanly, but lex
  order includes `"2"` (because `"2" > "10"` byte-wise) and excludes `"42"`.
  The validator catches the fully-reversed case (`42..100` is rejected as
  `from > to`) but not this subtler one. If your values are numeric, pad
  them or switch to a string shape that sorts correctly.

**Equal endpoints match nothing.** `FROM..FROM` is valid grammar but yields
zero rows (half-open `[a, a)` is empty), mirroring `WithUntilOffset` where
`since == until` returns no records. Use an exact segment (`keyPart=FROM`)
if that's what you meant.

`..` is reserved: the write path rejects any partition value that contains
`..` (otherwise a value like `a..b` would be unaddressable — any pattern
mentioning it would be parsed as a range). Escape / reshape the value on
the way in if your domain needs literal `..`.

## Access patterns

### Write

```go
// Groups records by PartitionKeyOf, one PUT per group
results, err := store.Write(ctx, records)

// Or skip the grouping step and write a pre-grouped batch
result, err := store.WriteWithKey(ctx, "charge_period=X/customer=Y", recs)
```

Writes are **atomic at the file level**: every attempt PUTs
`<token>.commit` last, and both read paths gate on its presence.
A writer that fails or crashes before the commit lands leaves an
orphan `<token>-<UUIDv7>.parquet` on S3, but every read path
filters it out — no in-flight states leak to readers. The library
never deletes the orphan; operators clean up via lifecycle rule
or manual prune (see
[Read stability](#guarantees) and
[Idempotent writes](#idempotent-writes)).

For retry-safe writes (orchestrator failover, crash-and-resume), see
[Idempotent writes](#idempotent-writes).

`Write` fans out per-partition work in parallel, bounded by the
`S3Target`-level `MaxInflightRequests` semaphore (default 32). The
semaphore is acquired by every PUT/GET/HEAD/LIST inside the
target, so net in-flight S3 requests stay ≤ `MaxInflightRequests`
regardless of fan-out axis — the partition fan-out balances
naturally against marker fan-out within a partition (a write
touching one wide partition with many markers drains all 8 slots
on those marker PUTs; a write touching 8 partitions with one
marker each spreads the slots across partitions). Each partition
is self-contained — on error the function cancels still-running
partitions and returns the results that already committed in
sorted-key order, alongside the first real error. Callers can
retry the failed partitions via `WriteWithKey` without re-writing
the ones that succeeded.

> The cap is per `S3Target`, not library-wide. A Writer and a
> Reader built from the same `S3Target` share the cap. Two
> Targets do not.
>
> The AWS SDK v2's default HTTP transport leaves
> `MaxConnsPerHost` unlimited and sets `MaxIdleConnsPerHost` to
> 100, so the library cap is what bounds parallelism for
> stock-configured clients — no transport tuning needed. Only if
> you've explicitly set a non-zero `MaxConnsPerHost` on your
> `*s3.Client`'s transport, make sure it's ≥ `MaxInflightRequests`
> (or excess requests queue at the transport instead of running
> in parallel).

### Stream — refs only

```go
entries, newOffset, err := store.Poll(ctx, lastOffset, 100)
```

Each `StreamEntry` carries the ref's `Offset` (opaque cursor, pass back as
`since` on the next call), the `Key` (partition key as written), and the
`DataPath` (S3 key of the parquet file). No GETs are issued — the entire
batch is one S3 LIST call over `_ref/`.

### Stream — typed records

```go
// When dedup is configured, the default is latest-per-key
// within each batch (Kafka compacted-topic semantics).
records, newOffset, err := store.PollRecords(ctx, lastOffset, 100)

// Opt out: every record in every referenced file, in ref order.
records, newOffset, err = store.PollRecords(ctx, lastOffset, 100,
    s3store.WithHistory())
```

Dedup runs on every read path when `EntityKeyOf` AND `VersionOf`
are both set on the Reader (`New` / `NewReader` reject partial
config). When neither is set, every record passes through in
decode order. `WithHistory()` forces the no-dedup path explicitly.

**Tie-break on equal max version (default dedup, no
`WithHistory`).** When two writes share the same version for the
same entity key — common on an at-least-once retry that replays a
batch with the same domain timestamp — the **lex-later filename
wins**. Input is sorted by `(entityKey, versionOf)` stable on
ties; files feed in lex order, so within a tied group the lex-
later file's record is the last one and dedupLatest's
`pending` advances onto it.

For the auto-token path the basename is `<UUIDv7>-<UUIDv7>` —
UUIDv7 sorts by embedded millisecond time, so lex-later =
wrote-later in practice. With `WithIdempotencyToken` the
filename starts with the caller's token; the tie-break follows
the token's lex order rather than wall-clock order. Use a
time-sortable token format (e.g. `{ISO-timestamp}-{suffix}`) if
you rely on chronological tie-breaking. Stable across repeated
reads either way. To make ties impossible, ensure `VersionOf`
strictly increases per write.

**Replicas under `WithHistory`.** Records that share
`(entity, version)` describe the same logical write — a retry,
zombie, or cross-node race — and are by definition equivalent.
Both backends collapse them to one record; *which physical
instance is yielded is implementation-defined* because the
contract treats them as equivalent. If your writer can produce
records that share `(entity, version)` but differ in content,
that's a writer-side data quality issue and dedup can't paper
over it.

### Per-record "when was this inserted"

If a consumer needs the write time of every record, set
`StoreConfig.InsertedAtField` to the name of a `time.Time` field on
`T`. The field must carry a non-empty, non-`"-"` parquet tag — the
writer populates it with its wall-clock time at write-start and
persists the value as a real parquet column, so both read paths
see the exact same timestamp on disk.

```go
type Event struct {
    Customer   string    `parquet:"customer"`
    Amount     float64   `parquet:"amount"`
    InsertedAt time.Time `parquet:"inserted_at"` // writer populates this column
}

s3store.StoreConfig[Event]{
    // ...
    InsertedAtField: "InsertedAt",
}

recs, _ := store.Read(ctx, []string{"*"})
// recs[i].InsertedAt is the writer's wall-clock at write-start.
```

Works on every read path (`Read`, `ReadIter`, `ReadPartitionIter`,
`ReadRangeIter`, `ReadPartitionRangeIter`, `ReadEntriesIter`,
`ReadPartitionEntriesIter`, `PollRecords`) — the column
round-trips like any other parquet field. The reader has no
special handling. Zero reflection cost when unset.

To use the writer-stamped time as the dedup version, reference
the field from `VersionOf`:

```go
VersionOf: func(r Event) int64 { return r.InsertedAt.UnixMicro() }
```

### Stream — time window

To read only records written within a time window (e.g. "yesterday's
activity"), use `ReadRangeIter` with `time.Time` bounds. The range is
half-open `[since, until)`, matching Kafka offset semantics.

`ReadRangeIter` returns an `iter.Seq2[T, error]` backed by the same
streaming pipeline that powers `ReadIter` — partition prefetch
(`WithReadAheadPartitions`, default 1 = one partition lookahead),
byte-budget streaming (`WithReadAheadBytes`, default 0 = uncapped),
cross-file pipelining, per-partition dedup. Memory is bounded by
whichever cap binds first:

```go
// All records written on 2026-04-17 (UTC).
start := time.Date(2026, 4, 17, 0, 0, 0, 0, time.UTC)
end   := time.Date(2026, 4, 18, 0, 0, 0, 0, time.UTC)
for r, err := range store.ReadRangeIter(ctx, start, end) {
    if err != nil { return err }
    // process r
}
```

Breaking out of the range loop cleanly cancels in-flight downloads.
The ref LIST walk runs upfront before the first record yields —
sub-100ms for typical windows, but for huge backfill windows chunk
via `since`/`until` to stream incrementally instead of walking
everything first. If you need to checkpoint offsets between batches
(resume on cancel), use `PollRecords` (Kafka-style batched API) —
`ReadRangeIter` does not surface per-batch offsets.

Half-open boundary semantics:

- A record written at `2026-04-17 23:59:59.999999` — **included** (offset < end).
- A record written exactly at `2026-04-18 00:00:00.000000` — **excluded** (that instant belongs to the next window).
- A record written exactly at `2026-04-17 00:00:00.000000` — **included**.

So to cover a full day, `end` is the start of the *next* day.

**Timezone**: `ReadRangeIter` compares in UTC microseconds internally
(bounds are encoded from `time.UnixMicro()`). `time.Date(..., time.UTC)`
gives UTC-day boundaries; `time.Date(..., loc)` gives local-day
boundaries — both work, as long as `since` and `until` use the same
timezone.

Pass `time.Time{}` (zero value) for `since` to start at the stream
head, or for `until` to walk to the live tip (settle-window cutoff
as of the call). The upper bound is **snapshotted at call entry**
so the walk terminates even under sustained writes — writes landing
after the call started are not picked up. To keep up with new
writes, use `PollRecords` and checkpoint between batches.

For CDC / change-processing where every update matters and the
caller checkpoints offsets between batches, use `PollRecords`
(cursor-based, Kafka-style):

```go
start := lastCheckpoint
for {
    records, next, err := store.PollRecords(ctx, start, 100,
        s3store.WithUntilOffset(store.OffsetAt(end)))
    if err != nil { return err }
    if len(records) == 0 { break }
    // process every record (no version collapse — see dedup below)
    start = next
    checkpoint(start)
}
```

**Dedup asymmetry between the two stream APIs**:

- `ReadRangeIter` (range-bounded, snapshot-style): default
  latest-per-entity per partition, matching `Read` / `ReadIter`.
  You're reading "the state of this window"; collapsing to the
  latest version is what you want.
- `PollRecords` (cursor-based, CDC-style): replica-dedup only —
  every distinct `(entity, version)` flows through. Per-batch
  latest-per-entity is meaningless on a cursor (the next batch
  may carry a newer version of the same entity), so it's not
  offered. `WithHistory` is accepted but is the default here.

`OffsetAt` is pure computation — no S3 call — and bridges
`time.Time` to the offset cursor used by `Poll` / `PollRecords` /
`WithUntilOffset`.

### Snapshot

```go
records, err := store.Read(ctx, []string{"charge_period=2026-03-17/customer=abc"})
```

Returns every record matching the glob, decoded directly into `[]T`
via parquet-go and the parquet tags on `T`. When dedup is configured
(see Stream above), the result is the latest version per key;
otherwise every version comes through.

### Snapshot — streaming (`ReadIter`)

`Read` buffers the full result set before returning. For month-scale or
otherwise unbounded ranges that's a memory problem; use `ReadIter`
instead — it yields records one at a time via Go 1.23's
`iter.Seq2[T, error]`:

```go
for r, err := range store.ReadIter(ctx, []string{"charge_period=2026-03-*/*"}) {
    if err != nil { return err }
    aggregate(r)            // fold into an aggregation map and forget r
}
// breaking out of the loop cancels in-flight downloads — no Close
```

Available on both `Store[T]` and the underlying `Reader[T]`.

**Memory profile**: `O(one partition's records)`. The pipeline
processes one partition at a time: files inside the partition
download in parallel, the decoded batch is sort+dedup'd
(latest-per-entity by default; replica-only on `WithHistory()`),
records are yielded in `(entity, version)` ascending order, then
the batch is dropped before the next partition starts. With
`EntityKeyOf` unset the sort is skipped entirely and records emit
in decode order (file lex order, then parquet row order).
Month-scale reads go from O(month) to O(partition) peak memory,
which is usually small enough for hourly/daily partitioning. If a
single partition is large enough that even one pre-dedup batch is
a problem, file an issue — we can follow up with a streaming fold
that trades the code simplicity for peak memory proportional to
unique entities rather than total records.

**Pipeline shape**: downloads are continuous and not partition-bound.
A worker pool of `MaxInflightRequests` goroutines pulls files in
partition+file order; cross-partition lookahead happens automatically.
A separate decoder walks partitions in lex order — for each, it waits
until all files are downloaded, parses each parquet footer to compute
the partition's exact uncompressed size, gates on the budget, decodes,
and pushes to the yield loop.

**Two budget knobs**, evaluated together — whichever binds first
holds the producer back:

- `WithReadAheadPartitions(n)`: at most `n` decoded partitions sit
  buffered ahead of the yield position. Default `1` — one-partition
  lookahead so decode of N+1 overlaps yield of N. Values < 1 are
  floored to 1 (strict-serial decode is no longer offered as a
  public mode; use the byte cap for tighter bounds).
- `WithReadAheadBytes(n int64)`: at most `n` uncompressed bytes (read
  from each parquet file's footer — exact, not a heuristic) sit
  decoded in the buffer. Default `0` disables the byte cap.

```go
// Many small partitions: prefetch generously by count.
for r, err := range store.ReadIter(ctx, []string{"*"},
    s3store.WithReadAheadPartitions(8)) { ... }

// Skewed partition sizes (mostly tiny + a few large): cap by bytes
// so prefetch self-throttles on the large ones. Decoded Go memory
// typically runs 1–2× the uncompressed size depending on data
// shape (string headers, slice/map pointer overhead).
for r, err := range store.ReadIter(ctx, []string{"*"},
    s3store.WithReadAheadBytes(2<<30)) { ... } // ≤ 2 GiB

// Combine — useful when both axes matter.
for r, err := range store.ReadIter(ctx, []string{"*"},
    s3store.WithReadAheadPartitions(8),
    s3store.WithReadAheadBytes(2<<30)) { ... }
```

**Trade-off**: peak memory ≈ `WithReadAheadBytes` (decoded
uncompressed bytes) + at most `max(MaxInflightRequests,
largest_partition_file_count)` compressed parquet bodies waiting
to be decoded + one in-flight decode. Downloaders apply
back-pressure when the compressed-body pool is full so the
"downloaded but not yet decoded" backlog can't grow without
bound; the floor at `largest_partition_file_count` guarantees
the largest partition can always be downloaded in full (the
decoder needs every file before the partition can be decoded).
Speed-up is bounded by how much of your per-partition yield time
would otherwise sit idle waiting for downloads.

**Single oversized partition**: if one partition's uncompressed
total exceeds `WithReadAheadBytes` on its own, that partition still
flows (the cap can't bind below partition granularity without
row-group-level streaming). The cap only prevents *additional*
partitions from joining the buffer.

**Per-partition dedup contract — uniform across every read path**:
`Read` / `ReadIter` / `ReadPartitionIter` / `ReadRangeIter` /
`ReadPartitionRangeIter` / `ReadEntriesIter` /
`ReadPartitionEntriesIter` / `PollRecords` all dedup within one
Hive partition at a time. **Correct only when the partition key
strictly
determines every component of `EntityKeyOf`** — i.e. no entity ever
spans two partitions. For layouts where entities can move between
partitions over time (e.g. a customer that switches region), pass
`WithHistory()` and dedup yourself.

For typical time-series shapes (`charge_period_start` leads both
`PartitionKeyParts` and the entity key) the contract holds. There is
no global-dedup escape hatch: every snapshot read returns the same
records (the per-partition pipeline is the single decode path), and
peak memory stays bounded by `WithReadAheadBytes` /
`WithReadAheadPartitions`.

**Order**: `ReadIter` visits partitions in lex order and downloads
files within a partition in lex order. Within a partition the user-
visible emission order is `(entity, version)` ascending when
`EntityKeyOf` is set; when no dedup is configured the sort is skipped
entirely and records emit in decode order (file lex order, then
parquet row order).

## Idempotent writes

s3store defaults to at-least-once on the write path: a retry after a
partial failure re-runs the whole write and produces duplicate data,
markers, and refs under fresh keys. For workloads where retries are
common (orchestrator failover, network flakiness, crash-and-resume),
`WithIdempotencyToken` makes the write retry-safe end-to-end.

```go
const token = "job-2026-04-22T10:15:00Z-batch42"

_, err := store.WriteWithKey(ctx, key, records,
    s3store.WithIdempotencyToken(token))
```

On retry with the same token:

- **Per-attempt data path** — every attempt lands at a fresh
  `{token}-{UUIDv7}.parquet`. Data and ref PUTs never overwrite
  by construction — sidesteps multi-site StorageGRID's
  eventual-consistency exposure on data-key overwrites; uniform
  behaviour across AWS S3, MinIO, and StorageGRID at any
  consistency level.
- **Upfront-HEAD dedup gate** — before generating a fresh
  attempt-id, the writer issues a single HEAD on
  `<dataPath>/<partition>/<token>.commit`. 200 → reconstruct the
  prior `WriteResult` from the marker's user-metadata
  (`attemptid`, `refmicrots`, `insertedat`) and return without
  re-uploading anything. 404 → proceed with a fresh attempt-id
  end-to-end. The auto-token (no `WithIdempotencyToken`) path
  skips the HEAD: a freshly generated UUIDv7 is guaranteed to
  404 by construction.
- **Recovery from arbitrarily long outages.** Same token works
  forever: retries across S3 outages, orchestrator restarts,
  process crashes — all converge automatically. There's no
  `ErrCommitWindowExpired`, no generation-suffix juggling on
  the orchestrator side.

**Cost: per-attempt orphans on failure.** Every failed attempt
under `WithIdempotencyToken` may leave a data file + ref on S3
as orphans (any attempt that crashed before the commit PUT).
Reader paths filter them out — both snapshot and stream reads
gate on the `<token>.commit` marker. The deferred operator-driven
sweeper reclaims them. A retry storm with sane orchestrator
backoff produces orphans bounded by `(retry rate) × (parquet
size)` per token.

#### Per-partition tokens

When a single `Write` call spans several partitions and each
partition has its own logical-write identifier (one outbox row
per partition, distinct upstream batch IDs, etc.), use
`WithIdempotencyTokenOf` instead of `WithIdempotencyToken`. The
closure runs once per partition with the partition's records in
hand and returns the token to use for that partition:

```go
_, err := store.Write(ctx, records,
    s3store.WithIdempotencyTokenOf(func(part []Rec) (string, error) {
        // The partition is non-empty by construction (Write
        // groups by PartitionKeyOf and skips empty groups).
        // part[0] is sufficient for any per-partition derivation.
        return outboxRowID(part[0]), nil
    }))
```

Each partition's commit marker, upfront HEAD, and per-attempt id
use the per-partition token; retries dedup independently per
partition. Mutually exclusive with `WithIdempotencyToken` —
combining them surfaces an error at option-resolution time. A
non-nil error from the closure aborts that partition's write
(under multi-partition fan-out, sibling partitions whose closure
succeeded may still commit — partial-success contract). Tokens
returned from the closure are validated per partition via the
same rules as the static option (non-empty, no `/`, no `..`,
printable ASCII, ≤200 chars).

### Optimistic commit (skip the upfront HEAD)

Every idempotent write does an upfront `HEAD` on `<token>.commit`
to detect a same-token retry. For workloads near the S3 request-
rate ceiling — typically large per-call partition fan-outs at
high frequency — that HEAD is the per-write cost worth optimising
away. Pass `WithOptimisticCommit()` to swap the *upfront* HEAD
for an *on-collision* HEAD: the writer skips the HEAD on the
fresh path and detects a prior commit by the conditional commit
PUT itself failing.

```go
store.Write(ctx, records,
    s3store.WithIdempotencyToken(token),
    s3store.WithOptimisticCommit())
```

**Mechanism.** The token-commit PUT carries `If-None-Match: *`
(modern S3 conditional-write semantics). When a prior commit
exists:

- Backends supporting conditional PUT (AWS S3 since November 2024,
  recent MinIO, StorageGRID 12.0+) return **412 Precondition
  Failed**.
- Backends behind a bucket policy denying `s3:PutOverwriteObject`
  on the commit subtree (older StorageGRID) return **403 Access
  Denied**.

The writer recognises both, runs one HEAD on `<token>.commit` to
recover the canonical attempt's metadata, and returns the prior
commit's `WriteResult` unchanged — same `DataPath`, `RefPath`,
`Offset`, `InsertedAt`, `RowCount` the caller would have got
from the legacy upfront-HEAD path.

**Trade-off.** The fresh path saves one `HEAD` per write
(5–15ms on AWS, ~20% of the per-Write request budget for a
3-PUT marker-less write). The retry-found-prior path leaves an
**orphan parquet + ref** behind for every same-token retry —
the data and ref PUTs from this attempt never get garbage-
collected unless the operator runs a cleanup. Both orphans are
invisible to readers via the commit gate (their attempt-id is
not the canonical one named in `<token>.commit` metadata).

The break-even sits around a 5% retry-found-prior rate. Below
that, the HEAD savings dominate; above that, the orphan and
bandwidth cost outweighs the savings. Healthy CDC pipelines
retry well under 1%, so the option is a clear win for high-
throughput workloads near the request-rate ceiling. Bulk
migrations with frequent orchestrator restarts (or any workload
where retries are routine) probably don't benefit.

The companion metric is `s3store.write.optimistic_commit.collisions`
— a counter incremented on each on-collision recovery. Chart it
against `s3store.method.calls{method="write"}` to see your
collision rate in production and confirm the option is paying off.

**Mutual compatibility.** `WithOptimisticCommit` composes with
`WithIdempotencyToken` and `WithIdempotencyTokenOf`. It has no
effect on auto-token writes (no caller-supplied token): the
upfront HEAD is already skipped there because a freshly-generated
UUIDv7 is guaranteed to 404 by construction.

#### Backend setup

The mechanism is portable across backends, but each one needs
support for *some* form of "fail this PUT if the object already
exists." Pick whichever path your backend supports.

**AWS S3 / MinIO / StorageGRID 12.0+: conditional PUT (preferred).**
No setup. The library sends `If-None-Match: *` on every
`<token>.commit` PUT under `WithOptimisticCommit`. Modern S3 APIs
honour the precondition atomically server-side, returning 412
when the object exists. Verify your backend version supports it
before enabling the option in production.

**StorageGRID (legacy versions): bucket policy denying
`s3:PutOverwriteObject` on the commit subtree.** This works by
rejecting any PUT that would overwrite an existing object whose
key matches `<prefix>/data/*/*.commit`. Concurrent same-token
writes (out of contract) are unaffected — the deny is post-
completion, not a mutex — but for sequential retries the second
PUT lands as 403 AccessDenied, which the library recognises and
routes to the same recovery path.

The boto3 snippet below installs the deny on a single
`<prefix>` (run it once per prefix you intend to use with
`WithOptimisticCommit` against a deny-policy backend):

```python
import json

import boto3

s3 = boto3.client("s3", endpoint_url="https://storagegrid.example.com")

bucket = "my-bucket"
prefix = "my-prefix"  # matches StoreConfig.Prefix

policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "DenyOverwriteOfTokenCommit",
            "Effect": "Deny",
            "Principal": "*",
            "Action": "s3:PutOverwriteObject",
            "Resource": (
                f"arn:aws:s3:::{bucket}/{prefix}/data/*/*.commit"
            ),
        }
    ],
}

s3.put_bucket_policy(
    Bucket=bucket,
    Policy=json.dumps(policy),
)
```

The deny scopes to `<prefix>/data/*/*.commit` — only the commit
markers are protected. Data files (`<token>-<UUIDv7>.parquet`)
and refs land under fresh per-attempt paths and never overwrite
by construction, so they don't need the deny. Markers under
`<prefix>/_projection/...` are intentionally overwriteable
(byte-equivalent zero-byte content), so excluding them keeps
projection writes working.

If you have multiple `Prefix` values on the same bucket, repeat
the `Statement` entry per prefix or widen the `Resource` glob
accordingly.

**Backends without either mechanism.** If the conditional PUT is
silently ignored *and* no deny policy is in place, the library
cannot detect a prior commit on the optimistic path. The second
commit PUT would silently overwrite, and the caller would receive
the *new* attempt's `WriteResult` instead of the prior commit's —
breaking the same-token-retry-returns-same-result contract. The
library does not detect this at construction time. **Verify
backend support before enabling `WithOptimisticCommit`.**

### Idempotency and reader dedup are complementary

Reader dedup (`EntityKeyOf` + `VersionOf`) and idempotency
tokens cover different concerns; pick by what your data and
your pipeline give you.

**Reader dedup is primarily latest-version selection.** When
the same entity is written multiple times with different
`VersionOf` values, dedup collapses to the latest version per
entity. This is its main job, and tokens don't replace it.

**Tokens collapse physical replicas of the same logical
record.** Sequential retries short-circuit at the upfront HEAD
on `<token>.commit`. Out-of-contract near-concurrent retries
are still absorbed at read time — the commit's `attemptid`
arbitrates one canonical attempt; both read paths filter
LIST/refs down to it. The reader sees one set of rows, not
two. This works only under **token stability**: the same
logical record must always derive the same token, across
retries, restarts, and process boundaries. (On multi-site
StorageGRID below `strong-global` the canonical-attempt choice
can differ transiently across sites during overwrite
propagation, but deterministic parquet encoding makes both
attempts' records byte-equivalent — undetectable to the
consumer.)

**Reader dedup also handles replica collapse when token
stability isn't achievable** — token store lost, derivation
differs across processes/replays, multiple writers emit the
same logical record under independent tokens. No cross-token
arbitration exists, so `EntityKeyOf` + `VersionOf` becomes the
only collapse point.

| Config | Storage layer | Consumer layer |
|---|---|---|
| No token, no dedup | at-least-once | at-least-once |
| No token, dedup configured | at-least-once | **exactly-once** (latest version per entity) |
| Stable token + dedup | at-least-once (per-attempt orphans on failure) | **exactly-once** (latest version per entity, across retry sessions) |
| Stable token alone | at-least-once (per-attempt orphans) | **exactly-once** per logical record; versions not collapsed |

**Recommendation**: enable reader dedup whenever the data has
versions to collapse (the typical case). Tokens are
orthogonal — they make retries cheap and absorb replica
duplication when token stability holds; reader dedup picks up
both replica and version collapse when stability doesn't.

### Cross-backend uniformity

Per-attempt-paths for data+refs plus the upfront-HEAD on
`<token>.commit` produce the same retry semantics on every
supported backend:

- **AWS S3 / MinIO** — read-after-new-write is strongly
  consistent out of the box. The upfront HEAD always observes
  any prior commit; data and ref PUTs always observe what was
  just written.
- **NetApp StorageGRID** — `ConsistencyControl` defaults to
  `strong-global` (the safe multi-site choice). Single-site
  deployments can downgrade to `strong-site` explicitly when the
  per-call cost matters; see
  [STORAGEGRID.md](STORAGEGRID.md) for the topology decision
  matrix. **No `s3:PutOverwriteObject` bucket policy required**
  — data and ref PUTs use per-attempt paths; the
  `<token>.commit` overwrite that arises only on declared-out-
  of-contract concurrent same-token retries is byte-equivalent
  record-wise (deterministic parquet encoding) regardless of
  which attempt's metadata wins.

### Probing a token without writing (`LookupCommit`)

`Writer.LookupCommit(ctx, partition, token)` returns the prior
`WriteResult` if `<token>.commit` already exists in the
partition, or `(WriteResult{}, false, nil)` when it doesn't. A
single HEAD against the marker — same primitive the write path
uses for upfront-dedup, exposed for orchestrators that need to
ask "did this logical step already commit?" without going
through `Write`:

```go
wr, ok, err := store.LookupCommit(ctx, partitionKey, token)
if err != nil { return err }
if ok {
    // the prior attempt of this token committed; skip the work
    return wr, nil
}
// proceed with the write…
```

Useful when retries are driven by an external orchestrator that
already knows the partition key and the token, and wants to
short-circuit before re-encoding parquet.

### Zombie writers and orchestrators

The library does not enforce single-writer-per-partition —
that's a caller invariant. Two cases:

- **Same token.** Sequential retries collapse via the
  upfront HEAD on `<token>.commit`. Concurrent writers
  (out of contract per [Concurrency contract](#guarantees)) both
  miss the prior commit on their upfront HEAD and both write
  their own per-attempt data + ref + commit; reader dedup via
  `(entity, version)` collapses the duplicate records.
- **Different tokens.** Two distinct per-attempt triples
  (different paths, different refs); reader dedup via
  `(entity, version)` collapses them at read time.

For orchestrator-driven jobs: **reuse the same token across
failovers** (persist it in your job state). A fresh token per
restart is valid but creates real storage duplication.

### Alternative: external outbox (Postgres / similar)

If you already run a transactional database alongside your writers,
an outbox pattern often composes better than s3store's ref stream:

1. On every successful write, `INSERT` into an outbox table with
   columns `(token, partition_key, data_path, created_at)` and a
   monotonic `id`.
2. Consumers read by `id`.
3. Unique constraint on `token` makes zombie/retry writes visible as
   constraint violations — Postgres is the authoritative dedup.

This moves the dedup primitive off S3, so on StorageGRID you can
downgrade `ConsistencyControl` from the default `strong-global`
to `read-after-new-write` (`ConsistencyDefault`) and still get
exactly-once at the consumer layer — the outbox absorbs every
storage-layer race. Any rare storage-layer
duplicate from a weak-consistency race becomes a "ghost" file that
isn't referenced by an outbox row — wasted bytes, not a visible
duplicate. The s3store ref stream still gets written alongside the
outbox; consumers just ignore it.

s3store does not ship this pattern; document it as a valid
alternative for callers who already have the transactional
infrastructure.

## Schema evolution

Reads tolerate missing columns out of the box: a field whose column
isn't in a given parquet file lands as Go's zero value, never an
error. That covers the common "added a column" case without any
extra configuration. parquet-go matches columns to struct fields by
the `parquet:` tag, so column order in the file doesn't matter and
unknown columns are ignored.

Renames, splits, and row-level computed derivations still require a
migration tool — rewrite the affected files with the new shape.

## Secondary projections

When a query filters on a column that isn't a partition key
(e.g. "list every customer that had usage of SKU X in period
P"), scanning every data file is prohibitive at scale. A
secondary projection solves this by writing one empty S3
*marker* per distinct tuple of the columns you want to query.
The query is a single LIST under the marker prefix — zero
parquet reads.

### Shape

A projection has two halves that are wired separately:

- **Write side (`ProjectionDef[T]`)** lives in `StoreConfig.Projections` /
  `WriterConfig.Projections`. `Of` returns the per-record column
  values as a `[]string` aligned to `Columns` (positional, in
  declared order). `Of` is optional — nil means the library
  reflects T's `parquet` tags + `Columns` once at `NewWriter` and
  emits markers without any caller code.
- **Read side (`ProjectionLookupDef[K]`)** is consumed by
  `s3store.NewProjectionReader(target, lookupDef)` to build a typed
  `ProjectionReader[K]` query handle. `From` projects each marker back into
  K. Same convention: nil means reflect K's parquet tags against
  `Columns`; a non-nil custom `From` overrides.

```go
// Usage carries parquet tags for the partition columns:
//   SKUID             string    `parquet:"sku_id"`
//   CausingCustomer   string    `parquet:"causing_customer"`
//   ChargePeriodStart time.Time `parquet:"charge_period_start"`
//   ChargePeriodEnd   time.Time `parquet:"charge_period_end"`

cfg.Projections = []s3store.ProjectionDef[Usage]{{
    Name: "sku_period_idx",
    Columns: []string{
        "sku_id", "charge_period_start",
        "causing_customer", "charge_period_end",
    },
    // String fields auto-project. The two time.Time columns
    // share one Layout.Time format — RFC3339 here. No Of needed.
    Layout: s3store.Layout{Time: time.RFC3339},
}}

store, _ := s3store.New[Usage](cfg)

// Build the typed query handle. From is nil, so the library
// reflects SkuPeriodEntry's parquet tags to project each marker
// back into a typed struct. K can carry time.Time fields directly
// — Layout.Time on the read side mirrors the write side, parsing
// path segments back into time.Time via time.Parse.
type SkuPeriodEntry struct {
    SKUID             string    `parquet:"sku_id"`
    ChargePeriodStart time.Time `parquet:"charge_period_start"`
    CausingCustomer   string    `parquet:"causing_customer"`
    ChargePeriodEnd   time.Time `parquet:"charge_period_end"`
}

skuIdx, _ := s3store.NewProjectionReader(store.Target(),
    s3store.ProjectionLookupDef[SkuPeriodEntry]{
        Name: "sku_period_idx",
        Columns: []string{
            "sku_id", "charge_period_start",
            "causing_customer", "charge_period_end",
        },
        Layout: s3store.Layout{Time: time.RFC3339}, // must match write-side
    })
```

When every `Columns` entry is already a `parquet:"..."`-tagged
string field on T, both `Of` and `Layout` can be left zero:

```go
cfg.Projections = []s3store.ProjectionDef[Usage]{{
    Name:    "sku_customer_idx",
    Columns: []string{"sku_id", "causing_customer"},
    // Of: nil — library auto-projects from Usage's parquet tags.
}}
```

When time.Time columns need different layouts per column, or
column values come from somewhere other than parquet-tagged
fields on T, fall back to writing `Of` explicitly:

```go
cfg.Projections = []s3store.ProjectionDef[Usage]{{
    Name:    "mixed_idx",
    Columns: []string{"sku_id", "month", "at"},
    Of: func(u Usage) ([]string, error) {
        return []string{
            u.SKUID,
            u.ChargePeriodStart.Format("2006-01"),    // month bucketing
            u.ChargePeriodStart.Format(time.RFC3339), // full timestamp
        }, nil
    },
}}
```

Projections are wired at construction time, so registration
cannot race with `Write` and "registered after the first Write"
is not a reachable state. Use `BackfillProjection` (below) to
retroactively cover records written before a projection existed.

`ProjectionReader[K]` is T-free — a read-only service can build
the handle without depending on the writer's record type.

Every `Write` call iterates each registered projection, collects
a deduplicated set of marker paths across the batch, and PUTs
one empty marker per distinct path under
`<Prefix>/_projection/<name>/<col>=<val>/.../m.proj`. Duplicate
writes are idempotent (same S3 key, same empty body).

#### Of and From: positional, aligned to Columns

`Of` returns `[]string` and `From` takes `[]string`, both
positional to `Columns`. Two consequences:

- **Ordering discipline.** A custom `Of`/`From` that disagrees
  with `Columns` (e.g. swapping the order of two values) writes
  the wrong markers — silently. The library checks length but
  not semantics. Refactor `Columns` and the function together,
  or rely on the auto-projection (nil) which can't get order
  wrong.
- **No helper needed for the default case.** With nil `Of` /
  nil `From`, the library does the reflection itself. Custom
  closures only when transformation is needed (formatted
  timestamps on write, post-processing on read) or when K has
  no parquet tags.

### Lookup

```go
hits, err := skuIdx.Lookup(ctx, []string{
    "sku_id=SKU-123/charge_period_start=2026-03-01..2026-04-01/"+
    "causing_customer=*/charge_period_end=*",
})
// hits []SkuPeriodEntry
```

The pattern grammar is the same one `Read` accepts (exact,
trailing-`*`, whole-segment `*`, `FROM..TO` range). Results are
unbounded — narrow the pattern if a projection has millions of
matches.

### Multi-pattern reads

The single-pattern grammar is Cartesian per segment — `period=*`
combined with `customer=abc` matches the cross product of all
periods × abc. When the caller has an arbitrary **set of tuples**
(e.g. `(period=2026-03, customer=abc), (period=2026-04,
customer=def)` but *not* the off-diagonal combinations), pass them
as additional elements of the same `[]string` patterns slice.
Every read entry point already takes `[]string`:

Every read entry point that takes patterns — `Read`, `ReadIter`,
`ReadPartitionIter`, `ProjectionReader.Lookup`,
`BackfillProjection` — accepts the same `[]string` shape:

```go
// Read across non-Cartesian tuples.
recs, _ := store.Read(ctx, []string{
    "period=2026-03-17/customer=abc",
    "period=2026-03-18/customer=def",
})

// Projection lookup over an arbitrary set of (col, col) tuples.
entries, _ := idx.Lookup(ctx, []string{
    "sku=s1/customer=abc",
    "sku=s4/customer=def",
})

// One-off migration across several partitions.
stats, _ := s3store.BackfillProjection(ctx, target, def,
    []string{"period=2026-03-*/customer=*", "period=2026-04-01/customer=*"},
    until, nil)
```

**Execution model:** LIST calls fan out across patterns with the
Target's `MaxInflightRequests` cap, overlapping patterns are
deduplicated at the key level, the GET+decode pool runs over the
unioned set, and dedup (if configured) applies per Hive partition
— an entity is kept as the latest version within its partition.
Under the per-partition dedup precondition (`EntityKeyOf` fully
determined by the partition key), an entity never spans two
partitions, so multi-pattern unions still surface one latest pick
per entity.

A single-element slice is the common case; the multi-pattern API
simply lets the caller add more when they need a non-Cartesian
set.

### What's in scope for v1

- Wire projections via `StoreConfig.Projections`; auto-write on `Write` +
  `Lookup` via the typed `ProjectionReader[K]` handle.
- Read-after-write on Lookup: the marker PUT and the marker LIST
  both inherit `ConsistencyControl` from the shared `S3Target`,
  so a `Lookup` issued immediately after `Write` sees the new
  marker without a settle delay. On AWS S3 / MinIO that's
  native; on StorageGRID, set the level once on the target and
  it flows to marker PUT + LIST automatically (see
  [STORAGEGRID.md](STORAGEGRID.md)).
- **Backfill** as a standalone package function (below).

### Backfill

The normal path is to wire a projection into `StoreConfig.Projections`
before the first `Write` so every record produces markers. When
that isn't possible — adding a projection to a store that
already has data — the typical shape is:

1. Deploy the live app with the projection in `StoreConfig.Projections`; every
   new `Write` emits markers from time T0 onward.
2. Capture `until := T0` — the watermark before which historical
   data is uncovered.
3. Run a **one-off migration job** using the package-level
   `s3store.BackfillProjection` that scans files with `LastModified <
   until` and PUTs the retroactive markers.

```go
stats, err := s3store.BackfillProjection(ctx,
    store.Target(),     // or construct via s3store.NewS3Target
    def,                // the same ProjectionDef the live app registered
    []string{"*"},      // patterns (PartitionKeyParts grammar)
    until,              // exclusive upper bound on LastModified (time.Time)
    func(path string) { slog.Warn("missing data", "path", path) },
)
// stats.DataObjects / Records / Markers
```

`BackfillProjection` is deliberately standalone — no `Writer` /
`Reader` argument — so the migration job doesn't need the live
app's full config. It runs through the same `S3Target`
abstraction used by the read-side `NewProjectionReader`, issuing both
parquet GETs and marker PUTs via `target.S3Client`. Typically
invoked from a dedicated binary (`cmd/backfill-<name>/main.go`
in your repo): a ~30-line `main` that builds an S3 client +
`S3Target` + the same `ProjectionDef[T]` the live app uses, then calls
`BackfillProjection`.

The pattern is evaluated against the target's `PartitionKeyParts`
(same grammar as `Read`), **not** against the projection's `Columns`
— backfill walks parquet data files, which are keyed by
partition. A migration job can shard itself by partition
(`period=2026-01-*` on one pod, `period=2026-02-*` on another)
instead of running a single multi-hour call. The `until` bound
lets the live writer and the migration job cooperate without
overlap: live markers for everything from T0, backfill markers
for everything before. Pass `time.Time{}` (the zero value) to
disable the bound (covers every file currently present —
harmless but redundant if the live writer has been up for a
while, since PUT is idempotent).

Idempotent at the S3 level (same empty marker, same key), so a
retry after cancel or crash is a no-op on work already done. Safe
to run while the live writer keeps emitting markers for fresh
records.

Backfilled markers are subject to the same read-after-write
contract as live-write markers: on a strong-consistent backend
(AWS, MinIO, or StorageGRID with `ConsistencyControl` set on
the target) a `Lookup` issued after `BackfillProjection` returns
sees every marker it just wrote.

### Not in v1 (deferred)

- **Delete projection** — no general delete path on the store yet.
- **Verification / orphan cleanup tools** — if `Of` changes
  semantically, stale markers remain. Backfill only adds
  missing markers; rebuild (delete-then-re-PUT) is a separate
  design.

### Column ordering matters (for performance, not correctness)

Put columns you typically filter on **first**. They form the
S3 LIST prefix, so a query that specifies them literally narrows
the LIST. Trailing columns are always parsed out of the marker
filename — correct but slower when there's nothing to prune on.

### Column values are strings on disk

Column values land on disk as hive-encoded strings — `Of`
returns `[]string`, marker keys are strings, and the default
binder accepts only `string` and `time.Time` fields on T / K.
String fields project directly. `time.Time` fields are
formatted (write) and parsed (read) via `Layout.Time`, which
must be set on both sides to the same layout.

For other types (numbers, bools, custom enums), format them in
a custom `Of` and post-process in a custom `From` — same way
`PartitionKeyOf` already does for data paths. Keeps the read
path a pure round-trip.

## Compression

Write uses snappy by default — same as Spark, DuckDB's parquet writer,
Trino, and Athena emit out of the box. Snappy cuts file size 2-3× on
typical data with negligible CPU cost. Change via `StoreConfig.Compression`:

```go
s3store.StoreConfig[T]{
    // ...
    Compression: s3store.CompressionZstd,
}
```

Accepted values:

- `CompressionSnappy` — fast, default.
- `CompressionZstd` — better ratio, higher CPU; good for cold / archive data.
- `CompressionGzip` — legacy, moderate CPU and ratio.
- `CompressionUncompressed` — no compression; only if CPU cost dwarfs S3 cost.

Zero value (empty string) resolves to snappy, so leaving the field unset
is safe. `New()` rejects any other string.

parquet-go auto-detects the codec on read, so switching compression
per Write doesn't require any read-side config. External engines
(DuckDB, Spark, Trino) do the same.

## Settle window

```
Refs on S3 (chronological):
... 1000 1001 1002 1003 1004 1005 1006 1007
                                 ↑
                          now - SettleWindow
                              ──────→ don't read yet
```

Refs don't appear in LIST in `refMicroTs` order. `refMicroTs` is
captured by the writer with `time.Now()` *before* the ref PUT
issues, but the ref isn't LIST-visible until the PUT is
acknowledged — and the PUT's duration varies per writer (per
request, per network blip, per retry). Two concurrent writers
that capture `refMicroTs` 10 ms apart can land in LIST in the
opposite order if their PUTs took 50 ms vs 200 ms. This is not a
consistency-model artifact; it happens on every backend
including AWS S3, MinIO, and StorageGRID at `strong-global`. So
`Poll` reads up to `now - SettleWindow` to give the slowest
in-flight PUT enough time to land before its `refMicroTs` falls
inside the cutoff.

`SettleWindow = CommitTimeout + MaxClockSkew` is **derived** from
the two persisted timing knobs (see
[Initializing a new dataset](#initializing-a-new-dataset) for
seeding). The two-knob shape decomposes what the cutoff actually
has to cover:

- **`CommitTimeout`** absorbs the per-PUT wall-clock duration
  drift described above. A successful write — by the writer-side
  enforcement check — has `time.Since(refMicroTs) ≤ CommitTimeout`
  at token-commit completion, which means its ref PUT (one of
  the two PUTs in the budget) also completed within
  `CommitTimeout` of `refMicroTs`. So if `refCutoff = now -
  SettleWindow` ever advances past a ref's `refMicroTs`, the ref
  has already had at least `CommitTimeout` to land in LIST and
  its `<token>.commit` to land for HEAD — both are visible to
  the reader (under read-after-new-write, which the library's
  backend assumptions require). The drift caused by parallel
  writers with varying PUT durations is exactly what
  `CommitTimeout` is sized to absorb; no separate
  "concurrent-writer drift" term is needed.
- **`MaxClockSkew`** absorbs writer↔reader wall-clock
  divergence. Refs encode `refMicroTs` directly, so the reader's
  comparison `refMicroTs ≤ now - SettleWindow` is across two
  different machines' clocks. `MaxClockSkew` is the operator's
  declared bound on that divergence (zero on tightly-clocked
  NTP-synced fleets).

Together they bound the cutoff so it can't overtake a ref whose
ref PUT or token-commit PUT might still be landing server-side.
This gives you a single monotonic offset with no seen-set or
dedup bookkeeping.

The formula assumes **read-after-new-write on the ref LIST and
the token-commit HEAD** — the library's documented backend
prerequisite (see [CLAUDE.md](CLAUDE.md) "Backend assumptions").
On a backend that doesn't honor that, no `SettleWindow` value is
sufficient; the cutoff math has nothing to lean on.

### Enforcement: the writer's elapsed-time check

The "`CommitTimeout` absorbs the per-PUT drift" claim above
holds **only if every successful write actually fits inside
`CommitTimeout`**. The writer enforces that directly. After the
token-commit PUT, the writer compares
`time.Since(time.UnixMicro(refMicroTs))` — elapsed wall-clock
from the moment just before the ref PUT — against
`CommitTimeout`. Pre-ref work (parquet encoding, marker PUTs,
data PUT) is deliberately outside the budget: only the
ref-LIST-visible → token-commit-visible window can put the
`SettleWindow` contract at risk. If exceeded, the writer returns
an error to the caller — the commit IS in place (data is
durable; snapshot reads see the new records immediately), but a
stream reader whose `SettleWindow` already advanced past
`refMicroTs` may have emitted past the ref. The error tells the
caller their write is at risk; a same-token retry recovers via
the upfront HEAD with the original `WriteResult`. Either way the
contract holds: a ref is permanently invisible (writer raised
the error → caller retries, fresh `refMicroTs`) or it landed
within budget (the absorber claim holds for it).

If any PUT in the sequence fails (markers, data, ref, or
token-commit), the call returns a wrapped error. The library
never deletes anything it has written; per-attempt paths mean a
failed attempt's data and ref don't conflict with a retry, and
both read paths gate on `<token>.commit` so an orphan parquet
remains invisible until an operator-driven prune removes it.
Under `WithIdempotencyToken` the retry runs the upfront HEAD
first — if the failed attempt happened to land its commit, the
retry returns its `WriteResult` unchanged; otherwise the retry
generates a fresh attempt-id and proceeds end-to-end.

```go
if _, err := store.Write(ctx, recs,
    s3store.WithIdempotencyToken(token),
); err != nil {
    // Some PUT didn't land, or the writer's elapsed exceeded
    // CommitTimeout. Retry with the same token — the upfront
    // HEAD on <token>.commit dedups the prior attempt if it
    // secretly succeeded; otherwise the retry writes a fresh
    // attempt end-to-end.
    return retry(...)
}
```

For **exactly-once at the consumer**, configure reader dedup
(`EntityKeyOf + VersionOf`). Near-concurrent retry overlap (out
of contract per the
[Concurrency contract](#guarantees) but still bounded if it
arises by accident) can leave two committed attempts whose
records share `(entity, version)`; dedup collapses them.

## StorageGRID

s3store works on AWS S3, MinIO, and NetApp StorageGRID. AWS and
MinIO need no configuration — both are strongly consistent on
LIST and GET by default and ignore the `Consistency-Control`
header. **StorageGRID setup:**

- `ConsistencyControl` defaults to `ConsistencyStrongGlobal` —
  the safe multi-site choice. Single-site grids (or multi-site
  grids with strictly co-located reader/writer pairs) can
  downgrade explicitly to `ConsistencyStrongSite` to avoid the
  cross-site PUT cost.

No bucket policy required: data and ref PUTs use per-attempt
paths, and the `<token>.commit` overwrite that arises only on
out-of-contract concurrent same-token retries is record-wise
byte-equivalent (deterministic parquet encoding). The
`s3:PutOverwriteObject` deny earlier versions required is no
longer used. See [STORAGEGRID.md](STORAGEGRID.md) for the full
topology decision matrix, operational notes on the 11.9
availability cliff, and the consistency-model reasoning that
motivates the `ConsistencyControl` default.

## Durability guarantees

The contract is **at-least-once** on both sides of the wire, plus
**read-after-write** on every operation except `Poll` /
`PollRecords` / `ReadRangeIter` / `ReadPartitionRangeIter`
(which deliberately lag the live tip by `SettleWindow` to
tolerate S3 LIST propagation skew — see
[Settle window](#settle-window)).

### Read-after-write

If `Write` (or `WriteWithKey`) returns success, every one of the
following operations issued from any process against the same
bucket sees the new records immediately — no sleep, no settle
delay:

- `Reader.Read` / `ReadIter` / `ReadPartitionIter`
- `ProjectionReader.Lookup`
- `BackfillProjection`

`Poll` / `PollRecords` / `ReadRangeIter` /
`ReadPartitionRangeIter` are the intentional exceptions: they
apply the `SettleWindow` cutoff so near-tip refs stay hidden
until S3 LIST propagation has had time to settle. A ref that's
written inside the window will be returned by a subsequent poll
issued after one `SettleWindow` has elapsed.

#### What you need to configure

- **AWS S3 / MinIO.** Nothing. Both backends give strong
  read-after-write on LIST and GET by default and ignore the
  `Consistency-Control` header — the default
  `ConsistencyStrongGlobal` is a no-op there.
- **StorageGRID (NetApp).** The default
  `ConsistencyStrongGlobal` is the safe multi-site choice and
  needs no further configuration. Single-site grids can
  downgrade to `ConsistencyStrongSite` explicitly to save the
  cross-site PUT cost. No bucket policy required; both options
  are documented in [STORAGEGRID.md](STORAGEGRID.md), which also
  explains why a weaker level (or asymmetric writer/reader
  levels) breaks the read-after-write contract.

### Write

If `Write` (or `WriteWithKey`) returns `nil`, every record in
the batch is durably stored in S3 and will be returned by
subsequent `Read`, `Poll`, `PollRecords`, and
`ProjectionReader.Lookup` for the lifetime of the data. The write
path commits in order: marker PUTs → data PUT → ref PUT →
`<token>.commit` PUT, returning success only after the
token-commit lands and the writer's ref→commit elapsed
(`time.Since(time.UnixMicro(refMicroTs))`) is within
`CommitTimeout`.

If `Write` returns an **error**, state is indeterminate — the
records may or may not be durable, and the commit may or may
not have landed. The caller must retry. A retry after partial
success may write some records twice; dedupe those on read via
`EntityKeyOf` + `VersionOf`. Multi-group `Write` returns
`([]WriteResult, error)` — consult the slice for records that
*did* commit before the error so a retry can skip them if needed.

To make retries dedup against prior successful attempts, pass
`WithIdempotencyToken` — see
[Idempotent writes](#idempotent-writes). Each attempt writes
its data and ref to per-attempt paths; the upfront HEAD on
`<token>.commit` dedups sequential retries (returning the prior
`WriteResult` unchanged); the writer's elapsed-time check
against `CommitTimeout` enforces the write-path budget,
returning a wrapped error if the commit landed too late so the
caller retries.

#### Context cancellation boundary

`Write` honours `ctx` cancellation through the data PUT (Step 5
of the write sequence). A cancel before the data PUT lands
returns `ctx.Err()` and leaves either nothing on S3 or an
orphan parquet — invisible to readers via the commit gate, dead
weight on S3 until an operator-driven prune.

Once the data PUT lands successfully, the ref PUT and the
`<token>.commit` PUT issue under `context.WithoutCancel(ctx)` —
they run to completion regardless of caller cancellation.
Without this boundary, a cancel between the data PUT and the
commit PUT would leave the most expensive form of orphan: a
multi-megabyte parquet that's invisible-but-durable when the
work to make it visible was two zero-byte PUTs away. Both PUTs
are bounded by the library's retry policy
(`retryMaxAttempts = 4` with cumulative ~1.4s backoff per call)
plus the AWS SDK's per-request timeouts; in practice they
complete in milliseconds. A caller that genuinely needs to
abort must do so before the data PUT returns; once `Write`
returns success, the cancellation that may have arrived during
the no-cancel window is moot — the records are committed.

### Read

A data-file GET that returns S3 `NoSuchKey` is operator-driven
(lifecycle policy, manual prune, or external delete — the library
itself never deletes data it has written). The library splits the
response by path:

- **Strict — fail loudly.** `Read`, `ReadIter`, and
  `ReadPartitionIter` propagate the `NoSuchKey` as a wrapped
  error. These paths LIST the partition tree first, so a missing
  file is genuinely a LIST-to-GET race (the file vanished in the
  millisecond window between LIST and GET); a caller retry
  resolves it because the next LIST won't include the deleted key.
- **Tolerant — skip and signal.** `PollRecords`, `ReadRangeIter`,
  `ReadPartitionRangeIter`, `ReadEntriesIter`,
  `ReadPartitionEntriesIter`, and `BackfillProjection` walk the
  ref stream / data tree on a long-running shape where a single
  missing file shouldn't poison the whole job. They log via
  `slog.Warn` (level WARN, key=path, method=poll_records /
  read_range_iter / read_partition_range_iter / read_entries_iter
  / read_partition_entries_iter / backfill) and increment the
  `s3store.read.missing_data` counter, then continue. The caller's
  slog handler decides what to do with the warning; metrics are
  picked up by any OTel-configured backend.

Every *other* GET error (throttle, network, auth, timeout) is
still fatal on every path — silently dropping records on
transient failure is worse than propagating.

## Configuration

```go
type StoreConfig[T any] struct {
    // S3 wiring (Bucket, Prefix, S3Client, PartitionKeyParts,
    // ConsistencyControl, MaxInflightRequests, MeterProvider) —
    // see the S3TargetConfig docstring for the full per-field
    // contract. Embedded so callers can build helpers against
    // *S3TargetConfig once and reuse them across StoreConfig,
    // WriterConfig (via NewS3Target), and ReaderConfig.
    S3TargetConfig

    // Required for Write
    PartitionKeyOf func(T) string  // derive key from record (Write)

    // Read-side dedup (used by every read path: Read / ReadIter /
    // ReadPartitionIter / ReadRangeIter / ReadPartitionRangeIter /
    // ReadEntriesIter / ReadPartitionEntriesIter / PollRecords).
    // Both or neither — explicit opt-in, no default. New rejects
    // partial config.
    EntityKeyOf func(T) string  // identifies a unique entity
    VersionOf   func(T) int64   // monotonic version per entity

    // CommitTimeout / MaxClockSkew are NOT StoreConfig fields. They're
    // persisted at <Prefix>/_config/commit-timeout and
    // <Prefix>/_config/max-clock-skew so writer and reader agree
    // by construction. Seed once via the boto3 snippet in
    // "Initializing a new dataset"; New(StoreConfig) GETs both
    // values at construction time and stamps them (plus the derived
    // SettleWindow = CommitTimeout + MaxClockSkew) on the Target.

    // Optional write-time column: if set, the writer populates
    // this field on T (must be `time.Time` with a non-empty,
    // non-"-" parquet tag like `parquet:"inserted_at"`) with
    // its wall-clock at write-start. The reader has no special
    // handling — the column round-trips on T like any parquet
    // field. Reference it from VersionOf to use the writer's
    // stamp as the dedup version:
    //   VersionOf: func(r T) int64 { return r.InsertedAt.UnixMicro() }
    InsertedAtField string

    // Parquet compression codec (default snappy).
    Compression CompressionCodec

    // Optional projections.
    Projections []ProjectionDef[T]
}
```

`StoreConfig` is the all-in-one form for `New(StoreConfig[T])`. Services
that only write or only read can use the narrower `WriterConfig[T]` /
`ReaderConfig[T]` directly with `NewWriter` / `NewReader` — see the
[Writer / Reader / Narrow-T reads](#writer--reader--narrow-t-reads) section.

## Observability

The library emits OpenTelemetry metrics through the `Metrics`
struct attached to every `S3Target`. Wiring is opt-in: pass a
`MeterProvider` on `S3TargetConfig`, or — easier — set one
globally with `otel.SetMeterProvider(...)` before constructing the
target and the library picks it up automatically.

```go
import (
    "go.opentelemetry.io/otel"
    sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// One-time at process start.
otel.SetMeterProvider(sdkmetric.NewMeterProvider(
    sdkmetric.WithReader(prometheusReader), // or OTLP, etc.
))

target, err := s3store.NewS3Target(ctx, s3store.S3TargetConfig{
    Bucket: "my-bucket", Prefix: "events",
    S3Client: s3Client,
    PartitionKeyParts: []string{"period", "customer"},
    // MeterProvider not set → falls back to otel.GetMeterProvider().
})
```

Every observation carries `s3store.bucket` and `s3store.prefix` as
constant attributes (so multi-target deployments can be split per
target in dashboards), plus `s3store.consistency_level` when
`ConsistencyControl` is non-empty. No bucket/key/partition-value
attributes are added per call — cardinality stays bounded.

### Instrument inventory

**S3 ops** (recorded at the wrapper layer in `S3Target.put` / `get`
/ `existsLocked` / `listPage`):

| Name | Kind | Unit |
|---|---|---|
| `s3store.s3.request.duration` | histogram | s |
| `s3store.s3.request.count` | counter | 1 |
| `s3store.s3.request.attempts` | histogram | 1 |
| `s3store.s3.request.body.size` | histogram | By |
| `s3store.s3.response.body.size` | histogram | By |

Per-op attributes: `s3store.operation` ∈ `put|get|head|list`,
`s3store.outcome` ∈ `success|error|canceled`, plus `error.type`
(`precondition_failed|not_found|slowdown|server|client|transport|canceled|other`)
on non-success. `s3store.s3.request.attempts` carries only
`s3store.operation` + `s3store.outcome` (it measures retry behavior,
not terminal error class — keeping cardinality bounded).

**Library methods** (recorded at the public entry point of `Write`,
`WriteWithKey`, `LookupCommit`, `Read`, `ReadIter`,
`ReadPartitionIter`, `ReadRangeIter`, `ReadPartitionRangeIter`,
`ReadEntriesIter`, `ReadPartitionEntriesIter`, `Poll`,
`PollRecords`, `ProjectionReader.Lookup`, `BackfillProjection`):

| Name | Kind | Unit |
|---|---|---|
| `s3store.method.duration` | histogram | s |
| `s3store.method.calls` | counter | 1 |
| `s3store.write.records` | histogram | 1 |
| `s3store.write.partitions` | histogram | 1 |
| `s3store.write.bytes` | histogram | By |
| `s3store.write.commit_after_timeout` | counter | 1 |
| `s3store.write.optimistic_commit.collisions` | counter | 1 |
| `s3store.read.records` | histogram | 1 |
| `s3store.read.bytes` | histogram | By |
| `s3store.read.files` | histogram | 1 |
| `s3store.read.partitions` | histogram | 1 |
| `s3store.read.missing_data` | counter | 1 |
| `s3store.read.malformed_refs` | counter | 1 |
| `s3store.read.commit_head` | counter | 1 |
| `s3store.read.commit_head_cache_hit` | counter | 1 |

`s3store.write.commit_after_timeout` increments when the
writer's elapsed time from `refMicroTs` (just before the ref PUT)
to token-commit-PUT completion exceeded `CommitTimeout`. The
write surfaces an error to the caller (commit is durable; the
stream reader's `SettleWindow` may already have advanced past
`refMicroTs` — a same-token retry recovers via the upfront HEAD).
Pre-ref work (parquet encoding, marker PUTs, data PUT) is outside
the budget by design.

`s3store.write.optimistic_commit.collisions` increments on each
`WithOptimisticCommit` write where the conditional commit PUT
was rejected because a prior `<token>.commit` already existed
(412 PreconditionFailed under conditional PUT, or 403 AccessDenied
under bucket-policy deny). The write recovers via a HEAD on the
commit marker and returns the prior `WriteResult` unchanged; the
orphan parquet + ref this attempt left behind are invisible to
readers via the commit gate. Chart against `s3store.method.calls{
method="write"}` to see the collision rate — past ~5% the option
is a net loss vs. the upfront-HEAD path.

`s3store.read.commit_head` increments on every commit-marker
HEAD that the read paths issue: snapshot reads HEAD only when
≥2 parquets share a token in one partition; stream reads HEAD
once per uncached `(partition, token)` per poll;
`Writer.LookupCommit` HEADs unconditionally. Carries
`s3store.method` so dashboards can split by which path issued
the HEAD. `s3store.read.commit_head_cache_hit` is the
companion: increments when a stream-read HEAD was satisfied
from the per-poll cache instead.

`s3store.read.missing_data` increments on `NoSuchKey` skips along
the tolerant read paths (`PollRecords`, `ReadRangeIter`,
`ReadPartitionRangeIter`, `ReadEntriesIter`,
`ReadPartitionEntriesIter`, `BackfillProjection`). Carries
`s3store.method` so dashboards can split by which path produced
the skip. Strict paths (`Read`, `ReadIter`, `ReadPartitionIter`)
fail instead of recording.

`s3store.read.malformed_refs` increments when a ref object's
filename fails to parse during a LIST on the ref stream. Skipped
after a `slog.Warn` so consumers don't crash on a future schema or
externally-written object — the counter makes the drift visible.
Surfaced under `s3store.method = poll` because the LIST that hit
it always runs in `Poll`, even when invoked indirectly by
`PollRecords` / `ReadRangeIter` / `ReadPartitionRangeIter`.

`s3store.read.partitions` records the distinct Hive partitions
touched per call on every method that funnels through the iter
pipeline: `Read` / `ReadIter` / `ReadPartitionIter` /
`ReadRangeIter` / `ReadPartitionRangeIter` / `ReadEntriesIter` /
`ReadPartitionEntriesIter` / `PollRecords`. Not recorded for
`Poll` (refs only — no decode), `ProjectionReader.Lookup`,
`Writer.LookupCommit`, or `BackfillProjection`. Mirrors
`s3store.write.partitions` on the write side.

`Write` and `WriteWithKey` show up as distinct `s3store.method`
values; `Write`'s per-partition dispatch is internal and does not
double-count.

**Target-level state** (per-Target semaphore):

| Name | Kind | Unit | What |
|---|---|---|---|
| `s3store.target.inflight` | up-down counter | 1 | currently holding a slot |
| `s3store.target.waiting` | up-down counter | 1 | currently blocked in `acquire()` (queue depth) |
| `s3store.target.semaphore.wait.duration` | histogram | s | wait time before a slot was granted |
| `s3store.target.semaphore.acquires` | counter | 1 | by `outcome` |

**Fan-out** (concurrency primitive used by writer/reader/projection work):

| Name | Kind | Unit | What |
|---|---|---|---|
| `s3store.fanout.workers` | histogram | 1 | worker goroutines per fan-out call |
| `s3store.fanout.items` | histogram | 1 | items per fan-out call |

Saturation = `s3store.target.inflight` / `MaxInflightRequests`.
Sustained `s3store.target.waiting > 0` means callers are queuing
on the semaphore; raise `MaxInflightRequests` (cost: more
concurrent S3 connections) or reduce upstream concurrency.

The AWS SDK v2 itself does not ship OTel metrics. If you also want
per-SDK-attempt observation (e.g. to distinguish smithy retries
from this library's outer `retry()` retries), attach a smithy
middleware to your `*s3.Client` directly — the wrapper-level
metrics above sit one layer outside that.

## Migration from earlier versions

Breaking changes in the single-package collapse:

- **`s3sql` package and DuckDB removed.** The SQL-on-Parquet half
  (DuckDB-backed `Query` returning `*sql.Rows`) is gone. If your
  workload needs SQL aggregation, use DuckDB directly (point its
  `httpfs` extension at the same bucket and `read_parquet()` over
  the data path) or pin an older release.
- **`QueryOption` renamed to `ReadOption`.** The read-path option
  type now matches the verbs it configures (`Read` / `ReadIter` /
  `ReadRangeIter`). `QueryOpts` and `WriteOpts` were also unexported
  to `readOpts` / `writeOpts` — the structs are an internal
  implementation detail now, but `[]s3store.ReadOption` and
  `[]s3store.WriteOption` slices built from the exported `With*`
  functions continue to work unchanged.
- **`WithIdempotentRead` removed.** The barrier was load-bearing
  only under single-writer-per-partition, and even there the
  upfront-HEAD-on-commit gate already short-circuits same-token
  retries via `WithIdempotencyToken` — recalculated bytes on a
  retry are discarded in favour of the prior commit's
  `WriteResult`. For retry-safe read-modify-write under
  `WithIdempotencyToken`, no read-side option is needed; for
  read-modify-write without tokens, callers must enforce single-
  writer themselves.
- **`Config[T]` renamed to `StoreConfig[T]`; S3 wiring moved to
  embedded `S3TargetConfig`.** The flattened wiring fields
  (`Bucket`, `Prefix`, `S3Client`, `PartitionKeyParts`,
  `ConsistencyControl`, `MaxInflightRequests`) now live on the
  embedded `S3TargetConfig` value — single source of truth for
  the S3 wiring across `StoreConfig`, `WriterConfig` (via
  `NewS3Target`), and `ReaderConfig`. Side knobs
  (`PartitionKeyOf`, `EntityKeyOf`, `VersionOf`, `Compression`,
  `InsertedAtField`, `Projections`) stay at the top level.

  ```go
  // Before
  s3store.Config[T]{
      Bucket: "...", Prefix: "...", S3Client: c,
      PartitionKeyParts:  []string{"period", "customer"},
      ConsistencyControl: s3store.ConsistencyStrongGlobal,
      PartitionKeyOf:     ...,
  }

  // After
  s3store.StoreConfig[T]{
      S3TargetConfig: s3store.S3TargetConfig{
          Bucket: "...", Prefix: "...", S3Client: c,
          PartitionKeyParts:  []string{"period", "customer"},
          ConsistencyControl: s3store.ConsistencyStrongGlobal,
      },
      PartitionKeyOf: ...,
  }
  ```

  Field-promotion still gives `cfg.Bucket = "..."` access for
  callers that mutate after construction.
- **`s3parquet` package collapsed into the root `s3store` package.**
  The single-package layout is the new home for everything: drop
  the `s3parquet/` import path and use `github.com/ueisele/s3store`
  directly. Every public symbol moved with the same name
  (`Writer[T]`, `Reader[T]`, `S3Target`, `ProjectionDef[T]`,
  `ProjectionReader[K]`, `BackfillProjection`, etc.).
- **DuckDB-only umbrella fields removed from `Config`.**
  `TableAlias`, `VersionColumn`, `EntityKeyColumns`,
  `ExtraInitSQL`, `Store.Query`, `Store.Close`, and `Store.SQL`
  are gone. Reader dedup uses `EntityKeyOf` + `VersionOf` (the
  parquet-side equivalent that already existed) — drop the SQL
  fields from your config and add the typed closures. `Store`
  no longer needs `Close` (no DuckDB connection to release).
- **`internal/testutil` package removed.** The MinIO test fixture
  is now in `fixture_test.go` in root. Only the integration build
  tag and your own integration tests are affected.
- **`DisableCleanup` field removed from `Config` and
  `WriterConfig`.** The library no longer DELETEs orphan data on
  marker / ref PUT failure paths — at-least-once on storage now
  means data files written to S3 stay until an operator-driven
  prune removes them (S3 lifecycle rule, or manual cleanup with
  readers quiesced). Drop the field from your config; service
  accounts that were granted `s3:DeleteObject` for the cleanup
  path can drop that permission too. Retries still work
  unchanged via the token + conditional-PUT path.
- **`OnMissingData` callback removed from `Config` and
  `ReaderConfig`; `BackfillProjection` no longer takes an
  `onMissingData` parameter.** Replaced with a built-in
  `slog.Warn` + `s3store.read.missing_data` OTel counter at the
  three tolerant call sites (`PollRecords`, `ReadRangeIter`,
  `BackfillProjection`). The strict paths (`Read`, `ReadIter`) now
  fail with a wrapped `NoSuchKey` error instead of skip-and-
  notify — a caller retry resolves the LIST-to-GET race. Drop
  the callback from your config and the trailing argument from
  `BackfillProjection` calls; configure your slog handler to route /
  count the warning, or alert on the counter via your metrics
  backend.

For anyone upgrading across multiple releases, the older
breaking-change history (projections-in-config, consistency-on-target,
S3Target-as-handle, idempotent-write, bloom-filter removal,
s3sql-as-Reader, the Projection refactor, the package split) is
preserved in git history — `git log -- README.md`.

## Testing

```
# Unit tests — pure Go, no C compiler needed.
go test -count=1 ./...

# Integration tests — full round-trip against a MinIO container.
# Uses testcontainers; one container is shared across the
# invocation.
go test -tags=integration -timeout=10m -count=1 ./...

# Lint (gofmt + govet + project linters in one shot).
golangci-lint run ./...
```

Integration tests require Docker and pull a pinned `pgsty/minio`
release on first run (see [`fixture_test.go`](fixture_test.go)).
After upstream `minio/minio` was archived in Feb 2026, the
community-maintained `pgsty/minio` fork continues the same
code under AGPLv3; it's a drop-in for the testcontainers MinIO
module. `-count=1` is the Go idiom for "bypass the test cache"
— without it, unchanged packages return cached results.

## Releasing

Versions are git tags following SemVer. The README badges auto-update
from the latest tag and pkg.go.dev — nothing to edit in this file.

```bash
# From a clean, pushed main:
git tag -a v0.2.0 -m "v0.2.0"
git push origin v0.2.0
```

Optionally mint GitHub release notes from the new tag:

```bash
gh release create v0.2.0 --title "v0.2.0" --generate-notes
```

`--generate-notes` auto-populates the body with commits since the
previous tag.

**Version-bump rules while pre-v1:**

- `v0.x.0` → any new feature or API change, however small.
- `v0.x.y` → bug fixes only, no API surface change.

**Immutability:** tags pushed to the public repo are cached immutably by
Go's module proxy, so a bad tag can't be replaced — cut `v0.x.(y+1)`
instead. Only delete a tag if nobody could have pulled it yet:

```bash
git tag -d v0.2.0
git push origin :refs/tags/v0.2.0
```

**Reaching v1.0.0:** when the API feels stable after real-world use, tag
`v1.0.0`. After that, breaking changes require a `v2.0.0` and a module
path rename to `github.com/ueisele/s3store/v2` — one-way door, so don't
rush it.

## Limitations

- **No SQL engine.** Reads are typed Go (`Read` / `ReadIter` /
  `ReadPartitionIter` / `ReadRangeIter` /
  `ReadPartitionRangeIter` / `ReadEntriesIter` /
  `ReadPartitionEntriesIter` / `PollRecords`) — no aggregation,
  no joins. For SQL workloads, point DuckDB at the same bucket
  via its `httpfs` extension and `read_parquet()` over the data
  path.
- **S3 key limit: 1024 bytes.** Long partition values reduce the budget.
- **Stream latency = poll interval + settle window.** Not real-time.
- **Upsert-only compacted mode.** There is no tombstone / key-delete
  mechanism — keys can only be updated, not removed.
- **Dedup is in-memory.** Large key cardinality can OOM; partition
  the dataset finely enough that any single partition's distinct
  entities fit comfortably in RAM, or read history with
  `WithHistory()` and dedup yourself.
- **Schema evolution is limited to tolerant reads.** "Column added
  to T that isn't in an old file" returns the Go zero value.
  Renames, splits, type changes, and row-level computed
  derivations require rewriting the affected files.

## Appendix: S3 calls per method

Every method's S3-call breakdown, for capacity-planning, cost
estimation, and operator dashboards. Counts are per call; multi-
pattern / multi-partition variants fan out independently and
deduplicate at the key level. All ops route through the per-
target `MaxInflightRequests` semaphore, carry the configured
`ConsistencyControl` header, and have their own retry budget
(`retryMaxAttempts = 4` with backoffs 200ms / 400ms / 800ms).

### Write

Fresh write (auto-token, or `WithIdempotencyToken` /
`WithIdempotencyTokenOf` with no prior commit):

| Op   | Count | Key                                                             |
|------|-------|-----------------------------------------------------------------|
| HEAD | 0–1   | `<Prefix>/data/<partition>/<token>.commit` (skipped on auto-token; 1× upfront on the idempotent path, returns 404) |
| PUT  | M     | `<Prefix>/_projection/<name>/.../m.proj` — one per distinct projection-marker key in the batch (M = 0 with no projections registered) |
| PUT  | 1     | `<Prefix>/data/<partition>/<token>-<attemptID>.parquet` (data) |
| PUT  | 1     | `<Prefix>/_ref/<refMicroTs>-<token>-<attemptID>;<hiveEsc>.ref` (ref) |
| PUT  | 1     | `<Prefix>/data/<partition>/<token>.commit` (token-commit, atomic visibility flip) |

Total: M+3 PUTs (auto-token) or 1 HEAD + M+3 PUTs (idempotent).

Idempotent retry (prior commit exists):

| Op   | Count | Key                                                |
|------|-------|----------------------------------------------------|
| HEAD | 1     | `<Prefix>/data/<partition>/<token>.commit` → 200, reconstruct WriteResult and return |

Total: 1 HEAD; no PUTs, no body re-upload.

Multi-partition `Write` (one logical call) fans out: each
partition runs the full sequence in parallel, capped by
`MaxInflightRequests`. Marker PUTs fan out across all partitions'
combined marker set.

### LookupCommit

| Op   | Count | Key                                                  |
|------|-------|------------------------------------------------------|
| HEAD | 1     | `<Prefix>/data/<partition>/<token>.commit`           |

200 → reconstructed WriteResult; 404 → ok=false; other → wrapped
error.

### Snapshot read

`Read` / `ReadIter` / `ReadPartitionIter` / `ReadRangeIter`
(snapshot half) / `ReadPartitionRangeIter` (snapshot half) /
`ReadEntriesIter` / `ReadPartitionEntriesIter` /
`BackfillProjection` share the same listing-and-gating skeleton:

| Op   | Count   | Key                                                           |
|------|---------|---------------------------------------------------------------|
| LIST | ≥1 per plan | `<Prefix>/data/<plan-prefix>/` — one plan per pattern, paginated (~1000 keys/page); per-plan results unioned and key-deduplicated |
| HEAD | 0–T     | `<Prefix>/data/<partition>/<token>.commit` — T = (partition, token) tuples with ≥2 parquets in the LIST (multi-attempt orphans). 0 in steady state |
| GET  | N       | `<Prefix>/data/<partition>/<token>-<attemptID>.parquet` — one per surviving parquet (post-commit-gate) |

`Read` materializes records into a slice; `ReadIter` /
`ReadRangeIter` stream them via iterator with bounded memory
(`WithReadAheadPartitions` / `WithReadAheadBytes`).
`ReadPartitionIter` / `ReadPartitionRangeIter` yield one
`HivePartition[T]` per partition (same LIST + GET shape;
the partition is the unit of yield).
`ReadEntriesIter` / `ReadPartitionEntriesIter` skip the LIST
stage entirely — the caller passes a pre-resolved
`[]StreamEntry` (typically from `Poll`), so the GET stage runs
directly.
`BackfillProjection` adds **1 PUT per derived marker per parquet**
(deduped within the parquet) on top of the GETs.

`ReadRangeIter` / `ReadPartitionRangeIter` walk the ref stream
first (see Stream read below) to collect data-file paths, then
run the GET stage above.

### Snapshot read — `ProjectionReader.Lookup`

LIST-only. Marker objects are zero-byte; key parsing alone
yields the projection's column values.

| Op   | Count        | Key                                          |
|------|--------------|----------------------------------------------|
| LIST | ≥1 per plan  | `<Prefix>/_projection/<name>/<plan-prefix>/` |

No GETs, no HEADs. The ref-stream LIST runs in `Poll`, but
`Lookup` doesn't touch the data tree.

### Stream read — `Poll`

| Op   | Count | Key                                                       |
|------|-------|-----------------------------------------------------------|
| LIST | ≥1    | `<Prefix>/_ref/` — one paginated LIST starting at `since`, capped by `maxEntries` (≤1000 per page) |
| HEAD | 0–U   | `<Prefix>/data/<partition>/<token>.commit` — U = unique (partition, token) tuples in the page; per-poll cache collapses repeats |

No GETs. Returns refs only.

`PollRecords` = `Poll` + **1 GET per returned ref** for the data
file body (decoded via the same byte-budget pipeline as
`ReadIter`).

`ReadRangeIter` / `ReadPartitionRangeIter` = one or more `Poll`
cycles to walk the `[since, until)` window into a flat ref list,
then **1 GET per data file** through the iter pipeline.

`ReadEntriesIter` / `ReadPartitionEntriesIter` skip the ref-LIST
walk entirely: the caller hands in `[]StreamEntry` directly
(typically from a prior `Poll`), so the only S3 ops are **1 GET
per entry** (plus the per-poll commit-cache HEADs, which the
caller's earlier `Poll` already paid for).

## License

See [LICENSE](LICENSE).
