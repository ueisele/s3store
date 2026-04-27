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
  refs. Dedupe on read via `EntityKeyOf` + `VersionOf`, or shrink
  the duplicate window at storage with `WithIdempotencyToken`.
- **Read-after-write on snapshot reads.** `Read` / `ReadIter` /
  `ProjectionReader.Lookup` / `BackfillProjection` see new records the moment
  `Write` returns. The change-stream APIs (`Poll`, `PollRecords`,
  `ReadRangeIter`) intentionally lag the tip by `SettleWindow` to
  tolerate S3 LIST propagation skew.
- **Read stability.** Two consecutive snapshot reads with no
  intervening writes return the same records — the library never
  deletes or rewrites data on its own.
- **No atomic write visibility.** `Write` PUTs the data file
  before its ref. Snapshot reads (`Read` / `ReadIter` /
  `ProjectionReader.Lookup`) LIST the data path directly, so they can
  observe a data file before its ref has committed — including
  orphans from a writer that crashed between the two PUTs. The
  change-stream APIs (`Poll` / `PollRecords` / `ReadRangeIter`)
  LIST the ref stream and filter those out: a ref only appears
  once its data file is durable. A multi-partition `Write` is
  also not atomic across partitions — refs become visible one at
  a time. For workloads where each write is a self-contained
  record version (typical CDC), partial visibility is benign:
  at-least-once dedup on `EntityKeyOf` / `VersionOf` collapses
  the duplicate. **For workloads that compute deltas against a
  previously-read snapshot, partial visibility is a correctness
  hazard** — read the base state via `PollRecords` /
  `ReadRangeIter` so the read boundary lines up with committed
  refs, and checkpoint by offset rather than wall-clock.

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

store, err := s3store.New[CostRecord](s3store.Config[CostRecord]{
    Bucket:            "warehouse",
    Prefix:            "billing",
    S3Client:          s3Client,
    PartitionKeyParts: []string{"charge_period", "customer"},
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
using `New(Config)` and get both through `Store`. Services that only
write or only read can construct a single half with a narrower config:

```go
// Build the shared S3 wiring once — Target is the untyped handle
// Writer, Reader, ProjectionReader, and BackfillProjection all speak.
target := s3store.NewS3Target(s3store.S3TargetConfig{
    Bucket:            "warehouse",
    Prefix:            "billing",
    S3Client:          s3Client,
    PartitionKeyParts: []string{"charge_period", "customer"},
})

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
| Both write and read, same `T` | `New(Config[T])` → `*Store[T]` |
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
store, _ := s3store.New[UsageFile](s3store.Config[UsageFile]{ /* ... */ })

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
        1710684000000000-a3f2e1b4.parquet
        1710770400000000-c7d9f0e2.parquet   ← recalculation (sorts after the first)
  _ref/
    1710684000000000-a3f2e1b4;charge_period=2026-03-17%2Fcustomer=abc.ref
    1710770400000000-c7d9f0e2;charge_period=2026-03-17%2Fcustomer=abc.ref
```

- `data/` holds the actual Parquet files, partitioned Hive-style.
- `_ref/` holds one **empty** file per write. The filename encodes
  the timestamp, a short UUID, and the partition key. `Poll` is a single
  S3 LIST over this prefix — no GETs.

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

Writes are atomic at the file level: if the ref PUT fails after the data
PUT succeeded, s3store best-effort deletes the orphan parquet (with a HEAD
check to detect lost-ack).

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
later file's record is the last one and dedupLatestSeq's
`pending` advances onto it.

For the auto-generated `{tsMicros}-{shortID}` id, lex-later =
wrote-later, so this is "wrote-later wins" in practice. With
`WithIdempotencyToken` the filename is the caller's token; the
tie-break follows the token's lex order rather than wall-clock
order. Use a time-sortable token format
(e.g. `{ISO-timestamp}-{suffix}`) if you rely on chronological
tie-breaking. Stable across repeated reads either way. To make
ties impossible, ensure `VersionOf` strictly increases per write.

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
`Config.InsertedAtField` to the name of a `time.Time` field on
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

s3store.Config[Event]{
    // ...
    InsertedAtField: "InsertedAt",
}

recs, _ := store.Read(ctx, []string{"*"})
// recs[i].InsertedAt is the writer's wall-clock at write-start.
```

Works on every read path (`Read`, `ReadIter`, `PollRecords`) —
the column round-trips like any other parquet field. The reader
has no special handling. Zero reflection cost when unset.

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

`WithIdempotentRead` is supported on `ReadRangeIter` (snapshot
semantics) and ignored on `PollRecords` (the offset cursor
already provides retry-safety).

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

For retry-safe read-modify-write, pair the `Read` with
`WithIdempotentRead(token)` and write with `WithIdempotencyToken(token)`;
see [Idempotent reads](#idempotent-reads-withidempotentread).

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
download in parallel, the decoded batch is deduped (or passed through
on `WithHistory()`), records are yielded in lex/insertion order, then
the batch is dropped before the next partition starts. Month-scale
reads go from O(month) to O(partition) peak memory, which is usually
small enough for hourly/daily partitioning. If a single partition is
large enough that even one pre-dedup batch is a problem, file an
issue — we can follow up with a streaming fold that trades the code
simplicity for peak memory proportional to unique entities rather
than total records.

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

**Per-partition dedup contract on `s3store.Reader.ReadIter`**: differs
from `Read`'s global dedup. The iter path buffers one partition at a
time, dedups within that partition, yields, then drops it. **Correct
only when the partition key strictly determines every component of
`EntityKeyOf`** — i.e. no entity ever spans two partitions. For layouts
where entities can move between partitions over time (e.g. a customer
that switches region), use `Read` (and pay the memory cost) or
`ReadIter(WithHistory())` and dedup yourself.

For typical time-series shapes (`charge_period_start` leads both
`PartitionKeyParts` and the entity key) the contract holds and
`ReadIter` produces the same records as `Read`, just with bounded peak
memory.

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

- **Data file path is deterministic** — the token replaces the
  default `{tsMicros}-{shortID}` id in the filename. The backend's
  overwrite-prevention rejects the second PUT, so the parquet body
  is not re-uploaded.
- **Ref dedup via scoped LIST** — the writer HEADs the existing
  data file, reads its `x-amz-meta-created-at` stamp, and LISTs
  refs from that timestamp forward. If a ref for this token
  already exists, the retry skips the ref PUT. If the original
  attempt wrote data but not the ref ("scenario B"), the retry
  completes by emitting the ref only.

### Idempotency and reader dedup are complementary

Tokens reduce *storage* duplication. They do **not** on their own
guarantee exactly-once at the consumer. For that, pair tokens with
reader-side dedup: configure `EntityKeyOf` + `VersionOf` so
`Read` / `ReadIter` / `PollRecords` collapse latest-per-entity.

| Config | Storage layer | Consumer layer |
|---|---|---|
| No token, no dedup | at-least-once | at-least-once |
| No token, dedup configured | at-least-once | **exactly-once** (per entity) |
| Token + dedup, strong consistency | at-least-once (minimal duplication) | **exactly-once** (across sessions) |
| Token + dedup, weak consistency (StorageGRID `read-after-new-write`) | at-least-once (some residual duplication) | **exactly-once** — reader dedup collapses storage replicas |

**Recommendation**: enable reader dedup whenever correctness matters.
Tokens are additive — they cut S3 cost and traffic on retry.

### Retry-detection mechanism

Idempotent writes always go through `If-None-Match: *` on the
data PUT. Three backend behaviours collapse onto the same
`ErrAlreadyExists` outcome:

- **AWS S3 / recent MinIO** honour `If-None-Match: *` natively
  and return 412 on a re-PUT.
- **StorageGRID with the `s3:PutOverwriteObject` deny policy
  applied** ignores the header but rejects the re-PUT with 403;
  the writer follows the 403 with a HEAD that confirms the
  object exists and returns the same `ErrAlreadyExists`.
- **Backends with neither mechanism** silently accept the re-PUT.
  No `ErrAlreadyExists` fires; the data file is overwritten with
  byte-identical content (deterministic encoding under a token),
  the markers re-PUT to byte-identical paths, and a fresh ref
  emits — reader dedup absorbs the rare visible duplicate.

In all three cases the data file at the token-derived path
matches the caller's bytes after the write returns. The only
behavioural difference is whether retries skip the body upload
(yes on the first two, no on the third) — a bandwidth concern,
not a correctness one.

There's no probe at constructor time, no detection-strategy
config knob, and no DELETE permission required for retry
detection.

### StorageGRID requires extra configuration

StorageGRID doesn't honour `If-None-Match: *` natively — the
conditional-PUT path above relies instead on a narrow
`s3:PutOverwriteObject` deny on `{prefix}/data/*` (the writer
treats the resulting 403 + HEAD as `ErrAlreadyExists`). That
deny plus a per-target `ConsistencyControl` strong enough to
make the scoped retry-LIST list-after-write across nodes are
the two backend-side settings the library can't apply for you.
Both are documented in [STORAGEGRID.md](STORAGEGRID.md), along
with what each consistency level means for every correctness-
critical call s3store makes and why `strong-*` is required on
both halves of the library.

### Zombie writers and orchestrators

The library does not enforce single-writer-per-partition —
that's a caller invariant. Two cases:

- **Same token.** Parquet encoding is deterministic under a
  token, so the data files are byte-identical regardless of
  who wins StorageGRID's [latest-wins][sgcwl] arbitration.
  *Sequential* retries collapse via the `s3:PutOverwriteObject`
  deny → `ErrAlreadyExists` → ref dedup. *Truly concurrent*
  writers both succeed at the storage layer (the deny is a
  post-completion gate — see
  [the post-completion-gate note][cdpg]) and both emit fresh
  refs; reader dedup via `(entity, version)` absorbs the
  duplicate records on read.
- **Different tokens.** Two distinct data files (different
  paths) and two distinct refs; reader dedup via
  `(entity, version)` collapses them at read time.

[cdpg]: STORAGEGRID.md#s3putoverwriteobject-deny-is-a-post-completion-gate-not-a-mutex

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
leave `ConsistencyControl` at the empty default and still get
exactly-once at the consumer layer. Any rare storage-layer
duplicate from a weak-consistency race becomes a "ghost" file that
isn't referenced by an outbox row — wasted bytes, not a visible
duplicate. The s3store ref stream still gets written alongside the
outbox; consumers just ignore it.

s3store does not ship this pattern; document it as a valid
alternative for callers who already have the transactional
infrastructure.

### Idempotent reads (`WithIdempotentRead`)

Read-modify-write under retry is the natural companion to
`WithIdempotencyToken`: the pattern is "read current state →
compute a diff → write the diff idempotently". If the write
succeeds but the process dies before returning, the retry re-runs
the full cycle. Without a read-side primitive, attempt 2 reads a
*different* state than attempt 1 (attempt 1's own write is now
visible, plus any concurrent data that landed in between), so the
diff and the bytes written diverge — defeating the point of the
idempotency token.

`WithIdempotentRead(token)` makes this cycle retry-safe in one
option. The read returns state as of the first attempt's write
time, excluding both the caller's own prior attempts and any data
written at or after them:

```go
const token = "job-2026-04-22T10:15:00Z-batch42"

existing, err := store.Read(ctx, pattern,
    s3store.WithIdempotentRead(token))
if err != nil { return err }

// compute diff against `existing` …

_, err = store.WriteWithKey(ctx, key, changes,
    s3store.WithIdempotencyToken(token))
```

The same token drives both sides. Persist only the token between
attempts — the write time of attempt 1 is discovered at read time
from the token file's `LastModified`, not from a caller-supplied
timestamp that could drift hours on retry.

**What the filter does** (LIST-time, no extra S3 calls):

- **Self-exclusion** — files whose basename equals
  `{token}.parquet` (the caller's own prior attempts) are dropped.
- **Later-write exclusion** — per partition, the writer records
  `barrier[partition] = min(LastModified)` across token-matching
  files. For every other file in that partition, files with
  `LastModified >= barrier[partition]` are dropped too.

Partitions where the token hasn't been written are unfiltered.
**On the first attempt, no barrier applies** — `Read` returns
current state unchanged, so the option is safe to set
unconditionally.

**Correctness under single-writer-per-partition**: exact
snapshotting requires that no other writer lands data in the
same partition between attempt 1's read-start and attempt 1's
first write. Under that condition, `min(LastModified)` of the
caller's own token-files is a sufficient barrier — nothing
could have snuck in during the read-compute-write window.

This is a **caller invariant** — the library does not enforce
single-writer-per-partition (see
[Zombie writers and orchestrators](#zombie-writers-and-orchestrators)).
If the invariant is violated (two writers on the same partition
with different tokens), attempt 2 may include a concurrent
writer's data attempt 1 didn't see; reader-side dedup
(`EntityKeyOf`+`VersionOf`) absorbs the overlap at the record
layer, but the read-modify-write diff itself is no longer
deterministic across attempts. Callers who need that guarantee
must enforce the single-writer invariant externally (e.g. via an
orchestrator's partition-ownership lease or by reusing the same
token across failovers so zombies produce byte-identical writes,
not diverging ones).

**Scope** — accepted by `Read`, `ReadIter`, `ReadRangeIter`, and
`PollRecords`.

**Performance impact** — the filter applies purely in memory on
a LIST (or ref-stream) that runs anyway, so there's no extra S3
work.

**Zero matches under a barrier**: `Read*` normalizes to an empty
result.

The token passes `ValidateIdempotencyToken` — same grammar as
`WithIdempotencyToken` (non-empty, no `/`, no `..`, printable
ASCII, ≤200 chars). Invalid tokens surface at Read time with a
clear error; no S3 call is issued.

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

- **Write side (`ProjectionDef[T]`)** lives in `Config.Projections` /
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

Every read entry point — `Read`, `ReadIter`, `ProjectionReader.Lookup`,
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
unioned set, and dedup (if configured) applies globally — an
entity appearing under two patterns is kept as the latest version
across the union, not per-pattern.

A single-element slice is the common case; the multi-pattern API
simply lets the caller add more when they need a non-Cartesian
set.

### What's in scope for v1

- Wire projections via `Config.Projections`; auto-write on `Write` +
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

The normal path is to wire a projection into `Config.Projections`
before the first `Write` so every record produces markers. When
that isn't possible — adding a projection to a store that
already has data — the typical shape is:

1. Deploy the live app with the projection in `Config.Projections`; every
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
typical data with negligible CPU cost. Change via `Config.Compression`:

```go
s3store.Config[T]{
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
                              now - 5s
                              ──────→ don't read yet
```

S3 PUTs aren't globally ordered, so `Poll` reads up to `now - SettleWindow`
(default 5s) to guarantee no newer-than-cutoff ref "sneaks in" behind an
already-read one. This gives you a single monotonic offset with no seen-set
or dedup bookkeeping.

### Enforcement: the ref PUT is budgeted against SettleWindow

The whole SettleWindow contract rests on one assumption: by the
time `Poll`'s cutoff (`now - SettleWindow`) advances past a ref's
`refTsMicros`, the ref is LIST-visible. The timestamp is captured
just before the ref PUT (not after), so `SettleWindow` has to
cover the PUT's own network time plus LIST propagation on the
backend.

The write path enforces this with a `SettleWindow / 2`
client-side timeout on the ref PUT. The reserved half covers
LIST propagation between the node that accepted the PUT and the
node a concurrent `Poll` hits — zero on strong-consistency
backends, nonzero on weak ones.

Real PUT latencies (tens of ms) fit comfortably inside
`SettleWindow / 2` for any reasonable `SettleWindow`, so the
halved budget doesn't cost the happy path anything. The budget
does **not** vary with `ConsistencyControl`.

If the ref PUT misses the deadline (or fails for any other
reason), the call returns a wrapped error. The data file is left
in S3 as an orphan — the library never deletes data it has
written. The caller retries — under `WithIdempotencyToken` the
retry is deterministic (same data path), conditional `If-None-
Match: *` on the data PUT routes into the retry-dedup branch, and
`findExistingRef`'s freshness filter ignores any stale ref a
previous attempt may have left behind, so the retry emits a fresh
in-budget ref rather than silently matching the stale one.

```go
if _, err := store.Write(ctx, recs,
    s3store.WithIdempotencyToken(token),
); err != nil {
    // ctx.DeadlineExceeded indicates the ref PUT missed its
    // SettleWindow/2 budget; any wrapped put error means the
    // ref didn't land. Retry with the same token.
    return retry(...)
}
```

For **exactly-once at the consumer**, configure reader dedup
(`EntityKeyOf + VersionOf`) — the rare duplicates produced by
the narrow ack-loss-after-server-persist window all share
`(entity, version)` and dedup collapses them.

**AWS S3 / MinIO users**: `ConsistencyControl` doesn't change the
ref-PUT budget — it's a uniform `SettleWindow / 2` regardless.
The header remains useful on StorageGRID for the data-PUT
overwrite-prevention and the scoped retry LIST, but for the ref
PUT specifically there's no strong-vs-weak distinction. Real PUT
latencies (tens of ms) fit comfortably inside the halved budget
for any reasonable `SettleWindow`.

## StorageGRID

s3store works on AWS S3, MinIO, and NetApp StorageGRID. AWS and
MinIO need no configuration — both are strongly consistent on
LIST and GET by default and ignore the `Consistency-Control`
header. **StorageGRID needs explicit setup:**

1. A bucket policy denying `s3:PutOverwriteObject` on the
   `data/` subtree of the s3store prefix.
2. `ConsistencyControl: ConsistencyStrongGlobal` (multi-site
   grid with cross-cutting traffic) or `ConsistencyStrongSite`
   (single-site grid, or co-located reader/writer pairs) on
   the `S3Target`.

See [STORAGEGRID.md](STORAGEGRID.md) for the full setup,
including a ready-to-run boto3 example for the bucket policy,
the topology decision matrix, operational notes on the 11.9
availability cliff, and the consistency-model reasoning that
motivates these requirements.

## Durability guarantees

The contract is **at-least-once** on both sides of the wire, plus
**read-after-write** on every operation except `Poll` / `PollRecords`
/ `ReadRangeIter` (which deliberately lag the live tip by
`SettleWindow` to tolerate S3 LIST propagation skew — see
[Settle window](#settle-window)).

### Read-after-write

If `Write` (or `WriteWithKey`) returns success, every one of the
following operations issued from any process against the same
bucket sees the new records immediately — no sleep, no settle
delay:

- `Reader.Read` / `ReadIter`
- `ProjectionReader.Lookup`
- `BackfillProjection`

`Poll` / `PollRecords` / `ReadRangeIter` are the intentional
exceptions: they apply the `SettleWindow` cutoff so near-tip refs
stay hidden until S3 LIST propagation has had time to settle. A
ref that's written inside the window will be returned by a
subsequent poll issued after one `SettleWindow` has elapsed.

#### What you need to configure

- **AWS S3 / MinIO.** Nothing. Both backends give strong
  read-after-write on LIST and GET by default. Leave
  `ConsistencyControl` unset.
- **StorageGRID (NetApp).** Set `ConsistencyControl:
  ConsistencyStrongGlobal` (multi-site) or
  `ConsistencyStrongSite` (single-site) on the target — and
  apply the bucket policy. Both are documented in
  [STORAGEGRID.md](STORAGEGRID.md), which also explains why a
  weaker level (or asymmetric writer/reader levels) breaks the
  read-after-write contract.

### Write

If `Write` (or `WriteWithKey`) returns `nil`, every record in
the batch is durably stored in S3 and will be returned by
subsequent `Read`, `Poll`, `PollRecords`, and `ProjectionReader.Lookup` for
the lifetime of the data. The write path commits in order: data
PUT → marker PUTs → ref PUT, returning success only after the
final step lands (or, on a lost PUT ack, after a HEAD confirms
the object is there).

If `Write` returns an **error**, state is indeterminate — the
records may or may not be durable. The caller must retry. A
retry after partial success writes some records twice; dedupe
those on read via `EntityKeyOf` + `VersionOf`. Multi-group
`Write` returns `([]WriteResult, error)` — consult the slice for
records that *did* commit before the error so a retry can skip
them if needed.

To make retries produce deterministic data paths at the storage
layer (no duplicate bytes, no duplicate refs within a bounded
window), pass `WithIdempotencyToken` — see
[Idempotent writes](#idempotent-writes). The ref PUT itself is
budgeted at `SettleWindow / 2`; a slow write that misses the
deadline returns a wrapped put-ref error so the caller retries
(see [Settle window](#settle-window)).

### Read

A data-file GET that returns S3 `NoSuchKey` is operator-driven
(lifecycle policy, manual prune, or external delete — the library
itself never deletes data it has written). The library splits the
response by path:

- **Strict — fail loudly.** `Read` and `ReadIter` propagate the
  `NoSuchKey` as a wrapped error. These paths LIST the partition
  tree first, so a missing file is genuinely a LIST-to-GET race
  (the file vanished in the millisecond window between LIST and
  GET); a caller retry resolves it because the next LIST won't
  include the deleted key.
- **Tolerant — skip and signal.** `PollRecords`, `ReadRangeIter`,
  and `BackfillProjection` walk the ref stream / data tree on a
  long-running shape where a single missing file shouldn't poison
  the whole job. They log via `slog.Warn` (level WARN, key=path,
  method=poll_records / read_range_iter / backfill) and increment
  the `s3store.read.missing_data` counter, then continue. The
  caller's slog handler decides what to do with the warning;
  metrics are picked up by any OTel-configured backend.

Every *other* GET error (throttle, network, auth, timeout) is
still fatal on every path — silently dropping records on
transient failure is worse than propagating.

## Configuration

```go
type Config[T any] struct {
    // Required
    Bucket            string     // S3 bucket name
    Prefix            string     // prefix under which data lives
    PartitionKeyParts []string   // ordered Hive partition key names
    S3Client          *s3.Client // AWS SDK v2 S3 client

    // Required for Write
    PartitionKeyOf func(T) string  // derive key from record (Write)

    // Read-side dedup (used by Read / ReadIter / PollRecords).
    // Both or neither — explicit opt-in, no default. New rejects
    // partial config.
    EntityKeyOf func(T) string  // identifies a unique entity
    VersionOf   func(T) int64   // monotonic version per entity

    // Stream
    SettleWindow time.Duration  // default: 5s

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

    // Consistency-Control HTTP header value applied to every
    // correctness-critical S3 operation (one knob, target-wide).
    // Empty on AWS / MinIO; set to ConsistencyStrongGlobal /
    // ConsistencyStrongSite on StorageGRID. See "Consistency
    // levels × S3 operations" for the full contract.
    ConsistencyControl ConsistencyLevel

    // Optional tuning knobs.
    MaxInflightRequests int
    Projections         []ProjectionDef[T]
}
```

`Config` is the all-in-one form for `New(Config[T])`. Services that
only write or only read can use the narrower `WriterConfig[T]` /
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

target := s3store.NewS3Target(s3store.S3TargetConfig{
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
`WriteWithKey`, `Read`, `ReadIter`, `ReadRangeIter`, `Poll`,
`PollRecords`, `ProjectionReader.Lookup`, `BackfillProjection`):

| Name | Kind | Unit |
|---|---|---|
| `s3store.method.duration` | histogram | s |
| `s3store.method.calls` | counter | 1 |
| `s3store.write.records` | histogram | 1 |
| `s3store.write.partitions` | histogram | 1 |
| `s3store.write.bytes` | histogram | By |
| `s3store.read.records` | histogram | 1 |
| `s3store.read.bytes` | histogram | By |
| `s3store.read.files` | histogram | 1 |
| `s3store.read.missing_data` | counter | 1 |
| `s3store.read.malformed_refs` | counter | 1 |

`s3store.read.missing_data` increments on `NoSuchKey` skips along
the tolerant read paths (`PollRecords`, `ReadRangeIter`,
`BackfillProjection`). Carries `s3store.method` so dashboards can
split by which path produced the skip. Strict paths (`Read`,
`ReadIter`) fail instead of recording.

`s3store.read.malformed_refs` increments when a ref object's
filename fails to parse during a LIST on the ref stream. Skipped
after a `slog.Warn` so consumers don't crash on a future schema or
externally-written object — the counter makes the drift visible.
Surfaced under `s3store.method = poll` because the LIST that hit
it always runs in `Poll`, even when invoked indirectly by
`PollRecords` or `ReadRangeIter`.

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
- **`s3parquet` package collapsed into the root `s3store` package.**
  The single-package layout is the new home for everything:

  ```go
  // Before
  import "github.com/ueisele/s3store/s3parquet"
  store, _ := s3store.New[T](s3store.Config[T]{ ... })

  // After
  import "github.com/ueisele/s3store"
  store, _ := s3store.New[T](s3store.Config[T]{ ... })
  ```

  Mechanical rename — every public symbol moved with the same
  name (`Writer[T]`, `Reader[T]`, `Config[T]`, `S3Target`,
  `ProjectionDef[T]`, `ProjectionReader[K]`, `BackfillProjection`, etc.).
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
  `PollRecords`) — no aggregation, no joins. For SQL workloads,
  point DuckDB at the same bucket via its `httpfs` extension and
  `read_parquet()` over the data path.
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

## License

See [LICENSE](LICENSE).
