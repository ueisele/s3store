# s3store

[![Go Reference](https://pkg.go.dev/badge/github.com/ueisele/s3store.svg)](https://pkg.go.dev/github.com/ueisele/s3store)
[![Release](https://img.shields.io/github/v/tag/ueisele/s3store?sort=semver&label=release)](https://github.com/ueisele/s3store/releases)

Append-only, versioned data storage on S3 with a change stream and optional
embedded DuckDB queries. No server. No broker. No coordinator. Just a Go
library and an S3 bucket.

## What it does

- **Write** typed records as Parquet, grouped into Hive-style partitions.
- **Stream** changes via lightweight "ref" files — one empty S3 object per
  write, all metadata in the object key.
- **Read** point-in-time deduplicated snapshots with glob support.
- **Query** the whole store with DuckDB SQL in a single embedded process.

## Packages

s3store ships as three packages. Pick the smallest one that covers your
needs:

| Package | cgo | Import path | Capabilities |
|---|---|---|---|
| `s3parquet` | **no** | `github.com/ueisele/s3store/s3parquet` | Writer + Reader: Write, WriteWithKey, Read, ReadIter, Poll, PollRecords, OffsetAt, secondary indexes. Pure Go / parquet-go; in-memory per-partition dedup. |
| `s3sql` | yes | `github.com/ueisele/s3store/s3sql` | **SQL-only, read-only.** Query returns `*sql.Rows`; the caller binds rows themselves. Construct with `NewReader(ReaderConfig)`; share the same `s3parquet.S3Target` with the Writer so the two halves can't drift. |
| `s3store` (umbrella) | yes | `github.com/ueisele/s3store` | Everything above behind one `Store[T]`. Composes `s3parquet.Writer` + `s3parquet.Reader` + `s3sql.Reader`; all three share one `S3Target` so they can't drift on S3 wiring. |

Binary size: DuckDB bundles a ~50 MB C++ library. `CGO_ENABLED=0 go build
./s3parquet/...` produces a small static binary with none of it. The
umbrella and `s3sql` both require cgo.

Both sub-packages share the S3 layout and ref-stream wire format, so the
same data is accessible through either.

## Install

```bash
go get github.com/ueisele/s3store@latest
```

s3store is pre-v1 — **minor version bumps (`v0.x.0`) may carry breaking
API changes**. Pin an exact version in your `go.mod` (or commit your
`go.sum`) to control when you pick them up. Requires Go 1.26.2+ (declared
in [go.mod](go.mod)). DuckDB's httpfs extension is auto-installed on first
use or pre-installed in air-gapped environments.

## Quick start — full umbrella

```go
type CostRecord struct {
    CustomerID   string    `parquet:"customer_id"`
    ChargePeriod string    `parquet:"charge_period"`
    SKU          string    `parquet:"sku"`
    NetCost      float64   `parquet:"net_cost"`
    Currency     string    `parquet:"currency"`
    CalculatedAt time.Time `parquet:"calculated_at,timestamp(millisecond)"`
}

store, err := s3store.New[CostRecord](ctx, s3store.Config[CostRecord]{
    Bucket:        "warehouse",
    Prefix:        "billing",
    S3Client:      s3Client,
    PartitionKeyParts:      []string{"charge_period", "customer"},
    VersionColumn: "calculated_at",
    TableAlias:    "costs",
    PartitionKeyOf: func(r CostRecord) string {
        return fmt.Sprintf("charge_period=%s/customer=%s",
            r.ChargePeriod, r.CustomerID)
    },
})
if err != nil {
    log.Fatal(err)
}
defer store.Close()

// Write — groups records by PartitionKeyOf, one Parquet file per group
_, err = store.Write(ctx, records)

// Snapshot read — deduplicated by VersionColumn via DuckDB
latest, err := store.Read(ctx, []string{"charge_period=2026-03-17/customer=abc"})

// SQL query — DuckDB handles the aggregation
rows, err := store.Query(ctx, []string{"charge_period=2026-03-*/*"},
    "SELECT customer, SUM(net_cost) FROM costs GROUP BY customer")
```

## Quick start — cgo-free (`s3parquet` only)

For write-heavy services or consumers that don't need SQL:

```go
store, err := s3parquet.New[CostRecord](ctx, s3parquet.Config[CostRecord]{
    Bucket:   "warehouse",
    Prefix:   "billing",
    S3Client: s3Client,
    PartitionKeyParts: []string{"charge_period", "customer"},
    PartitionKeyOf: func(r CostRecord) string {
        return fmt.Sprintf("charge_period=%s/customer=%s",
            r.ChargePeriod, r.CustomerID)
    },
    // Optional: enable latest-per-entity dedup on Read / PollRecords.
    // EntityKeyOf and VersionOf are required together — both or
    // neither. NewReader rejects partial config.
    EntityKeyOf: func(r CostRecord) string {
        return r.CustomerID + "|" + r.SKU
    },
    VersionOf: func(r CostRecord) int64 {
        return r.CalculatedAt.UnixNano()
    },
})

// parquet-go decodes directly into []CostRecord via the parquet tags
latest, err := store.Read(ctx, []string{"charge_period=2026-03-17/customer=abc"})
```

`s3parquet` drives typed results off the parquet struct tags on `T`
via parquet-go's `GenericReader[T]` — no `ScanFunc` or manual
column-order bookkeeping. A field whose column is missing from a given
file lands as Go's zero value. `s3sql.Query` returns `*sql.Rows`
unchanged; the caller binds rows with the standard `database/sql`
contract.

## Writer / Reader / View (`s3parquet`)

`s3parquet.Store[T]` is a composition of two public halves: a `Writer[T]`
for the write path and a `Reader[T]` for the read path. Most users keep
using `New(Config)` and get both through `Store`. Services that only
write or only read can construct a single half with a narrower config:

```go
// Build the shared S3 wiring once — Target is the untyped handle
// Writer, Reader, Index, and BackfillIndex all speak.
target := s3parquet.NewS3Target(
    "warehouse", "billing", s3Client,
    []string{"charge_period", "customer"},
)

// Write-only service: no read-side knobs in config.
w, err := s3parquet.NewWriter[CostRecord](s3parquet.WriterConfig[CostRecord]{
    Target:         target,
    PartitionKeyOf: func(r CostRecord) string { /* ... */ },
})

// Read-only service: no PartitionKeyOf / Compression.
r, err := s3parquet.NewReader[CostRecord](s3parquet.ReaderConfig[CostRecord]{
    Target:          target,
    InsertedAtField: "InsertedAt",
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

store, _ := s3parquet.New[FullRec](ctx, cfg)
view, _ := s3parquet.NewReaderFromStore[NarrowRec, FullRec](store,
    s3parquet.ReaderConfig[NarrowRec]{})

recs, _ := view.Read(ctx, "*") // []NarrowRec — ProcessLog not fetched
```

The narrow `ReaderConfig[T']` carries the read-side knobs
(`EntityKeyOf`, `VersionOf`, `OnMissingData`,
`ConsistencyControl`); the constructor overwrites the `Target`
field from the source Writer/Store so `SettleWindow` and the
S3 wiring are inherited automatically. Dedup closures are typed
over `T'`, so you supply them explicitly for the narrow shape
when needed.

The relationship between constructors:

| Want | Call |
|---|---|
| Both write and read, same `T` | `New(ctx, Config[T])` → `*Store[T]` |
| Write only, narrow config | `NewWriter(ctx, WriterConfig[T])` → `*Writer[T]` |
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
store, _ := s3parquet.New[UsageFile](ctx, s3parquet.Config[UsageFile]{ /* ... */ })

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
monetary values — DuckDB reads it as a real decimal so `SUM(amount)` just
works. Use `int64` backing for precision ≤ 18, `[N]byte` for more.

## S3 layout

```
s3://warehouse/billing/
  data/
    charge_period=2026-03-17/
      customer=abc/
        1710684000000000-a3f2e1b4.parquet
        1710770400000000-c7d9f0e2.parquet   ← recalculation (sorts after the first)
  _stream/
    refs/
      1710684000000000-a3f2e1b4;charge_period=2026-03-17%2Fcustomer=abc.ref
      1710770400000000-c7d9f0e2;charge_period=2026-03-17%2Fcustomer=abc.ref
```

- `data/` holds the actual Parquet files, partitioned Hive-style.
- `_stream/refs/` holds one **empty** file per write. The filename encodes
  the timestamp, a short UUID, and the partition key. `Poll` is a single
  S3 LIST over this prefix — no GETs.

### Partition naming: column or path-only

`PartitionKeyParts` names can either match Parquet column names or be entirely
separate — both patterns are supported.

**Pattern A — partition name == Parquet column name** (all the examples above).
The partition key is a real record attribute, and `PartitionKeyOf` reads it
directly from the struct. On the `s3sql` read path, DuckDB exposes the Hive
partition column *alongside* the Parquet column of the same name; the
Hive-derived value wins when they collide. Because `Write` derives the path
from the same record the Parquet file holds, they always agree, so the
"hive wins" rule is invisible in normal code. The one way to hit it is to
call `WriteWithKey` with a key inconsistent with the record's fields —
don't do that.

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

Partition values live only in the path, saving storage. `s3sql` still
surfaces them as columns and you can `SELECT` or `WHERE` on them.
`s3parquet` ignores Hive paths entirely — if you need `year`/`month`
on the Go-only read path, reconstruct them from `Ts`.

Pick A when the partition is a first-class attribute (customer, tenant);
pick B when it's a derived time bucket. Mixing within one store is fine.

## Glob grammar

Both `s3parquet` and `s3sql` share one grammar, validated identically:

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

Ranges enable partition pruning: both read paths extract the common prefix
of `FROM` and `TO` as an S3 `LIST` prefix so only potentially-matching keys
are enumerated. The SQL path additionally pushes the bounds down as a
`WHERE` predicate on the hive partition column, so DuckDB skips non-matching
files at plan time.

**Bounds are compared lexicographically** — byte-wise on both read paths
(`s3parquet` uses Go string compare; `s3sql` runs the `WHERE` against the
Hive partition column as `VARCHAR`, since we pass `hive_types_autocast=false`
so DuckDB never reinterprets the value as DATE / INT). That makes the two
paths agree exactly, but it also means the range matches *characters*, not
numbers or dates. **Partition values must be chosen so lex order matches
intent:**

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
batch is one S3 LIST call over `_stream/refs/`.

### Stream — typed records

```go
// When dedup is configured, the default is latest-per-key
// within each batch (Kafka compacted-topic semantics).
records, newOffset, err := store.PollRecords(ctx, lastOffset, 100)

// Opt out: every record in every referenced file, in ref order.
records, newOffset, err = store.PollRecords(ctx, lastOffset, 100,
    s3store.WithHistory())
```

Whether dedup actually runs depends on which read path you use:

- **`s3parquet.PollRecords` / `Read` / `ReadIter` (and the umbrella
  delegations)** — dedup when `EntityKeyOf` AND `VersionOf` are both
  set (NewReader rejects partial config). When neither is set, every
  record passes through in decode order.
- **`s3sql.Query` (and the umbrella delegation)** — dedup when
  `EntityKeyColumns` + `VersionColumn` are both set (DuckDB
  `QUALIFY ROW_NUMBER()` CTE). When either is empty, every record
  passes through.

`WithHistory()` forces the no-dedup path in every case.

**Tie-break on equal max version (default dedup, no
`WithHistory`).** When two writes share the same version for the
same entity key — common on an at-least-once retry that replays a
batch with the same domain timestamp — both backends resolve the
tie the same way: **lex-later filename wins**.

- **`s3parquet`**: input is sorted by `(entityKey, versionOf)`
  stable on ties; preparePartitions feeds files in lex order, so
  within a tied group the lex-later file's record is the last one
  and dedupLatestSeq's `pending` advances onto it.
- **`s3sql`**: secondary `ORDER BY filename DESC` in the dedup CTE
  picks the lex-later filename explicitly.

For the auto-generated `{tsMicros}-{shortID}` id, lex-later =
wrote-later, so this is "wrote-later wins" in practice. With
`WithIdempotencyToken` the filename is the caller's token; the
tie-break follows the token's lex order rather than wall-clock
order. Use a time-sortable token format
(e.g. `{ISO-timestamp}-{suffix}`) if you rely on chronological
tie-breaking. Stable across repeated reads either way. To make
ties impossible, ensure `VersionColumn` / `VersionOf` strictly
increases per write.

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

Works on every typed read path (`s3parquet.Read`, `ReadIter`,
`PollRecords`) and on the SQL side too — DuckDB decodes the
column natively via `SELECT *` (s3sql exposes it as a regular
column on the `*sql.Rows` cursor). The reader has no special
handling: the column round-trips like any other parquet field.
Zero reflection cost when unset.

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
`WithUntilOffset`. All four APIs — `OffsetAt`, `WithUntilOffset`,
`ReadRangeIter`, `PollRecords` — are available on the umbrella,
`s3parquet`, and `s3sql`.

### Stream — opting out (`DisableRefStream`)

Every `Write` / `WriteWithKey` call issues one extra S3 PUT to
`_stream/refs/` per distinct partition key it touches. `Write` groups
records by key and calls `WriteWithKey` once per group, so a batch
spanning N partitions produces N ref PUTs. For pure batch / analytics
workloads that never tail the stream, that's pure overhead. Set
`DisableRefStream: true` on the umbrella `Config`, `s3parquet.Config`,
or `s3parquet.S3Target` to skip the ref PUT:

```go
s3store.Config[CostRecord]{
    // ...
    DisableRefStream: true,
}
```

Effect on each method:

- **`Write` / `WriteWithKey`** — one fewer S3 PUT per call;
  `WriteResult.Offset` and `WriteResult.RefPath` are empty.
- **`Read` / `ReadIter` / `Query` / `Lookup` / `BackfillIndex`**
  — unaffected. They LIST `data/` (or `_index/<name>/`) directly
  and never consult refs.
- **`Poll` / `PollRecords` / `ReadRangeIter`** — return
  `s3store.ErrRefStreamDisabled` (shared sentinel, matches
  `errors.Is` across packages). `ReadRangeIter` surfaces the
  error via the first yielded `(zero, err)` tuple.
- **`OffsetAt`** — still works. It's pure timestamp encoding with no
  S3 dependency, so it keeps returning well-formed offsets even
  though there's no stream to compare them against.

Per-write irreversible: data written with `DisableRefStream: true`
has no refs, so flipping the flag back does not retroactively make
`Poll` see the historical writes.

Both sides of the deployment (writer and reader) must agree on the
flag — set it on the umbrella `Config`, or on the
`s3parquet.S3Target` shared by `s3parquet.Writer` and the readers.
The two failure modes if they drift:

- **Writer disabled + reader enabled** — `Poll` walks an empty
  `_stream/refs/` prefix and silently returns zero entries with no
  error. Easy to mistake for "stream is quiet."
- **Writer enabled + reader disabled** — `Poll` refuses with
  `ErrRefStreamDisabled` even though refs actually exist in S3.
  Unset `DisableRefStream` on the reader to recover; no data is
  lost.

`s3parquet` users constructing a `Store` via `New(Config)` can't
drift — the flag lives on the shared `S3Target` and both halves
read the same value.

### Snapshot

```go
records, err := store.Read(ctx, []string{"charge_period=2026-03-17/customer=abc"})
```

Returns every record matching the glob, decoded directly into `[]T`
via parquet-go and the parquet tags on `T`. When dedup is configured
(see Stream above), the result is the latest version per key;
otherwise every version comes through.

For retry-safe read-modify-write, pair the `Read` with
`WithIdempotentRead(token)` and write with `WithIdempotencyToken(token,
…)`; see [Idempotent reads](#idempotent-reads-withidempotentread).

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

Two callable surfaces: `s3parquet.Reader.ReadIter` and the umbrella
`s3store.Store.ReadIter` (forwards to `s3parquet.Reader`).

**Memory profile**: `O(one partition's records)`. The pure-Go path
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
    s3parquet.WithReadAheadPartitions(8)) { ... }

// Skewed partition sizes (mostly tiny + a few large): cap by bytes
// so prefetch self-throttles on the large ones. Decoded Go memory
// typically runs 1–2× the uncompressed size depending on data
// shape (string headers, slice/map pointer overhead).
for r, err := range store.ReadIter(ctx, []string{"*"},
    s3parquet.WithReadAheadBytes(2<<30)) { ... } // ≤ 2 GiB

// Combine — useful when both axes matter.
for r, err := range store.ReadIter(ctx, []string{"*"},
    s3parquet.WithReadAheadPartitions(8),
    s3parquet.WithReadAheadBytes(2<<30)) { ... }
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

**Per-partition dedup contract on `s3parquet.Reader.ReadIter`**: differs
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

**`s3sql` side has no such caveat** — DuckDB plans across the union of
files and runs `QUALIFY` over the full result, so dedup is global
regardless of how rows are streamed back.

**Order**: `s3parquet.Reader.ReadIter` visits partitions in lex order
and downloads files within a partition in lex order. Within a
partition the user-visible emission order is `(entity, version)`
ascending when `EntityKeyOf` is set; when no dedup is configured the
sort is skipped entirely and records emit in decode order (file lex
order, then parquet row order). SQL queries return rows in DuckDB's
query order — add `ORDER BY` in your `Query` call if you need
stability.

### SQL query (umbrella or `s3sql`)

```go
rows, err := store.Query(ctx, []string{"charge_period=2026-03-*/*"},
    "SELECT customer, sku, SUM(net_cost) AS total "+
        "FROM costs GROUP BY customer, sku")

// Query aggregates across an arbitrary set of partition tuples.
rows2, err := store.Query(ctx, []string{
    "charge_period=2026-03-17/customer=abc",
    "charge_period=2026-03-18/customer=def",
}, "SELECT SUM(net_cost) AS total FROM costs")
```

Deduplicated by default when `EntityKeyColumns` + `VersionColumn` are
configured. Pass `s3store.WithHistory()` to see all versions. `Query`
returns `*sql.Rows`; bind rows with the standard `database/sql`
contract.

The `*sql.Rows` cursor can be materialized into typed records with
`s3sql.ScanAll[T](rows)` — maps DuckDB columns to T fields by
`parquet:"…"` tag, NULL-safe, supports `sql.Scanner` and composite
shapes (LIST/STRUCT/MAP).

> **Tuning DuckDB.** DuckDB auto-detects two knobs at open
> time: `threads` (defaults to the host's logical CPU count) caps
> parallel scans/joins/aggregations, and `memory_limit` (defaults
> to ~80% of system RAM) caps in-process working set before
> DuckDB spills to disk. Override either via `ExtraInitSQL`:
>
> ```go
> ExtraInitSQL: []string{
>     "SET threads = 8",
>     "SET memory_limit = '4GB'",
> }
> ```
>
> Full list of settings:
> [duckdb.org/docs/stable/configuration/overview](https://duckdb.org/docs/stable/configuration/overview).
>
> These are independent of `MaxInflightRequests` — that knob
> bounds the Go-side aws-sdk client (Writer PUTs, the LIST that
> resolves URIs for `Query`, s3parquet Reader GETs); DuckDB's
> httpfs GETs for parquet bodies run on DuckDB's own thread pool,
> outside the semaphore.

## Idempotent writes

s3store defaults to at-least-once on the write path: a retry after a
partial failure re-runs the whole write and produces duplicate data,
markers, and refs under fresh keys. For workloads where retries are
common (orchestrator failover, network flakiness, crash-and-resume),
`WithIdempotencyToken` makes the write retry-safe end-to-end.

```go
const token = "job-2026-04-22T10:15:00Z-batch42"

_, err := store.WriteWithKey(ctx, key, records,
    s3store.WithIdempotencyToken(token, 6*time.Hour))
```

On retry with the same token:

- **Data file path is deterministic** — the token replaces the
  default `{tsMicros}-{shortID}` id in the filename. The backend's
  overwrite-prevention rejects the second PUT, so the parquet body
  is not re-uploaded.
- **Ref dedup via scoped LIST** — bounded by `maxRetryAge`. If a ref
  for this token already exists in the window, the retry skips the
  ref PUT. If the original attempt wrote data but not the ref
  ("scenario B"), the retry completes by emitting the ref only.

### Idempotency and reader dedup are complementary

Tokens reduce *storage* duplication. They do **not** on their own
guarantee exactly-once at the consumer. For that, pair tokens with
reader-side dedup:

- `EntityKeyOf` + `VersionOf` on `s3parquet.Reader` (and the
  umbrella, for `Read` / `ReadIter` / `PollRecords`)
- `EntityKeyColumns` + `VersionColumn` on `s3sql.Reader` (and the
  umbrella, for `Query`)

| Config | Storage layer | Consumer layer |
|---|---|---|
| No token, no dedup | at-least-once | at-least-once |
| No token, dedup configured | at-least-once | **exactly-once** (per entity) |
| Token + dedup, strong consistency | at-least-once (minimal duplication) | **exactly-once** (within `maxRetryAge` across sessions) |
| Token + dedup, weak consistency (StorageGRID `read-after-new-write`) | at-least-once (some residual duplication) | **exactly-once** — reader dedup collapses storage replicas |

**Recommendation**: enable reader dedup whenever correctness matters.
Tokens are additive — they cut S3 cost and traffic on retry.

### `maxRetryAge` — tuning guide

Bounds the scoped LIST on the retry path. Cost is
`O(writes × maxRetryAge / 1000)` LIST pages per retry. No library
default — pick based on your retry SLA.

| Value | Use case |
|---|---|
| `0` | Disable scoped LIST; retry always writes a duplicate ref. Cheapest retry path, relies on reader dedup. |
| `1 * time.Hour` | Fast-retry streaming; orchestrator retries within the hour. |
| `6 * time.Hour` | Same-day recovery (cron-driven jobs that may rerun mid-day). |
| `24 * time.Hour` | Cross-day orchestrator recovery (overnight batch retries). |

Tokens older than `maxRetryAge` produce a duplicate ref on retry
(documented tradeoff). Reader dedup absorbs it.

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

### StorageGRID / NetApp setup

StorageGRID doesn't honor `If-None-Match: *` directly; instead,
apply a bucket policy that denies `s3:PutOverwriteObject` — but
**scope the deny to `{prefix}/data/*` only**, not the whole bucket:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DenyDataOverwrite",
      "Effect": "Deny",
      "Principal": "*",
      "Action": ["s3:PutOverwriteObject"],
      "Resource": [
        "arn:aws:s3:::YOUR-BUCKET/YOUR-PREFIX/data/*"
      ]
    }
  ]
}
```

Apply with:

```python
s3.put_bucket_policy(Bucket="YOUR-BUCKET", Policy=json.dumps(policy))
```

> **Why the narrow scope matters.** s3store's layout has three
> subtrees under `{prefix}`:
>
> - `data/` — parquet files. Deterministic paths under
>   `WithIdempotencyToken`; the deny fires here on retry so the body
>   isn't re-uploaded. **This is what needs the deny.**
> - `_index/` — secondary-index markers. **Byte-identical idempotent
>   overwrites by design** — the same `{col}={value}/m.idx` marker
>   is written every time that `(col, value)` tuple recurs in a
>   batch. A bucket-wide deny rejects the second such write with
>   `403 AccessDenied` and breaks index writes entirely.
> - `_stream/refs/` — ref files. Unique per-PUT keys (refTsMicros
>   prefix), so they never overwrite either way.
>
> **Symptom of an over-scoped deny**: data writes succeed, then
> markers fail with `403 AccessDenied` as soon as any `(col, value)`
> tuple is seen in a second batch. The library attempts a cleanup
> DELETE on the orphan data, which often fails with the same 403,
> leaving orphan parquet under `data/` with no markers and no ref.

Then set `ConsistencyControl` to one of the stronger levels so the
scoped retry-LIST sees prior refs across nodes:

```go
s3store.Config[T]{
    // ...
    ConsistencyControl: s3parquet.ConsistencyStrongGlobal, // or ConsistencyStrongSite
}
```

`WriterConfig.ConsistencyControl` and `ReaderConfig.ConsistencyControl`
must match — NetApp requires paired PUT and GET to use the same
consistency level. The umbrella `Config` forwards a single value to
both halves. On AWS / MinIO the header is an unknown string and is
ignored, so leaving `ConsistencyControl` empty is correct there.

The configured `ConsistencyControl` drives every
correctness-critical S3 call on both halves:

- **Write path.** Data PUT (idempotent), ref PUT, index marker
  PUTs, and the scoped retry-LIST used by `WithIdempotencyToken`
  to dedup refs on retry.
- **Read path — `s3parquet`.** Data-file partition LIST, data
  GET, `Index.Lookup` marker LIST,
  `BackfillIndex` data LIST + data GET + marker PUT (from
  `IndexDef.ConsistencyControl`).
- **Read path — `s3sql`.** Partition LIST (sends the header);
  data GET goes through DuckDB's `httpfs` and does **not** carry
  the header — see the [DuckDB caveat](#duckdb-httpfs-has-no-per-request-header-hook).
- **Ref-stream `Poll` LIST.** The same header is sent on the
  paginator so a newly-written ref is LIST-visible before `Poll`'s
  cutoff can advance past it on StorageGRID `strong-*`. Without
  this, a ref can become LIST-visible later than `SettleWindow`
  assumes and be skipped silently.
- **Ref-PUT budget against `SettleWindow`.** The PUT gets a
  uniform `SettleWindow / 2` client-side timeout regardless of
  consistency level — the other half is reserved for LIST
  propagation between writer and consumer nodes. A PUT that
  misses the deadline returns a wrapped error so the caller
  retries; see [Settle window](#settle-window) for the full
  contract.

### Zombie writers and orchestrators

The library does not enforce single-writer-per-partition — that's a
caller invariant. If two writers race on the same partition with the
**same token**, they produce identical physical writes (both get
rejected after the first; deterministic and safe). With **different
tokens**, they produce two distinct data files (different paths), and
reader dedup via `(entity, version)` collapses them at read time.

For orchestrator-driven jobs: **reuse the same token across
failovers** (persist it in your job state). A fresh token per
restart is valid but creates real storage duplication.

### Alternative: external outbox (Postgres / similar)

If you already run a transactional database alongside your writers,
an outbox pattern often composes better than s3store's ref stream:

1. Set `DisableRefStream: true` on the s3store Config.
2. On every successful write, `INSERT` into an outbox table with
   columns `(token, partition_key, data_path, created_at)` and a
   monotonic `id`.
3. Consumers read by `id`.
4. Unique constraint on `token` makes zombie/retry writes visible as
   constraint violations — Postgres is the authoritative dedup.

This moves the dedup primitive off S3, so on StorageGRID you can
leave `ConsistencyControl` at the empty default and still get
exactly-once at the consumer layer. Any rare storage-layer
duplicate from a weak-consistency race becomes a "ghost" file that
isn't referenced by an outbox row — wasted bytes, not a visible
duplicate.

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
    s3store.WithIdempotencyToken(token, 6*time.Hour))
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

**Scope** — accepted by every read path:

- `s3parquet.Reader`: `Read`, `ReadIter`, `ReadRangeIter`,
  `PollRecords`.
- `s3sql.Reader`: `Query`.
- Umbrella `Store`: all of the above forward through.

**Performance impact** — `s3parquet` and poll-based paths apply
the filter purely in memory on a LIST (or ref-stream) that runs
anyway, so there's no extra S3 work. `s3sql.Query` already drive 
a Go-side LIST to assemble the URI list for `read_parquet([...])`, 
so the barrier filter folds in for free.

**Zero matches under a barrier**: `Query` and `Read*` normalize to
empty results as they do.

The token passes `ValidateIdempotencyToken` — same grammar as
`WithIdempotencyToken` (non-empty, no `/`, no `..`, printable
ASCII, ≤200 chars). Invalid tokens surface at Read time with a
clear error; no S3 call is issued.

## Schema evolution

Both read paths tolerate missing columns out of the box: a field whose
column isn't in a given parquet file lands as Go's zero value, never an
error. That covers the common "added a column" case without any extra
configuration.

- **`s3parquet`** — parquet-go matches columns to struct fields by the
  `parquet:` tag, so column order in the file doesn't matter and unknown
  columns are ignored.
- **`s3sql`** — `Query` returns `*sql.Rows` directly; bind columns
  with the standard `database/sql` contract. DuckDB's `union_by_name`
  read mode tolerates schema drift across the file union, so
  reading old + new files in one query just works.

Renames, splits, and row-level computed derivations still require a
migration tool — rewrite the affected files with the new shape.

## Secondary indexes

When a query filters on a column that isn't a partition key
(e.g. "list every customer that had usage of SKU X in period
P"), scanning every data file is prohibitive at scale. A
secondary index solves this by writing
one empty S3 *marker* per distinct tuple of the columns you want
to query. The query is a single LIST under the marker prefix —
zero parquet reads, no cgo, no DuckDB.

### Shape

You define an index by a typed entry struct (one parquet-tagged
string field per column) and an `Of` function that emits
zero-or-more entries per record:

```go
type SkuPeriodEntry struct {
    SKUID             string `parquet:"sku_id"`
    ChargePeriodStart string `parquet:"charge_period_start"`
    CausingCustomer   string `parquet:"causing_customer"`
    ChargePeriodEnd   string `parquet:"charge_period_end"`
}

store, _ := s3store.New[Usage](ctx, cfg)

skuIdx, err := s3store.NewIndex[Usage, SkuPeriodEntry](store,
    s3store.IndexDef[Usage, SkuPeriodEntry]{
        IndexLookupDef: s3store.IndexLookupDef[SkuPeriodEntry]{
            Name:    "sku_period_idx",
            Columns: []string{
                "sku_id", "charge_period_start",
                "causing_customer", "charge_period_end",
            },
        },
        Of: func(u Usage) []SkuPeriodEntry {
            return []SkuPeriodEntry{{
                SKUID:             u.SKUID,
                ChargePeriodStart: u.ChargePeriodStart.Format(time.RFC3339),
                CausingCustomer:   u.CausingCustomer,
                ChargePeriodEnd:   u.ChargePeriodEnd.Format(time.RFC3339),
            }}
        },
    })
// skuIdx is *s3store.Index[SkuPeriodEntry] — T-free, so a
// read-only service can share the same handle type.
```

The returned `Index[K]` is T-free. `IndexDef[T, K]` embeds
`IndexLookupDef[K]` (just `Name` + `Columns`); read-only callers
that don't need `Of` can build an `Index[K]` directly from an
`S3Target` + `IndexLookupDef[K]` via `s3parquet.NewIndex`.

Every `Write` call iterates each registered index, collects a
deduplicated set of entries across the batch, and PUTs one empty
marker per distinct entry under
`<Prefix>/_index/<name>/<col>=<val>/.../m.idx`. Duplicate writes
are idempotent (same S3 key, same empty body).

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
unbounded — narrow the pattern if an index has millions of
matches.

### Multi-pattern reads

The single-pattern grammar is Cartesian per segment — `period=*`
combined with `customer=abc` matches the cross product of all
periods × abc. When the caller has an arbitrary **set of tuples**
(e.g. `(period=2026-03, customer=abc), (period=2026-04,
customer=def)` but *not* the off-diagonal combinations), pass them
as additional elements of the same `[]string` patterns slice.
Every read entry point already takes `[]string`:

| Entry point | Where |
|---|---|
| `Reader.Read` / `ReadIter` | `s3parquet.Reader`, umbrella |
| `Reader.Query` | `s3sql.Reader`, umbrella |
| `Index.Lookup` | `s3parquet`, umbrella |
| `BackfillIndex` | `s3parquet`, umbrella |

```go
// Pure-Go read across non-Cartesian tuples.
recs, _ := store.Read(ctx, []string{
    "period=2026-03-17/customer=abc",
    "period=2026-03-18/customer=def",
})

// DuckDB aggregation across the union — one query, global
// GROUP BY / SUM / join across the tuple set.
rows, _ := store.Query(ctx, []string{
    "period=2026-03-17/customer=abc",
    "period=2026-03-18/customer=def",
}, "SELECT customer, SUM(amount) FROM records GROUP BY customer")

// Index lookup over an arbitrary set of (col, col) tuples.
entries, _ := idx.Lookup(ctx, []string{
    "sku=s1/customer=abc",
    "sku=s4/customer=def",
})

// One-off migration across several partitions.
stats, _ := s3store.BackfillIndex(ctx, target, def,
    []string{"period=2026-03-*/customer=*", "period=2026-04-01/customer=*"},
    until, nil)
```

**Execution model (s3parquet path):** LIST calls fan out across
patterns with the Target's `MaxInflightRequests` cap, overlapping
patterns are deduplicated at the key level, the GET+decode pool
runs over the unioned set, and dedup (if configured) applies
globally — an entity appearing under two patterns is kept as
the latest version across the union, not per-pattern.

**Execution model (s3sql path):** every `Query` pre-LISTs in Go
(per pattern, with the same bounded cap) to produce the exact file
URI list, then runs **one** `read_parquet([...])` so DuckDB plans
once over the whole set — the analytical win (`SUM` / `GROUP BY` /
joins across non-Cartesian tuples) that N separate `Query` calls
would force the caller to do in Go.

A single-element slice is the common case for queries over one
pattern; the multi-pattern API simply lets the caller add more
when they need a non-Cartesian set.

### What's in scope for v1

- Register + auto-write on `Write` + `Lookup` via the typed handle.
- Read-after-write on Lookup: the marker PUT and the marker LIST
  both carry the writer's `ConsistencyControl`, so a `Lookup`
  issued immediately after `Write` sees the new marker without a
  settle delay. On AWS S3 / MinIO that's native; on StorageGRID,
  set `ConsistencyStrongGlobal` / `ConsistencyStrongSite` on the
  Writer and it flows to marker PUT + LIST automatically.
- **Backfill** as a standalone package function (below).

### Backfill

The normal path is to register an index before the first `Write`
so every record produces markers. When that isn't possible —
adding an index to a store that already has data — the typical
shape is:

1. Deploy the live app with the index registered (`NewIndex`);
   every new `Write` emits markers from time T0 onward.
2. Capture `until := store.OffsetAt(T0)` — the watermark before
   which historical data is uncovered.
3. Run a **one-off migration job** using the package-level
   `s3store.BackfillIndex` that scans files with `LastModified <
   until` and PUTs the retroactive markers.

```go
stats, err := s3store.BackfillIndex(ctx,
    store.Target(),     // or construct via s3parquet.NewS3Target
    def,                // the same IndexDef the live app registered
    []string{"*"},      // patterns (PartitionKeyParts grammar)
    until,              // exclusive upper bound on LastModified
    func(path string) { slog.Warn("missing data", "path", path) },
)
// stats.DataObjects / Records / Markers
```

`BackfillIndex` is deliberately standalone — no `Writer` /
`Reader` argument — so the migration job doesn't need the live
app's full config. It runs through the same `S3Target`
abstraction used by `NewIndex`, issuing both parquet GETs and
marker PUTs via `target.S3Client`. Typically invoked from a
dedicated binary (`cmd/backfill-<name>/main.go` in your repo): a
~30-line `main` that builds an S3 client + `S3Target` + `def`
and calls `BackfillIndex` is the idiomatic shape.

The pattern is evaluated against the target's `PartitionKeyParts`
(same grammar as `Read`), **not** against the index's `Columns`
— backfill walks parquet data files, which are keyed by
partition. A migration job can shard itself by partition
(`period=2026-01-*` on one pod, `period=2026-02-*` on another)
instead of running a single multi-hour call. The `until` bound
lets the live writer and the migration job cooperate without
overlap: live markers for everything from T0, backfill markers
for everything before. Pass `s3store.OffsetUnbounded` to disable
the bound (covers every file currently present — harmless but
redundant if the live writer has been up for a while, since PUT
is idempotent).

Idempotent at the S3 level (same empty marker, same key), so a
retry after cancel or crash is a no-op on work already done. Safe
to run while the live writer keeps emitting markers for fresh
records.

Backfilled markers are subject to the same read-after-write
contract as live-write markers: on a strong-consistent backend
(AWS, MinIO, or StorageGRID with `ConsistencyControl` set on the
`IndexDef`) a `Lookup` issued after `BackfillIndex` returns sees
every marker it just wrote.

### Not in v1 (deferred)

- **Delete index** — no general delete path on the store yet.
- **Verification / orphan cleanup tools** — if `Of` changes
  semantically, stale markers remain. Backfill only adds
  missing markers; rebuild (delete-then-re-PUT) is a separate
  design.

### Column ordering matters (for performance, not correctness)

Put columns you typically filter on **first**. They form the
S3 LIST prefix, so a query that specifies them literally narrows
the LIST. Trailing columns are always parsed out of the marker
filename — correct but slower when there's nothing to prune on.

### String-only entry fields

`K`'s fields must be Go `string` (validated at `NewIndex`).
Format times and numbers in your `Of` function, the same way
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

Readers (s3parquet and DuckDB's httpfs/parquet) auto-detect the codec on
read, so switching compression per Write doesn't require any read-side
config.

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
reason), the call returns a wrapped error and the orphan data
file is best-effort deleted. The caller retries — under
`WithIdempotencyToken` the retry is deterministic (same data
path); `findExistingRef`'s freshness filter ignores any stale
ref a previous attempt may have left behind, so the retry emits
a fresh in-budget ref rather than silently matching the stale
one.

```go
if _, err := store.Write(ctx, recs,
    s3store.WithIdempotencyToken(token, time.Hour),
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

| Operation | Sub-package |
|---|---|
| `s3parquet.Reader.Read` / `ReadIter` | `s3parquet` |
| `s3parquet.Index.Lookup` | `s3parquet` |
| `s3parquet.BackfillIndex` | `s3parquet` |
| `s3sql.Reader.Query` | `s3sql` |
| Umbrella `Store.Read` / `ReadIter` | forwards to `s3parquet` |
| Umbrella `Store.Query` | forwards to `s3sql` |

On the `s3sql` read path, the file discovery LIST is done in Go
(carries the header); the parquet-body GET runs through DuckDB's
`httpfs` and **does not** carry the header. That's fine because
data files are write-once-immutable, and StorageGRID's default
`read-after-new-write` covers first-read-of-new-key — DuckDB's
GET sees the committed bytes regardless. See the
[DuckDB note](#duckdb-httpfs-has-no-per-request-header-hook) for
the full reasoning.

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
  ConsistencyStrongGlobal` (multi-site) or `ConsistencyStrongSite`
  (single-site) on the `Config` / `WriterConfig` / `ReaderConfig`
  you build. The library propagates the `Consistency-Control`
  header through every PUT, GET, HEAD, and LIST that carries a
  correctness-critical payload (data, refs, index markers,
  partition listings). `BackfillIndex` takes the level from
  `IndexDef.ConsistencyControl` — set it explicitly on the def
  when running a backfill on StorageGRID. See
  [StorageGRID / NetApp setup](#storagegrid--netapp-setup) for
  the full configuration, including the overwrite-prevention
  bucket policy.

#### DuckDB httpfs has no per-request header hook

DuckDB reads parquet files through its `httpfs` extension, which
supports `s3_endpoint` / `s3_region` / `s3_use_ssl` /
`s3_url_style` settings but exposes **no mechanism to attach an
arbitrary HTTP header per request** for `s3://` URLs (`CREATE
SECRET TYPE HTTP`'s `EXTRA_HTTP_HEADERS` works for `https://`
URLs only). Verified against DuckDB 1.5.2, the current latest.

This means we can't ask DuckDB to send `Consistency-Control` on
the parquet-body GET. Two reasons it doesn't matter:

- **The LIST does carry it.** Every `s3sql` read path resolves
  the file URI list via a Go-side S3 LIST first, then hands the
  explicit list to `read_parquet([…])`. DuckDB never expands
  globs through `httpfs` on the read paths, so its lack of header
  control over LIST is moot. (Earlier versions had a single-pattern
  fast path that did expose this gap; it was removed.)
- **The GET is safe on write-once data.** Data files have
  deterministic immutable paths under `WithIdempotencyToken` and
  unique `{tsMicros}-{shortID}` paths otherwise — they never get
  overwritten. StorageGRID's default `read-after-new-write` is
  exactly the guarantee that the first GET of a new key returns
  the committed bytes, so any DuckDB GET of a key our LIST
  surfaced sees the right contents without needing a stronger
  consistency level on the GET itself.

The corollary: on a backend whose default is *weaker* than
`read-after-new-write` (very rare in practice — neither AWS nor
MinIO nor StorageGRID falls into this category), the `s3sql`
GETs would be exposed. The mitigation is to raise the bucket's
default consistency on the storage side. The library can't help
from above DuckDB.

### Write

If `Write` (or `WriteWithKey`) returns `nil`, every record in
the batch is durably stored in S3 and will be returned by
subsequent `Read`, `Poll`, `PollRecords`, and `Index.Lookup` for
the lifetime of the data. The write path commits in order: data
PUT → marker PUTs → ref PUT, returning success only after the
final step lands (or, on a lost PUT ack, after a HEAD confirms
the object is there).

If `Write` returns an **error**, state is indeterminate — the
records may or may not be durable. The caller must retry. A
retry after partial success writes some records twice; dedupe
those on read via `EntityKeyColumns` + `VersionColumn` (umbrella
/ `s3sql`) or `EntityKeyOf` + `VersionOf` (`s3parquet`).
Multi-group `Write` returns `([]WriteResult, error)` — consult
the slice for records that *did* commit before the error so a
retry can skip them if needed.

To make retries produce deterministic data paths at the storage
layer (no duplicate bytes, no duplicate refs within a bounded
window), pass `WithIdempotencyToken` — see
[Idempotent writes](#idempotent-writes). The ref PUT itself is
budgeted at `SettleWindow / 2`; a slow write that misses the
deadline returns a wrapped put-ref error so the caller retries
(see [Settle window](#settle-window)).

### Read

`Read`, `PollRecords`, and `BackfillIndex` tolerate a missing
data file (S3 `NoSuchKey`) by skipping it and invoking an
optional `Config.OnMissingData(dataPath)` hook. This covers two
rare but real scenarios:

- **Dangling ref from a write-cleanup race.** The ref PUT "failed"
  with a lost ack, the cleanup HEAD also failed transiently, and
  the cleanup DELETE removed the data. Ref lives on in S3 pointing
  at nothing.
- **LIST-to-GET race.** A data file disappears between a `Read`'s
  LIST and its GET (e.g. lifecycle deletion).

Failing the entire read on either would turn a single-record
anomaly into permanent breakage. Every *other* GET error
(throttle, network, auth, timeout) is still fatal — silently
dropping records on transient failure is worse than propagating.

```go
s3parquet.Config[T]{
    // ...
    OnMissingData: func(dataPath string) {
        slog.Warn("s3store: data file missing, skipping",
            "path", dataPath)
    },
}
```

The hook is called from the download worker pool and must be
safe for concurrent invocation. `nil` means "skip silently."

> **Limitation.** `OnMissingData` is honored by `s3parquet` only.
> The umbrella's `Query` goes through `s3sql` (DuckDB's
> `read_parquet`), where a missing file fails the whole call. See
> [Limitations](#limitations).

## Configuration — umbrella

```go
type Config[T any] struct {
    // Required
    Bucket     string                     // S3 bucket name
    Prefix     string                     // prefix under which data lives
    PartitionKeyParts   []string                   // ordered Hive partition key names
    TableAlias string                     // name used in Query SQL
    S3Client   *s3.Client                 // AWS SDK v2 S3 client

    // Required for Write
    PartitionKeyOf func(T) string         // derive key from record (Write)

    // Parquet-side dedup (used by Read / ReadIter / PollRecords).
    // Both or neither — explicit opt-in, no default. NewReader
    // rejects partial config.
    EntityKeyOf func(T) string            // identifies a unique entity
    VersionOf   func(T) int64             // monotonic version per entity

    // SQL-side dedup (used by Query via DuckDB CTE).
    // Both or neither.
    EntityKeyColumns []string             // columns that identify an entity
    VersionColumn    string               // column to ORDER BY for latest

    // Stream
    SettleWindow time.Duration            // default: 5s

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

    // Idempotent-write knobs (optional; see "Idempotent writes"
    // for the full contract).
    DisableCleanup     bool
    ConsistencyControl s3parquet.ConsistencyLevel

    // DuckDB extras
    ExtraInitSQL []string                 // SET / CREATE SECRET / LOAD
                                          // statements run after the
                                          // auto-derived S3 settings
}
```

`S3Endpoint` is no longer in the config — endpoint, region, URL style,
and use_ssl are auto-derived from `S3Client.Options()` at `New()` time.
Credentials are not auto-derived (they can rotate); pass them via
`ExtraInitSQL` with `SET s3_access_key_id=...` or, on real AWS with IAM
roles, use `CREATE SECRET ... PROVIDER credential_chain` so DuckDB
resolves them itself and stays fresh.

Configuration for the sub-packages is narrower — see
`s3parquet.Config[T]`, `s3parquet.WriterConfig[T]`,
`s3parquet.ReaderConfig[T]`, and `s3sql.ReaderConfig[T]` for
the exact fields.

## Migration from earlier versions

Breaking changes in the simplification release:

- **`s3parquet.S3Target` is now a constructed live handle, not a
  struct literal.** The user-facing config moved to a new
  `s3parquet.S3TargetConfig` value type; `S3Target` itself holds
  the config plus an unexported per-target semaphore (sized at
  `MaxInflightRequests`). All public fields are gone — read via
  accessor methods (`target.Bucket()`, `target.Prefix()`, …) or
  the `target.Config()` getter.

  ```go
  // Before
  target := s3parquet.S3Target{
      Bucket:            "b",
      Prefix:            "store",
      S3Client:          cli,
      PartitionKeyParts: []string{"period", "customer"},
  }

  // After
  target := s3parquet.NewS3Target(s3parquet.S3TargetConfig{
      Bucket:            "b",
      Prefix:            "store",
      S3Client:          cli,
      PartitionKeyParts: []string{"period", "customer"},
  })
  ```

  Pass the same `S3Target` value to `WriterConfig.Target` and
  `ReaderConfig.Target` so the Writer and Reader share one
  semaphore (Targets are passed by value but the chan inside is
  a reference). `s3parquet.New(cfg)` and `s3store.New(cfg)`
  construct the target internally — single-Config callers see no
  change beyond the field name swap below.

- **`PartitionWriteConcurrency` → `MaxInflightRequests`**, moved
  from `WriterConfig` / `Config` onto the shared
  `s3parquet.S3TargetConfig`. One knob caps every fan-out
  (partition, file, pattern, marker) via the per-target
  semaphore — peak in-flight per Target is bounded by the
  configured value regardless of axis.

  ```go
  // Before
  cfg := s3parquet.Config[T]{ ..., PartitionWriteConcurrency: 16 }

  // After
  cfg := s3parquet.Config[T]{ ..., MaxInflightRequests: 16 }
  ```

  See the Write section for the `http.Transport.MaxConnsPerHost`
  interaction when running many Targets concurrently.

- **`s3parquet.ReaderExtras[T']` removed.**
  `NewReaderFromWriter` / `NewReaderFromStore` now take a full
  `ReaderConfig[T']`; the constructor overrides the `Target`
  from the source Writer/Store. Pass an empty `ReaderConfig{}` if
  you only want field-projection without dedup.

  ```go
  // Before
  view, _ := s3parquet.NewReaderFromStore[NarrowRec](store,
      s3parquet.ReaderExtras[NarrowRec]{})

  // After
  view, _ := s3parquet.NewReaderFromStore[NarrowRec](store,
      s3parquet.ReaderConfig[NarrowRec]{})
  ```

Breaking changes in the idempotent-write release:

- **`DuplicateWriteDetection` config field + factory functions
  (`DuplicateWriteDetectionByOverwritePrevention`,
  `DuplicateWriteDetectionByHEAD`,
  `DuplicateWriteDetectionByProbe`) removed.** Idempotent writes
  always use `If-None-Match: *` now (with the StorageGRID
  403+HEAD fallback inside `putIfAbsent`) — the constructor
  no longer probes the backend. On backends that don't enforce
  any overwrite prevention, retries silently re-PUT byte-identical
  data; reader dedup absorbs the harmless extra ref. Drop the
  field from your config; nothing else changes.

- **`s3store.New`, `s3parquet.New`, `s3parquet.NewWriter` no
  longer take `context.Context`** — the constructor performs no
  S3 I/O, so the parameter is gone. `NewReader` /
  `s3sql.NewReader` were already context-free.

  ```go
  // Before
  store, err := s3store.New[T](ctx, cfg)

  // After
  store, err := s3store.New[T](cfg)
  ```

- **`s3sql.WithReadAheadPartitions` removed.** Was a documented
  no-op on the s3sql read path. The umbrella + s3parquet exports
  remain. Drop the call site if you had one.

- **`Write`, `WriteWithKey` accept a variadic `...WriteOption`
  tail** for `WithIdempotencyToken`. Existing callers that don't
  pass options compile unchanged — the tail is variadic.

Breaking changes in the bloom-filter removal:

- **`BloomFilterColumns` removed** from `s3parquet.Config`,
  `s3parquet.WriterConfig`, and umbrella `s3store.Config`. The
  feature didn't pay off for typical workloads (a single row
  group per file leaves nothing to prune; partition columns are
  already pruned at file-selection time). Files written with
  the old config remain readable on both paths; files written
  by the new code emit no bloom filters. If your queries
  actually relied on bloom-filter pruning (multi-row-group
  files plus equality filters on a non-partition column), pin
  an older version or open an issue.

Breaking changes in the `s3sql`-as-Reader refactor:

- **`s3sql.Store[T]` → `s3sql.Reader[T]`.** The type only ever
  read; the rename makes that contract explicit.
- **`s3sql.Config[T]` → `s3sql.ReaderConfig[T]`.** The S3-wiring
  fields (`Bucket`, `Prefix`, `S3Client`, `PartitionKeyParts`,
  `SettleWindow`, `DisableRefStream`) moved under a single
  `Target s3parquet.S3Target` field. Build the target once and
  share it between `s3parquet.WriterConfig` and
  `s3sql.ReaderConfig` so the two halves can't drift.
- **`s3sql.New` → `s3sql.NewReader`.**
- **Umbrella `s3store.Store[T]` internals recomposed** around
  `s3parquet.Writer[T]` + `s3sql.Reader[T]` (was `s3parquet.Store[T]`
  + `s3sql.Store[T]`). `s3store.Config[T]` is unchanged; the
  umbrella now exposes `.Writer()` and `.Reader()` accessors if
  a caller needs the underlying halves.
- **`s3parquet.S3Target.settleWindow()` → `EffectiveSettleWindow()`
  and `validate()/validateLookup()` → `Validate()/ValidateLookup()`.**
  Internal-only methods promoted so `s3sql.NewReader` can reuse
  them without duplicating validation logic.



Breaking changes in the Index refactor (s3parquet only; umbrella
`s3store.NewIndex` still bundles register + handle in one call):

- **`Index[T, K]` → `Index[K]`.** Lookup never touches T, so the
  query handle is now T-free. A read-only service (dashboard,
  query API) can share one `Index[K]` across any `T` that
  produced the markers.
- **`IndexDef` split.** `IndexLookupDef[K]` holds the read-side
  `Name` + `Columns`; `IndexDef[T, K]` embeds it and adds `Of`.
  Existing struct literals need an extra layer:

  ```go
  s3parquet.IndexDef[T, K]{
      IndexLookupDef: s3parquet.IndexLookupDef[K]{
          Name:    "...",
          Columns: []string{"..."},
      },
      Of: func(T) []K { ... },
  }
  ```

- **`s3parquet.NewIndex` re-signed.** The primary `NewIndex` now
  takes an untyped `S3Target` + `IndexLookupDef[K]` — pure-read
  flow, no writer registration, no `T` on the call site. For the
  old "register + handle" shape use
  `s3parquet.NewIndexWithRegister(writer, def)` or
  `s3parquet.NewIndexFromStoreWithRegister(store, def)`.
- **`Index.Backfill` removed; use the package-level
  `s3parquet.BackfillIndex` / `s3store.BackfillIndex`.** The new
  function takes an `S3Target` + `IndexDef` + `[]string` patterns
  + `until Offset` + `onMissingData` hook, with no writer/reader
  argument. The common shape is a standalone migration job; see
  the [Backfill](#backfill) section for the expected flow.
- **`s3parquet.Store.Close()` removed.** The method was a documented
  no-op (pure-Go Store held no resources). Delete the call sites.
  `s3sql.Reader.Close()` and `s3store.Store.Close()` still exist — only
  the parquet side lost it.
- **`WriterConfig` / `ReaderConfig` restructured.** The five
  S3-wiring fields (`Bucket`, `Prefix`, `S3Client`,
  `PartitionKeyParts`, `SettleWindow`) moved under a single
  `Target s3parquet.S3Target` field. Move those fields into a
  `Target: s3parquet.S3Target{...}` literal, or build the target
  once via `s3parquet.NewS3Target(...)` and reuse it between
  `WriterConfig` and `ReaderConfig`. The unified umbrella
  `s3store.Config[T]` is unchanged — its flat layout still
  projects onto the narrower configs internally.

Breaking changes in the package-split refactor:

- **`Config.KeyFunc` → `Config.PartitionKeyOf`** — new name better reflects
  the field's role.
- **`Config.KeyParts` → `Config.PartitionKeyParts`** — follows the same
  `PartitionKey*` naming family.
- **`Config.S3Endpoint` removed** — auto-derived from
  `S3Client.Options().BaseEndpoint`. For local S3-compatible setups
  (MinIO fork, SeaweedFS, etc.), just pass the full URL
  (`http://localhost:9000`) on your `s3.Options`.
- **Glob grammar narrowed** — `?`, `[abc]`, `{a,b}` alternation, and
  leading/middle `*` in values are rejected. Only whole-segment `*` and a
  single trailing `*` per value are accepted. If you relied on the richer
  DuckDB glob dialect, file an issue — we can relax the parser if there's
  real usage.
- **`Config.ScanFunc` removed** — `s3sql` now reflects over `T`'s parquet
  tags to build a NULL-safe row binder at `New()`. Drop your `ScanFunc`
  closure; the library decodes into `[]T` directly. Custom types need to
  implement `sql.Scanner` (e.g. `shopspring/decimal.Decimal` already
  does).
- **`Config.ColumnAliases` / `Config.ColumnDefaults` removed** — the
  "missing column → Go zero" contract is now built in to both read paths.
  For non-zero defaults, apply them in your app code or use `Query` with
  `COALESCE`. For column renames, rewrite the affected files.
- **`Config.DeduplicateBy` → `Config.EntityKeyColumns`, no default** —
  mirrors `s3parquet.Config.EntityKeyOf`: explicit opt-in to latest-per-
  entity dedup, no silent partition-key default. `VersionColumn` and
  `EntityKeyColumns` must now be set together (both or neither); `New()`
  rejects one without the other. If you relied on the old default, set
  `EntityKeyColumns = PartitionKeyParts` explicitly.

## Testing

```
# Unit tests, cgo required (s3sql + umbrella embed DuckDB).
go test -count=1 ./...

# Unit tests on the cgo-free subset — no C compiler needed.
CGO_ENABLED=0 go test -count=1 ./s3parquet/... ./internal/...

# Integration tests — full round-trip against a MinIO container.
# Uses testcontainers; one container is shared across every
# package in the invocation.
go test -tags=integration -timeout=10m -count=1 ./...
```

Integration tests require Docker and pull a pinned `pgsty/minio`
release on first run (see
[`internal/testutil/minio.go`](internal/testutil/minio.go)).
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

- **Single-process reads** (umbrella / `s3sql`). DuckDB runs embedded in
  your Go process. `s3parquet` has no embedded engine — reads are just
  parquet-go + S3 calls.
- **S3 key limit: 1024 bytes.** Long partition values reduce the budget.
- **Stream latency = poll interval + settle window.** Not real-time.
- **Upsert-only compacted mode.** There is no tombstone / key-delete
  mechanism — keys can only be updated, not removed.
- **`s3parquet` dedup is in-memory.** Large key cardinality can OOM;
  route those workloads to `s3sql` which streams through DuckDB.
- **Missing-data tolerance only in `s3parquet`.** `OnMissingData`
  (skip a parquet file whose GET returns `NoSuchKey`) is honored
  by `s3parquet.Read` / `PollRecords` / `BackfillIndex`. The
  umbrella's `Query` goes through `s3sql`'s DuckDB path, which
  treats its input URI list as authoritative — a dangling ref
  fails the whole call. Use `s3parquet` directly (or the umbrella's
  `Read` / `ReadIter` / `PollRecords`, which delegate to
  `s3parquet`) if you need the tolerant read path.
- **`s3sql` parquet-body GET can't carry `ConsistencyControl`**
  (DuckDB `httpfs` has no per-request HTTP header setting on
  `s3://` URLs). Not a problem on AWS / MinIO / StorageGRID:
  data files are write-once and the GET is the first read of a
  new key, which `read-after-new-write` already covers. Only
  matters on a backend whose default is weaker than
  read-after-new-write — raise the bucket default in that case.
  See [Read-after-write](#read-after-write).
- **Schema evolution is limited to tolerant reads.** Both packages handle
  "column added to T that isn't in an old file" by returning the Go zero
  value. Renames, splits, type changes, and row-level computed
  derivations require rewriting the affected files.

## License

See [LICENSE](LICENSE).
