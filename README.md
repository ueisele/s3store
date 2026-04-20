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
| `s3parquet` | **no** | `github.com/ueisele/s3store/s3parquet` | Write, WriteWithKey, Read, Poll, PollRecords. Pure Go / parquet-go; in-memory dedup. |
| `s3sql` | yes | `github.com/ueisele/s3store/s3sql` | **Read-only.** Read, Query, QueryRow, Poll, PollRecords via embedded DuckDB. Construct with `NewReader(ReaderConfig)`; share the same `s3parquet.S3Target` with the Writer so the two halves can't drift. |
| `s3store` (umbrella) | yes | `github.com/ueisele/s3store` | Everything above behind one `Store[T]`. Composes `s3parquet.Writer` + `s3sql.Reader`; both halves share one `S3Target` so they can't drift on S3 wiring. |

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

store, err := s3store.New[CostRecord](s3store.Config[CostRecord]{
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
latest, err := store.Read(ctx, "charge_period=2026-03-17/customer=abc")

// SQL query — DuckDB handles the aggregation
rows, err := store.Query(ctx, "charge_period=2026-03-*/*",
    "SELECT customer, SUM(net_cost) FROM costs GROUP BY customer")
```

## Quick start — cgo-free (`s3parquet` only)

For write-heavy services or consumers that don't need SQL:

```go
store, err := s3parquet.New[CostRecord](s3parquet.Config[CostRecord]{
    Bucket:   "warehouse",
    Prefix:   "billing",
    S3Client: s3Client,
    PartitionKeyParts: []string{"charge_period", "customer"},
    PartitionKeyOf: func(r CostRecord) string {
        return fmt.Sprintf("charge_period=%s/customer=%s",
            r.ChargePeriod, r.CustomerID)
    },
    // Optional: enable latest-per-entity dedup on Read / PollRecords.
    // With only EntityKeyOf set, VersionOf defaults to DefaultVersionOf
    // (wrote-last-wins using the parquet file's write time).
    EntityKeyOf: func(r CostRecord) string {
        return r.CustomerID + "|" + r.SKU
    },
    // Optional: override the default with a business timestamp.
    VersionOf: func(r CostRecord, insertedAt time.Time) int64 {
        return r.CalculatedAt.UnixNano()
    },
})

// parquet-go decodes directly into []CostRecord via the parquet tags
latest, err := store.Read(ctx, "charge_period=2026-03-17/customer=abc")
```

Both packages drive typed results off the parquet struct tags on `T` —
`s3parquet` via parquet-go's `GenericReader[T]`, `s3sql` via a NULL-safe
reflection binder built at `New()`. No `ScanFunc` or manual column-order
bookkeeping on either side. A field whose column is missing from a given
file lands as Go's zero value.

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

store, _ := s3parquet.New[FullRec](cfg)
view, _ := s3parquet.NewReaderFromStore[NarrowRec, FullRec](store,
    s3parquet.ReaderExtras[NarrowRec]{})

recs, _ := view.Read(ctx, "*") // []NarrowRec — ProcessLog not fetched
```

`ReaderExtras[T']` carries the read-side knobs that aren't shared
with the Writer (`EntityKeyOf`, `VersionOf`, `InsertedAtField`,
`OnMissingData`). `SettleWindow` is inherited from the Writer
(and its `S3Target`), so the view sees the same window as the
source. Dedup closures are typed over `T'`, so you supply them
explicitly for the narrow shape when needed.

The relationship between constructors:

| Want | Call |
|---|---|
| Both write and read, same `T` | `New(Config[T])` → `*Store[T]` |
| Write only, narrow config | `NewWriter(WriterConfig[T])` → `*Writer[T]` |
| Read only, narrow config | `NewReader(ReaderConfig[T])` → `*Reader[T]` |
| Same-/narrower-`T'` Reader over a Writer | `NewReaderFromWriter[T', U](writer, extras)` |
| Same-/narrower-`T'` Reader over a Store | `NewReaderFromStore[T', U](store, extras)` |

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
store, _ := s3parquet.New[UsageFile](s3parquet.Config[UsageFile]{ /* ... */ })

// Writes:
files := make([]UsageFile, len(usages))
for i, u := range usages {
    f, err := toFile(u); if err != nil { return err }
    files[i] = f
}
_, err := store.Write(ctx, files)

// Reads:
files, err := store.Read(ctx, "...")
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
surfaces them as columns and you can `SELECT` or `WHERE` on them; the
reflection-based row binder silently discards them when they have no
matching struct field. `s3parquet` ignores Hive paths entirely — if you
need `year`/`month` on the Go-only read path, reconstruct them from `Ts`.

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

`Write` fans out per-partition work in parallel (bounded by
`PartitionWriteConcurrency`, default 8), so a single call with many
groups completes in roughly the time of the slowest group instead of
their sum. Raise the cap for workloads with many small partitions per
Write (e.g. a DB poll that fans out to dozens of Hive keys at once);
leave at the default when per-partition parquet buffers are large
enough that N × buffer-size dominates memory. Each partition is
self-contained — on error the function cancels still-running
partitions and returns the results that already committed in
sorted-key order, alongside the first real error. Callers can retry
the failed partitions via `WriteWithKey` without re-writing the ones
that succeeded.

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

Whether dedup actually runs depends on which package you use:

- **Umbrella / `s3sql.PollRecords`** — dedup when `VersionColumn` is set
  (DuckDB `QUALIFY ROW_NUMBER()`). When `VersionColumn` is empty, every
  record passes through.
- **`s3parquet.PollRecords`** — dedup when `EntityKeyOf` is set. If
  `VersionOf` is nil, it defaults to `DefaultVersionOf` (the file's
  write time). When `EntityKeyOf` is nil, every record passes through.

`WithHistory()` forces the no-dedup path in every case.

**Deterministic tie-break on equal versions.** When two writes share the
same version for the same entity key — common on an at-least-once retry
that replays a batch with the same domain timestamp — both paths resolve
the tie deterministically:

- **`s3parquet`**: first write wins (first occurrence, stable across
  repeated reads).
- **`s3sql`**: later write wins (secondary `ORDER BY filename DESC` in
  the dedup CTE; data filenames embed the write tsMicros, so lex-later
  = wrote-later). Stable across repeated reads.

The two packages differ in *which* record wins, but each is deterministic
on its own. Pick the package whose tie-break matches your retry
semantics — or make the tie impossible by ensuring `VersionColumn` /
`VersionOf` strictly increases per write.

### Per-record "when was this inserted"

If a consumer needs the S3 write time of every record without
storing it as a data column, set `Config.InsertedAtField` to the
name of a `time.Time` field on `T`. The field must carry
`parquet:"-"` so it stays off the parquet schema — the library
populates it on decode from the source file's tsMicros.

```go
type Event struct {
    Customer   string    `parquet:"customer"`
    Amount     float64   `parquet:"amount"`
    InsertedAt time.Time `parquet:"-"` // populated by the library
}

s3store.Config[Event]{
    // ...
    InsertedAtField: "InsertedAt",
}

recs, _ := store.Read(ctx, "*")
// recs[i].InsertedAt is the parquet file's write time
```

Works on `s3parquet.Read` / `PollRecords` and, for the umbrella,
`s3sql.Read` / `PollRecords` (via DuckDB's `read_parquet(filename=true)`
— the helper column is parsed post-scan and never touches the
parquet schema on disk). Zero reflection cost when unset.

### Stream — time window

To read only records written within a time window (e.g. "yesterday's
activity"), use `OffsetAt` to turn a wall-clock time into a stream offset.
The range is half-open `[since, until)`, matching Kafka offset semantics.

`PollRecordsAll` is the convenience entry point for bounded windows —
one call, internal batching, no manual paging:

```go
// All records written on 2026-04-17 (UTC).
start := store.OffsetAt(time.Date(2026, 4, 17, 0, 0, 0, 0, time.UTC))
end   := store.OffsetAt(time.Date(2026, 4, 18, 0, 0, 0, 0, time.UTC))
records, err := store.PollRecordsAll(ctx, start, end)
```

Half-open boundary semantics:

- A record written at `2026-04-17 23:59:59.999999` — **included** (offset < end).
- A record written exactly at `2026-04-18 00:00:00.000000` — **excluded** (that instant belongs to the next window).
- A record written exactly at `2026-04-17 00:00:00.000000` — **included**.

So to cover a full day, `end` is the start of the *next* day.

**Timezone**: `OffsetAt` compares in UTC microseconds internally (offsets
are encoded from `time.UnixMicro()`). `time.Date(..., time.UTC)` gives
UTC-day boundaries; `time.Date(..., loc)` gives local-day boundaries —
both work, as long as `start` and `end` use the same timezone.

Pass `s3store.Offset("")` (or `s3parquet.Offset("")` / `s3sql.Offset("")`
when using the sub-packages directly) for `since` to start at the stream
head, or for `until` to read to the live tip (settle-window cutoff).

For streaming (bounded memory on long windows, or processing in batches),
use `PollRecords` with `WithUntilOffset`:

```go
start := store.OffsetAt(yesterdayStart)
end   := store.OffsetAt(yesterdayEnd)
for {
    records, next, err := store.PollRecords(ctx, start, 100,
        s3store.WithUntilOffset(end))
    if err != nil { return err }
    if len(records) == 0 { break }
    // process records
    start = next
}
```

`OffsetAt` is pure computation — no S3 call. `WithUntilOffset` breaks
the paginator early once offsets reach `until`, so long streams aren't
scanned past the window of interest. All three APIs — `OffsetAt`,
`WithUntilOffset`, `PollRecordsAll` — are available on the umbrella,
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
- **`Read` / `Query` / `QueryRow` / `ReadMany` / `QueryMany` /
  `QueryRowMany` / `Lookup` / `BackfillIndex`** — unaffected. They
  LIST `data/` (or `_index/<name>/`) directly and never consult refs.
- **`Poll` / `PollRecords` / `PollRecordsAll`** — return
  `s3store.ErrRefStreamDisabled` (shared sentinel, matches
  `errors.Is` across packages).
- **`OffsetAt`** — still works. It's pure timestamp encoding with no
  S3 dependency, so it keeps returning well-formed offsets even
  though there's no stream to compare them against.

Per-write irreversible: data written with `DisableRefStream: true`
has no refs, so flipping the flag back does not retroactively make
`Poll` see the historical writes.

Both sides of the deployment (writer and `s3sql` reader) must agree
on the flag — set it on the umbrella `Config`, or on the
`s3parquet.S3Target` shared by `s3parquet.Writer` and
`s3sql.Reader`. The two failure modes if they drift:

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
records, err := store.Read(ctx, "charge_period=2026-03-17/customer=abc")
```

Returns every record matching the glob, decoded directly into `[]T` via
the parquet tags (parquet-go for `s3parquet`, the reflection binder for
`s3sql`). When dedup is configured (see Stream above), the result is the
latest version per key; otherwise every version comes through.

### Snapshot — streaming (`ReadIter`)

`Read` and `ReadMany` buffer the full result set before returning. For
month-scale or otherwise unbounded ranges that's a memory problem; use
`ReadIter` / `ReadManyIter` instead — they yield records one at a time
via Go 1.23's `iter.Seq2[T, error]`:

```go
for r, err := range store.ReadIter(ctx, "charge_period=2026-03-*/*") {
    if err != nil { return err }
    aggregate(r)            // fold into an aggregation map and forget r
}
// breaking out of the loop cancels in-flight downloads — no Close
```

Three callable surfaces, matching `Read` / `ReadMany` exactly:
`s3parquet.Reader.ReadIter`, `s3sql.Reader.ReadIter`, and the umbrella
`s3store.Store.ReadIter` (forwards to s3sql). Same for `*ManyIter`.

**Memory profile** depends on which backend and which dedup mode:

| Path | Default (dedup on) | `WithHistory()` |
|---|---|---|
| `s3parquet.Reader.ReadIter` | O(one partition's records) | O(one partition's records) |
| `s3sql.Reader.ReadIter` | O(DuckDB query plan) | same |

The pure-Go path processes one partition at a time: files inside the
partition download 8-wide in parallel, the decoded batch is deduped
(or passed through on `WithHistory()`), records are yielded in
lex/insertion order, then the batch is dropped before the next
partition starts. Month-scale reads go from O(month) to O(partition)
peak memory, which is usually small enough for hourly/daily
partitioning. If a single partition is large enough that even one
pre-dedup batch is a problem, file an issue — we can follow up with a
streaming fold that trades the code simplicity for peak memory
proportional to unique entities rather than total records.

**Prefetch with `WithReadAheadPartitions(n)`**: by default, the next
partition's download only starts after the current partition's yield
loop finishes. For layouts with many small partitions where S3
round-trip latency dominates, pass `WithReadAheadPartitions(n)` to
run a background producer that keeps up to `n` partitions prefetched
ahead of the yield position:

```go
for r, err := range store.ReadIter(ctx, "*",
    s3parquet.WithReadAheadPartitions(4)) {
    // ... consumer work overlaps with downloads of the next 4 partitions
}
```

Trade-off: memory grows to at most `n + 2` partitions' worth of
records (one being yielded, `n` buffered, one in the producer's hand
while waiting to send). Speed-up is bounded by how much of your
per-partition yield time would otherwise sit idle waiting for
downloads. Default `0` preserves strict-serial semantics — existing
callers see no change. No-op on the s3sql / umbrella read paths since
DuckDB already streams rows across the full file union.

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

**Order**: `s3parquet.Reader.ReadIter` visits partitions in lex order;
files within a partition in lex order (= write-time order, since data
filenames start with their `tsMicros`). `s3sql.Reader.ReadIter` returns
rows in DuckDB's query order — add `ORDER BY` in a custom `Query` call
if you need stability.

### SQL query (umbrella or `s3sql`)

```go
rows, err := store.Query(ctx, "charge_period=2026-03-*/*",
    "SELECT customer, sku, SUM(net_cost) AS total "+
        "FROM costs GROUP BY customer, sku")

var total float64
err = store.QueryRow(ctx, "charge_period=2026-03-17/*",
    "SELECT SUM(net_cost) FROM costs").Scan(&total)
```

Deduplicated by default. Pass `s3store.WithHistory()` to see all versions.
`QueryRow` is the `database/sql` convention for queries that return at
most one row — construction-time errors surface through the returned
`*sql.Row` at `Scan` time.

## Schema evolution

Both read paths tolerate missing columns out of the box: a field whose
column isn't in a given parquet file lands as Go's zero value, never an
error. That covers the common "added a column" case without any extra
configuration.

- **`s3parquet`** — parquet-go matches columns to struct fields by the
  `parquet:` tag, so column order in the file doesn't matter and unknown
  columns are ignored.
- **`s3sql`** — the reflection binder does the same for DuckDB results:
  unused columns are discarded, missing columns leave the field at its
  Go zero, and user types implementing `sql.Scanner` (e.g.
  `shopspring/decimal.Decimal`) are supported natively.

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

store, _ := s3store.New[Usage](cfg)

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
hits, err := skuIdx.Lookup(ctx,
    "sku_id=SKU-123/charge_period_start=2026-03-01..2026-04-01/"+
    "causing_customer=*/charge_period_end=*")
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
customer=def)` but *not* the off-diagonal combinations), pass
them as a slice instead. The full family is available on both
read paths and the index layer:

| Entry point | Where |
|---|---|
| `Reader.ReadMany` | `s3parquet.Reader`, `s3sql.Reader`, umbrella |
| `Reader.QueryMany` / `QueryRowMany` | `s3sql.Reader`, umbrella |
| `Index.LookupMany` | `s3parquet`, umbrella |
| `BackfillIndexMany` | `s3parquet`, umbrella |

```go
// Pure-Go read across non-Cartesian tuples.
recs, _ := store.ReadMany(ctx, []string{
    "period=2026-03-17/customer=abc",
    "period=2026-03-18/customer=def",
})

// DuckDB aggregation across the union — one query, global
// GROUP BY / SUM / join across the tuple set.
rows, _ := store.QueryMany(ctx, []string{
    "period=2026-03-17/customer=abc",
    "period=2026-03-18/customer=def",
}, "SELECT customer, SUM(amount) FROM records GROUP BY customer")

// Index lookup over an arbitrary set of (col, col) tuples.
entries, _ := idx.LookupMany(ctx, []string{
    "sku=s1/customer=abc",
    "sku=s4/customer=def",
})

// One-off migration across several partitions.
stats, _ := s3store.BackfillIndexMany(ctx, target, def,
    []string{"period=2026-03-*/customer=*", "period=2026-04-01/customer=*"},
    until, nil)
```

**Execution model (s3parquet path):** LIST calls fan out across
patterns with a bounded 8-wide concurrency cap, overlapping
patterns are deduplicated at the key level, the GET+decode pool
runs over the unioned set, and dedup (if configured) applies
globally — an entity appearing under two patterns is kept as
the latest version across the union, not per-pattern.

**Execution model (s3sql path):** single-pattern fast-paths stay
on DuckDB's glob + plan-time partition pruning. Multi-pattern
pre-LISTs in Go with the same 8-wide cap to produce the exact
file URI list, then runs **one** `read_parquet([...])` so
DuckDB plans once over the whole set — the analytical win
(`SUM` / `GROUP BY` / joins across non-Cartesian tuples) that N
separate `Query` calls would force the caller to do in Go.

Single-pattern `Read` / `Query` / `QueryRow` / `Lookup` /
`BackfillIndex` are one-line sugar over their `-Many`
counterparts and stay unchanged. Passing a one-element slice to
a `-Many` method takes the same fast path as calling the
single-pattern form directly — no Go-side LIST, no extra
machinery.

### What's in scope for v1

- Register + auto-write on `Write` + `Lookup` via the typed handle.
- SettleWindow applies to Lookup: markers LIST-visible but with
  `LastModified` inside `now - SettleWindow` are hidden, matching
  `Poll`'s guarantees so index and data views agree within the
  window.
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
    store.Target(), // or construct via s3parquet.NewS3Target
    def,            // the same IndexDef the live app registered
    "*",            // pattern (PartitionKeyParts grammar)
    until,          // exclusive upper bound on LastModified
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
for everything before. Pass `s3store.Offset("")` to disable the
bound (covers every file currently present — harmless but
redundant if the live writer has been up for a while, since PUT
is idempotent).

Idempotent at the S3 level (same empty marker, same key), so a
retry after cancel or crash is a no-op on work already done. Safe
to run while the live writer keeps emitting markers for fresh
records.

Lookup's SettleWindow applies to backfilled markers too — expect
up to one SettleWindow of lag before a just-backfilled index
fully reports.

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

## Durability guarantees

The contract is **at-least-once** on both sides of the wire.

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
> The umbrella's `Read` / `Query` / `PollRecords` go through
> `s3sql` (DuckDB's `read_parquet`), where a missing file fails
> the whole call. See [Limitations](#limitations).

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

    // SQL-side dedup (used by Read / PollRecords / Query).
    // Both or neither — explicit opt-in, no default.
    EntityKeyColumns []string             // columns that identify an entity
    VersionColumn    string               // column to ORDER BY for latest

    // Stream
    SettleWindow time.Duration            // default: 5s

    // Optional metadata hook: if set, Read / PollRecords
    // populate this field on T (must be `time.Time`, tagged
    // `parquet:"-"`) with the source file's write timestamp.
    InsertedAtField string

    // Parquet compression codec (default snappy).
    Compression CompressionCodec

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
  function takes an `S3Target` + `IndexDef` + `pattern` + `until
  Offset` + `onMissingData` hook, with no writer/reader argument.
  The common shape is a standalone migration job; see the
  [Backfill](#backfill) section for the expected flow.
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
  umbrella's `Read` / `Query` / `PollRecords` go through
  `s3sql`'s DuckDB path, which treats its input URI list as
  authoritative — a dangling ref fails the whole call. Use
  `s3parquet` directly if you need the tolerant read path.
- **Schema evolution is limited to tolerant reads.** Both packages handle
  "column added to T that isn't in an old file" by returning the Go zero
  value. Renames, splits, type changes, and row-level computed
  derivations require rewriting the affected files.

## License

See [LICENSE](LICENSE).
