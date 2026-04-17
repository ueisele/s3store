# s3store

Append-only, versioned data storage on S3 with a change stream and optional
embedded DuckDB queries. No server. No broker. No coordinator. Just a Go
library and an S3 bucket.

## What it does

- **Write** typed records as Parquet, grouped into Hive-style partitions.
- **Stream** changes via lightweight "ref" files — one empty S3 object per
  write, all metadata in the object key.
- **Read** point-in-time deduplicated snapshots with glob support.
- **Query** the whole store with DuckDB SQL, including schema evolution,
  column aliases, and defaults — in a single embedded process.

## Packages

s3store ships as three packages. Pick the smallest one that covers your
needs:

| Package | cgo | Import path | Capabilities |
|---|---|---|---|
| `s3parquet` | **no** | `github.com/ueisele/s3store/s3parquet` | Write, WriteWithKey, Read, Poll, PollRecords. Pure Go / parquet-go; in-memory dedup. |
| `s3sql` | yes | `github.com/ueisele/s3store/s3sql` | Read, Query, QueryRow, Poll, PollRecords. Embedded DuckDB; SQL; schema-evolution transforms. |
| `s3store` (umbrella) | yes | `github.com/ueisele/s3store` | Everything above behind one `Store[T]`. Forwards to `s3parquet` for writes/poll, `s3sql` for typed reads. |

Binary size: DuckDB bundles a ~50 MB C++ library. `CGO_ENABLED=0 go build
./s3parquet/...` produces a small static binary with none of it. The
umbrella and `s3sql` both require cgo.

Both sub-packages share the S3 layout and ref-stream wire format, so the
same data is accessible through either.

## Install

```
go get github.com/ueisele/s3store
```

Requires Go 1.26.2+ (declared in [go.mod](go.mod)). DuckDB's httpfs
extension is auto-installed on first use or pre-installed in air-gapped
environments.

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
    ScanFunc: func(rows *sql.Rows) (CostRecord, error) {
        var r CostRecord
        err := rows.Scan(
            &r.CustomerID, &r.ChargePeriod, &r.SKU,
            &r.NetCost, &r.Currency, &r.CalculatedAt)
        return r, err
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

// parquet-go decodes directly into []CostRecord — no ScanFunc needed
latest, err := store.Read(ctx, "charge_period=2026-03-17/customer=abc")
```

`s3parquet` has no `ScanFunc` (parquet-go handles typed decode via struct
tags), no `ColumnAliases` / `ColumnDefaults` (those are SQL-expression
features), and a narrower glob grammar (see below).

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

## Glob grammar

Both `s3parquet` and `s3sql` share one grammar, validated identically:

| Pattern | Accepted? |
|---|---|
| `*` (literal) — match everything | ✓ |
| `charge_period=2026-03-17/customer=abc` — exact | ✓ |
| `charge_period=2026-03-*/customer=abc` — trailing `*` in value | ✓ |
| `*/customer=abc` — whole-segment `*` | ✓ |
| `charge_period=*-17/customer=abc` — leading `*` | ✗ |
| `charge_period=2026-*-17/customer=abc` — middle `*` | ✗ |
| `charge_period=[0-9]/customer=abc` — char class | ✗ |
| `charge_period={2026,2027}/customer=abc` — alternation | ✗ |

Truncated patterns (fewer segments than `PartitionKeyParts`) and mislabelled
segments (part name in the wrong position) are also rejected.

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

### Snapshot

```go
records, err := store.Read(ctx, "charge_period=2026-03-17/customer=abc")
```

Returns every record matching the glob, typed via `ScanFunc` (or decoded
directly into T via parquet-go for `s3parquet`). When dedup is configured
(see Stream above), the result is the latest version per key; otherwise
every version comes through.

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

## Schema evolution (s3sql only)

```go
Config[T]{
    // Fill NULLs or materialize missing columns with a default
    ColumnDefaults: map[string]string{
        "currency": "'EUR'",
    },
    // Handle column renames: each new column can chain to old names
    ColumnAliases: map[string][]string{
        "cost_per_unit": {"price_per_unit", "unit_price"},
    },
}
```

Applied at query time across all SQL-path reads (`Read`, `Query`,
`QueryRow`, `PollRecords` via the umbrella). s3store introspects the
scanned files' schemas and picks the right SQL shape — `SELECT * REPLACE`
when the target column already exists, `SELECT *, alias` when it doesn't,
`NULL AS col` when neither side is present. The old-name columns are
`EXCLUDE`d so they don't linger next to the aliased column.

`s3parquet.Read` relies on parquet-go's native schema tolerance instead
(fields missing from a file decode as zero-values; unknown fields in a
file are ignored). For renames or SQL-expression defaults, use `s3sql`.

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

## Configuration — umbrella

```go
type Config[T any] struct {
    // Required
    Bucket     string                     // S3 bucket name
    Prefix     string                     // prefix under which data lives
    PartitionKeyParts   []string                   // ordered Hive partition key names
    TableAlias string                     // name used in Query SQL
    S3Client   *s3.Client                 // AWS SDK v2 S3 client

    // Required for Write / typed reads
    PartitionKeyOf func(T) string         // derive key from record (Write)
    ScanFunc       func(*sql.Rows) (T, error) // map DuckDB row → typed record

    // SQL-side dedup (used by Read / PollRecords / Query)
    VersionColumn string                  // column to ORDER BY for latest
    DeduplicateBy []string                // dedup keys (default: PartitionKeyParts)

    // Stream
    SettleWindow time.Duration            // default: 5s

    // Schema evolution (SQL path only)
    ColumnDefaults map[string]string      // SQL expr per missing column
    ColumnAliases  map[string][]string    // new name → old name chain

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

Configuration for `s3parquet` and `s3sql` directly is narrower — see each
package's `Config[T]` for the exact fields.

## Migration from earlier versions

Breaking changes in the package-split refactor:

- **`Config.KeyFunc` → `Config.PartitionKeyOf`** — new name better reflects
  the field's role.
- **`Config.KeyParts` → `Config.PartitionKeyParts`** — follows the same
  `PartitionKey*` naming family.
- **`Config.S3Endpoint` removed** — auto-derived from
  `S3Client.Options().BaseEndpoint`. For MinIO-style setups, just pass the
  full URL (`http://minio:9000`) on your `s3.Options`.
- **Glob grammar narrowed** — `?`, `[abc]`, `{a,b}` alternation, and
  leading/middle `*` in values are rejected. Only whole-segment `*` and a
  single trailing `*` per value are accepted. If you relied on the richer
  DuckDB glob dialect, file an issue — we can relax the parser if there's
  real usage.

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

Integration tests require Docker and pull `minio/minio:latest` on first
run. `-count=1` is the Go idiom for "bypass the test cache" — without it,
unchanged packages return cached results.

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
- **Schema evolution** differs by package: `s3sql` applies SQL-expression
  transforms (`ColumnAliases`, `ColumnDefaults`) at query time, good for
  renames and column defaults. `s3parquet` relies on parquet-go's native
  schema tolerance (missing fields decode as zero values; unknown fields
  are ignored) — no SQL rewrites. Type changes, splits, or row-level
  computed derivations still require a migration tool in both cases.

## License

See [LICENSE](LICENSE).
