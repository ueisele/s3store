# s3store

Append-only, versioned data storage on S3 with a change stream and embedded
DuckDB queries. No server. No broker. No coordinator. Just a Go library and
an S3 bucket.

## What it does

- **Write** typed records as Parquet, grouped into Hive-style partitions.
- **Stream** changes via lightweight "ref" files — one empty S3 object per
  write, all metadata in the object key.
- **Read** point-in-time deduplicated snapshots with glob support.
- **Query** the whole store with DuckDB SQL, including schema evolution,
  column aliases, and defaults — in a single embedded process.

Reads are unified through one engine (DuckDB) so schema evolution works
consistently across every access pattern. Writes use `parquet-go` directly.

## Install

```
go get github.com/ueisele/s3store
```

Requires Go 1.22+ and the DuckDB httpfs extension (auto-installed on first
use, or pre-installed in air-gapped environments).

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
    Bucket:        "warehouse",
    Prefix:        "billing",
    S3Client:      s3Client,
    KeyParts:      []string{"charge_period", "customer"},
    VersionColumn: "calculated_at",
    TableAlias:    "costs",
    KeyFunc: func(r CostRecord) string {
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

// Write — groups records by KeyFunc, one Parquet file per group
_, err = store.Write(ctx, records)

// Snapshot read — deduplicated by VersionColumn
latest, err := store.Read(ctx, "charge_period=2026-03-17/customer=abc")

// Glob across partitions
march, err := store.Read(ctx, "charge_period=2026-03-*/customer=abc")

// SQL query — DuckDB handles the aggregation
rows, err := store.Query(ctx, "charge_period=2026-03-*/*",
    "SELECT customer, SUM(net_cost) FROM costs GROUP BY customer")
```

## S3 layout

```
s3://warehouse/billing/
  data/
    charge_period=2026-03-17/
      customer=abc/
        a3f2e1b4.parquet
        c7d9f0e2.parquet            ← recalculation
  _stream/
    refs/
      1710684000000000-a3f2e1b4;charge_period=2026-03-17%2Fcustomer=abc.ref
      1710770400000000-c7d9f0e2;charge_period=2026-03-17%2Fcustomer=abc.ref
```

- `data/` holds the actual Parquet files, partitioned Hive-style.
- `_stream/refs/` holds one **empty** file per write. The filename encodes
  the timestamp, a short UUID, and the partition key. `Poll` is a single
  S3 LIST over this prefix — no GETs.

## Access patterns

### Write

```go
// Groups records by KeyFunc, one PUT per group
results, err := store.Write(ctx, records)

// Or skip the grouping step and write a pre-grouped batch
result, err := store.WriteWithKey(ctx, "charge_period=X/customer=Y", recs)
```

Writes are atomic at the file level: if the ref PUT fails after the data
PUT succeeded, s3store best-effort deletes the orphan parquet (with a HEAD
check to detect lost-ack).

### Stream — refs only

```go
// Returns up to 100 stream entries past the given offset
entries, newOffset, err := store.Poll(ctx, lastOffset, 100)
for _, e := range entries {
    fmt.Println(e.Key, e.DataPath)
}
```

Returns only metadata (partition key and parquet path). Use when you want
to react to changes without fetching the data itself.

### Stream — typed records

```go
// Default: Kafka compacted-topic semantics — latest-per-key
// within each batch, deduped by VersionColumn
records, newOffset, err := store.PollRecords(ctx, lastOffset, 100)

// Full stream: every record in every referenced file
records, newOffset, err = store.PollRecords(ctx, lastOffset, 100,
    s3store.WithHistory())
```

Default is **compacted-topic semantics**: within each batch, only the
latest version of each key (by `VersionColumn`) is returned. Consumers that
apply each record as an upsert to a local store converge on the latest-per-
key view — the primary use case for materialized-view consumers like the
billing example above.

`WithHistory()` opts out of dedup and returns every record in every
referenced file. Use it for audit logs, or whenever you need to observe
superseded versions.

`WithHistory()` is the same option that works on `Query` and `QueryRow` —
every read API in s3store shares the "dedup by default, `WithHistory()` to
see all versions" convention. When `VersionColumn` is empty, dedup is a
no-op regardless.

Schema evolution (`ColumnAliases`, `ColumnDefaults`) applies to both modes.

### Snapshot

```go
records, err := store.Read(ctx, "charge_period=2026-03-17/customer=abc")
```

Returns the deduplicated latest version of every record matching the glob,
typed via `ScanFunc`. Glob syntax matches `KeyParts`:

| Pattern | Meaning |
|---|---|
| `"charge_period=X/customer=Y"` | exact |
| `"charge_period=X/*"` | all customers for period X |
| `"charge_period=2026-03-*/customer=abc"` | partial-value glob |
| `"*"` | all data |

Truncated patterns (fewer segments than `KeyParts`) are rejected with a
clear error.

### SQL query

```go
rows, err := store.Query(ctx, "charge_period=2026-03-*/*",
    "SELECT customer, sku, SUM(net_cost) AS total "+
        "FROM costs GROUP BY customer, sku")
```

Deduplicated by default. Pass `s3store.WithHistory()` to see all versions.

## Schema evolution

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

Both are applied at query time across all read paths (`Read`, `Query`,
`PollRecords`). s3store introspects the scanned files' schemas and emits
the right SQL shape:

- If the target column already exists in some files: `SELECT * REPLACE
  (COALESCE(newName, old1, old2) AS newName)` — fills gaps while
  preserving column position.
- If the target column doesn't exist in any file yet: emits it as a new
  column (from olds, or as a constant default, or as `NULL` if nothing
  is available).

So you can add a `ColumnAlias` or `ColumnDefault` before any file contains
the new column, and it just works.

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

## Configuration

```go
type Config[T any] struct {
    Bucket         string             // S3 bucket name
    Prefix         string             // prefix under which data lives
    KeyParts       []string           // ordered Hive partition key names
    KeyFunc        func(T) string     // how to derive the key from a record
    ScanFunc       func(*sql.Rows) (T, error) // how to map a DuckDB row to T
    VersionColumn  string             // column to order by for dedup
    DeduplicateBy  []string           // dedup columns (defaults to KeyParts)
    TableAlias     string             // name used in Query SQL
    SettleWindow   time.Duration      // default: 5s
    ColumnDefaults map[string]string  // SQL expressions for missing columns
    ColumnAliases  map[string][]string // new → old column chains
    S3Client       *s3.Client
    S3Endpoint     string             // override endpoint (MinIO, etc.)
}
```

## Testing

```
go test ./...                                # unit tests, no dependencies
go test -tags integration -timeout 5m ./...  # full round-trip vs. a MinIO
                                             # container via testcontainers
```

Integration tests require Docker and pull `minio/minio:latest` on first run.

## Limitations

- **Single-process reads.** DuckDB runs embedded in your Go process.
- **S3 key limit: 1024 bytes.** Long partition values reduce the budget.
- **Stream latency = poll interval + settle window.** Not real-time.
- **Upsert-only compacted mode.** There is no tombstone / key-delete
  mechanism — keys can only be updated, not removed.
- **Schema evolution is query-time.** Good for renames, new columns, and
  column defaults. Type changes, splits, or row-level computed derivations
  still require a migration tool.

## License

See [LICENSE](LICENSE).
