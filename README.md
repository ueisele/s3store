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

Requires Go 1.26.2+ (declared in [go.mod](go.mod); the code also depends on
iterator-returning `maps.Keys` which is Go 1.23+, so that's the absolute
minimum if you override the toolchain). DuckDB's httpfs extension is
auto-installed on first use, or pre-installed in air-gapped environments.

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
    fmt.Println(e.Offset, e.Key, e.DataPath)
}
// Pass newOffset back on the next call to continue where you left off
```

Each `StreamEntry` carries the ref's `Offset` (opaque cursor, pass back as
`since` on the next call), the `Key` (partition key as written), and the
`DataPath` (S3 key of the parquet file). No GETs are issued — the entire
batch is one S3 LIST call over `_stream/refs/`. Use this when you want to
react to *which* keys changed without fetching the data itself.

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
// Multi-row query
rows, err := store.Query(ctx, "charge_period=2026-03-*/*",
    "SELECT customer, sku, SUM(net_cost) AS total "+
        "FROM costs GROUP BY customer, sku")

// Single-row query — errors surface via the returned *sql.Row on Scan
var total float64
err = store.QueryRow(ctx, "charge_period=2026-03-17/*",
    "SELECT SUM(net_cost) FROM costs").Scan(&total)
```

Deduplicated by default. Pass `s3store.WithHistory()` to see all versions.
`QueryRow` is the `database/sql` convention for queries that should return
at most one row — construction-time errors (invalid key pattern, column
introspection failure) surface through the returned `*sql.Row` at `Scan`
time, matching stdlib behavior.

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
`QueryRow`, `PollRecords`). s3store introspects the scanned files' schemas
and picks the right SQL shape:

- **Target column already exists in some files** —
  `SELECT * EXCLUDE (old1, old2) REPLACE (COALESCE(newName, old1, old2) AS newName) FROM _raw`.
  The new column keeps its original position; the old names are removed
  from the output so they don't linger next to the aliased column.
- **Target column does not exist in any file yet** —
  `SELECT * EXCLUDE (old1, old2), COALESCE(old1, old2) AS newName FROM _raw`.
  The alias is appended as a new column, the old names are still removed.
- **Nothing on either side of the alias exists** — `NULL AS newName` is
  appended, so the output schema is still well-formed.
- **`ColumnDefault` on a column that no file has** — the default is
  emitted as a constant-valued column (`'EUR' AS currency`).

So you can add a `ColumnAlias` or `ColumnDefault` before any file contains
the new column, and the first read that produces a file with the new name
will transparently switch from "appended" to "REPLACEd in place". The
column order visible to `ScanFunc` is always `_raw.*` minus EXCLUDEs, with
REPLACEs in place, plus additions at the end (in sorted-key order).

When two aliases absorb the same old column, the `EXCLUDE` list is
deduplicated automatically.

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
    // Required
    Bucket     string                     // S3 bucket name
    Prefix     string                     // prefix under which data lives
    KeyParts   []string                   // ordered Hive partition key names
    TableAlias string                     // name used in Query SQL
    S3Client   *s3.Client                 // AWS SDK v2 S3 client

    // Required for Write / PollRecords / Read (respectively)
    KeyFunc  func(T) string               // derive key from record (Write)
    ScanFunc func(*sql.Rows) (T, error)   // map DuckDB row to typed record

    // Deduplication
    VersionColumn string                  // column to ORDER BY for latest
    DeduplicateBy []string                // dedup keys (default: KeyParts)

    // Stream
    SettleWindow time.Duration            // default: 5s

    // Schema evolution (applied on all read paths)
    ColumnDefaults map[string]string      // SQL expr per missing column
    ColumnAliases  map[string][]string    // new name → old name chain

    // S3 endpoint / extensions
    S3Endpoint   string                   // host:port, no scheme; for MinIO
    ExtraInitSQL []string                 // DuckDB SET/LOAD/CREATE SECRET
                                          // statements run after the default
                                          // init — used for S3 credentials,
                                          // SSL toggles, extra extensions
}
```

`ExtraInitSQL` is how you pass S3 credentials and SSL settings through to
DuckDB's httpfs extension for non-default endpoints. For example, a MinIO
integration test uses:

```go
ExtraInitSQL: []string{
    "SET s3_use_ssl=false",
    "SET s3_access_key_id='minioadmin'",
    "SET s3_secret_access_key='minioadmin'",
    "SET s3_region='us-east-1'",
},
```

On real AWS, both the SDK and DuckDB's httpfs pick up credentials from the
default AWS provider chain (IAM role, env vars, `~/.aws/credentials`), so
`ExtraInitSQL` is typically empty.

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
