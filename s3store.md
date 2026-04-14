# s3store – Complete Source

Append-only versioned data storage on S3 with stream semantics and embedded DuckDB queries.

No server. No broker. No coordinator. Just a Go library and S3.

## Architecture

```
Writer                               Consumer (Stream)
  store.Write(records)                 store.Poll(since, max)    → []StreamEntry
  store.WriteWithKey(key, records)     store.PollRecords(since, max) → []T (flat)
    │
    ├─ KeyFunc → group by key        Consumer (Snapshot)
    ├─ PUT data/{key}/{id}.parquet     store.Read(keyPattern)    → []T (deduplicated)
    └─ PUT _stream/refs/{ts}-{id}
       --{key}.ref (empty file)      Consumer (Analytics)
                                       store.Query(keyGlob, sql) → *sql.Rows

Write: parquet-go
All reads: DuckDB (schema evolution, dedup, globs – one engine)
```

## S3 Layout

```
s3://warehouse/billing/
  data/
    charge_period=2026-03-17/
      customer=abc/
        a3f2e1b4.parquet
        c7d9f0e2.parquet            ← recalculation
      customer=def/
        b8e3a4c6.parquet
  _stream/
    refs/
      1710684000000000-a3f2e1b4--charge_period=2026-03-17--customer=abc.ref
      1710684001000000-b8e3a4c6--charge_period=2026-03-17--customer=def.ref
      1710770400000000-c7d9f0e2--charge_period=2026-03-17--customer=abc.ref
```

## Settle Window

```
Refs on S3 (chronological):

... 999 1000 1001 1002 1003 1004 1005 1006 1007
                                     ↑
                                now - 5s
                                ──────→ don't read yet
←───────────────────────────→
safe zone: all PUTs complete
```

## Usage Example

```go
type CostRecord struct {
    CustomerID   string    `parquet:"customer_id"`
    ChargePeriod string    `parquet:"charge_period"`
    SKU          string    `parquet:"sku"`
    Quantity     float64   `parquet:"quantity"`
    UnitPrice    float64   `parquet:"unit_price"`
    NetCost      float64   `parquet:"net_cost"`
    Currency     string    `parquet:"currency"`
    CalculatedAt time.Time `parquet:"calculated_at,timestamp(millisecond)"`
}

store, _ := s3store.New[CostRecord](s3store.Config[CostRecord]{
    Bucket:        "warehouse",
    Prefix:        "billing",
    S3Client:      s3Client,
    KeyParts:      []string{"charge_period", "customer"},
    VersionColumn: "calculated_at",
    TableAlias:    "costs",
    SettleWindow:  5 * time.Second,
    ColumnDefaults: map[string]string{
        "currency": "'EUR'",
    },
    ColumnAliases: map[string][]string{
        "cost_per_unit": {"price_per_unit", "unit_price"},
    },
    KeyFunc: func(r CostRecord) string {
        return fmt.Sprintf("charge_period=%s/customer=%s",
            r.ChargePeriod, r.CustomerID)
    },
    ScanFunc: func(rows *sql.Rows) (CostRecord, error) {
        var r CostRecord
        err := rows.Scan(
            &r.CustomerID, &r.ChargePeriod, &r.SKU,
            &r.Quantity, &r.UnitPrice, &r.NetCost,
            &r.Currency, &r.CalculatedAt)
        return r, err
    },
})
defer store.Close()

// Write
store.Write(ctx, allCostRecords)

// Stream – refs only
entries, offset, _ := store.Poll(ctx, lastOffset, 100)

// Stream – flat records with schema evolution
records, offset, _ := store.PollRecords(ctx, lastOffset, 100)

// Read – deduplicated snapshot, any glob pattern
records, _ := store.Read(ctx, "charge_period=2026-03-17/customer=abc")
records, _ = store.Read(ctx, "charge_period=2026-03-*/customer=abc")

// Query – SQL
rows, _ := store.Query(ctx, "charge_period=2026-03-*/*",
    "SELECT customer, sku, SUM(net_cost) FROM costs "+
        "GROUP BY customer, sku")
```

---

## go.mod

```go
module github.com/your-org/s3store

go 1.22

require (
    github.com/aws/aws-sdk-go-v2 v1.30.0
    github.com/aws/aws-sdk-go-v2/service/s3 v1.58.0
    github.com/google/uuid v1.6.0
    github.com/marcboeker/go-duckdb v1.7.0
    github.com/parquet-go/parquet-go v0.23.0
)
```

---

## doc.go

```go
// Package s3store provides append-only, versioned data storage on
// S3 with Parquet data files, a change stream, and embedded DuckDB
// queries.
//
// No server, no broker, no coordinator. S3 is the only dependency.
//
// Write uses parquet-go for encoding. All reads use DuckDB,
// providing consistent schema evolution (union_by_name,
// ColumnAliases, ColumnDefaults) across all access patterns.
//
// Access patterns:
//
//   - Write / WriteWithKey: append Parquet + stream ref
//   - Poll: stream of refs (which keys changed)
//   - PollRecords: flat stream of typed records
//   - Read: typed, deduplicated snapshot with glob support
//   - Query: DuckDB SQL with auto-dedup and schema evolution
//
// Poll = lightweight refs. PollRecords/Read = typed []T via
// ScanFunc. Query = *sql.Rows for aggregations and complex SQL.
package s3store
```

---

## types.go

```go
package s3store

import (
    "database/sql"
    "time"

    "github.com/aws/aws-sdk-go-v2/service/s3"
)

// Config defines how a Store is set up. T is the record type.
type Config[T any] struct {
    // S3 bucket name.
    Bucket string

    // Prefix under which data files are stored.
    Prefix string

    // KeyParts defines the Hive-partition key segments in order.
    KeyParts []string

    // KeyFunc extracts the Hive-partition key from a record.
    // Used by Write() to group records.
    KeyFunc func(T) string

    // ScanFunc maps a sql.Rows row to a record.
    // Used by Read() and PollRecords() to return typed []T.
    // Column order must match SELECT * with ColumnAliases and
    // ColumnDefaults applied.
    ScanFunc func(*sql.Rows) (T, error)

    // VersionColumn is the column name used for deduplication.
    // Leave empty to disable.
    VersionColumn string

    // DeduplicateBy defines the columns that identify a unique
    // record. If empty, partitions by all KeyParts.
    DeduplicateBy []string

    // TableAlias is the name used in SQL queries.
    TableAlias string

    // SettleWindow is how far behind the stream tip the consumer
    // reads. Default: 5s.
    SettleWindow time.Duration

    // ColumnDefaults maps column names to default SQL expressions
    // for files that predate the column.
    ColumnDefaults map[string]string

    // ColumnAliases maps new column names to a chain of old names.
    // Generates COALESCE(new, old1, old2, ...).
    ColumnAliases map[string][]string

    // S3Client is the AWS S3 client to use.
    S3Client *s3.Client

    // S3Endpoint overrides the S3 endpoint.
    S3Endpoint string
}

func (c Config[T]) settleWindow() time.Duration {
    if c.SettleWindow > 0 {
        return c.SettleWindow
    }
    return 5 * time.Second
}

// Offset represents a position in the stream.
type Offset string

// StreamEntry is a lightweight ref.
type StreamEntry struct {
    Offset   Offset
    Key      string
    DataPath string
}

// QueryOption configures query behavior.
type QueryOption func(*queryOpts)

type queryOpts struct {
    includeHistory bool
}

// WithHistory disables deduplication.
func WithHistory() QueryOption {
    return func(o *queryOpts) {
        o.includeHistory = true
    }
}

// WriteResult contains metadata about a completed write.
type WriteResult struct {
    Offset   Offset
    DataPath string
    RefPath  string
}

func duckDBInitSQL(endpoint string) []string {
    stmts := []string{
        "INSTALL httpfs",
        "LOAD httpfs",
        "SET s3_url_style='path'",
    }
    if endpoint != "" {
        stmts = append(stmts,
            "SET s3_endpoint='"+endpoint+"'")
    }
    return stmts
}

func openDuckDB(endpoint string) (*sql.DB, error) {
    db, err := sql.Open("duckdb", "")
    if err != nil {
        return nil, err
    }
    for _, stmt := range duckDBInitSQL(endpoint) {
        if _, err := db.Exec(stmt); err != nil {
            db.Close()
            return nil, err
        }
    }
    return db, nil
}
```

---

## store.go

```go
package s3store

import (
    "database/sql"
    "fmt"
    "strings"

    "github.com/aws/aws-sdk-go-v2/service/s3"
)

// Store provides append-only storage on S3 with Parquet data
// files, a change stream, and embedded DuckDB for all reads.
type Store[T any] struct {
    cfg      Config[T]
    s3       *s3.Client
    db       *sql.DB
    dataPath string
    refPath  string
}

func New[T any](cfg Config[T]) (*Store[T], error) {
    if cfg.Bucket == "" {
        return nil, fmt.Errorf("s3store: Bucket is required")
    }
    if cfg.Prefix == "" {
        return nil, fmt.Errorf("s3store: Prefix is required")
    }
    if cfg.TableAlias == "" {
        return nil, fmt.Errorf(
            "s3store: TableAlias is required")
    }
    if cfg.S3Client == nil {
        return nil, fmt.Errorf(
            "s3store: S3Client is required")
    }
    if len(cfg.KeyParts) == 0 {
        return nil, fmt.Errorf(
            "s3store: KeyParts is required")
    }

    db, err := openDuckDB(cfg.S3Endpoint)
    if err != nil {
        return nil, fmt.Errorf(
            "s3store: failed to open DuckDB: %w", err)
    }

    return &Store[T]{
        cfg:      cfg,
        s3:       cfg.S3Client,
        db:       db,
        dataPath: cfg.Prefix + "/data",
        refPath:  cfg.Prefix + "/_stream/refs",
    }, nil
}

func (s *Store[T]) Close() error {
    return s.db.Close()
}

func (s *Store[T]) s3URI(key string) string {
    return fmt.Sprintf("s3://%s/%s", s.cfg.Bucket, key)
}

func (s *Store[T]) buildParquetURI(keyPattern string) string {
    if keyPattern == "*" || keyPattern == "" {
        return s.s3URI(s.dataPath + "/**/*.parquet")
    }
    parts := strings.Split(keyPattern, "/")
    for i, p := range parts {
        if p == "*" {
            parts[i] = "**"
        }
    }
    return s.s3URI(
        s.dataPath + "/" + strings.Join(parts, "/") +
            "/*.parquet")
}

func (s *Store[T]) dedupColumns() []string {
    if len(s.cfg.DeduplicateBy) > 0 {
        return s.cfg.DeduplicateBy
    }
    return s.cfg.KeyParts
}

func (s *Store[T]) encodeRefKey(
    tsMicros int64, shortID string, key string,
) string {
    encodedKey := strings.ReplaceAll(key, "/", "--")
    return fmt.Sprintf("%s/%d-%s--%s.ref",
        s.refPath, tsMicros, shortID, encodedKey)
}

func (s *Store[T]) parseRefKey(refKey string) (
    key string, shortID string, err error,
) {
    name := refKey
    if idx := strings.LastIndex(name, "/"); idx >= 0 {
        name = name[idx+1:]
    }
    name = strings.TrimSuffix(name, ".ref")

    parts := strings.SplitN(name, "--", 2)
    if len(parts) != 2 {
        return "", "", fmt.Errorf(
            "s3store: invalid ref key: %s", refKey)
    }

    tsAndID := parts[0]
    dashIdx := strings.Index(tsAndID, "-")
    if dashIdx < 0 {
        return "", "", fmt.Errorf(
            "s3store: invalid ref key: %s", refKey)
    }
    shortID = tsAndID[dashIdx+1:]
    key = strings.ReplaceAll(parts[1], "--", "/")
    return key, shortID, nil
}

func (s *Store[T]) buildDataPath(
    key string, shortID string,
) string {
    return fmt.Sprintf("%s/%s/%s.parquet",
        s.dataPath, key, shortID)
}

// scanAll reads all rows from a DuckDB result set into typed
// records via ScanFunc.
func (s *Store[T]) scanAll(rows *sql.Rows) ([]T, error) {
    if s.cfg.ScanFunc == nil {
        return nil, fmt.Errorf(
            "s3store: ScanFunc is required")
    }
    var records []T
    for rows.Next() {
        r, err := s.cfg.ScanFunc(rows)
        if err != nil {
            return nil, fmt.Errorf(
                "s3store: scan row: %w", err)
        }
        records = append(records, r)
    }
    if err := rows.Err(); err != nil {
        return nil, fmt.Errorf(
            "s3store: iterate rows: %w", err)
    }
    return records, nil
}
```

---

## write.go

```go
package s3store

import (
    "bytes"
    "context"
    "fmt"
    "strings"
    "time"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/service/s3"
    "github.com/google/uuid"
    "github.com/parquet-go/parquet-go"
)

// Write extracts the key from each record via KeyFunc, groups
// by key, and writes one Parquet file + stream ref per key.
func (s *Store[T]) Write(
    ctx context.Context, records []T,
) ([]WriteResult, error) {
    if len(records) == 0 {
        return nil, fmt.Errorf(
            "s3store: records must not be empty")
    }
    if s.cfg.KeyFunc == nil {
        return nil, fmt.Errorf(
            "s3store: KeyFunc is required for Write; " +
                "use WriteWithKey for explicit keys")
    }

    grouped := s.groupByKey(records)

    var results []WriteResult
    for key, group := range grouped {
        result, err := s.WriteWithKey(ctx, key, group)
        if err != nil {
            return results, err
        }
        results = append(results, *result)
    }
    return results, nil
}

// WriteWithKey encodes records as Parquet, uploads to S3, and
// writes an empty ref file with all metadata in the key name.
// Ref timestamp is generated AFTER the data PUT completes.
func (s *Store[T]) WriteWithKey(
    ctx context.Context, key string, records []T,
) (*WriteResult, error) {
    if len(records) == 0 {
        return nil, fmt.Errorf(
            "s3store: records must not be empty")
    }
    if err := s.validateKey(key); err != nil {
        return nil, err
    }

    parquetBytes, err := s.encodeParquet(records)
    if err != nil {
        return nil, fmt.Errorf(
            "s3store: parquet encode: %w", err)
    }

    shortID := uuid.New().String()[:8]

    dataKey := s.buildDataPath(key, shortID)
    if err := s.putObject(
        ctx, dataKey, parquetBytes,
        "application/octet-stream",
    ); err != nil {
        return nil, fmt.Errorf(
            "s3store: put data: %w", err)
    }

    tsMicros := time.Now().UnixMicro()

    refKey := s.encodeRefKey(tsMicros, shortID, key)
    if err := s.putObject(
        ctx, refKey, []byte{}, "application/octet-stream",
    ); err != nil {
        return nil, fmt.Errorf(
            "s3store: put ref: %w", err)
    }

    return &WriteResult{
        Offset:   Offset(refKey),
        DataPath: dataKey,
        RefPath:  refKey,
    }, nil
}

func (s *Store[T]) groupByKey(records []T) map[string][]T {
    grouped := make(map[string][]T)
    for _, r := range records {
        key := s.cfg.KeyFunc(r)
        grouped[key] = append(grouped[key], r)
    }
    return grouped
}

func (s *Store[T]) validateKey(key string) error {
    for _, part := range s.cfg.KeyParts {
        if !strings.Contains(key, part+"=") {
            return fmt.Errorf(
                "s3store: key %q missing part %q",
                key, part)
        }
    }
    return nil
}

func (s *Store[T]) encodeParquet(records []T) ([]byte, error) {
    var buf bytes.Buffer
    writer := parquet.NewGenericWriter[T](&buf)
    if _, err := writer.Write(records); err != nil {
        return nil, err
    }
    if err := writer.Close(); err != nil {
        return nil, err
    }
    return buf.Bytes(), nil
}

func (s *Store[T]) putObject(
    ctx context.Context,
    key string,
    data []byte,
    contentType string,
) error {
    _, err := s.s3.PutObject(ctx, &s3.PutObjectInput{
        Bucket:      aws.String(s.cfg.Bucket),
        Key:         aws.String(key),
        Body:        bytes.NewReader(data),
        ContentType: aws.String(contentType),
    })
    return err
}
```

---

## poll.go

```go
package s3store

import (
    "context"
    "fmt"
    "strings"
    "time"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/service/s3"
)

// Poll returns up to maxEntries stream entries (refs only) after
// the given offset, up to now - settle_window.
// Single S3 LIST, no GETs.
func (s *Store[T]) Poll(
    ctx context.Context,
    since Offset,
    maxEntries int32,
) ([]StreamEntry, Offset, error) {
    cutoff := time.Now().Add(-s.cfg.settleWindow())
    cutoffPrefix := fmt.Sprintf("%s/%d",
        s.refPath, cutoff.UnixMicro())

    input := &s3.ListObjectsV2Input{
        Bucket:  aws.String(s.cfg.Bucket),
        Prefix:  aws.String(s.refPath + "/"),
        MaxKeys: aws.Int32(maxEntries),
    }
    if since != "" {
        input.StartAfter = aws.String(string(since))
    }

    var entries []StreamEntry
    var lastKey string

    paginator := s3.NewListObjectsV2Paginator(s.s3, input)
outer:
    for paginator.HasMorePages() {
        page, err := paginator.NextPage(ctx)
        if err != nil {
            return nil, since,
                fmt.Errorf("s3store: list refs: %w", err)
        }
        for _, obj := range page.Contents {
            objKey := aws.ToString(obj.Key)
            if objKey > cutoffPrefix {
                break outer
            }
            key, shortID, err := s.parseRefKey(objKey)
            if err != nil {
                return nil, since, err
            }
            entries = append(entries, StreamEntry{
                Offset:   Offset(objKey),
                Key:      key,
                DataPath: s.buildDataPath(key, shortID),
            })
            lastKey = objKey
        }
    }

    if lastKey != "" {
        return entries, Offset(lastKey), nil
    }
    return nil, since, nil
}

// PollRecords returns a flat slice of typed records from all
// files referenced by up to maxEntries refs after the offset.
//
// Uses a single DuckDB query with union_by_name, so schema
// evolution (ColumnAliases, ColumnDefaults) is handled
// consistently even during catch-up across schema generations.
func (s *Store[T]) PollRecords(
    ctx context.Context,
    since Offset,
    maxEntries int32,
) ([]T, Offset, error) {
    entries, newOffset, err :=
        s.Poll(ctx, since, maxEntries)
    if err != nil {
        return nil, since, err
    }
    if len(entries) == 0 {
        return nil, since, nil
    }

    // Build file list for DuckDB
    uris := make([]string, len(entries))
    for i, e := range entries {
        uris[i] = "'" + s.s3URI(e.DataPath) + "'"
    }

    // One DuckDB query, all files
    scanExpr := fmt.Sprintf(
        "SELECT * FROM read_parquet([%s], "+
            "union_by_name=true)",
        strings.Join(uris, ", "))

    query := s.wrapScanExpr(scanExpr,
        "SELECT * FROM "+s.cfg.TableAlias, false)

    rows, err := s.db.QueryContext(ctx, query)
    if err != nil {
        return nil, since,
            fmt.Errorf("s3store: poll query: %w", err)
    }
    defer rows.Close()

    records, err := s.scanAll(rows)
    if err != nil {
        return nil, since, err
    }

    return records, newOffset, nil
}
```

---

## read.go

```go
package s3store

import (
    "context"
)

// Read returns the latest version of all records matching the
// key pattern. Uses DuckDB with union_by_name for schema
// evolution and QUALIFY for deduplication.
//
// Supports arbitrary glob patterns:
//   - "charge_period=2026-03-17/customer=abc"  → exact key
//   - "charge_period=2026-03-17/*"             → all customers
//   - "charge_period=2026-03-*/customer=abc"   → March, one customer
//   - "*"                                       → all data
func (s *Store[T]) Read(
    ctx context.Context, keyPattern string,
) ([]T, error) {
    rows, err := s.Query(ctx, keyPattern,
        "SELECT * FROM "+s.cfg.TableAlias)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    return s.scanAll(rows)
}
```

---

## query.go

```go
package s3store

import (
    "context"
    "database/sql"
    "fmt"
    "strings"
)

// Query executes a SQL query scoped to files matching the given
// key pattern. Supports arbitrary globs via DuckDB.
// Deduplicated by default. Use WithHistory() for all versions.
func (s *Store[T]) Query(
    ctx context.Context,
    key string,
    sqlQuery string,
    opts ...QueryOption,
) (*sql.Rows, error) {
    o := &queryOpts{}
    for _, opt := range opts {
        opt(o)
    }

    parquetURI := s.buildParquetURI(key)
    scanExpr := fmt.Sprintf(
        "SELECT * FROM read_parquet('%s', "+
            "hive_partitioning=true, union_by_name=true)",
        parquetURI)

    wrapped := s.wrapScanExpr(scanExpr, sqlQuery,
        o.includeHistory)
    return s.db.QueryContext(ctx, wrapped)
}

// QueryRow executes a query returning at most one row.
func (s *Store[T]) QueryRow(
    ctx context.Context,
    key string,
    sqlQuery string,
    opts ...QueryOption,
) *sql.Row {
    o := &queryOpts{}
    for _, opt := range opts {
        opt(o)
    }

    parquetURI := s.buildParquetURI(key)
    scanExpr := fmt.Sprintf(
        "SELECT * FROM read_parquet('%s', "+
            "hive_partitioning=true, union_by_name=true)",
        parquetURI)

    wrapped := s.wrapScanExpr(scanExpr, sqlQuery,
        o.includeHistory)
    return s.db.QueryRowContext(ctx, wrapped)
}

// wrapScanExpr wraps a base scan expression with column
// transforms, deduplication CTE, and the user's SQL query.
// Shared by Query, Read, and PollRecords.
func (s *Store[T]) wrapScanExpr(
    scanExpr string,
    userSQL string,
    includeHistory bool,
) string {
    selectExprs := s.buildColumnTransforms()

    var rawCTE string
    if len(selectExprs) > 0 {
        rawCTE = fmt.Sprintf("_raw AS (%s),\n", scanExpr)
        scanExpr = fmt.Sprintf(
            "SELECT *, %s FROM _raw",
            strings.Join(selectExprs, ", "))
    }

    var sb strings.Builder
    sb.WriteString("WITH ")

    if rawCTE != "" {
        sb.WriteString(rawCTE)
    }

    if !includeHistory && s.cfg.VersionColumn != "" {
        dedupCols := strings.Join(s.dedupColumns(), ", ")
        sb.WriteString(fmt.Sprintf(
            "%s AS (\n  %s\n  QUALIFY ROW_NUMBER() OVER "+
                "(PARTITION BY %s ORDER BY %s DESC"+
                ") = 1\n)\n",
            s.cfg.TableAlias, scanExpr,
            dedupCols, s.cfg.VersionColumn))
    } else {
        sb.WriteString(fmt.Sprintf(
            "%s AS (\n  %s\n)\n",
            s.cfg.TableAlias, scanExpr))
    }

    sb.WriteString(userSQL)
    return sb.String()
}

func (s *Store[T]) buildColumnTransforms() []string {
    var exprs []string

    for newName, oldNames := range s.cfg.ColumnAliases {
        args := append([]string{newName}, oldNames...)
        exprs = append(exprs, fmt.Sprintf(
            "COALESCE(%s) AS %s",
            strings.Join(args, ", "), newName))
    }

    for col, defaultVal := range s.cfg.ColumnDefaults {
        if _, isAlias := s.cfg.ColumnAliases[col]; isAlias {
            continue
        }
        exprs = append(exprs, fmt.Sprintf(
            "COALESCE(%s, %s) AS %s",
            col, defaultVal, col))
    }

    return exprs
}
```

---

## Domain-Specific Wrapper Example

```go
package billing

import (
    "context"
    "database/sql"
    "fmt"
    "time"

    "github.com/aws/aws-sdk-go-v2/service/s3"
    "github.com/your-org/s3store"
)

type CostRecord struct {
    CustomerID   string    `parquet:"customer_id"`
    ChargePeriod string    `parquet:"charge_period"`
    SKU          string    `parquet:"sku"`
    Quantity     float64   `parquet:"quantity"`
    UnitPrice    float64   `parquet:"unit_price"`
    NetCost      float64   `parquet:"net_cost"`
    Currency     string    `parquet:"currency"`
    CalculatedAt time.Time `parquet:"calculated_at,timestamp(millisecond)"`
}

func scanCost(rows *sql.Rows) (CostRecord, error) {
    var r CostRecord
    err := rows.Scan(
        &r.CustomerID, &r.ChargePeriod, &r.SKU,
        &r.Quantity, &r.UnitPrice, &r.NetCost,
        &r.Currency, &r.CalculatedAt)
    return r, err
}

type CostStore struct {
    store *s3store.Store[CostRecord]
}

func NewCostStore(s3Client *s3.Client) (*CostStore, error) {
    s, err := s3store.New[CostRecord](
        s3store.Config[CostRecord]{
            Bucket:        "warehouse",
            Prefix:        "billing",
            S3Client:      s3Client,
            KeyParts:      []string{"charge_period", "customer"},
            VersionColumn: "calculated_at",
            TableAlias:    "costs",
            SettleWindow:  5 * time.Second,
            ColumnDefaults: map[string]string{
                "currency": "'EUR'",
            },
            KeyFunc: func(r CostRecord) string {
                return fmt.Sprintf(
                    "charge_period=%s/customer=%s",
                    r.ChargePeriod, r.CustomerID)
            },
            ScanFunc: scanCost,
        })
    if err != nil {
        return nil, err
    }
    return &CostStore{store: s}, nil
}

func (c *CostStore) Close() error {
    return c.store.Close()
}

func (c *CostStore) Write(
    ctx context.Context, records []CostRecord,
) error {
    _, err := c.store.Write(ctx, records)
    return err
}

func (c *CostStore) Poll(
    ctx context.Context, since s3store.Offset, max int32,
) ([]s3store.StreamEntry, s3store.Offset, error) {
    return c.store.Poll(ctx, since, max)
}

func (c *CostStore) PollRecords(
    ctx context.Context, since s3store.Offset, max int32,
) ([]CostRecord, s3store.Offset, error) {
    return c.store.PollRecords(ctx, since, max)
}

func (c *CostStore) ReadCustomerDay(
    ctx context.Context, period string, customer string,
) ([]CostRecord, error) {
    return c.store.Read(ctx, fmt.Sprintf(
        "charge_period=%s/customer=%s", period, customer))
}

func (c *CostStore) ReadDay(
    ctx context.Context, period string,
) ([]CostRecord, error) {
    return c.store.Read(ctx, fmt.Sprintf(
        "charge_period=%s/*", period))
}

func (c *CostStore) ReadCustomerMonth(
    ctx context.Context, month string, customer string,
) ([]CostRecord, error) {
    return c.store.Read(ctx, fmt.Sprintf(
        "charge_period=%s-*/customer=%s", month, customer))
}

func (c *CostStore) MonthlyAggregation(
    ctx context.Context, month string,
) (*sql.Rows, error) {
    return c.store.Query(ctx,
        fmt.Sprintf("charge_period=%s-*/*", month),
        "SELECT customer, sku, SUM(net_cost) AS total "+
            "FROM costs GROUP BY customer, sku")
}

func (c *CostStore) DailyTotal(
    ctx context.Context, period string,
) (*sql.Rows, error) {
    return c.store.Query(ctx,
        fmt.Sprintf("charge_period=%s/*", period),
        "SELECT SUM(net_cost) AS total FROM costs")
}
```

---

## Design Decisions

- **One read engine**: All reads via DuckDB. Schema evolution
  (ColumnAliases, ColumnDefaults, union_by_name) works
  consistently across Poll, PollRecords, Read, and Query.
  parquet-go only for Write (encoding).
- **ScanFunc**: One function per type to map DuckDB rows to
  Go structs. Explicit, type-safe, no reflection.
- **Append-only**: Files never overwritten or deleted.
- **No coordinator**: S3 PUTs on unique keys.
- **Refs are empty files**: All metadata in key name.
  Poll is a single LIST, no GETs.
- **Settle window**: No duplicates, no seen set. One offset.
- **Timestamp after data PUT**: Settle window covers only
  ref PUT latency.
- **Pagination via MaxKeys**: No rollup needed.
- **ColumnAliases chains**: Supports multiple renames over
  time via COALESCE(new, old1, old2, ...).
- **PollRecords**: One DuckDB query for entire batch. DuckDB
  parallelizes file loading internally.
- **Read**: Delegates to Query internally. DuckDB handles
  glob matching, partition pruning, schema evolution, dedup.

## Limitations

- Stream latency = poll interval + settle window.
- DuckDB is single-process.
- S3 key limit 1024 bytes.
- ScanFunc column order must match the DuckDB SELECT output.