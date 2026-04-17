# Refactor plan — split into cgo-free and cgo packages

## Goal

Let write-only and simple-read-only consumers build with `CGO_ENABLED=0`. DuckDB
stays opt-in for arbitrary SQL and richer schema-evolution transforms. Zero
behavior change for users of today's top-level `s3store.Store`.

## Module layout

```
github.com/ueisele/s3store/              ← umbrella (cgo), backward-compatible façade
  internal/core/                         ← pure Go, shared primitives
  s3parquet/                             ← pure Go: Write + Read + streaming
  s3sql/                                 ← cgo: DuckDB-powered queries
```

## Dependency graph

```
core ←── s3parquet
core ←── s3sql
       ↑
       s3store (umbrella: imports s3parquet + s3sql; inherits cgo from s3sql)
```

## Capability matrix

| Feature | `s3parquet` | `s3sql` | Umbrella |
|---|---|---|---|
| cgo required | **no** | yes | yes |
| `Write`, `WriteWithKey` | ✓ | — | ✓ |
| `Read` (typed batch by key pattern) | ✓ (parquet-go, in-memory dedup) | ✓ (DuckDB, schema-evolution transforms) | ✓ (→ `s3sql`, today's behavior) |
| `Poll` (refs only, S3 LIST) | ✓ | ✓ | ✓ (→ `s3parquet`) |
| `PollRecords` | ✓ (parquet-go) | ✓ (DuckDB) | ✓ (→ `s3sql`) |
| `Query`, `QueryRow` (arbitrary SQL) | — | ✓ | ✓ |
| `ColumnAliases`, `ColumnDefaults` | — | ✓ | ✓ |
| Schema evolution (add/remove fields on `T`) | ✓ (parquet-go native) | ✓ (`union_by_name`) | ✓ |
| Glob patterns | whole-segment `*` + trailing-prefix `*` | same (shared validator) | same |

## What lives where

### `internal/core` — pure Go, primitives only

**Types:**
- `Offset`, `StreamEntry`, `WriteResult`
- `QueryOption`, `WithHistory()`
- `BaseConfig` (helper container, not embedded in sub-configs)

**Pure functions (no behavior, just strings and types):**
- `ValidateKeyParts(parts []string) error`
- `ValidateKeyPattern(pattern string, keyParts []string) error` — shared glob grammar
- `RefPath(prefix)` / `DataPath(prefix)` — path constructors
- `EncodeRefKey(refPath, tsMicros, shortID, key)` / `ParseRefKey(refKey)`
- `BuildDataFilePath(dataPath, key, shortID)`
- `RefCutoff(refPath, now, settleWindow)` — settle-window upper-bound prefix

### `s3parquet` — pure Go, no cgo

**`Config[T]` (flat fields):**

```go
Bucket, Prefix string
KeyParts []string
S3Client *s3.Client
PartitionKeyOf func(T) string    // for Write (was KeyFunc)
SettleWindow time.Duration
EntityKeyOf func(T) string       // optional; enables dedup
VersionOf func(T) int64          // optional; enables dedup
```

**`Store[T]`, `New(cfg)`:**
- `Write(ctx, records) ([]WriteResult, error)`
- `WriteWithKey(ctx, key, records) (*WriteResult, error)`
- `Read(ctx, keyPattern) ([]T, error)` — LIST + parquet-go decode + optional in-memory dedup
- `Poll(ctx, since, max) ([]StreamEntry, Offset, error)` — owns its paginator loop
- `PollRecords(ctx, since, max, opts...) ([]T, Offset, error)` — parallel downloads (limit 8) + decode + optional dedup
- `Close() error` — no-op

**Limitations (documented):**
- No `ColumnAliases` / `ColumnDefaults` (SQL-expression features)
- Glob grammar: whole-segment `*` + trailing-prefix `*` per segment value (stricter than DuckDB's full glob)
- In-memory dedup — large key cardinality can OOM; escalate to `s3sql` for that

### `s3sql` — cgo, DuckDB

**`Config[T]` (flat fields):**

```go
Bucket, Prefix string
KeyParts []string
S3Client *s3.Client
ScanFunc func(*sql.Rows) (T, error)
TableAlias string
DeduplicateBy []string            // column names (SQL-shaped)
VersionColumn string              // column name (SQL-shaped)
ColumnDefaults map[string]string
ColumnAliases map[string][]string
SettleWindow time.Duration
ExtraInitSQL []string             // user escape hatch
```

**Dropped:** `S3Endpoint` — auto-derived from `S3Client.Options()` (endpoint,
region, path-style, use_ssl). `ExtraInitSQL` still available for overrides and
`CREATE SECRET`.

**`Store[T]`, `New(cfg)`:**
- `Read(ctx, keyPattern) ([]T, error)` — DuckDB + schema-evolution transforms
- `Query(ctx, keyPattern, sql, opts...) (*sql.Rows, error)`
- `QueryRow(ctx, keyPattern, sql, opts...) *sql.Row`
- `Poll(ctx, since, max) ([]StreamEntry, Offset, error)` — owns its own paginator loop using core primitives
- `PollRecords(ctx, since, max, opts...) ([]T, Offset, error)`
- `Close() error` — closes the DuckDB connection

**Internal:** `openDuckDB`, `ensureHTTPFS`, `deriveDuckDBS3Settings`,
`wrapScanExpr`, `buildColumnTransforms`, `introspectColumns`, `scanAll`,
`errorRow`.

### `s3store` (root) — umbrella, cgo

- **Re-exports:** `Offset`, `StreamEntry`, `WriteResult`, `QueryOption`,
  `WithHistory`.
- **`Config[T]`:** one flat struct containing every sub-config field (same
  shape as today, minus `S3Endpoint`, plus rename `KeyFunc` →
  `PartitionKeyOf`).
- **`Store[T]`:** composes `*s3parquet.Store[T]` and `*s3sql.Store[T]`.
  Forwards current method names:
  - `Write`, `WriteWithKey` → `s3parquet.Store`
  - `Poll` → `s3parquet.Store`
  - `Read`, `Query`, `QueryRow`, `PollRecords` → `s3sql.Store` (preserves
    today's DuckDB semantics)
- **`Close()`** calls both sub-store `Close()`s.

## Non-trivial Go types — user's responsibility

For types parquet-go can't encode (`decimal.Decimal`, custom wrappers): the
user defines a parquet-friendly `File` struct in their package, plus `toFile`
/ `fromFile` translation functions. Library sees only
`s3parquet.Store[UsageFile]` or `s3sql.Store[UsageFile]`. Documented with a
worked example in the README using a `Usage` struct.

## Shared glob grammar (minimal)

Defined and validated once in `core.ValidateKeyPattern`. Both `s3parquet` and
`s3sql` call it.

**Allowed:**
- `*` (matches everything)
- `period=2026-03-17/customer=abc` (exact)
- `period=2026-03-*/customer=abc` (trailing `*`)
- `period=2026-*/customer=*` (whole-segment `*`)

**Rejected** with a clear error pointing to the grammar:
- Leading/middle `*` (`period=*-17`, `period=2026-*-17`)
- Multiple `*` in a segment value
- Char classes `[abc]`, alternation `{a,b}`, `?`

## Breaking changes (pre-v1, applied outright)

- `Config.KeyFunc` → `Config.PartitionKeyOf`
- `Config.S3Endpoint` removed — auto-derived from `S3Client`
- Sub-packages introduced — `s3store.Store` preserves all current method
  names/signatures otherwise
- Glob grammar narrowed — char classes, alternation, `?`, leading/middle `*`
  now rejected

Listed in a migration note at the top of the README.

## Design decisions (locked)

- **No `MappingStore`** — user translates domain types to/from parquet-layout
  types in their own package.
- **`EntityKeyOf` + `VersionOf`** in `s3parquet` for in-memory dedup;
  **`DeduplicateBy` + `VersionColumn`** in `s3sql` for SQL dedup.
- **Flat config** — no embedded `BaseConfig`.
- **Same `Read` name** in both packages — import path disambiguates; umbrella
  forwards to `s3sql.Read`.
- **`Poll` owned by each store** (not in `core`); `core` provides primitives.
- **`internal/core`** — starts internal, can be promoted later.
- **Parallelism** in `PollRecords`: fixed at 8 downloads.

## Implementation steps

1. **`internal/core`** — types + primitives (`ValidateKeyParts`,
   `ValidateKeyPattern`, path builders, ref parsers, cutoff formatter). Root
   package wired to use them. Tests stay green.
2. **`s3parquet`** — bundle write + read. Move write path from root. Build new
   pure-Go read (parquet-go + in-memory dedup + minimal glob matcher). Own
   `Poll` loop. Unit tests with S3 mocks, runnable under `CGO_ENABLED=0`.
3. **`s3sql`** — move all DuckDB code + `Read`, `Query`, `QueryRow`,
   `PollRecords`. Own `Poll` loop (re-using core primitives). Add
   `deriveDuckDBS3Settings` from `S3Client`.
4. **Umbrella cleanup** — root `Store[T]` composes the two sub-stores.
   Flatten `Config[T]` to include every sub-config field; map explicitly at
   `New()`. Backward-compat checked against existing tests.
5. **CGO-free build + lint check** — CI step `CGO_ENABLED=0 go build
   ./s3parquet/... ./internal/...` + lint step grepping for forbidden imports
   (`duckdb-go`) in those paths.
6. **Docs** — update `doc.go` + README. Three-package capability table.
   Worked `Usage`/`UsageFile` translation example. Glob grammar. Migration
   note. Per-package `doc.go` headers naming the cgo/no-cgo status.
7. **Integration test sweep** — confirm MinIO-based tests still pass via
   umbrella. Add one `CGO_ENABLED=0` unit test run in CI covering
   `s3parquet`.

Testing strategy: unit + S3 mocks inside each sub-package; MinIO-backed
integration tests remain at the umbrella level exercising the composed API.

Commits: one logical commit per step (or finer if a step is large). Not one
giant PR.
