//go:build integration

package s3sql_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ueisele/s3store/internal/testutil"
	"github.com/ueisele/s3store/s3parquet"
	"github.com/ueisele/s3store/s3sql"
)

// Rec is the on-disk shape used by these tests. Parquet tags
// drive the s3parquet write; s3sql Query only needs
// the column names — no row binding happens on the SQL side.
type Rec struct {
	Period   string    `parquet:"period"`
	Customer string    `parquet:"customer"`
	SKU      string    `parquet:"sku"`
	Amount   float64   `parquet:"amount"`
	Currency string    `parquet:"currency"`
	Ts       time.Time `parquet:"ts,timestamp(millisecond)"`
}

func partitionKeyOfRec(r Rec) string {
	return fmt.Sprintf("period=%s/customer=%s", r.Period, r.Customer)
}

// sqlOpts dials in the dedup config a given test cares about.
type sqlOpts struct {
	versionColumn    string
	entityKeyColumns []string
}

// testFixture bundles the per-test MinIO bucket + a matching
// s3parquet writer + an s3sql reader pointed at the same
// S3Target.
type testFixture struct {
	bucket string
	writer *s3parquet.Writer[Rec]
	sql    *s3sql.Reader
}

// newFixture creates a fresh bucket on the shared MinIO,
// constructs a matching Writer and Reader against a shared
// S3Target, and wires cleanup.
func newFixture(t *testing.T, opts sqlOpts) *testFixture {
	t.Helper()
	f := testutil.New(t)
	// MinIO is in fact strongly consistent; the strong-global
	// claim on the target keeps idempotent writes on the
	// conditional-PUT path and lets the scoped retry LIST
	// linearize against prior writes — and the umbrella reader
	// inherits the same level by sharing the target.
	target := s3parquet.NewS3Target(s3parquet.S3TargetConfig{
		Bucket:             f.Bucket,
		Prefix:             "store",
		S3Client:           f.S3Client,
		PartitionKeyParts:  []string{"period", "customer"},
		SettleWindow:       300 * time.Millisecond,
		ConsistencyControl: s3parquet.ConsistencyStrongGlobal,
	})
	w, err := s3parquet.NewWriter(s3parquet.WriterConfig[Rec]{
		Target:         target,
		PartitionKeyOf: partitionKeyOfRec,
	})
	if err != nil {
		t.Fatalf("s3parquet.NewWriter: %v", err)
	}

	s, err := s3sql.NewReader(s3sql.ReaderConfig{
		Target:           target,
		TableAlias:       "records",
		VersionColumn:    opts.versionColumn,
		EntityKeyColumns: opts.entityKeyColumns,
		ExtraInitSQL:     f.DuckDBCredentials(),
	})
	if err != nil {
		t.Fatalf("s3sql.NewReader: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	return &testFixture{bucket: f.Bucket, writer: w, sql: s}
}

// writeSome writes records through the parquet writer and
// sleeps long enough for them to fall past the SettleWindow.
func (f *testFixture) writeSome(t *testing.T, recs []Rec) {
	t.Helper()
	if _, err := f.writer.Write(context.Background(), recs); err != nil {
		t.Fatalf("Write: %v", err)
	}
	time.Sleep(400 * time.Millisecond)
}

// TestQuery exercises the SQL aggregation surface end-to-end
// with dedup enabled: latest-per-entity wins before the SUM.
func TestQuery(t *testing.T) {
	f := newFixture(t, sqlOpts{
		versionColumn:    "ts",
		entityKeyColumns: []string{"period", "customer", "sku"},
	})

	f.writeSome(t, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Amount: 10, Currency: "USD", Ts: time.UnixMilli(1)},
		{Period: "2026-03-17", Customer: "abc", SKU: "s2", Amount: 20, Currency: "USD", Ts: time.UnixMilli(2)},
		{Period: "2026-03-17", Customer: "def", SKU: "s1", Amount: 30, Currency: "USD", Ts: time.UnixMilli(3)},
	})

	ctx := context.Background()
	rows, err := f.sql.Query(ctx, []string{"*"},
		"SELECT customer, SUM(amount) AS total FROM records "+
			"GROUP BY customer ORDER BY customer")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()

	type agg struct {
		customer string
		total    float64
	}
	var got []agg
	for rows.Next() {
		var a agg
		if err := rows.Scan(&a.customer, &a.total); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		got = append(got, a)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	want := []agg{
		{customer: "abc", total: 30},
		{customer: "def", total: 30},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d: %+v", len(got), len(want), got)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("[%d] got %+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestQuery_AggregationAcrossPatterns proves Query runs ONE
// DuckDB query over the deduplicated union of patterns, so
// aggregations and joins span the full set rather than per-pattern.
func TestQuery_AggregationAcrossPatterns(t *testing.T) {
	f := newFixture(t, sqlOpts{})

	f.writeSome(t, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Amount: 10,
			Currency: "USD", Ts: time.UnixMilli(1)},
		{Period: "2026-03-17", Customer: "abc", SKU: "s2", Amount: 20,
			Currency: "USD", Ts: time.UnixMilli(2)},
		{Period: "2026-03-18", Customer: "def", SKU: "s3", Amount: 100,
			Currency: "USD", Ts: time.UnixMilli(3)},
		// Excluded from the query: period=2026-03-18/customer=abc.
		{Period: "2026-03-18", Customer: "abc", SKU: "s4", Amount: 999,
			Currency: "USD", Ts: time.UnixMilli(4)},
	})

	ctx := context.Background()
	rows, err := f.sql.Query(ctx, []string{
		"period=2026-03-17/customer=abc",
		"period=2026-03-18/customer=def",
	},
		"SELECT SUM(amount) AS total FROM records")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("Query: no rows")
	}
	var total float64
	if err := rows.Scan(&total); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	// 10 + 20 + 100 = 130. The off-diagonal 999 must NOT be
	// included.
	if total != 130 {
		t.Errorf("total = %v, want 130", total)
	}
}

// TestQuery_NoMatch covers the empty-cursor synthesis path:
// when no patterns match any files, the caller still gets a
// well-formed *sql.Rows that yields zero rows on Next().
func TestQuery_NoMatch(t *testing.T) {
	f := newFixture(t, sqlOpts{})

	ctx := context.Background()
	rows, err := f.sql.Query(ctx,
		[]string{"period=2099-01-01/customer=nobody"},
		"SELECT 1 FROM records")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Fatal("expected zero rows from empty match, got at least one")
	}
}

// newIdempotentFixture builds a fixture wired for Phase 3b
// idempotent-read tests: dedup on (period, customer, sku) so
// the at-most-one invariant is provable at the reader level.
func newIdempotentFixture(t *testing.T) *testFixture {
	t.Helper()
	f := testutil.New(t)
	target := s3parquet.NewS3Target(s3parquet.S3TargetConfig{
		Bucket:             f.Bucket,
		Prefix:             "store",
		S3Client:           f.S3Client,
		PartitionKeyParts:  []string{"period", "customer"},
		SettleWindow:       300 * time.Millisecond,
		ConsistencyControl: s3parquet.ConsistencyStrongGlobal,
	})
	w, err := s3parquet.NewWriter(s3parquet.WriterConfig[Rec]{
		Target:         target,
		PartitionKeyOf: partitionKeyOfRec,
	})
	if err != nil {
		t.Fatalf("s3parquet.NewWriter: %v", err)
	}

	s, err := s3sql.NewReader(s3sql.ReaderConfig{
		Target:           target,
		TableAlias:       "records",
		VersionColumn:    "ts",
		EntityKeyColumns: []string{"period", "customer", "sku"},
		ExtraInitSQL:     f.DuckDBCredentials(),
	})
	if err != nil {
		t.Fatalf("s3sql.NewReader: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return &testFixture{bucket: f.Bucket, writer: w, sql: s}
}

// TestIdempotentRead_Query drives the Go-side LIST + URI-list path
// that Query takes when the barrier is set: attempt-1's own file
// is excluded, so the count reflects only the baseline records.
func TestIdempotentRead_Query(t *testing.T) {
	ctx := context.Background()
	f := newIdempotentFixture(t)

	key := "period=2026-04-22/customer=alice"

	// Seed a baseline.
	if _, err := f.writer.WriteWithKey(ctx, key, []Rec{{
		Period: "2026-04-22", Customer: "alice",
		SKU: "baseline", Amount: 1, Ts: time.UnixMilli(1),
	}}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	time.Sleep(1100 * time.Millisecond)

	const token = "2026-04-22T10:15:00Z-query-rmw"

	// Attempt-1 writes with token.
	if _, err := f.writer.WriteWithKey(ctx, key, []Rec{{
		Period: "2026-04-22", Customer: "alice",
		SKU: "derived", Amount: 100, Ts: time.UnixMilli(10),
	}}, s3parquet.WithIdempotencyToken(token, time.Hour)); err != nil {
		t.Fatalf("attempt-1 write: %v", err)
	}
	time.Sleep(400 * time.Millisecond)

	// Query with the barrier: only the baseline survives.
	rows, err := f.sql.Query(ctx, []string{key},
		"SELECT COUNT(*) FROM records",
		s3sql.WithIdempotentRead(token))
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("Query: expected one count row")
	}
	var n int
	if err := rows.Scan(&n); err != nil {
		t.Fatalf("Query scan: %v", err)
	}
	if n != 1 {
		t.Errorf("Query with barrier got count=%d, want 1 (baseline only)", n)
	}
}

// TestIdempotentRead_QueryRejectsBadToken guards that an invalid
// barrier token surfaces from ValidateIdempotencyToken before any
// DuckDB work.
func TestIdempotentRead_QueryRejectsBadToken(t *testing.T) {
	ctx := context.Background()
	f := newIdempotentFixture(t)
	if _, err := f.sql.Query(ctx, []string{"period=2026-04-22/customer=*"},
		"SELECT 1 FROM records",
		s3sql.WithIdempotentRead("has/slash")); err == nil {
		t.Fatal("want error for barrier token with '/', got nil")
	}
}

// TestIdempotentRead_QueryZeroMatches guards that a barrier
// which filters every file away yields a clean empty iteration on
// Query — matching the unified contract that "zero matches" is an
// empty *sql.Rows, not an error. Callers using the standard
// for-rows.Next loop see no rows and no error.
func TestIdempotentRead_QueryZeroMatches(t *testing.T) {
	ctx := context.Background()
	f := newIdempotentFixture(t)

	key := "period=2026-04-22/customer=alice"
	const token = "zero-match-token"

	// Write a single file with the matching token, no other files
	// in the partition. The barrier picks up the token file as its
	// own and self-excludes it — zero files survive.
	if _, err := f.writer.WriteWithKey(ctx, key, []Rec{{
		Period: "2026-04-22", Customer: "alice",
		SKU: "only", Amount: 1, Ts: time.UnixMilli(1),
	}}, s3parquet.WithIdempotencyToken(token, time.Hour)); err != nil {
		t.Fatalf("write: %v", err)
	}
	time.Sleep(400 * time.Millisecond)

	rows, err := f.sql.Query(ctx, []string{key},
		"SELECT COUNT(*) FROM records",
		s3sql.WithIdempotentRead(token))
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Error("Query: expected zero rows, got at least one")
	}
	if err := rows.Err(); err != nil {
		t.Errorf("Query: rows.Err = %v, want nil", err)
	}
}
