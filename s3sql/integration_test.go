//go:build integration

package s3sql_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/ueisele/s3store/internal/testutil"
	"github.com/ueisele/s3store/s3parquet"
	"github.com/ueisele/s3store/s3sql"
)

// Rec is the on-disk shape used by these tests. All fields are
// parquet-go-friendly; the struct tags drive both the write
// (via s3parquet) and the SQL column names DuckDB sees.
type Rec struct {
	Period   string    `parquet:"period"`
	Customer string    `parquet:"customer"`
	SKU      string    `parquet:"sku"`
	Amount   float64   `parquet:"amount"`
	Currency string    `parquet:"currency"`
	Ts       time.Time `parquet:"ts,timestamp(millisecond)"`
}

// RecOld is the same record without Amount / Currency — used
// to exercise schema evolution via ColumnAliases and
// ColumnDefaults.
type RecOld struct {
	Period    string    `parquet:"period"`
	Customer  string    `parquet:"customer"`
	SKU       string    `parquet:"sku"`
	OldAmount float64   `parquet:"old_amount"`
	Ts        time.Time `parquet:"ts,timestamp(millisecond)"`
}

func scanRec(rows *sql.Rows) (Rec, error) {
	var r Rec
	err := rows.Scan(
		&r.Period, &r.Customer, &r.SKU,
		&r.Amount, &r.Currency, &r.Ts)
	return r, err
}

func partitionKeyOfRec(r Rec) string {
	return fmt.Sprintf("period=%s/customer=%s", r.Period, r.Customer)
}

// sqlOpts dials in the dedup and schema-evolution config a
// given test cares about.
type sqlOpts struct {
	versionColumn  string
	dedupBy        []string
	columnAliases  map[string][]string
	columnDefaults map[string]string
}

// testFixture bundles the per-test MinIO bucket + a matching
// s3parquet writer + an s3sql reader pointed at the same
// Bucket/Prefix.
type testFixture struct {
	bucket string
	writer *s3parquet.Store[Rec]
	sql    *s3sql.Store[Rec]
}

// newFixture creates a fresh bucket on the shared MinIO,
// constructs a matching writer (s3parquet) and reader
// (s3sql) against it, and wires cleanup.
func newFixture(t *testing.T, opts sqlOpts) *testFixture {
	t.Helper()
	f := testutil.New(t)
	w, err := s3parquet.New[Rec](s3parquet.Config[Rec]{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		PartitionKeyOf:    partitionKeyOfRec,
		SettleWindow:      10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("s3parquet.New: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	s, err := s3sql.New[Rec](s3sql.Config[Rec]{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		ScanFunc:          scanRec,
		TableAlias:        "records",
		VersionColumn:     opts.versionColumn,
		DeduplicateBy:     opts.dedupBy,
		ColumnAliases:     opts.columnAliases,
		ColumnDefaults:    opts.columnDefaults,
		SettleWindow:      10 * time.Millisecond,
		ExtraInitSQL:      f.DuckDBCredentials(),
	})
	if err != nil {
		t.Fatalf("s3sql.New: %v", err)
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
	time.Sleep(30 * time.Millisecond)
}

// TestRead_WithDedup exercises the SQL read path end-to-end,
// including DuckDB's QUALIFY-based dedup via VersionColumn.
func TestRead_WithDedup(t *testing.T) {
	f := newFixture(t, sqlOpts{versionColumn: "ts"})

	f.writeSome(t, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Amount: 10, Currency: "USD", Ts: time.UnixMilli(100)},
	})
	f.writeSome(t, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Amount: 99, Currency: "USD", Ts: time.UnixMilli(200)},
	})

	got, err := f.sql.Read(context.Background(),
		"period=2026-03-17/customer=abc")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d records, want 1", len(got))
	}
	if got[0].Amount != 99 {
		t.Errorf("got Amount=%v, want 99 (latest Ts)", got[0].Amount)
	}
}

// TestRead_WithHistory verifies every version is returned when
// WithHistory disables dedup.
func TestRead_WithHistory(t *testing.T) {
	f := newFixture(t, sqlOpts{versionColumn: "ts"})

	for i := int64(0); i < 3; i++ {
		f.writeSome(t, []Rec{
			{Period: "2026-03-17", Customer: "abc", SKU: "s1",
				Amount: float64(i), Currency: "USD",
				Ts: time.UnixMilli((i + 1) * 100)},
		})
	}

	got, err := f.sql.Read(context.Background(),
		"period=2026-03-17/customer=abc", s3sql.WithHistory())
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d records, want 3", len(got))
	}
}

// TestQuery covers the arbitrary-SQL path with an aggregation.
func TestQuery(t *testing.T) {
	f := newFixture(t, sqlOpts{
		versionColumn: "ts",
		dedupBy:       []string{"period", "customer", "sku"},
	})

	f.writeSome(t, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Amount: 10, Currency: "USD", Ts: time.UnixMilli(1)},
		{Period: "2026-03-17", Customer: "abc", SKU: "s2", Amount: 20, Currency: "USD", Ts: time.UnixMilli(2)},
		{Period: "2026-03-17", Customer: "def", SKU: "s1", Amount: 30, Currency: "USD", Ts: time.UnixMilli(3)},
	})

	ctx := context.Background()
	rows, err := f.sql.Query(ctx, "*",
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

// TestQueryRow uses the single-row API and also exercises the
// errorRow path: an invalid key pattern must surface as an
// error at Scan time (database/sql convention).
func TestQueryRow(t *testing.T) {
	f := newFixture(t, sqlOpts{versionColumn: "ts"})

	f.writeSome(t, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Amount: 42, Currency: "USD", Ts: time.UnixMilli(1)},
	})

	ctx := context.Background()

	var total float64
	err := f.sql.QueryRow(ctx,
		"period=2026-03-17/customer=abc",
		"SELECT SUM(amount) FROM records").Scan(&total)
	if err != nil {
		t.Fatalf("QueryRow: %v", err)
	}
	if total != 42 {
		t.Errorf("got total=%v, want 42", total)
	}

	err = f.sql.QueryRow(ctx, "period=X/customer=Y/extra=Z",
		"SELECT 1").Scan(&total)
	if err == nil {
		t.Error("expected error for invalid pattern, got nil")
	}
}

// TestPoll exercises s3sql's own Poll loop (not the
// s3parquet-forwarded version used by the umbrella). Entries
// must be chronological and the cursor must advance correctly.
func TestPoll(t *testing.T) {
	f := newFixture(t, sqlOpts{versionColumn: "ts"})

	for i := 0; i < 3; i++ {
		f.writeSome(t, []Rec{
			{Period: "2026-03-17", Customer: fmt.Sprintf("c%d", i),
				SKU: "s1", Amount: float64(i),
				Currency: "USD", Ts: time.UnixMilli(int64(i) + 1)},
		})
	}

	ctx := context.Background()
	entries, newOffset, err := f.sql.Poll(ctx, "", 100)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("got %d entries, want 3", len(entries))
	}

	empty, off2, err := f.sql.Poll(ctx, newOffset, 100)
	if err != nil {
		t.Fatalf("Poll past offset: %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("got %d entries past offset, want 0", len(empty))
	}
	if off2 != newOffset {
		t.Errorf("offset drifted: %q -> %q", newOffset, off2)
	}
}

// TestPollRecords covers the DuckDB-powered PollRecords path,
// including dedup semantics against a multi-write key.
func TestPollRecords(t *testing.T) {
	f := newFixture(t, sqlOpts{versionColumn: "ts"})

	f.writeSome(t, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Amount: 10, Currency: "USD", Ts: time.UnixMilli(100)},
	})
	f.writeSome(t, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Amount: 99, Currency: "USD", Ts: time.UnixMilli(200)},
	})

	ctx := context.Background()

	deduped, off, err := f.sql.PollRecords(ctx, "", 100)
	if err != nil {
		t.Fatalf("PollRecords: %v", err)
	}
	if len(deduped) != 1 || deduped[0].Amount != 99 {
		t.Errorf("deduped: got %+v, want one record with Amount=99", deduped)
	}
	if string(off) == "" {
		t.Error("offset empty after non-empty PollRecords")
	}

	full, _, err := f.sql.PollRecords(ctx, "", 100, s3sql.WithHistory())
	if err != nil {
		t.Fatalf("PollRecords history: %v", err)
	}
	if len(full) != 2 {
		t.Errorf("history: got %d, want 2", len(full))
	}
}

// TestColumnAliasesAndDefaults exercises schema-evolution
// transforms — the SQL-path feature s3parquet doesn't support.
// Writes an old-shape file and a new-shape file under the same
// key and verifies the aliased column + default value surface
// correctly through the SQL read.
func TestColumnAliasesAndDefaults(t *testing.T) {
	testFix := testutil.New(t)

	// Old-shape writer + file.
	wOld, err := s3parquet.New[RecOld](s3parquet.Config[RecOld]{
		Bucket:            testFix.Bucket,
		Prefix:            "store",
		S3Client:          testFix.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		PartitionKeyOf: func(r RecOld) string {
			return fmt.Sprintf("period=%s/customer=%s", r.Period, r.Customer)
		},
		SettleWindow: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("s3parquet.New(RecOld): %v", err)
	}
	t.Cleanup(func() { _ = wOld.Close() })
	if _, err := wOld.Write(context.Background(), []RecOld{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", OldAmount: 10, Ts: time.UnixMilli(100)},
	}); err != nil {
		t.Fatalf("Write RecOld: %v", err)
	}
	time.Sleep(30 * time.Millisecond)

	// New-shape writer + file.
	wNew, err := s3parquet.New[Rec](s3parquet.Config[Rec]{
		Bucket:            testFix.Bucket,
		Prefix:            "store",
		S3Client:          testFix.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		PartitionKeyOf:    partitionKeyOfRec,
		SettleWindow:      10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("s3parquet.New(Rec): %v", err)
	}
	t.Cleanup(func() { _ = wNew.Close() })
	if _, err := wNew.Write(context.Background(), []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Amount: 99, Currency: "EUR", Ts: time.UnixMilli(200)},
	}); err != nil {
		t.Fatalf("Write Rec: %v", err)
	}
	time.Sleep(30 * time.Millisecond)

	// Reader with alias + default.
	s, err := s3sql.New[Rec](s3sql.Config[Rec]{
		Bucket:            testFix.Bucket,
		Prefix:            "store",
		S3Client:          testFix.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		ScanFunc:          scanRec,
		TableAlias:        "records",
		VersionColumn:     "ts",
		ColumnAliases: map[string][]string{
			"amount": {"old_amount"},
		},
		ColumnDefaults: map[string]string{
			"currency": "'USD'",
		},
		SettleWindow: 10 * time.Millisecond,
		ExtraInitSQL: testFix.DuckDBCredentials(),
	})
	if err != nil {
		t.Fatalf("s3sql.New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	// Use Query with explicit columns to avoid depending on
	// the exact column order produced by union_by_name +
	// transforms.
	ctx := context.Background()
	rows, err := s.Query(ctx,
		"period=2026-03-17/customer=abc",
		"SELECT amount, currency FROM records ORDER BY ts",
		s3sql.WithHistory())
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()

	type row struct {
		amount   float64
		currency string
	}
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.amount, &r.currency); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("got %d records, want 2", len(got))
	}
	if got[0].amount != 10 {
		t.Errorf("[0] amount=%v, want 10 (alias from old_amount)", got[0].amount)
	}
	if got[0].currency != "USD" {
		t.Errorf("[0] currency=%q, want USD (default)", got[0].currency)
	}
	if got[1].amount != 99 {
		t.Errorf("[1] amount=%v, want 99", got[1].amount)
	}
	if got[1].currency != "EUR" {
		t.Errorf("[1] currency=%q, want EUR (native)", got[1].currency)
	}
}
