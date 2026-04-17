//go:build integration

package s3sql_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync/atomic"
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

var (
	fixture  *testutil.Fixture
	prefixCt atomic.Int64
)

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	ctx := context.Background()
	f, err := testutil.Start(ctx, "s3sql-it")
	if err != nil {
		fmt.Fprintf(os.Stderr, "fixture: %v\n", err)
		return 1
	}
	fixture = f
	defer func() {
		if err := fixture.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "fixture close: %v\n", err)
		}
	}()
	return m.Run()
}

func uniquePrefix(kind string) string {
	return fmt.Sprintf("it-%s-%d-%d", kind,
		time.Now().UnixNano(), prefixCt.Add(1))
}

// writerFor returns an s3parquet.Store sharing the same prefix
// as the companion s3sql.Store — needed because s3sql is
// read-only; every integration test writes via s3parquet first.
func writerFor[T any](
	t *testing.T, prefix string, partitionKeyOf func(T) string,
) *s3parquet.Store[T] {
	t.Helper()
	w, err := s3parquet.New[T](s3parquet.Config[T]{
		Bucket:         fixture.Bucket,
		Prefix:         prefix,
		S3Client:       fixture.S3Client,
		KeyParts:       []string{"period", "customer"},
		PartitionKeyOf: partitionKeyOf,
		SettleWindow:   10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("s3parquet.New: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w
}

// sqlOpts dials in the dedup and schema-evolution config a
// given test cares about.
type sqlOpts struct {
	versionColumn  string
	dedupBy        []string
	columnAliases  map[string][]string
	columnDefaults map[string]string
}

// newSQLStore constructs a s3sql.Store against a given prefix
// with the shared MinIO fixture's credentials wired in.
func newSQLStore(t *testing.T, prefix string, opts sqlOpts) *s3sql.Store[Rec] {
	t.Helper()
	s, err := s3sql.New[Rec](s3sql.Config[Rec]{
		Bucket:         fixture.Bucket,
		Prefix:         prefix,
		S3Client:       fixture.S3Client,
		KeyParts:       []string{"period", "customer"},
		ScanFunc:       scanRec,
		TableAlias:     "records",
		VersionColumn:  opts.versionColumn,
		DeduplicateBy:  opts.dedupBy,
		ColumnAliases:  opts.columnAliases,
		ColumnDefaults: opts.columnDefaults,
		SettleWindow:   10 * time.Millisecond,
		ExtraInitSQL:   fixture.DuckDBCredentials(),
	})
	if err != nil {
		t.Fatalf("s3sql.New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func partitionKeyOfRec(r Rec) string {
	return fmt.Sprintf("period=%s/customer=%s", r.Period, r.Customer)
}

// writeSome writes records and returns once they've settled
// enough that the next Poll will see them.
func writeSome[T any](
	t *testing.T, w *s3parquet.Store[T], records []T,
) {
	t.Helper()
	if _, err := w.Write(context.Background(), records); err != nil {
		t.Fatalf("Write: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
}

// TestRead_WithDedup exercises the SQL read path end-to-end,
// including DuckDB's QUALIFY-based dedup via VersionColumn.
func TestRead_WithDedup(t *testing.T) {
	prefix := uniquePrefix("read")
	w := writerFor[Rec](t, prefix, partitionKeyOfRec)
	s := newSQLStore(t, prefix, sqlOpts{versionColumn: "ts"})

	writeSome(t, w, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Amount: 10, Currency: "USD", Ts: time.UnixMilli(100)},
	})
	writeSome(t, w, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Amount: 99, Currency: "USD", Ts: time.UnixMilli(200)},
	})

	got, err := s.Read(context.Background(),
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
	prefix := uniquePrefix("read-hist")
	w := writerFor[Rec](t, prefix, partitionKeyOfRec)
	s := newSQLStore(t, prefix, sqlOpts{versionColumn: "ts"})

	for i := int64(0); i < 3; i++ {
		writeSome(t, w, []Rec{
			{Period: "2026-03-17", Customer: "abc", SKU: "s1",
				Amount: float64(i), Currency: "USD",
				Ts: time.UnixMilli((i + 1) * 100)},
		})
	}

	got, err := s.Read(context.Background(),
		"period=2026-03-17/customer=abc", s3sql.WithHistory())
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d records, want 3", len(got))
	}
}

// TestQuery covers the arbitrary-SQL path with an aggregation,
// the primary use case the umbrella forwards here.
func TestQuery(t *testing.T) {
	prefix := uniquePrefix("query")
	w := writerFor[Rec](t, prefix, partitionKeyOfRec)
	// Dedup at sku granularity so multiple SKUs per (period,
	// customer) are treated as distinct entities — the realistic
	// billing scenario for a SUM(amount) GROUP BY customer.
	s := newSQLStore(t, prefix, sqlOpts{
		versionColumn: "ts",
		dedupBy:       []string{"period", "customer", "sku"},
	})

	writeSome(t, w, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Amount: 10, Currency: "USD", Ts: time.UnixMilli(1)},
		{Period: "2026-03-17", Customer: "abc", SKU: "s2", Amount: 20, Currency: "USD", Ts: time.UnixMilli(2)},
		{Period: "2026-03-17", Customer: "def", SKU: "s1", Amount: 30, Currency: "USD", Ts: time.UnixMilli(3)},
	})

	ctx := context.Background()
	rows, err := s.Query(ctx, "*",
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
	prefix := uniquePrefix("queryrow")
	w := writerFor[Rec](t, prefix, partitionKeyOfRec)
	s := newSQLStore(t, prefix, sqlOpts{versionColumn: "ts"})

	writeSome(t, w, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Amount: 42, Currency: "USD", Ts: time.UnixMilli(1)},
	})

	ctx := context.Background()

	var total float64
	err := s.QueryRow(ctx,
		"period=2026-03-17/customer=abc",
		"SELECT SUM(amount) FROM records").Scan(&total)
	if err != nil {
		t.Fatalf("QueryRow: %v", err)
	}
	if total != 42 {
		t.Errorf("got total=%v, want 42", total)
	}

	// Invalid pattern — segment count mismatch. Error surfaces
	// through Scan, not through the QueryRow call itself.
	err = s.QueryRow(ctx, "period=X/customer=Y/extra=Z",
		"SELECT 1").Scan(&total)
	if err == nil {
		t.Error("expected error for invalid pattern, got nil")
	}
}

// TestPoll exercises s3sql's own Poll loop (not the
// s3parquet-forwarded version used by the umbrella). Entries
// must be chronological and the cursor must advance correctly.
func TestPoll(t *testing.T) {
	prefix := uniquePrefix("poll")
	w := writerFor[Rec](t, prefix, partitionKeyOfRec)
	s := newSQLStore(t, prefix, sqlOpts{versionColumn: "ts"})

	for i := 0; i < 3; i++ {
		writeSome(t, w, []Rec{
			{Period: "2026-03-17", Customer: fmt.Sprintf("c%d", i),
				SKU: "s1", Amount: float64(i),
				Currency: "USD", Ts: time.UnixMilli(int64(i) + 1)},
		})
	}

	ctx := context.Background()
	entries, newOffset, err := s.Poll(ctx, "", 100)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("got %d entries, want 3", len(entries))
	}

	empty, off2, err := s.Poll(ctx, newOffset, 100)
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
	prefix := uniquePrefix("pollrec")
	w := writerFor[Rec](t, prefix, partitionKeyOfRec)
	s := newSQLStore(t, prefix, sqlOpts{versionColumn: "ts"})

	writeSome(t, w, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Amount: 10, Currency: "USD", Ts: time.UnixMilli(100)},
	})
	writeSome(t, w, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Amount: 99, Currency: "USD", Ts: time.UnixMilli(200)},
	})

	ctx := context.Background()

	deduped, off, err := s.PollRecords(ctx, "", 100)
	if err != nil {
		t.Fatalf("PollRecords: %v", err)
	}
	if len(deduped) != 1 || deduped[0].Amount != 99 {
		t.Errorf("deduped: got %+v, want one record with Amount=99", deduped)
	}
	if string(off) == "" {
		t.Error("offset empty after non-empty PollRecords")
	}

	full, _, err := s.PollRecords(ctx, "", 100, s3sql.WithHistory())
	if err != nil {
		t.Fatalf("PollRecords history: %v", err)
	}
	if len(full) != 2 {
		t.Errorf("history: got %d, want 2", len(full))
	}
}

// TestColumnAliasesAndDefaults exercises schema-evolution
// transforms — the SQL-path feature s3parquet doesn't support.
// Old files have old_amount and no currency; the aliased
// column chains old_amount → amount, and currency gets a
// default value when missing.
func TestColumnAliasesAndDefaults(t *testing.T) {
	prefix := uniquePrefix("evo")

	// Write an old-shape file first.
	wOld := writerFor[RecOld](t, prefix, func(r RecOld) string {
		return fmt.Sprintf("period=%s/customer=%s", r.Period, r.Customer)
	})
	writeSome(t, wOld, []RecOld{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", OldAmount: 10, Ts: time.UnixMilli(100)},
	})

	// Now a new-shape file for the same key: amount (new name),
	// currency added.
	wNew := writerFor[Rec](t, prefix, partitionKeyOfRec)
	writeSome(t, wNew, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Amount: 99, Currency: "EUR", Ts: time.UnixMilli(200)},
	})

	s := newSQLStore(t, prefix, sqlOpts{
		versionColumn: "ts",
		columnAliases: map[string][]string{
			"amount": {"old_amount"},
		},
		columnDefaults: map[string]string{
			"currency": "'USD'",
		},
	})

	// Read uses scanRec, which depends on the full column order
	// coming out of union_by_name + transforms. Schema-evolution
	// tests want to verify just the alias/default columns without
	// getting tangled in column-order shifts, so use Query and
	// pull exactly the columns we care about.
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
	// First row came from the old file: alias chain must have
	// mapped old_amount=10 to amount, and the default must have
	// filled in currency.
	if got[0].amount != 10 {
		t.Errorf("[0] amount=%v, want 10 (alias from old_amount)",
			got[0].amount)
	}
	if got[0].currency != "USD" {
		t.Errorf("[0] currency=%q, want USD (default)",
			got[0].currency)
	}
	// Second row is from the new file — amount is native,
	// currency comes from the file itself (EUR, not the
	// default).
	if got[1].amount != 99 {
		t.Errorf("[1] amount=%v, want 99", got[1].amount)
	}
	if got[1].currency != "EUR" {
		t.Errorf("[1] currency=%q, want EUR (native value)",
			got[1].currency)
	}
}
