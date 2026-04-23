//go:build integration

package s3store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ueisele/s3store/internal/testutil"
	"github.com/ueisele/s3store/s3parquet"
)

// IntRecord is the umbrella-test record shape. Parquet tags
// drive both the write path (s3parquet) and the SQL-side row
// binder (s3sql).
type IntRecord struct {
	Period   string    `parquet:"period"`
	Customer string    `parquet:"customer"`
	SKU      string    `parquet:"sku"`
	Amount   float64   `parquet:"amount"`
	Ts       time.Time `parquet:"ts,timestamp(millisecond)"`
}

// newStore builds an umbrella Store against a fresh bucket on
// the shared MinIO fixture. Exposes the knobs individual tests
// care about; everything else is the same realistic
// billing-ish configuration.
type storeOpts struct {
	settleWindow     time.Duration
	entityKeyColumns []string // dedup key; empty disables dedup
}

func newStore(t *testing.T, opts storeOpts) *Store[IntRecord] {
	t.Helper()
	f := testutil.New(t)

	if opts.settleWindow == 0 {
		opts.settleWindow = 100 * time.Millisecond
	}
	// VersionColumn is paired with EntityKeyColumns: both or
	// neither, matching New()'s validation.
	versionColumn := ""
	if len(opts.entityKeyColumns) > 0 {
		versionColumn = "ts"
	}
	store, err := New[IntRecord](context.Background(), Config[IntRecord]{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		TableAlias:        "records",
		VersionColumn:     versionColumn,
		EntityKeyColumns:  opts.entityKeyColumns,
		SettleWindow:      opts.settleWindow,
		// MinIO is in fact strongly consistent; claiming it lets the
		// ref PUT use the full SettleWindow as its budget (the tight
		// 10ms test setting doesn't tolerate the default half-budget).
		ConsistencyControl: s3parquet.ConsistencyStrongGlobal,
		PartitionKeyOf: func(r IntRecord) string {
			return fmt.Sprintf(
				"period=%s/customer=%s",
				r.Period, r.Customer)
		},
		ExtraInitSQL: f.DuckDBCredentials(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// TestUmbrella_WriteRead covers the most basic end-to-end path:
// Write via the umbrella writes through s3parquet; Read via the
// umbrella reads through s3sql. If the sub-store composition
// ever mis-wires either side, this is the test that fails first.
func TestUmbrella_WriteRead(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{})

	in := []IntRecord{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Amount: 10, Ts: time.UnixMilli(1000)},
		{Period: "2026-03-17", Customer: "abc", SKU: "s2", Amount: 20, Ts: time.UnixMilli(2000)},
		{Period: "2026-03-17", Customer: "def", SKU: "s1", Amount: 30, Ts: time.UnixMilli(3000)},
	}
	if _, err := store.Write(ctx, in); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// newStore was called without entityKeyColumns, so no dedup:
	// every written record comes back.
	got, err := store.Read(ctx, "*")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != len(in) {
		t.Fatalf("got %d records, want %d", len(got), len(in))
	}
}

// TestUmbrella_WritePoll verifies umbrella.Poll forwards to
// s3parquet.Poll: refs written through the umbrella show up in
// Poll's output, in chronological order, with reconstructed
// data paths.
func TestUmbrella_WritePoll(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{})

	keys := []string{
		"period=2026-03-17/customer=abc",
		"period=2026-03-17/customer=def",
		"period=2026-03-18/customer=abc",
	}
	for _, k := range keys {
		if _, err := store.WriteWithKey(ctx, k, []IntRecord{
			{Period: "p", Customer: "c", SKU: "s", Ts: time.Now()},
		}); err != nil {
			t.Fatalf("Write %s: %v", k, err)
		}
	}
	time.Sleep(200 * time.Millisecond)

	entries, newOffset, err := store.Poll(ctx, "", 100)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(entries) != len(keys) {
		t.Fatalf("got %d entries, want %d", len(entries), len(keys))
	}
	if string(newOffset) == "" {
		t.Error("newOffset empty after non-empty Poll")
	}
	for i, e := range entries {
		if e.Key != keys[i] {
			t.Errorf("[%d] Key=%q, want %q", i, e.Key, keys[i])
		}
		if e.DataPath == "" {
			t.Errorf("[%d] DataPath empty", i)
		}
	}
}

// TestUmbrella_WritePollRecords verifies umbrella.PollRecords
// forwards to s3sql.PollRecords (DuckDB-powered dedup), and that
// WithHistory opts out of dedup.
func TestUmbrella_WritePollRecords(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{
		entityKeyColumns: []string{"period", "customer", "sku"},
	})

	key := "period=2026-03-17/customer=abc"
	if _, err := store.WriteWithKey(ctx, key, []IntRecord{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Amount: 10, Ts: time.UnixMilli(100)},
	}); err != nil {
		t.Fatalf("first Write: %v", err)
	}
	if _, err := store.WriteWithKey(ctx, key, []IntRecord{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Amount: 99, Ts: time.UnixMilli(200)},
	}); err != nil {
		t.Fatalf("second Write: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	deduped, _, err := store.PollRecords(ctx, "", 100)
	if err != nil {
		t.Fatalf("PollRecords: %v", err)
	}
	if len(deduped) != 1 || deduped[0].Amount != 99 {
		t.Errorf("deduped: got %+v, want Amount=99", deduped)
	}

	full, _, err := store.PollRecords(ctx, "", 100, WithHistory())
	if err != nil {
		t.Fatalf("PollRecords history: %v", err)
	}
	if len(full) != 2 {
		t.Errorf("history: got %d records, want 2", len(full))
	}
}

// TestUmbrella_Query verifies umbrella.Query forwards to
// s3sql.Query with the right TableAlias plumbing, proving the
// SQL sub-store is constructed from the umbrella Config
// correctly. Aggregation is the canonical workload that only
// the SQL side can handle.
func TestUmbrella_Query(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{
		entityKeyColumns: []string{"period", "customer", "sku"},
	})

	if _, err := store.Write(ctx, []IntRecord{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Amount: 10, Ts: time.UnixMilli(1)},
		{Period: "2026-03-17", Customer: "abc", SKU: "s2", Amount: 20, Ts: time.UnixMilli(2)},
		{Period: "2026-03-17", Customer: "def", SKU: "s1", Amount: 30, Ts: time.UnixMilli(3)},
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	rows, err := store.Query(ctx, "*",
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
	want := []agg{{"abc", 30}, {"def", 30}}
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d: %+v", len(got), len(want), got)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("[%d] got %+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestUmbrella_PartitionRange proves the FROM..TO range syntax
// forwards through the umbrella to the SQL sub-store. Half-open
// [from, to) bounds on a partition column are the canonical use
// case the range form was added for.
func TestUmbrella_PartitionRange(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{})

	if _, err := store.Write(ctx, []IntRecord{
		{Period: "2026-02-28", Customer: "abc", SKU: "s", Ts: time.UnixMilli(1)},
		{Period: "2026-03-01", Customer: "abc", SKU: "s", Ts: time.UnixMilli(2)},
		{Period: "2026-03-15", Customer: "abc", SKU: "s", Ts: time.UnixMilli(3)},
		{Period: "2026-04-01", Customer: "abc", SKU: "s", Ts: time.UnixMilli(4)},
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := store.Read(ctx,
		"period=2026-03-01..2026-04-01/customer=abc")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("got %d records, want 2 (half-open [from, to))", len(got))
	}
}

// TestUmbrella_QueryRowInvalidPattern guards that an invalid
// key pattern surfaces via *sql.Row at Scan time
// (database/sql convention), not via a panic or silent zero
// result. Proves the errorRow path survives the umbrella
// forward.
func TestUmbrella_QueryRowInvalidPattern(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{})

	var ignore int64
	err := store.QueryRow(ctx,
		"period=X/customer=Y/extra=Z",
		"SELECT 1").Scan(&ignore)
	if err == nil {
		t.Error("expected error for invalid pattern, got nil")
	}
}

// TestUmbrella_SettleWindow is the one behavioral test that
// only makes sense at the umbrella level: SettleWindow must
// propagate from the flat umbrella Config to the s3parquet
// sub-store (which owns Poll). Writes inside the window are
// invisible; after the window closes they surface.
func TestUmbrella_SettleWindow(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{
		settleWindow: 500 * time.Millisecond,
	})

	if _, err := store.Write(ctx, []IntRecord{
		{Period: "2026-03-17", Customer: "alpha", SKU: "x", Ts: time.Now()},
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Inside the settle window: nothing visible.
	entries, _, err := store.Poll(ctx, "", 100)
	if err != nil {
		t.Fatalf("Poll inside window: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("inside settle window: got %d entries, want 0",
			len(entries))
	}

	// Poll in a loop until visible or a hard deadline passes —
	// lets slow/virtualised hosts take a little extra time
	// without risking a flaky fixed sleep.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		entries, _, err = store.Poll(ctx, "", 100)
		if err != nil {
			t.Fatalf("Poll waiting for settle: %v", err)
		}
		if len(entries) > 0 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("ref never became visible past the settle window")
}

// TestUmbrella_Close verifies both sub-stores are closed and
// the joined error is nil on a clean shutdown. A composition
// bug where only one sub-store was closed would leak the other.
func TestUmbrella_Close(t *testing.T) {
	f := testutil.New(t)
	store, err := New[IntRecord](context.Background(), Config[IntRecord]{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		TableAlias:        "records",
		PartitionKeyOf: func(r IntRecord) string {
			return fmt.Sprintf("period=%s/customer=%s",
				r.Period, r.Customer)
		},
		ExtraInitSQL: f.DuckDBCredentials(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// TestUmbrella_ReadIter exercises the umbrella's iter forwarder
// end-to-end. The umbrella routes ReadIter through s3sql.Reader
// (DuckDB row streaming), so this test mostly proves the
// forwarder is wired correctly — the s3sql-side iter contract is
// covered in s3sql/integration_test.go.
func TestUmbrella_ReadIter(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{
		entityKeyColumns: []string{"period", "customer", "sku"},
	})

	in := []IntRecord{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Amount: 10, Ts: time.UnixMilli(1)},
		{Period: "2026-03-17", Customer: "abc", SKU: "s2", Amount: 20, Ts: time.UnixMilli(2)},
		{Period: "2026-03-17", Customer: "def", SKU: "s1", Amount: 30, Ts: time.UnixMilli(3)},
	}
	if _, err := store.Write(ctx, in); err != nil {
		t.Fatalf("Write: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	count := 0
	for r, err := range store.ReadIter(ctx, "*") {
		if err != nil {
			t.Fatalf("ReadIter: %v", err)
		}
		_ = r
		count++
	}
	if count != len(in) {
		t.Errorf("got %d records, want %d", count, len(in))
	}
}
