//go:build integration

package s3sql_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/ueisele/s3store/internal/testutil"
	"github.com/ueisele/s3store/s3parquet"
	"github.com/ueisele/s3store/s3sql"
)

// Rec is the on-disk shape used by these tests. Parquet tags
// drive both the write (via s3parquet) and the SQL-side row
// binder (via s3sql) — one schema declaration covers both.
type Rec struct {
	Period   string    `parquet:"period"`
	Customer string    `parquet:"customer"`
	SKU      string    `parquet:"sku"`
	Amount   float64   `parquet:"amount"`
	Currency string    `parquet:"currency"`
	Ts       time.Time `parquet:"ts,timestamp(millisecond)"`
}

// RecNarrow is Rec without Amount / Currency — used to simulate
// older files that pre-date those columns, so reads against the
// new Rec shape must zero-fill them.
type RecNarrow struct {
	Period   string    `parquet:"period"`
	Customer string    `parquet:"customer"`
	SKU      string    `parquet:"sku"`
	Ts       time.Time `parquet:"ts,timestamp(millisecond)"`
}

func partitionKeyOfRec(r Rec) string {
	return fmt.Sprintf("period=%s/customer=%s", r.Period, r.Customer)
}

// sqlOpts dials in the dedup config a given test cares about.
type sqlOpts struct {
	versionColumn      string
	entityKeyColumns   []string
	bloomFilterColumns []string
}

// testFixture bundles the per-test MinIO bucket + a matching
// s3parquet writer + an s3sql reader pointed at the same
// S3Target.
type testFixture struct {
	bucket string
	writer *s3parquet.Writer[Rec]
	sql    *s3sql.Reader[Rec]
}

// newFixture creates a fresh bucket on the shared MinIO,
// constructs a matching Writer and Reader against a shared
// S3Target, and wires cleanup.
func newFixture(t *testing.T, opts sqlOpts) *testFixture {
	t.Helper()
	f := testutil.New(t)
	target := s3parquet.S3Target{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		SettleWindow:      10 * time.Millisecond,
	}
	w, err := s3parquet.NewWriter(s3parquet.WriterConfig[Rec]{
		Target:             target,
		PartitionKeyOf:     partitionKeyOfRec,
		BloomFilterColumns: opts.bloomFilterColumns,
	})
	if err != nil {
		t.Fatalf("s3parquet.NewWriter: %v", err)
	}

	s, err := s3sql.NewReader(s3sql.ReaderConfig[Rec]{
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
	time.Sleep(30 * time.Millisecond)
}

// TestRead_WithDedup exercises the SQL read path end-to-end,
// including DuckDB's QUALIFY-based dedup via VersionColumn.
func TestRead_WithDedup(t *testing.T) {
	f := newFixture(t, sqlOpts{
		versionColumn:    "ts",
		entityKeyColumns: []string{"period", "customer", "sku"},
	})

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

// TestInsertedAtField_Populate covers the s3sql side of the
// InsertedAtField hook: Read (which drives Query + scanAll)
// populates a parquet-untagged time.Time field with the source
// file's write timestamp, parsed out of DuckDB's `filename`
// column. Validates the hot-path wiring end-to-end: scan-expr
// emits filename=true, CTE keeps the column, scanAll routes it
// through core.ParseDataFileName into the struct field.
func TestInsertedAtField_Populate(t *testing.T) {
	type RecWithMeta struct {
		Period     string    `parquet:"period"`
		Customer   string    `parquet:"customer"`
		SKU        string    `parquet:"sku"`
		Amount     float64   `parquet:"amount"`
		Ts         time.Time `parquet:"ts,timestamp(millisecond)"`
		InsertedAt time.Time `parquet:"-"`
	}

	f := testutil.New(t)
	target := s3parquet.S3Target{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		SettleWindow:      10 * time.Millisecond,
	}
	w, err := s3parquet.NewWriter(s3parquet.WriterConfig[RecWithMeta]{
		Target: target,
		PartitionKeyOf: func(r RecWithMeta) string {
			return fmt.Sprintf("period=%s/customer=%s",
				r.Period, r.Customer)
		},
	})
	if err != nil {
		t.Fatalf("s3parquet.NewWriter: %v", err)
	}

	sq, err := s3sql.NewReader(s3sql.ReaderConfig[RecWithMeta]{
		Target:          target,
		TableAlias:      "records",
		ExtraInitSQL:    f.DuckDBCredentials(),
		InsertedAtField: "InsertedAt",
	})
	if err != nil {
		t.Fatalf("s3sql.NewReader: %v", err)
	}
	t.Cleanup(func() { _ = sq.Close() })

	before := time.Now()
	if _, err := w.Write(context.Background(), []RecWithMeta{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1",
			Amount: 10, Ts: time.UnixMilli(100)},
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	after := time.Now()
	time.Sleep(30 * time.Millisecond)

	got, err := sq.Read(context.Background(), "*")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d records, want 1", len(got))
	}
	ia := got[0].InsertedAt
	if ia.Before(before.Add(-time.Millisecond)) ||
		ia.After(after.Add(time.Millisecond)) {
		t.Errorf("InsertedAt=%v outside [%v, %v]",
			ia, before, after)
	}
	// Data columns still scan through the normal binder —
	// InsertedAtField plumbing mustn't disrupt them.
	if got[0].Amount != 10 {
		t.Errorf("Amount=%v, want 10 (binder regression)",
			got[0].Amount)
	}
}

// TestRead_DedupVersionTieDeterministic covers the edge case
// where two writes share the exact VersionColumn value — without
// a secondary tie-break, DuckDB's ROW_NUMBER would pick a winner
// arbitrarily and re-runs could disagree. The dedup CTE orders
// by filename DESC as the tie-breaker, so the lexicographically-
// later data file (= the later Write, since filenames are
// prefixed with write tsMicros) always wins and the result is
// stable across repeated reads.
func TestRead_DedupVersionTieDeterministic(t *testing.T) {
	f := newFixture(t, sqlOpts{
		versionColumn:    "ts",
		entityKeyColumns: []string{"period", "customer", "sku"},
	})

	tied := time.UnixMilli(100)
	f.writeSome(t, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1",
			Amount: 10, Currency: "USD", Ts: tied},
	})
	f.writeSome(t, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1",
			Amount: 99, Currency: "USD", Ts: tied},
	})

	// Read three times — determinism matters more than which
	// specific record wins, but we also assert the later Write
	// wins (its data filename sorts greater on tsMicros, so
	// filename DESC picks it).
	ctx := context.Background()
	var first float64
	for i := 0; i < 3; i++ {
		got, err := f.sql.Read(ctx,
			"period=2026-03-17/customer=abc")
		if err != nil {
			t.Fatalf("Read #%d: %v", i, err)
		}
		if len(got) != 1 {
			t.Fatalf("Read #%d: got %d records, want 1",
				i, len(got))
		}
		if i == 0 {
			first = got[0].Amount
			if first != 99 {
				t.Errorf("winner Amount: got %v, want 99 "+
					"(later Write's filename sorts greater)",
					first)
			}
			continue
		}
		if got[0].Amount != first {
			t.Errorf("non-deterministic: Read #%d got %v, "+
				"first Read got %v", i, got[0].Amount, first)
		}
	}
}

// TestRead_WithHistory verifies every version is returned when
// WithHistory disables dedup.
func TestRead_WithHistory(t *testing.T) {
	f := newFixture(t, sqlOpts{
		versionColumn:    "ts",
		entityKeyColumns: []string{"period", "customer", "sku"},
	})

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

// TestRead_PartitionRange exercises the FROM..TO range form on
// the SQL read path: common-prefix glob + WHERE predicate on the
// hive partition column. Covers inclusive-lower / exclusive-upper,
// the month-boundary case that partition pruning is most useful
// for, and the non-prefix-expressible case where the common
// prefix doesn't fully constrain the glob.
func TestRead_PartitionRange(t *testing.T) {
	f := newFixture(t, sqlOpts{})

	f.writeSome(t, []Rec{
		{Period: "2026-02-28", Customer: "abc", SKU: "s", Ts: time.UnixMilli(1)},
		{Period: "2026-03-01", Customer: "abc", SKU: "s", Ts: time.UnixMilli(2)},
		{Period: "2026-03-15", Customer: "abc", SKU: "s", Ts: time.UnixMilli(3)},
		{Period: "2026-04-01", Customer: "abc", SKU: "s", Ts: time.UnixMilli(4)},
		{Period: "2026-04-10", Customer: "abc", SKU: "s", Ts: time.UnixMilli(5)},
	})

	got, err := f.sql.Read(context.Background(),
		"period=2026-03-01..2026-04-01/customer=abc")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	// hive_types_autocast=false keeps partition values as VARCHAR,
	// so Period roundtrips string-for-string — compare exactly.
	want := []string{"2026-03-01", "2026-03-15"}
	var periods []string
	for _, r := range got {
		periods = append(periods, r.Period)
	}
	if !reflect.DeepEqual(periods, want) {
		t.Errorf("got periods %v, want %v (half-open [from, to))",
			periods, want)
	}
}

// TestRead_PartitionRangeWithHistory proves range filtering and
// dedup compose in the correct order — the range WHERE runs
// before the dedup QUALIFY, not after. The distinguishing
// scenario: the absolute-latest version of an entity lives
// *outside* the range. Correct order picks the latest
// *in-range* version; swapping the order would pick the absolute
// latest, then drop it as out-of-range (0 records).
func TestRead_PartitionRangeWithHistory(t *testing.T) {
	// Entity = (customer, sku). Period is NOT part of the entity
	// key, so one entity can have versions in different period
	// partitions.
	f := newFixture(t, sqlOpts{
		versionColumn:    "ts",
		entityKeyColumns: []string{"customer", "sku"},
	})

	f.writeSome(t, []Rec{
		{Period: "2026-03-15", Customer: "abc", SKU: "s1", Amount: 10, Ts: time.UnixMilli(100)},
	})
	f.writeSome(t, []Rec{
		{Period: "2026-03-25", Customer: "abc", SKU: "s1", Amount: 15, Ts: time.UnixMilli(150)},
	})
	// Latest by Ts, but out of range:
	f.writeSome(t, []Rec{
		{Period: "2026-04-15", Customer: "abc", SKU: "s1", Amount: 20, Ts: time.UnixMilli(200)},
	})

	const pattern = "period=2026-03-01..2026-04-01/customer=abc"
	ctx := context.Background()

	deduped, err := f.sql.Read(ctx, pattern)
	if err != nil {
		t.Fatalf("Read deduped: %v", err)
	}
	if len(deduped) != 1 || deduped[0].Amount != 15 {
		t.Errorf("deduped: got %+v, want 1 record with Amount=15 "+
			"(latest in-range; Amount=20 is the absolute latest "+
			"but out of range)", deduped)
	}

	full, err := f.sql.Read(ctx, pattern, s3sql.WithHistory())
	if err != nil {
		t.Fatalf("Read history: %v", err)
	}
	if len(full) != 2 {
		t.Fatalf("history: got %d records, want 2 "+
			"(both in-range versions)", len(full))
	}
	for _, r := range full {
		if r.Amount == 20 {
			t.Errorf("out-of-range record leaked into history result: %+v", r)
		}
	}
}

// TestRead_BloomFilterRoundTrip guards that files written with a
// bloom filter on "sku" stay fully readable through DuckDB, and
// that equality filters on the bloomed column return correct
// rows. Bloom filters are an opt-in perf hint; they must not
// change semantics regardless of whether DuckDB actually uses
// them for row-group pruning.
func TestRead_BloomFilterRoundTrip(t *testing.T) {
	f := newFixture(t, sqlOpts{
		bloomFilterColumns: []string{"sku"},
	})

	f.writeSome(t, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Amount: 10, Currency: "USD", Ts: time.UnixMilli(1)},
		{Period: "2026-03-17", Customer: "abc", SKU: "s2", Amount: 20, Currency: "USD", Ts: time.UnixMilli(2)},
	})
	f.writeSome(t, []Rec{
		{Period: "2026-03-18", Customer: "abc", SKU: "s3", Amount: 30, Currency: "USD", Ts: time.UnixMilli(3)},
	})

	ctx := context.Background()

	// Full scan returns every record.
	all, err := f.sql.Read(ctx, "*")
	if err != nil {
		t.Fatalf("Read all: %v", err)
	}
	if len(all) != 3 {
		t.Errorf("Read all: got %d records, want 3", len(all))
	}

	// Equality on the bloomed column: s2 lives in the first file
	// only. DuckDB may or may not use the bloom for pruning —
	// either way the result must be the one row.
	rows, err := f.sql.Query(ctx, "*",
		"SELECT sku, amount FROM records WHERE sku = 's2'")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()

	var hits int
	for rows.Next() {
		var sku string
		var amount float64
		if err := rows.Scan(&sku, &amount); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		if sku != "s2" || amount != 20 {
			t.Errorf("got sku=%q amount=%v, want s2/20", sku, amount)
		}
		hits++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if hits != 1 {
		t.Errorf("got %d hits for sku=s2, want 1", hits)
	}
}

// TestQuery covers the arbitrary-SQL path with an aggregation.
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
	f := newFixture(t, sqlOpts{
		versionColumn:    "ts",
		entityKeyColumns: []string{"period", "customer", "sku"},
	})

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
	// No dedup config — Poll doesn't exercise dedup.
	f := newFixture(t, sqlOpts{})

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
	f := newFixture(t, sqlOpts{
		versionColumn:    "ts",
		entityKeyColumns: []string{"period", "customer", "sku"},
	})

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

// TestPollTimeWindow mirrors the s3parquet time-window test on
// the SQL side: OffsetAt + WithUntilOffset bound Poll /
// PollRecords to a half-open [start, end) range. Guards that
// the underlying ref ordering is identical (shared core codec)
// and that the SQL package honors Until in both Poll and
// PollRecords.
func TestPollTimeWindow(t *testing.T) {
	f := newFixture(t, sqlOpts{})
	ctx := context.Background()

	beforeFirst := time.Now()
	f.writeSome(t, []Rec{
		{Period: "2026-03-17", Customer: "a", SKU: "s1",
			Amount: 1, Currency: "USD", Ts: time.UnixMilli(1)},
	})
	time.Sleep(5 * time.Millisecond)
	afterFirst := time.Now()
	time.Sleep(5 * time.Millisecond)
	f.writeSome(t, []Rec{
		{Period: "2026-03-17", Customer: "b", SKU: "s2",
			Amount: 2, Currency: "USD", Ts: time.UnixMilli(2)},
	})
	time.Sleep(5 * time.Millisecond)
	beforeThird := time.Now()
	time.Sleep(5 * time.Millisecond)
	f.writeSome(t, []Rec{
		{Period: "2026-03-17", Customer: "c", SKU: "s3",
			Amount: 3, Currency: "USD", Ts: time.UnixMilli(3)},
	})

	// Full stream: three entries.
	all, _, err := f.sql.Poll(ctx, "", 100)
	if err != nil {
		t.Fatalf("Poll all: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("baseline: got %d entries, want 3", len(all))
	}

	// Window [afterFirst, beforeThird) — middle write only.
	start := f.sql.OffsetAt(afterFirst)
	end := f.sql.OffsetAt(beforeThird)
	window, _, err := f.sql.Poll(ctx, start, 100,
		s3sql.WithUntilOffset(end))
	if err != nil {
		t.Fatalf("Poll window: %v", err)
	}
	if len(window) != 1 {
		t.Fatalf("window: got %d entries, want 1", len(window))
	}
	if window[0].Key != "period=2026-03-17/customer=b" {
		t.Errorf("window[0]: got %q, want customer=b",
			window[0].Key)
	}

	// PollRecords respects Until too — same window should
	// surface exactly one record.
	recs, _, err := f.sql.PollRecords(ctx, start, 100,
		s3sql.WithUntilOffset(end))
	if err != nil {
		t.Fatalf("PollRecords window: %v", err)
	}
	if len(recs) != 1 || recs[0].Customer != "b" {
		t.Errorf("PollRecords window: got %+v, want 1 rec customer=b",
			recs)
	}

	// Empty-window case — cursor must not drift.
	offZero := f.sql.OffsetAt(beforeFirst.Add(-time.Hour))
	offEnd := f.sql.OffsetAt(beforeFirst.Add(-time.Minute))
	empty, off2, err := f.sql.Poll(ctx, offZero, 100,
		s3sql.WithUntilOffset(offEnd))
	if err != nil {
		t.Fatalf("Poll empty: %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("empty window: got %d entries, want 0",
			len(empty))
	}
	if off2 != offZero {
		t.Errorf("empty window: offset drifted %q -> %q",
			offZero, off2)
	}
}

// TestPollRecordsAll exercises the convenience wrapper on the
// SQL side. Writes several records, then drains the window in
// one call, asserting the count and bounded-window behavior.
func TestPollRecordsAll(t *testing.T) {
	f := newFixture(t, sqlOpts{})
	ctx := context.Background()

	before := time.Now()
	for i := range 5 {
		f.writeSome(t, []Rec{{
			Period:   "2026-03-17",
			Customer: fmt.Sprintf("c%d", i),
			SKU:      "s1",
			Amount:   float64(i),
			Currency: "USD",
			Ts:       time.UnixMilli(int64(i + 1)),
		}})
	}
	after := time.Now()

	got, err := f.sql.PollRecordsAll(ctx,
		f.sql.OffsetAt(before), f.sql.OffsetAt(after))
	if err != nil {
		t.Fatalf("PollRecordsAll: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("got %d, want 5", len(got))
	}

	// Empty window returns nil without error.
	empty, err := f.sql.PollRecordsAll(ctx,
		f.sql.OffsetAt(before.Add(-time.Hour)),
		f.sql.OffsetAt(before.Add(-time.Minute)))
	if err != nil {
		t.Fatalf("PollRecordsAll empty: %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("empty: got %d, want 0", len(empty))
	}
}

// TestRead_MissingColumnZeroFills guards the "added a new
// column to T" contract on the SQL side: a file missing a
// column the reader expects must come back with that column as
// NULL, which the reflection binder maps to the field's Go
// zero value — not an error.
func TestRead_MissingColumnZeroFills(t *testing.T) {
	testFix := testutil.New(t)

	// Old-shape file: no Amount, no Currency.
	target := s3parquet.S3Target{
		Bucket:            testFix.Bucket,
		Prefix:            "store",
		S3Client:          testFix.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		SettleWindow:      10 * time.Millisecond,
	}
	wOld, err := s3parquet.NewWriter(s3parquet.WriterConfig[RecNarrow]{
		Target: target,
		PartitionKeyOf: func(r RecNarrow) string {
			return fmt.Sprintf("period=%s/customer=%s",
				r.Period, r.Customer)
		},
	})
	if err != nil {
		t.Fatalf("s3parquet.NewWriter(RecNarrow): %v", err)
	}
	if _, err := wOld.Write(context.Background(), []RecNarrow{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Ts: time.UnixMilli(100)},
	}); err != nil {
		t.Fatalf("Write RecNarrow: %v", err)
	}
	time.Sleep(30 * time.Millisecond)

	// Reader scans into Rec (which has Amount + Currency).
	s, err := s3sql.NewReader(s3sql.ReaderConfig[Rec]{
		Target:       target,
		TableAlias:   "records",
		ExtraInitSQL: testFix.DuckDBCredentials(),
	})
	if err != nil {
		t.Fatalf("s3sql.NewReader: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	got, err := s.Read(context.Background(),
		"period=2026-03-17/customer=abc")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d records, want 1", len(got))
	}
	if got[0].Amount != 0 {
		t.Errorf("got Amount=%v, want 0 (NULL→zero)", got[0].Amount)
	}
	if got[0].Currency != "" {
		t.Errorf("got Currency=%q, want \"\" (NULL→zero)",
			got[0].Currency)
	}
	if got[0].SKU != "s1" {
		t.Errorf("got SKU=%q, want s1", got[0].SKU)
	}
}

// TagRec exercises the composite-type code path on the SQL
// side: parquet-go writes Tags as a LIST<VARCHAR>, and the
// s3sql reflection binder decodes it back into a []string via
// mapstructure.
type TagRec struct {
	Period   string    `parquet:"period"`
	Customer string    `parquet:"customer"`
	Tags     []string  `parquet:"tags"`
	Ts       time.Time `parquet:"ts,timestamp(millisecond)"`
}

// TestRead_SliceField round-trips a []string field through S3:
// s3parquet writes it as a parquet LIST, DuckDB reads it back
// through s3sql, and the binder decodes into []string. Guards
// the "composite types work end-to-end" contract.
func TestRead_SliceField(t *testing.T) {
	f := testutil.New(t)
	ctx := context.Background()

	target := s3parquet.S3Target{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		SettleWindow:      10 * time.Millisecond,
	}
	w, err := s3parquet.NewWriter(s3parquet.WriterConfig[TagRec]{
		Target: target,
		PartitionKeyOf: func(r TagRec) string {
			return fmt.Sprintf("period=%s/customer=%s",
				r.Period, r.Customer)
		},
	})
	if err != nil {
		t.Fatalf("s3parquet.NewWriter: %v", err)
	}

	if _, err := w.Write(ctx, []TagRec{
		{
			Period:   "2026-03-17",
			Customer: "abc",
			Tags:     []string{"alpha", "beta", "gamma"},
			Ts:       time.UnixMilli(100),
		},
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	time.Sleep(30 * time.Millisecond)

	s, err := s3sql.NewReader(s3sql.ReaderConfig[TagRec]{
		Target:       target,
		TableAlias:   "records",
		ExtraInitSQL: f.DuckDBCredentials(),
	})
	if err != nil {
		t.Fatalf("s3sql.NewReader: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	got, err := s.Read(ctx, "period=2026-03-17/customer=abc")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d records, want 1", len(got))
	}
	want := []string{"alpha", "beta", "gamma"}
	if !reflect.DeepEqual(got[0].Tags, want) {
		t.Errorf("Tags: got %v, want %v", got[0].Tags, want)
	}
}

// ProcessField is a named int8 enum — the shape a go-enum
// generator typically produces. Testing the named variant (not
// plain int8) proves both parquet-go's write side and the
// binder's read side dispatch on reflect.Kind, not exact type.
type ProcessField int8

const (
	ProcessFieldUnknown ProcessField = iota
	ProcessFieldPrimary
	ProcessFieldSecondary
)

// ProcessLog is the nested struct: primitive + named int8
// enum + map, all inside a list at the outer level. Mirrors a
// realistic JSONB-style payload ported to native columns.
type ProcessLog struct {
	Processor  string            `parquet:"processor"`
	Field      ProcessField      `parquet:"field"`
	Attributes map[string]string `parquet:"attributes"`
}

// JobRec is the outer record type; Logs is a LIST<STRUCT<...,
// MAP<VARCHAR, VARCHAR>>>. Exercises the full composite stack
// in one field.
type JobRec struct {
	Period   string       `parquet:"period"`
	Customer string       `parquet:"customer"`
	JobID    string       `parquet:"job_id"`
	Logs     []ProcessLog `parquet:"logs"`
	Ts       time.Time    `parquet:"ts,timestamp(millisecond)"`
}

// TestRead_NestedListOfStructsWithMap round-trips the
// JSONB-style shape: a list of structs, each with a named-int
// field and a map-of-string field. Passing means s3store can
// replace a Postgres JSONB column with a native columnar layout
// end-to-end — write via s3parquet, read via s3sql, typed
// decode via the reflection binder + mapstructure.
func TestRead_NestedListOfStructsWithMap(t *testing.T) {
	f := testutil.New(t)
	ctx := context.Background()

	target := s3parquet.S3Target{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		SettleWindow:      10 * time.Millisecond,
	}
	w, err := s3parquet.NewWriter(s3parquet.WriterConfig[JobRec]{
		Target: target,
		PartitionKeyOf: func(r JobRec) string {
			return fmt.Sprintf("period=%s/customer=%s",
				r.Period, r.Customer)
		},
	})
	if err != nil {
		t.Fatalf("s3parquet.NewWriter: %v", err)
	}

	in := JobRec{
		Period:   "2026-03-17",
		Customer: "abc",
		JobID:    "job-1",
		Logs: []ProcessLog{
			{
				Processor: "ingest",
				Field:     ProcessFieldPrimary,
				Attributes: map[string]string{
					"region": "eu-west-1",
					"stage":  "raw",
				},
			},
			{
				Processor: "enrich",
				Field:     ProcessFieldSecondary,
				Attributes: map[string]string{
					"model": "v2",
				},
			},
		},
		Ts: time.UnixMilli(100),
	}

	if _, err := w.Write(ctx, []JobRec{in}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	time.Sleep(30 * time.Millisecond)

	s, err := s3sql.NewReader(s3sql.ReaderConfig[JobRec]{
		Target:       target,
		TableAlias:   "records",
		ExtraInitSQL: f.DuckDBCredentials(),
	})
	if err != nil {
		t.Fatalf("s3sql.NewReader: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	got, err := s.Read(ctx, "period=2026-03-17/customer=abc")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d records, want 1", len(got))
	}
	if !reflect.DeepEqual(got[0].Logs, in.Logs) {
		t.Errorf("Logs round-trip mismatch:\n got  %+v\n want %+v",
			got[0].Logs, in.Logs)
	}
}

// TestReadMany_NonCartesian proves the multi-pattern fast path:
// ReadMany covers an arbitrary tuple set that a single Cartesian
// pattern can't express without over-reading.
func TestReadMany_NonCartesian(t *testing.T) {
	f := newFixture(t, sqlOpts{})

	f.writeSome(t, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Amount: 10,
			Currency: "USD", Ts: time.UnixMilli(1)},
		{Period: "2026-03-17", Customer: "def", SKU: "s2", Amount: 20,
			Currency: "USD", Ts: time.UnixMilli(2)},
		{Period: "2026-03-18", Customer: "abc", SKU: "s3", Amount: 30,
			Currency: "USD", Ts: time.UnixMilli(3)},
		{Period: "2026-03-18", Customer: "def", SKU: "s4", Amount: 40,
			Currency: "USD", Ts: time.UnixMilli(4)},
	})

	got, err := f.sql.ReadMany(context.Background(), []string{
		"period=2026-03-17/customer=abc",
		"period=2026-03-18/customer=def",
	})
	if err != nil {
		t.Fatalf("ReadMany: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d records, want 2: %+v", len(got), got)
	}
	amounts := map[float64]bool{}
	for _, r := range got {
		amounts[r.Amount] = true
	}
	if !amounts[10] || !amounts[40] {
		t.Errorf("got %v, want {10, 40}", amounts)
	}
	if amounts[20] || amounts[30] {
		t.Errorf("off-diagonal leaked: %v", amounts)
	}
}

// TestQueryMany_AggregationAcrossPatterns is the killer use
// case: one DuckDB query with GROUP BY runs over the unioned
// file list, so aggregation is global. N separate Query calls
// would force the caller to aggregate in Go across N result
// sets.
func TestQueryMany_AggregationAcrossPatterns(t *testing.T) {
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
	rows, err := f.sql.QueryMany(ctx, []string{
		"period=2026-03-17/customer=abc",
		"period=2026-03-18/customer=def",
	},
		"SELECT SUM(amount) AS total FROM records")
	if err != nil {
		t.Fatalf("QueryMany: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("QueryMany: no rows")
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

// TestQueryRowMany covers the single-row sibling on a matching
// pair of patterns + an aggregation.
func TestQueryRowMany(t *testing.T) {
	f := newFixture(t, sqlOpts{})

	f.writeSome(t, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Amount: 10,
			Currency: "USD", Ts: time.UnixMilli(1)},
		{Period: "2026-03-18", Customer: "def", SKU: "s2", Amount: 20,
			Currency: "USD", Ts: time.UnixMilli(2)},
	})

	var total float64
	err := f.sql.QueryRowMany(context.Background(), []string{
		"period=2026-03-17/customer=abc",
		"period=2026-03-18/customer=def",
	},
		"SELECT SUM(amount) FROM records").Scan(&total)
	if err != nil {
		t.Fatalf("QueryRowMany: %v", err)
	}
	if total != 30 {
		t.Errorf("total = %v, want 30", total)
	}
}

// TestReadMany_Overlap covers the case where two patterns match
// the same file (wide pattern + narrow subset). The file must
// be scanned once and produce one copy of each record in the
// result.
func TestReadMany_Overlap(t *testing.T) {
	f := newFixture(t, sqlOpts{})

	f.writeSome(t, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Amount: 1,
			Currency: "USD", Ts: time.UnixMilli(1)},
		{Period: "2026-03-17", Customer: "def", SKU: "s2", Amount: 2,
			Currency: "USD", Ts: time.UnixMilli(2)},
	})

	got, err := f.sql.ReadMany(context.Background(), []string{
		"*",
		"period=2026-03-17/customer=abc",
	})
	if err != nil {
		t.Fatalf("ReadMany: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("got %d, want 2 (overlap must not double-count)",
			len(got))
	}
}

// TestReadMany_Empty covers the empty-slice contract: every
// *Many method normalises "no input" to an empty result rather
// than propagating DuckDB's "No files found" error.
func TestReadMany_Empty(t *testing.T) {
	f := newFixture(t, sqlOpts{})
	ctx := context.Background()

	got, err := f.sql.ReadMany(ctx, nil)
	if err != nil {
		t.Errorf("ReadMany(nil): %v", err)
	}
	if got != nil {
		t.Errorf("ReadMany(nil): got %v, want nil", got)
	}

	rows, err := f.sql.QueryMany(ctx, nil, "SELECT 1 FROM records")
	if err != nil {
		t.Errorf("QueryMany(nil): %v", err)
	} else {
		if rows.Next() {
			t.Error("QueryMany(nil).Next(): got true, want false")
		}
		if err := rows.Err(); err != nil {
			t.Errorf("QueryMany(nil).Err(): %v", err)
		}
		_ = rows.Close()
	}

	var x int
	err = f.sql.QueryRowMany(ctx, nil,
		"SELECT 1 FROM records").Scan(&x)
	if err != sql.ErrNoRows {
		t.Errorf("QueryRowMany(nil).Scan: got %v, want sql.ErrNoRows",
			err)
	}
}

// TestReadMany_NoFilesMatched covers Fix 1: a pattern that
// matches zero files returns an empty result, not an error.
// Previously DuckDB's "No files found" propagated through
// single-pattern Read; ReadMany / QueryMany / QueryRowMany all
// normalise that to empty now.
func TestReadMany_NoFilesMatched(t *testing.T) {
	f := newFixture(t, sqlOpts{})
	ctx := context.Background()

	// Write one record so the store isn't empty, but query a
	// partition that has nothing.
	f.writeSome(t, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1",
			Amount: 1, Currency: "USD", Ts: time.UnixMilli(1)},
	})

	// Single-pattern Read, zero matches.
	got, err := f.sql.Read(ctx,
		"period=does-not-exist/customer=also-missing")
	if err != nil {
		t.Errorf("Read (no files): %v", err)
	}
	if got != nil {
		t.Errorf("Read (no files): got %v, want nil", got)
	}

	// Single-pattern ReadMany, zero matches — takes the
	// fast-path branch that catches DuckDB's error.
	got, err = f.sql.ReadMany(ctx, []string{
		"period=does-not-exist/customer=also-missing",
	})
	if err != nil {
		t.Errorf("ReadMany single (no files): %v", err)
	}
	if got != nil {
		t.Errorf("ReadMany single (no files): got %v, want nil", got)
	}

	// Multi-pattern ReadMany, zero matches — takes the
	// pre-LIST branch that checks the URI list size.
	got, err = f.sql.ReadMany(ctx, []string{
		"period=nope/customer=nada",
		"period=neither/customer=nope",
	})
	if err != nil {
		t.Errorf("ReadMany multi (no files): %v", err)
	}
	if got != nil {
		t.Errorf("ReadMany multi (no files): got %v, want nil", got)
	}

	// QueryMany single + multi — both produce an empty *sql.Rows.
	for _, patterns := range [][]string{
		{"period=nope/customer=nada"},
		{"period=nope/customer=nada", "period=nix/customer=none"},
	} {
		rows, err := f.sql.QueryMany(ctx, patterns,
			"SELECT 1 FROM records")
		if err != nil {
			t.Errorf("QueryMany %v: %v", patterns, err)
			continue
		}
		if rows.Next() {
			t.Errorf("QueryMany %v: Next() true, want false",
				patterns)
		}
		_ = rows.Close()
	}

	// QueryRowMany: Scan returns sql.ErrNoRows.
	var x int
	err = f.sql.QueryRowMany(ctx,
		[]string{"period=nope/customer=nada"},
		"SELECT 1 FROM records").Scan(&x)
	if err != sql.ErrNoRows {
		t.Errorf("QueryRowMany single (no files).Scan: got %v, "+
			"want sql.ErrNoRows", err)
	}
}

// TestReadMany_WithHistory guards that opts pass through to the
// dedup path: with dedup configured, ReadMany + WithHistory()
// returns every record, without WithHistory() returns latest
// per entity. Covers both the single-pattern fast path and the
// multi-pattern pre-LIST branch.
func TestReadMany_WithHistory(t *testing.T) {
	f := newFixture(t, sqlOpts{
		versionColumn:    "ts",
		entityKeyColumns: []string{"period", "customer", "sku"},
	})
	ctx := context.Background()

	// Same (period, customer, sku) written twice at different
	// timestamps — dedup should keep the latest.
	f.writeSome(t, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1",
			Amount: 1, Currency: "USD", Ts: time.UnixMilli(100)},
	})
	f.writeSome(t, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1",
			Amount: 99, Currency: "USD", Ts: time.UnixMilli(200)},
	})

	// Single-pattern ReadMany, default dedup → 1 record, latest.
	got, err := f.sql.ReadMany(ctx, []string{
		"period=2026-03-17/customer=abc",
	})
	if err != nil {
		t.Fatalf("ReadMany: %v", err)
	}
	if len(got) != 1 || got[0].Amount != 99 {
		t.Errorf("dedup: got %+v, want one record with Amount=99",
			got)
	}

	// Single-pattern ReadMany + WithHistory → both records.
	got, err = f.sql.ReadMany(ctx, []string{
		"period=2026-03-17/customer=abc",
	}, s3sql.WithHistory())
	if err != nil {
		t.Fatalf("ReadMany history: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("history: got %d records, want 2", len(got))
	}

	// Multi-pattern ReadMany + WithHistory via two patterns
	// that overlap on the same file. Exercises the pre-LIST
	// branch and proves unionKeys dedups + opts still pass
	// through to the scan.
	got, err = f.sql.ReadMany(ctx, []string{
		"period=2026-03-17/customer=abc",
		"*",
	}, s3sql.WithHistory())
	if err != nil {
		t.Fatalf("ReadMany multi history: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("multi history: got %d records, want 2 "+
			"(dedup by file URI, not by row)", len(got))
	}
}

// TestReadMany_CrossPatternDedup proves dedup runs globally
// across the union: one entity appearing under two different
// partition tuples is collapsed to the latest version, not
// deduped per-pattern. The dedup CTE runs over the unioned
// scan so this works automatically — the test locks it in.
//
// The EntityKey here (SKU alone) is deliberately orthogonal to
// the partition columns (period, customer), so one entity can
// legitimately appear under two different (period, customer)
// tuples.
func TestReadMany_CrossPatternDedup(t *testing.T) {
	f := newFixture(t, sqlOpts{
		versionColumn:    "ts",
		entityKeyColumns: []string{"sku"},
	})
	ctx := context.Background()

	// Same SKU in two different partitions, different
	// timestamps. Cross-pattern dedup should keep the Ts=200
	// row.
	f.writeSome(t, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1",
			Amount: 10, Currency: "USD", Ts: time.UnixMilli(100)},
	})
	f.writeSome(t, []Rec{
		{Period: "2026-03-18", Customer: "def", SKU: "s1",
			Amount: 99, Currency: "USD", Ts: time.UnixMilli(200)},
	})

	got, err := f.sql.ReadMany(ctx, []string{
		"period=2026-03-17/customer=abc",
		"period=2026-03-18/customer=def",
	})
	if err != nil {
		t.Fatalf("ReadMany: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("cross-pattern dedup: got %d records, want 1: "+
			"%+v", len(got), got)
	}
	if got[0].Amount != 99 {
		t.Errorf("cross-pattern dedup: got Amount=%v, want 99 "+
			"(latest Ts across the union)", got[0].Amount)
	}
}

// TestDisableRefStream_s3sql covers the read-side half of the
// DisableRefStream contract: a store configured with the flag
// refuses Poll / PollRecords / PollRecordsAll with the shared
// sentinel, while Read and OffsetAt stay fully functional.
//
// Data is written through a sibling s3parquet writer also
// configured with DisableRefStream so the scenario matches a
// real deployment where both halves agree on "no refs here".
func TestDisableRefStream_s3sql(t *testing.T) {
	ctx := context.Background()
	f := testutil.New(t)

	target := s3parquet.S3Target{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		SettleWindow:      10 * time.Millisecond,
		DisableRefStream:  true,
	}
	writer, err := s3parquet.NewWriter(s3parquet.WriterConfig[Rec]{
		Target:         target,
		PartitionKeyOf: partitionKeyOfRec,
	})
	if err != nil {
		t.Fatalf("s3parquet.NewWriter: %v", err)
	}
	s, err := s3sql.NewReader(s3sql.ReaderConfig[Rec]{
		Target:       target,
		TableAlias:   "records",
		ExtraInitSQL: f.DuckDBCredentials(),
	})
	if err != nil {
		t.Fatalf("s3sql.NewReader: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if _, err := writer.Write(ctx, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1",
			Amount: 10, Currency: "USD", Ts: time.UnixMilli(100)},
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Read still works — it LISTs /_data/ directly.
	got, err := s.Read(ctx, "*")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("Read: got %d, want 1", len(got))
	}

	// Stream methods all refuse with the shared sentinel.
	if _, _, err := s.Poll(ctx, "", 10); !errors.Is(err, s3sql.ErrRefStreamDisabled) {
		t.Errorf("Poll: got %v, want ErrRefStreamDisabled", err)
	}
	if _, _, err := s.PollRecords(ctx, "", 10); !errors.Is(err, s3sql.ErrRefStreamDisabled) {
		t.Errorf("PollRecords: got %v, want ErrRefStreamDisabled", err)
	}
	if _, err := s.PollRecordsAll(ctx, "", ""); !errors.Is(err, s3sql.ErrRefStreamDisabled) {
		t.Errorf("PollRecordsAll: got %v, want ErrRefStreamDisabled", err)
	}

	// Sentinel is shared across packages.
	if !errors.Is(s3sql.ErrRefStreamDisabled, s3parquet.ErrRefStreamDisabled) {
		t.Error("sentinel mismatch between s3sql and s3parquet")
	}

	// OffsetAt still works.
	if s.OffsetAt(time.Now()) == "" {
		t.Error("OffsetAt: got empty offset, want non-empty")
	}
}
