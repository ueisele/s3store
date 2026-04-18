//go:build integration

package s3sql_test

import (
	"context"
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
	versionColumn string
	dedupBy       []string
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
		TableAlias:        "records",
		VersionColumn:     opts.versionColumn,
		DeduplicateBy:     opts.dedupBy,
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
	wOld, err := s3parquet.New[RecNarrow](s3parquet.Config[RecNarrow]{
		Bucket:            testFix.Bucket,
		Prefix:            "store",
		S3Client:          testFix.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		PartitionKeyOf: func(r RecNarrow) string {
			return fmt.Sprintf("period=%s/customer=%s",
				r.Period, r.Customer)
		},
		SettleWindow: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("s3parquet.New(RecNarrow): %v", err)
	}
	t.Cleanup(func() { _ = wOld.Close() })
	if _, err := wOld.Write(context.Background(), []RecNarrow{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Ts: time.UnixMilli(100)},
	}); err != nil {
		t.Fatalf("Write RecNarrow: %v", err)
	}
	time.Sleep(30 * time.Millisecond)

	// Reader scans into Rec (which has Amount + Currency).
	s, err := s3sql.New[Rec](s3sql.Config[Rec]{
		Bucket:            testFix.Bucket,
		Prefix:            "store",
		S3Client:          testFix.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		TableAlias:        "records",
		SettleWindow:      10 * time.Millisecond,
		ExtraInitSQL:      testFix.DuckDBCredentials(),
	})
	if err != nil {
		t.Fatalf("s3sql.New: %v", err)
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

	w, err := s3parquet.New[TagRec](s3parquet.Config[TagRec]{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		PartitionKeyOf: func(r TagRec) string {
			return fmt.Sprintf("period=%s/customer=%s",
				r.Period, r.Customer)
		},
		SettleWindow: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("s3parquet.New: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

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

	s, err := s3sql.New[TagRec](s3sql.Config[TagRec]{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		TableAlias:        "records",
		SettleWindow:      10 * time.Millisecond,
		ExtraInitSQL:      f.DuckDBCredentials(),
	})
	if err != nil {
		t.Fatalf("s3sql.New: %v", err)
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

	w, err := s3parquet.New[JobRec](s3parquet.Config[JobRec]{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		PartitionKeyOf: func(r JobRec) string {
			return fmt.Sprintf("period=%s/customer=%s",
				r.Period, r.Customer)
		},
		SettleWindow: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("s3parquet.New: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

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

	s, err := s3sql.New[JobRec](s3sql.Config[JobRec]{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		TableAlias:        "records",
		SettleWindow:      10 * time.Millisecond,
		ExtraInitSQL:      f.DuckDBCredentials(),
	})
	if err != nil {
		t.Fatalf("s3sql.New: %v", err)
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
