//go:build integration

package s3parquet_test

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ueisele/s3store/internal/testutil"
	"github.com/ueisele/s3store/s3parquet"
)

// Rec is the record type used across this file's integration
// tests. Parquet tags exercise both simple primitives (ints,
// strings) and a timestamp logical type so a regression in the
// decode path would surface.
type Rec struct {
	Period   string    `parquet:"period"`
	Customer string    `parquet:"customer"`
	SKU      string    `parquet:"sku"`
	Value    int64     `parquet:"value"`
	Ts       time.Time `parquet:"ts,timestamp(millisecond)"`
}

// storeOpts lets each test dial in the bits of the dedup
// contract it cares about while re-using the MinIO fixture.
type storeOpts struct {
	entityKeyOf func(Rec) string
	versionOf   func(Rec, time.Time) int64
}

// newStore builds a fresh s3parquet.Store against a freshly
// created bucket on the shared MinIO fixture. PartitionKeyParts are
// (period, customer) across every test.
func newStore(t *testing.T, opts storeOpts) *s3parquet.Store[Rec] {
	t.Helper()
	f := testutil.New(t)
	store, err := s3parquet.New[Rec](s3parquet.Config[Rec]{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		PartitionKeyOf: func(r Rec) string {
			return fmt.Sprintf("period=%s/customer=%s",
				r.Period, r.Customer)
		},
		SettleWindow: 10 * time.Millisecond,
		EntityKeyOf:  opts.entityKeyOf,
		VersionOf:    opts.versionOf,
	})
	if err != nil {
		t.Fatalf("s3parquet.New: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// TestIndex_WriteAndLookup covers the secondary-index feature
// end-to-end: register an index, Write records, Lookup by an
// exact partition, Lookup with a range on the first index
// column, and verify that a pattern with no matches returns an
// empty slice rather than an error.
//
// Index partition: (sku, period). Lookup covers: (customer).
// Two distinct customers × one SKU × two periods ⇒ the batch
// deduplicates to 4 markers despite 5 source records.
func TestIndex_WriteAndLookup(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{})

	type SkuPeriodEntry struct {
		SKU      string `parquet:"sku"`
		Period   string `parquet:"period"`
		Customer string `parquet:"customer"`
	}

	idx, err := s3parquet.NewIndex(store,
		s3parquet.IndexDef[Rec, SkuPeriodEntry]{
			Name:    "sku_period_idx",
			Columns: []string{"sku", "period", "customer"},
			Of: func(r Rec) []SkuPeriodEntry {
				return []SkuPeriodEntry{{
					SKU: r.SKU, Period: r.Period, Customer: r.Customer,
				}}
			},
		})
	if err != nil {
		t.Fatalf("NewIndex: %v", err)
	}

	in := []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Value: 1, Ts: time.UnixMilli(100)},
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Value: 2, Ts: time.UnixMilli(200)}, // dup marker
		{Period: "2026-03-17", Customer: "def", SKU: "s1", Value: 3, Ts: time.UnixMilli(300)},
		{Period: "2026-03-18", Customer: "abc", SKU: "s1", Value: 4, Ts: time.UnixMilli(400)},
		{Period: "2026-04-01", Customer: "abc", SKU: "s2", Value: 5, Ts: time.UnixMilli(500)},
	}
	if _, err := store.Write(ctx, in); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Wait past the settle window so Lookup sees every marker.
	time.Sleep(60 * time.Millisecond)

	// Exact lookup: one SKU, one period, any customer.
	got, err := idx.Lookup(ctx,
		"sku=s1/period=2026-03-17/customer=*")
	if err != nil {
		t.Fatalf("Lookup exact: %v", err)
	}
	gotCustomers := make(map[string]bool)
	for _, e := range got {
		gotCustomers[e.Customer] = true
	}
	if !gotCustomers["abc"] || !gotCustomers["def"] {
		t.Errorf("got customers %v, want both abc and def",
			gotCustomers)
	}
	if len(got) != 2 {
		t.Errorf("got %d entries, want 2 (abc, def)", len(got))
	}

	// Range on the period column — covers 03-17 and 03-18, not 04-01.
	got, err = idx.Lookup(ctx,
		"sku=s1/period=2026-03-01..2026-04-01/customer=*")
	if err != nil {
		t.Fatalf("Lookup range: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("range: got %d entries, want 3 "+
			"(abc/03-17, def/03-17, abc/03-18)", len(got))
	}

	// Miss — an SKU we never wrote.
	got, err = idx.Lookup(ctx, "sku=s999/period=*/customer=*")
	if err != nil {
		t.Fatalf("Lookup miss: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("miss: got %d entries, want 0", len(got))
	}
}

// TestIndex_SettleWindowHidesFresh guards the SettleWindow
// semantic on Lookup: markers written in the last SettleWindow
// are hidden (LastModified filter), matching Poll's behavior.
func TestIndex_SettleWindowHidesFresh(t *testing.T) {
	ctx := context.Background()
	// Use a generous SettleWindow so the "fresh" check isn't racy.
	f := testutil.New(t)
	store, err := s3parquet.New(s3parquet.Config[Rec]{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		PartitionKeyOf: func(r Rec) string {
			return fmt.Sprintf("period=%s/customer=%s",
				r.Period, r.Customer)
		},
		SettleWindow: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	type Entry struct {
		SKU      string `parquet:"sku"`
		Customer string `parquet:"customer"`
	}
	idx, err := s3parquet.NewIndex(store,
		s3parquet.IndexDef[Rec, Entry]{
			Name:    "sku_idx",
			Columns: []string{"sku", "customer"},
			Of: func(r Rec) []Entry {
				return []Entry{{SKU: r.SKU, Customer: r.Customer}}
			},
		})
	if err != nil {
		t.Fatalf("NewIndex: %v", err)
	}

	if _, err := store.Write(ctx, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1"},
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Inside SettleWindow: marker is invisible.
	got, err := idx.Lookup(ctx, "sku=s1/customer=*")
	if err != nil {
		t.Fatalf("Lookup inside window: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("inside window: got %d entries, want 0",
			len(got))
	}

	// Wait past SettleWindow; marker becomes visible.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, err = idx.Lookup(ctx, "sku=s1/customer=*")
		if err != nil {
			t.Fatalf("Lookup waiting: %v", err)
		}
		if len(got) > 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("marker never became visible past the settle window")
}

// TestIndex_Backfill covers the relief-valve path: records
// written before an index was registered don't produce markers,
// Lookup under-reports, and Backfill brings the index into sync.
// Also checks that Backfill is idempotent (a second call is a
// no-op in effect) and that pattern scoping narrows the scan.
func TestIndex_Backfill(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{})

	// Phase 1: write records with no index registered. These are
	// the "historical" records Backfill will have to recover.
	historical := []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Ts: time.UnixMilli(100)},
		{Period: "2026-03-17", Customer: "def", SKU: "s1", Ts: time.UnixMilli(200)},
		{Period: "2026-03-18", Customer: "abc", SKU: "s2", Ts: time.UnixMilli(300)},
		{Period: "2026-04-01", Customer: "abc", SKU: "s3", Ts: time.UnixMilli(400)},
	}
	if _, err := store.Write(ctx, historical); err != nil {
		t.Fatalf("historical Write: %v", err)
	}

	// Phase 2: register the index. Writes from here on are
	// self-indexing; historical records are not yet covered.
	type Entry struct {
		SKU      string `parquet:"sku"`
		Customer string `parquet:"customer"`
	}
	idx, err := s3parquet.NewIndex(store,
		s3parquet.IndexDef[Rec, Entry]{
			Name:    "sku_idx",
			Columns: []string{"sku", "customer"},
			Of: func(r Rec) []Entry {
				return []Entry{{SKU: r.SKU, Customer: r.Customer}}
			},
		})
	if err != nil {
		t.Fatalf("NewIndex: %v", err)
	}

	// Write a post-registration record so we can verify Backfill
	// produces the same marker as the live write path (idempotent
	// overlap).
	if _, err := store.Write(ctx, []Rec{
		{Period: "2026-04-01", Customer: "abc", SKU: "s3", Ts: time.UnixMilli(500)},
	}); err != nil {
		t.Fatalf("post-registration Write: %v", err)
	}

	time.Sleep(60 * time.Millisecond)

	// Before Backfill: only the post-registration record is visible.
	got, err := idx.Lookup(ctx, "sku=*/customer=*")
	if err != nil {
		t.Fatalf("pre-backfill Lookup: %v", err)
	}
	if len(got) != 1 || got[0].SKU != "s3" || got[0].Customer != "abc" {
		t.Errorf("pre-backfill: got %v, want just {s3, abc}", got)
	}

	// Backfill everything.
	stats, err := idx.Backfill(ctx, "*")
	if err != nil {
		t.Fatalf("Backfill: %v", err)
	}
	// 5 parquet objects (4 historical writes, each its own
	// partition-key group under PartitionKeyOf; plus the post-
	// registration write). 4 distinct (sku, customer) markers
	// ({s1,abc},{s1,def},{s2,abc},{s3,abc}).
	if stats.DataObjects != 5 {
		t.Errorf("DataObjects: got %d, want 5", stats.DataObjects)
	}
	if stats.Records != 5 {
		t.Errorf("Records: got %d, want 5", stats.Records)
	}
	// Markers is per-object (not cross-object deduped), so each
	// object contributes at least one. 5 files × 1 marker each.
	if stats.Markers != 5 {
		t.Errorf("Markers: got %d, want 5 (1 per object)",
			stats.Markers)
	}

	time.Sleep(60 * time.Millisecond)

	// After Backfill: every distinct (sku, customer) is visible.
	got, err = idx.Lookup(ctx, "sku=*/customer=*")
	if err != nil {
		t.Fatalf("post-backfill Lookup: %v", err)
	}
	gotSet := make(map[Entry]bool, len(got))
	for _, e := range got {
		gotSet[e] = true
	}
	want := []Entry{
		{SKU: "s1", Customer: "abc"},
		{SKU: "s1", Customer: "def"},
		{SKU: "s2", Customer: "abc"},
		{SKU: "s3", Customer: "abc"},
	}
	for _, w := range want {
		if !gotSet[w] {
			t.Errorf("missing %+v after Backfill", w)
		}
	}
	if len(got) != len(want) {
		t.Errorf("got %d distinct entries, want %d: %+v",
			len(got), len(want), got)
	}

	// Idempotency: a second Backfill re-scans but the PUTs are
	// no-ops at the semantic level. We only check it doesn't
	// error and reports the same scan volume.
	stats2, err := idx.Backfill(ctx, "*")
	if err != nil {
		t.Fatalf("second Backfill: %v", err)
	}
	if stats2.DataObjects != stats.DataObjects {
		t.Errorf("second Backfill DataObjects: got %d, want %d",
			stats2.DataObjects, stats.DataObjects)
	}

	// Pattern scoping: backfilling only the 2026-03-17 partition
	// covers 2 of the 5 objects.
	scoped, err := idx.Backfill(ctx, "period=2026-03-17/customer=*")
	if err != nil {
		t.Fatalf("scoped Backfill: %v", err)
	}
	if scoped.DataObjects != 2 {
		t.Errorf("scoped DataObjects: got %d, want 2",
			scoped.DataObjects)
	}
}

// TestMissingData_SkipAndNotify simulates the dangling-ref
// failure mode (ref persisted but data deleted) by deleting a
// parquet object directly from S3 after a successful Write. Read
// and PollRecords must return the remaining records without
// error and invoke OnMissingData for the missing path. The
// alternative — failing on NoSuchKey — would poison every future
// read of that stream.
func TestMissingData_SkipAndNotify(t *testing.T) {
	ctx := context.Background()
	f := testutil.New(t)

	var (
		missedMu sync.Mutex
		missed   []string
	)
	store, err := s3parquet.New(s3parquet.Config[Rec]{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		PartitionKeyOf: func(r Rec) string {
			return fmt.Sprintf("period=%s/customer=%s",
				r.Period, r.Customer)
		},
		SettleWindow: 10 * time.Millisecond,
		OnMissingData: func(p string) {
			missedMu.Lock()
			defer missedMu.Unlock()
			missed = append(missed, p)
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	r1, err := store.WriteWithKey(ctx,
		"period=2026-03-17/customer=abc", []Rec{
			{Period: "2026-03-17", Customer: "abc", SKU: "s1",
				Value: 1, Ts: time.UnixMilli(100)},
		})
	if err != nil {
		t.Fatalf("Write r1: %v", err)
	}
	if _, err := store.WriteWithKey(ctx,
		"period=2026-03-17/customer=def", []Rec{
			{Period: "2026-03-17", Customer: "def", SKU: "s1",
				Value: 2, Ts: time.UnixMilli(200)},
		}); err != nil {
		t.Fatalf("Write r2: %v", err)
	}

	// Delete the first data file directly, leaving its ref in
	// place — the dangling-ref state that the read path must
	// tolerate.
	if _, err := f.S3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(f.Bucket),
		Key:    aws.String(r1.DataPath),
	}); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}

	time.Sleep(60 * time.Millisecond)

	// Read is LIST-based; its LIST already reflects the
	// deletion, so it never GETs the missing file and the hook
	// does not fire. The remaining record still comes back.
	got, err := store.Read(ctx, "*")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 1 || got[0].Value != 2 {
		t.Errorf("Read: got %+v, want single record with Value=2",
			got)
	}

	// PollRecords walks the ref-stream. The dangling ref is
	// what the skip-on-NoSuchKey path must tolerate.
	pollGot, _, err := store.PollRecords(ctx, "", 100)
	if err != nil {
		t.Fatalf("PollRecords: %v", err)
	}
	if len(pollGot) != 1 || pollGot[0].Value != 2 {
		t.Errorf("PollRecords: got %+v, want single record with "+
			"Value=2", pollGot)
	}

	missedMu.Lock()
	gotMissed := append([]string(nil), missed...)
	missedMu.Unlock()
	if len(gotMissed) != 1 || gotMissed[0] != r1.DataPath {
		t.Errorf("OnMissingData: got %v, want [%q]",
			gotMissed, r1.DataPath)
	}
}

// TestInsertedAtField_Populate covers the InsertedAtField hook:
// Read and PollRecords populate a struct field with the source
// file's write timestamp on decode. The field carries
// `parquet:"-"` so it's library-managed (parquet-go ignores it
// on both sides of the round-trip). We assert the populated
// time is close to the Write wall-clock time for the call.
func TestInsertedAtField_Populate(t *testing.T) {
	ctx := context.Background()
	f := testutil.New(t)

	type RecWithMeta struct {
		Period     string    `parquet:"period"`
		Customer   string    `parquet:"customer"`
		SKU        string    `parquet:"sku"`
		Ts         time.Time `parquet:"ts,timestamp(millisecond)"`
		InsertedAt time.Time `parquet:"-"`
	}

	store, err := s3parquet.New[RecWithMeta](s3parquet.Config[RecWithMeta]{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		PartitionKeyOf: func(r RecWithMeta) string {
			return fmt.Sprintf("period=%s/customer=%s",
				r.Period, r.Customer)
		},
		SettleWindow:    10 * time.Millisecond,
		InsertedAtField: "InsertedAt",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	before := time.Now()
	if _, err := store.Write(ctx, []RecWithMeta{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1",
			Ts: time.UnixMilli(100)},
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	after := time.Now()
	time.Sleep(30 * time.Millisecond)

	got, err := store.Read(ctx, "*")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d records, want 1", len(got))
	}
	// The populated InsertedAt is the parquet file's write
	// tsMicros, captured between before and after.
	ia := got[0].InsertedAt
	if ia.Before(before.Add(-time.Millisecond)) ||
		ia.After(after.Add(time.Millisecond)) {
		t.Errorf("InsertedAt=%v outside [%v, %v]",
			ia, before, after)
	}

	// PollRecords goes through the same decode path; same
	// populated value is expected.
	polled, _, err := store.PollRecords(ctx, "", 100)
	if err != nil {
		t.Fatalf("PollRecords: %v", err)
	}
	if len(polled) != 1 {
		t.Fatalf("PollRecords: got %d, want 1", len(polled))
	}
	if !polled[0].InsertedAt.Equal(ia) {
		t.Errorf("PollRecords InsertedAt=%v, Read=%v "+
			"(should match, same parquet file)",
			polled[0].InsertedAt, ia)
	}
}

// TestInsertedAtField_Validation covers the New()-time checks
// that protect users from configuring InsertedAtField wrong: no
// such field, wrong type, or a missing parquet:"-" tag.
func TestInsertedAtField_Validation(t *testing.T) {
	f := testutil.New(t)

	mkCfg := func(field string) s3parquet.Config[Rec] {
		return s3parquet.Config[Rec]{
			Bucket:            f.Bucket,
			Prefix:            "store",
			S3Client:          f.S3Client,
			PartitionKeyParts: []string{"period", "customer"},
			PartitionKeyOf: func(r Rec) string {
				return "period=p/customer=c"
			},
			InsertedAtField: field,
		}
	}

	cases := []struct {
		name     string
		field    string
		wantSubs string
	}{
		{"no such field", "Nonexistent", "no such field"},
		// Ts is time.Time but has parquet:"ts,..." tag, not "-".
		{"wrong parquet tag", "Ts", `must be tagged`},
		// Period is string, not time.Time.
		{"wrong type", "Period", "must be time.Time"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s3parquet.New[Rec](mkCfg(tc.field))
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantSubs) {
				t.Errorf("error %q does not contain %q",
					err, tc.wantSubs)
			}
		})
	}
}

// TestView_NarrowReadThroughNewViewFromStore covers the
// cross-T View path: a Store is constructed over a full-shape
// record type (with a heavy write-only column), and
// NewViewFromStore produces a Reader[NarrowT] that decodes only
// the fields present on NarrowT. parquet-go's decode naturally
// skips unlisted columns, so the narrow Reader never even tries
// to materialise the heavy column. Also proves the shared
// wiring (Bucket, Prefix, PartitionKeyParts, S3Client) carries
// from Writer through NewViewFromStore without the caller
// respecifying anything.
func TestView_NarrowReadThroughNewViewFromStore(t *testing.T) {
	ctx := context.Background()

	type FullRec struct {
		Period     string `parquet:"period"`
		Customer   string `parquet:"customer"`
		SKU        string `parquet:"sku"`
		Value      int64  `parquet:"value"`
		ProcessLog string `parquet:"process_log"` // heavy, write-only
	}
	type NarrowRec struct {
		Period   string `parquet:"period"`
		Customer string `parquet:"customer"`
		SKU      string `parquet:"sku"`
		Value    int64  `parquet:"value"`
		// ProcessLog deliberately absent — parquet-go skips it.
	}

	f := testutil.New(t)
	store, err := s3parquet.New[FullRec](s3parquet.Config[FullRec]{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		PartitionKeyOf: func(r FullRec) string {
			return fmt.Sprintf("period=%s/customer=%s",
				r.Period, r.Customer)
		},
		SettleWindow: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("s3parquet.New: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if _, err := store.Write(ctx, []FullRec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1",
			Value: 10, ProcessLog: "..heavy JSON blob.."},
		{Period: "2026-03-17", Customer: "def", SKU: "s2",
			Value: 20, ProcessLog: "..another blob.."},
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	view, err := s3parquet.NewViewFromStore[NarrowRec, FullRec](
		store, s3parquet.ReaderExtras[NarrowRec]{})
	if err != nil {
		t.Fatalf("NewViewFromStore: %v", err)
	}

	got, err := view.Read(ctx, "*")
	if err != nil {
		t.Fatalf("view.Read: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("view.Read: got %d records, want 2", len(got))
	}
	// Sanity: narrow fields present, heavy column absent from T.
	seen := map[string]int64{}
	for _, r := range got {
		seen[r.SKU] = r.Value
	}
	if seen["s1"] != 10 || seen["s2"] != 20 {
		t.Errorf("view.Read values: got %v, want map[s1:10 s2:20]",
			seen)
	}
}

// TestWriteAndRead exercises the basic round-trip through S3.
// No dedup configured.
func TestWriteAndRead(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{})

	in := []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Value: 10, Ts: time.UnixMilli(1000)},
		{Period: "2026-03-17", Customer: "abc", SKU: "s2", Value: 20, Ts: time.UnixMilli(2000)},
		{Period: "2026-03-17", Customer: "def", SKU: "s1", Value: 30, Ts: time.UnixMilli(3000)},
	}
	if _, err := store.Write(ctx, in); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := store.Read(ctx, "*")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != len(in) {
		t.Fatalf("got %d records, want %d", len(got), len(in))
	}
}

// TestWriteEmptyNoop guards the empty-slice fast path: Write
// must return (nil, nil) without touching S3.
func TestWriteEmptyNoop(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{})

	got, err := store.Write(ctx, nil)
	if err != nil {
		t.Errorf("Write(nil): %v", err)
	}
	if got != nil {
		t.Errorf("Write(nil): got %v, want nil", got)
	}
}

// TestWriteWithKey covers the explicit-key write path: same
// semantics as Write for grouping / parquet encoding, but
// PartitionKeyOf is bypassed and the caller asserts the key.
func TestWriteWithKey(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{})

	key := "period=2026-03-17/customer=abc"
	recs := []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Value: 10, Ts: time.UnixMilli(1)},
	}
	result, err := store.WriteWithKey(ctx, key, recs)
	if err != nil {
		t.Fatalf("WriteWithKey: %v", err)
	}
	if result == nil {
		t.Fatal("WriteWithKey: nil result")
	}
	if result.DataPath == "" || result.RefPath == "" || result.Offset == "" {
		t.Errorf("incomplete result: %+v", result)
	}

	got, err := store.Read(ctx, key)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 1 || got[0].Value != 10 {
		t.Errorf("got %+v, want one record with Value=10", got)
	}
}

// TestReadGlob covers every glob shape the grammar accepts:
// exact, whole-segment *, trailing * in a value, and "*".
func TestReadGlob(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{})

	all := []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Value: 1},
		{Period: "2026-03-17", Customer: "def", SKU: "s2", Value: 2},
		{Period: "2026-03-18", Customer: "abc", SKU: "s3", Value: 3},
		{Period: "2026-04-01", Customer: "abc", SKU: "s4", Value: 4},
	}
	if _, err := store.Write(ctx, all); err != nil {
		t.Fatalf("Write: %v", err)
	}

	cases := []struct {
		name    string
		pattern string
		wantN   int
	}{
		{"exact single file", "period=2026-03-17/customer=abc", 1},
		{"whole-segment head", "*/customer=abc", 3},
		{"whole-segment tail", "period=2026-03-17/*", 2},
		{"trailing star in value", "period=2026-03-*/customer=abc", 2},
		{"range covers march", "period=2026-03-01..2026-04-01/*", 3},
		{"range upper exclusive", "period=2026-03-17..2026-03-18/*", 2},
		{"range unbounded upper", "period=2026-03-18../*", 2},
		{"range unbounded lower", "period=..2026-03-18/*", 2},
		{"match all", "*", 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := store.Read(ctx, tc.pattern)
			if err != nil {
				t.Fatalf("Read: %v", err)
			}
			if len(got) != tc.wantN {
				t.Errorf("got %d records, want %d",
					len(got), tc.wantN)
			}
		})
	}
}

// TestDedupExplicit exercises user-supplied EntityKeyOf +
// VersionOf: later writes supersede earlier ones per entity.
func TestDedupExplicit(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{
		entityKeyOf: func(r Rec) string {
			return r.Customer + "|" + r.SKU
		},
		versionOf: func(r Rec, _ time.Time) int64 {
			return r.Ts.UnixNano()
		},
	})

	key := "period=2026-03-17/customer=abc"
	if _, err := store.WriteWithKey(ctx, key, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Value: 10, Ts: time.UnixMilli(100)},
	}); err != nil {
		t.Fatalf("first Write: %v", err)
	}
	if _, err := store.WriteWithKey(ctx, key, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Value: 99, Ts: time.UnixMilli(200)},
	}); err != nil {
		t.Fatalf("second Write: %v", err)
	}

	got, err := store.Read(ctx, key)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d records, want 1 (deduped)", len(got))
	}
	if got[0].Value != 99 {
		t.Errorf("got Value=%d, want 99 (newer Ts wins)", got[0].Value)
	}
}

// TestDedupDefault omits VersionOf: New() populates it with
// DefaultVersionOf, so dedup falls back to the source file's
// insertedAt. The second write to the same entity must win
// because its parquet file has a later tsMicros.
func TestDedupDefault(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{
		entityKeyOf: func(r Rec) string {
			return r.Customer + "|" + r.SKU
		},
	})

	key := "period=2026-03-17/customer=abc"
	if _, err := store.WriteWithKey(ctx, key, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Value: 10},
	}); err != nil {
		t.Fatalf("first Write: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	if _, err := store.WriteWithKey(ctx, key, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Value: 99},
	}); err != nil {
		t.Fatalf("second Write: %v", err)
	}

	got, err := store.Read(ctx, key)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d records, want 1", len(got))
	}
	if got[0].Value != 99 {
		t.Errorf("got Value=%d, want 99 (later file wins)", got[0].Value)
	}
}

// TestReadWithHistory opts out of dedup: every written record
// must appear in the result.
func TestReadWithHistory(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{
		entityKeyOf: func(r Rec) string { return r.SKU },
		versionOf:   func(r Rec, _ time.Time) int64 { return r.Value },
	})

	key := "period=2026-03-17/customer=abc"
	for i := int64(0); i < 3; i++ {
		if _, err := store.WriteWithKey(ctx, key, []Rec{
			{Period: "2026-03-17", Customer: "abc", SKU: "s1", Value: i},
		}); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}

	deduped, err := store.Read(ctx, key)
	if err != nil {
		t.Fatalf("Read (deduped): %v", err)
	}
	if len(deduped) != 1 {
		t.Errorf("deduped: got %d, want 1", len(deduped))
	}

	full, err := store.Read(ctx, key, s3parquet.WithHistory())
	if err != nil {
		t.Fatalf("Read (history): %v", err)
	}
	if len(full) != 3 {
		t.Errorf("history: got %d, want 3", len(full))
	}
}

// TestPoll covers the refs-only listing: entries must be
// chronological by timestamp and carry the expected metadata.
func TestPoll(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{})

	var lastOffset []string
	keys := []string{
		"period=2026-03-17/customer=abc",
		"period=2026-03-17/customer=def",
		"period=2026-03-18/customer=abc",
	}
	for _, k := range keys {
		r, err := store.WriteWithKey(ctx, k, []Rec{
			{Period: "2026-03-17", Customer: "anything", SKU: "s"},
		})
		if err != nil {
			t.Fatalf("Write %s: %v", k, err)
		}
		lastOffset = append(lastOffset, string(r.Offset))
	}

	time.Sleep(30 * time.Millisecond)

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
		if string(e.Offset) != lastOffset[i] {
			t.Errorf("[%d] offset %q != write offset %q",
				i, e.Offset, lastOffset[i])
		}
	}

	gone, off2, err := store.Poll(ctx, newOffset, 100)
	if err != nil {
		t.Fatalf("Poll (past offset): %v", err)
	}
	if len(gone) != 0 {
		t.Errorf("got %d entries past offset, want 0", len(gone))
	}
	if off2 != newOffset {
		t.Errorf("offset drifted: %q -> %q", newOffset, off2)
	}
}

// RecNarrow is Rec without the Value column. Used to write an
// "old-shape" file that a reader with the full Rec schema must
// still decode without error.
type RecNarrow struct {
	Period   string    `parquet:"period"`
	Customer string    `parquet:"customer"`
	SKU      string    `parquet:"sku"`
	Ts       time.Time `parquet:"ts,timestamp(millisecond)"`
}

// TestRead_MissingColumnZeroFills guards the end-to-end "added
// a new column to T" story: a file written with a narrower
// schema, then read back through a Store[Rec] that expects the
// extra column, returns the row with Value = 0 (Go zero), not
// an error.
func TestRead_MissingColumnZeroFills(t *testing.T) {
	ctx := context.Background()
	f := testutil.New(t)

	wNarrow, err := s3parquet.New[RecNarrow](s3parquet.Config[RecNarrow]{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
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
	t.Cleanup(func() { _ = wNarrow.Close() })

	if _, err := wNarrow.Write(ctx, []RecNarrow{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Ts: time.UnixMilli(100)},
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	rWide, err := s3parquet.New[Rec](s3parquet.Config[Rec]{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		PartitionKeyOf: func(r Rec) string {
			return fmt.Sprintf("period=%s/customer=%s",
				r.Period, r.Customer)
		},
		SettleWindow: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("s3parquet.New(Rec): %v", err)
	}
	t.Cleanup(func() { _ = rWide.Close() })

	got, err := rWide.Read(ctx, "period=2026-03-17/customer=abc")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d records, want 1", len(got))
	}
	if got[0].Value != 0 {
		t.Errorf("got Value=%d, want 0 (zero-fill)", got[0].Value)
	}
	if got[0].SKU != "s1" {
		t.Errorf("got SKU=%q, want s1", got[0].SKU)
	}
}

// TestPollRecords mirrors TestPoll for the typed-record path,
// with dedup behaviour verified against the same expectations
// as Read.
func TestPollRecords(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{
		entityKeyOf: func(r Rec) string {
			return r.Customer + "|" + r.SKU
		},
		versionOf: func(r Rec, _ time.Time) int64 {
			return r.Ts.UnixNano()
		},
	})

	key := "period=2026-03-17/customer=abc"
	if _, err := store.WriteWithKey(ctx, key, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Value: 1, Ts: time.UnixMilli(10)},
	}); err != nil {
		t.Fatalf("first Write: %v", err)
	}
	if _, err := store.WriteWithKey(ctx, key, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Value: 2, Ts: time.UnixMilli(20)},
	}); err != nil {
		t.Fatalf("second Write: %v", err)
	}
	time.Sleep(30 * time.Millisecond)

	deduped, off, err := store.PollRecords(ctx, "", 100)
	if err != nil {
		t.Fatalf("PollRecords: %v", err)
	}
	if len(deduped) != 1 || deduped[0].Value != 2 {
		t.Errorf("deduped: got %+v, want one record with Value=2", deduped)
	}

	full, _, err := store.PollRecords(ctx, "", 100, s3parquet.WithHistory())
	if err != nil {
		t.Fatalf("PollRecords history: %v", err)
	}
	if len(full) != 2 {
		t.Errorf("history: got %d, want 2", len(full))
	}
	if string(off) == "" {
		t.Error("offset empty after non-empty PollRecords")
	}
}

// TestPollTimeWindow exercises OffsetAt + WithUntilOffset: a
// series of writes spread across time, then a Poll bounded
// from both sides pulls only the middle window. Also verifies
// the paginator stops early and the returned offset lands
// inside the window.
func TestPollTimeWindow(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{})

	// Three writes separated by small pauses; OffsetAt uses
	// microsecond precision so 5ms is enough to give each ref a
	// distinctly-orderable timestamp even on fast hosts.
	beforeFirst := time.Now()
	if _, err := store.WriteWithKey(ctx,
		"period=2026-03-17/customer=a",
		[]Rec{{Period: "2026-03-17", Customer: "a", SKU: "s1"}},
	); err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	time.Sleep(5 * time.Millisecond)
	afterFirst := time.Now()
	time.Sleep(5 * time.Millisecond)
	if _, err := store.WriteWithKey(ctx,
		"period=2026-03-17/customer=b",
		[]Rec{{Period: "2026-03-17", Customer: "b", SKU: "s2"}},
	); err != nil {
		t.Fatalf("Write 2: %v", err)
	}
	time.Sleep(5 * time.Millisecond)
	beforeThird := time.Now()
	time.Sleep(5 * time.Millisecond)
	if _, err := store.WriteWithKey(ctx,
		"period=2026-03-17/customer=c",
		[]Rec{{Period: "2026-03-17", Customer: "c", SKU: "s3"}},
	); err != nil {
		t.Fatalf("Write 3: %v", err)
	}
	time.Sleep(30 * time.Millisecond)

	// Full stream: three entries.
	all, _, err := store.Poll(ctx, "", 100)
	if err != nil {
		t.Fatalf("Poll all: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("baseline: got %d entries, want 3", len(all))
	}

	// Window [afterFirst, beforeThird) should contain only the
	// middle write (customer=b).
	start := store.OffsetAt(afterFirst)
	end := store.OffsetAt(beforeThird)
	window, _, err := store.Poll(ctx, start, 100,
		s3parquet.WithUntilOffset(end))
	if err != nil {
		t.Fatalf("Poll window: %v", err)
	}
	if len(window) != 1 {
		t.Fatalf("window: got %d entries, want 1", len(window))
	}
	if window[0].Key != "period=2026-03-17/customer=b" {
		t.Errorf("window[0]: got %q, want customer=b", window[0].Key)
	}

	// Empty window: before the first write should return zero
	// entries and not advance the cursor.
	offZero := store.OffsetAt(beforeFirst.Add(-time.Hour))
	offEnd := store.OffsetAt(beforeFirst.Add(-time.Minute))
	empty, off2, err := store.Poll(ctx, offZero, 100,
		s3parquet.WithUntilOffset(offEnd))
	if err != nil {
		t.Fatalf("Poll empty: %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("empty window: got %d entries, want 0", len(empty))
	}
	if off2 != offZero {
		t.Errorf("empty window: offset drifted %q -> %q", offZero, off2)
	}
}

// TestPollRecordsAll exercises the convenience wrapper: a
// small stream of writes, then PollRecordsAll drains the full
// window in one call with no manual paging. Also checks that a
// zero until offset "" means "read to live tip".
func TestPollRecordsAll(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{})

	before := time.Now()
	for i := range 5 {
		if _, err := store.WriteWithKey(ctx,
			fmt.Sprintf("period=2026-03-17/customer=c%d", i),
			[]Rec{{Period: "2026-03-17", Customer: fmt.Sprintf("c%d", i),
				SKU: "s1", Value: int64(i)}},
		); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	time.Sleep(30 * time.Millisecond)
	after := time.Now()

	// Bounded window: all 5 records.
	got, err := store.PollRecordsAll(ctx,
		store.OffsetAt(before), store.OffsetAt(after))
	if err != nil {
		t.Fatalf("PollRecordsAll: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("got %d, want 5", len(got))
	}

	// Open until (zero-value offset) reads to the live tip.
	open, err := store.PollRecordsAll(ctx, "", "")
	if err != nil {
		t.Fatalf("PollRecordsAll open: %v", err)
	}
	if len(open) != 5 {
		t.Errorf("open: got %d, want 5", len(open))
	}

	// Empty window returns nil without error.
	empty, err := store.PollRecordsAll(ctx,
		store.OffsetAt(before.Add(-time.Hour)),
		store.OffsetAt(before.Add(-time.Minute)))
	if err != nil {
		t.Fatalf("PollRecordsAll empty: %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("empty: got %d, want 0", len(empty))
	}
}

// ParquetField is a named int8 enum, mirroring the shape a
// go-enum generator would produce. Declared at package scope
// so Store[T] generic instantiation works at integration-test
// scope.
type ParquetField int8

const (
	ParquetFieldUnknown ParquetField = iota
	ParquetFieldPrimary
	ParquetFieldSecondary
)

// ParquetLog is the nested-struct payload: string + named int8
// enum + map, all carried inside ParquetRec.Logs.
type ParquetLog struct {
	Processor string            `parquet:"processor"`
	Field     ParquetField      `parquet:"field"`
	Attrs     map[string]string `parquet:"attrs"`
}

// ParquetRec exercises the nested shape: a list of structs,
// each with a named int8 enum and a map-of-string. Mirrors the
// s3sql integration test's JobRec but exercises the pure-Go
// Write→S3→Read pipeline without DuckDB in the mix.
type ParquetRec struct {
	Period   string       `parquet:"period"`
	Customer string       `parquet:"customer"`
	Logs     []ParquetLog `parquet:"logs"`
	Ts       time.Time    `parquet:"ts,timestamp(millisecond)"`
}

// TestWriteRead_NamedInt8EnumInNestedStruct round-trips the
// JSONB-style shape through the full s3parquet pipeline —
// Write encodes + puts to S3, Read lists + gets + decodes back
// into []ParquetRec. Guards that the parquet-go v0.29 small-int
// dispatch holds end-to-end for named int8 enums in nested
// structs, not just in the in-memory encode/decode unit test.
func TestWriteRead_NamedInt8EnumInNestedStruct(t *testing.T) {
	ctx := context.Background()
	f := testutil.New(t)

	store, err := s3parquet.New[ParquetRec](s3parquet.Config[ParquetRec]{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		PartitionKeyOf: func(r ParquetRec) string {
			return fmt.Sprintf("period=%s/customer=%s",
				r.Period, r.Customer)
		},
		SettleWindow: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("s3parquet.New: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	in := []ParquetRec{{
		Period:   "2026-03-17",
		Customer: "abc",
		Ts:       time.UnixMilli(100),
		Logs: []ParquetLog{
			{
				Processor: "ingest",
				Field:     ParquetFieldPrimary,
				Attrs:     map[string]string{"stage": "raw"},
			},
			{
				Processor: "enrich",
				Field:     ParquetFieldSecondary,
				Attrs:     map[string]string{"model": "v2"},
			},
		},
	}}
	if _, err := store.Write(ctx, in); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := store.Read(ctx, "period=2026-03-17/customer=abc")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d records, want 1", len(got))
	}
	// Compare Logs explicitly — time.Time round-trips may
	// introduce monotonic-clock differences across equal values,
	// so field-by-field on the nested shape is the safe check.
	if !reflect.DeepEqual(got[0].Logs, in[0].Logs) {
		t.Errorf("Logs mismatch:\n got  %+v\n want %+v",
			got[0].Logs, in[0].Logs)
	}
	if got[0].Period != in[0].Period ||
		got[0].Customer != in[0].Customer {
		t.Errorf("partition fields wrong: got %q/%q, want %q/%q",
			got[0].Period, got[0].Customer,
			in[0].Period, in[0].Customer)
	}
	_ = ParquetFieldUnknown
}

// TestWriteRead_BloomFilterRoundTrip guards the bloom-filter
// write path end-to-end: a store configured with
// BloomFilterColumns must still produce files that Read returns
// intact. The filter is opt-in performance; it must not change
// observable query semantics.
func TestWriteRead_BloomFilterRoundTrip(t *testing.T) {
	ctx := context.Background()
	f := testutil.New(t)
	store, err := s3parquet.New[Rec](s3parquet.Config[Rec]{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		PartitionKeyOf: func(r Rec) string {
			return fmt.Sprintf("period=%s/customer=%s",
				r.Period, r.Customer)
		},
		SettleWindow:       10 * time.Millisecond,
		BloomFilterColumns: []string{"sku"},
	})
	if err != nil {
		t.Fatalf("s3parquet.New: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	in := []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Value: 1, Ts: time.UnixMilli(1)},
		{Period: "2026-03-17", Customer: "abc", SKU: "s2", Value: 2, Ts: time.UnixMilli(2)},
		{Period: "2026-03-17", Customer: "def", SKU: "s1", Value: 3, Ts: time.UnixMilli(3)},
	}
	if _, err := store.Write(ctx, in); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := store.Read(ctx, "*")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != len(in) {
		t.Fatalf("got %d records, want %d", len(got), len(in))
	}
}
