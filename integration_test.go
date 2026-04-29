//go:build integration

package s3store

import (
	"context"
	"errors"
	"fmt"
	"path"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
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
	versionOf   func(Rec) int64
	projections []ProjectionDef[Rec]
}

// testCommitTimeout is the CommitTimeout integration tests seed
// before constructing a Store. Has to clear MinIO's per-second
// LastModified granularity (HEAD returns RFC 1123 second-precision
// values; LIST is truncated to seconds at the commit-marker layer
// for cross-source consistency — see truncLMToSecond), so two
// PUTs straddling a wall-clock second boundary appear 1s apart
// even when they completed in milliseconds. 2s leaves headroom
// for the post-marker timeliness check (marker.LM - data.LM <
// CommitTimeout) on the worst-case spanning case while still
// keeping settle-window sleeps short.
const testCommitTimeout = 2 * time.Second

// testMaxClockSkew is the MaxClockSkew integration tests seed.
// Localhost has ~microsecond skew between processes; 100ms is
// generous headroom so a slightly contended scheduler tick
// doesn't trip refCutoff.
const testMaxClockSkew = 100 * time.Millisecond

// testSettleWindow is the derived sum testCommitTimeout +
// testMaxClockSkew. Used by the sleep-past-settle-window call
// sites; same role as the value the Target stamps via SettleWindow().
const testSettleWindow = testCommitTimeout + testMaxClockSkew

// newStore builds a fresh Store against a freshly
// created bucket on the shared MinIO fixture. PartitionKeyParts are
// (period, customer) across every test.
func newStore(t *testing.T, opts storeOpts) *Store[Rec] {
	t.Helper()
	f := newFixture(t)
	f.SeedTimingConfig(t, "store", testCommitTimeout, testMaxClockSkew)
	store, err := New[Rec](t.Context(), Config[Rec]{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		PartitionKeyOf: func(r Rec) string {
			return fmt.Sprintf("period=%s/customer=%s",
				r.Period, r.Customer)
		},
		ConsistencyControl: ConsistencyStrongGlobal,
		EntityKeyOf:        opts.entityKeyOf,
		VersionOf:          opts.versionOf,
		Projections:        opts.projections,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return store
}

// TestProjection_WriteAndLookup covers the secondary-projection
// feature end-to-end: register a projection, Write records,
// Lookup by an exact partition, Lookup with a range on the first
// projection column, and verify that a pattern with no matches
// returns an empty slice rather than an error.
//
// Projection partition: (sku, period). Lookup covers: (customer).
// Two distinct customers × one SKU × two periods ⇒ the batch
// deduplicates to 4 markers despite 5 source records.
func TestProjection_WriteAndLookup(t *testing.T) {
	ctx := context.Background()

	type SkuPeriodEntry struct {
		SKU      string `parquet:"sku"`
		Period   string `parquet:"period"`
		Customer string `parquet:"customer"`
	}

	store := newStore(t, storeOpts{
		projections: []ProjectionDef[Rec]{{
			Name:    "sku_period_idx",
			Columns: []string{"sku", "period", "customer"},
			Of: func(r Rec) ([]string, error) {
				return []string{r.SKU, r.Period, r.Customer}, nil
			},
		}},
	})

	idx, err := NewProjectionReader(store.Target(),
		ProjectionLookupDef[SkuPeriodEntry]{
			Name:    "sku_period_idx",
			Columns: []string{"sku", "period", "customer"},
		})
	if err != nil {
		t.Fatalf("NewProjectionReader: %v", err)
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

	// Exact lookup: one SKU, one period, any customer.
	got, err := idx.Lookup(ctx, []string{
		"sku=s1/period=2026-03-17/customer=*"})
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
	got, err = idx.Lookup(ctx, []string{
		"sku=s1/period=2026-03-01..2026-04-01/customer=*"})
	if err != nil {
		t.Fatalf("Lookup range: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("range: got %d entries, want 3 "+
			"(abc/03-17, def/03-17, abc/03-18)", len(got))
	}

	// Miss — an SKU we never wrote.
	got, err = idx.Lookup(ctx, []string{"sku=s999/period=*/customer=*"})
	if err != nil {
		t.Fatalf("Lookup miss: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("miss: got %d entries, want 0", len(got))
	}
}

// TestProjection_LookupReadAfterWrite guards the contract that
// Lookup is read-after-write when ConsistencyControl is strong: a
// marker written by Write MUST be returned by the very next
// Lookup, with no sleep and no SettleWindow filter. Together with
// the header propagation on marker PUT and marker LIST this is
// the whole reason Projection doesn't need a settle cutoff.
func TestProjection_LookupReadAfterWrite(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)

	type Entry struct {
		SKU      string `parquet:"sku"`
		Customer string `parquet:"customer"`
	}
	projectionDef := ProjectionDef[Rec]{
		Name:    "sku_idx",
		Columns: []string{"sku", "customer"},
		Of: func(r Rec) ([]string, error) {
			return []string{r.SKU, r.Customer}, nil
		},
	}
	f.SeedTimingConfig(t, "store", testCommitTimeout, testMaxClockSkew)
	store, err := New(ctx, Config[Rec]{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		PartitionKeyOf: func(r Rec) string {
			return fmt.Sprintf("period=%s/customer=%s",
				r.Period, r.Customer)
		},
		ConsistencyControl: ConsistencyStrongGlobal,
		Projections:        []ProjectionDef[Rec]{projectionDef},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	idx, err := NewProjectionReader(store.Target(),
		ProjectionLookupDef[Entry]{
			Name:    "sku_idx",
			Columns: []string{"sku", "customer"},
		})
	if err != nil {
		t.Fatalf("NewProjectionReader: %v", err)
	}

	if _, err := store.Write(ctx, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1"},
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := idx.Lookup(ctx, []string{"sku=s1/customer=*"})
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("read-after-write: got %d entries, want 1", len(got))
	}
}

// TestWrite_MarkersFirst guards the Phase 3 ordering invariant:
// projection markers PUT *before* the data PUT, so a forced
// data-PUT failure cannot leave a data file behind without its
// markers. The contract is "any data file on S3 implies all R1
// markers landed" — verifying the contrapositive (markers can
// land without data) confirms the order, since the reverse order
// would fail the data PUT *after* the data file already existed.
//
// Mechanism: a smithy middleware on the S3 client returns an
// error for any PutObject whose key contains "/data/" and ends
// with ".parquet". Marker PUTs (under "/_projection/") and the
// timing-config GETs flow through unaffected.
func TestWrite_MarkersFirst(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	f.SeedTimingConfig(t, "store", testCommitTimeout, testMaxClockSkew)

	failClient := newDataPUTFailingClient(t, f)

	store, err := New(ctx, Config[Rec]{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          failClient,
		PartitionKeyParts: []string{"period", "customer"},
		PartitionKeyOf: func(r Rec) string {
			return fmt.Sprintf("period=%s/customer=%s",
				r.Period, r.Customer)
		},
		ConsistencyControl: ConsistencyStrongGlobal,
		Projections: []ProjectionDef[Rec]{{
			Name:    "sku_idx",
			Columns: []string{"sku", "customer"},
			Of: func(r Rec) ([]string, error) {
				return []string{r.SKU, r.Customer}, nil
			},
		}},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, err = store.Write(ctx, []Rec{
		{Period: "2026-04-22", Customer: "abc", SKU: "s1"},
	})
	if err == nil {
		t.Fatal("Write: want error from data-PUT failure, got nil")
	}
	if !strings.Contains(err.Error(), "put data") {
		t.Errorf("error %q: want 'put data' phase to be the failure, "+
			"got something else (markers-first ordering broken)", err)
	}

	// Marker exists: confirms markers PUT ran before data PUT.
	markerPrefix := "store/_projection/sku_idx/"
	mkOut, err := f.S3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(f.Bucket),
		Prefix: aws.String(markerPrefix),
	})
	if err != nil {
		t.Fatalf("list markers: %v", err)
	}
	if len(mkOut.Contents) == 0 {
		t.Errorf("no markers under %s — markers-first ordering "+
			"broken (markers should land before data PUT)",
			markerPrefix)
	}

	// No data file: confirms the failed data PUT left no parquet.
	dataPrefix := "store/data/"
	dOut, err := f.S3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(f.Bucket),
		Prefix: aws.String(dataPrefix),
	})
	if err != nil {
		t.Fatalf("list data: %v", err)
	}
	for _, obj := range dOut.Contents {
		if strings.HasSuffix(aws.ToString(obj.Key), ".parquet") {
			t.Errorf("unexpected data file %q after failed PUT",
				aws.ToString(obj.Key))
		}
	}
}

// newDataPUTFailingClient returns an *s3.Client wired against the
// same MinIO endpoint as f.S3Client but with a smithy middleware
// that errors on any PutObject whose key contains "/data/" and
// ends with ".parquet". Used by markers-first tests to assert
// the projection markers PUT completes before the data PUT — a
// data-PUT failure must leave projection markers visible (no
// orphan data files), proving the order.
func newDataPUTFailingClient(t *testing.T, f *fixture) *s3.Client {
	t.Helper()
	return s3.NewFromConfig(aws.Config{
		Region:      "us-east-1",
		Credentials: f.S3Client.Options().Credentials,
	}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://" + f.HostPort)
		o.UsePathStyle = true
		o.APIOptions = append(o.APIOptions,
			func(stack *middleware.Stack) error {
				return stack.Build.Add(
					middleware.BuildMiddlewareFunc(
						"s3store-test.failDataPUTs",
						func(
							ctx context.Context,
							in middleware.BuildInput,
							next middleware.BuildHandler,
						) (middleware.BuildOutput, middleware.Metadata, error) {
							req, ok := in.Request.(*smithyhttp.Request)
							if ok && req.Method == "PUT" &&
								strings.Contains(req.URL.Path, "/data/") &&
								strings.HasSuffix(req.URL.Path, ".parquet") {
								return middleware.BuildOutput{},
									middleware.Metadata{},
									fmt.Errorf("test: forced data-PUT failure")
							}
							return next.HandleBuild(ctx, in)
						}),
					middleware.After)
			})
	})
}

// TestBackfillProjection covers the relief-valve path: records
// written before a projection was registered don't produce markers,
// Lookup under-reports, and BackfillProjection brings the projection into
// sync. Also checks idempotence (a second call is semantically a
// no-op) and that pattern scoping narrows the scan.
//
// BackfillProjection is a standalone package function — it takes an
// S3Target, so a migration job can run it without building a
// full Writer/Store. The test mirrors that shape: it derives the
// target from the store but passes it explicitly to the backfill
// call.
func TestBackfillProjection(t *testing.T) {
	ctx := context.Background()

	// Phase 1: build a store with no projection, write the "historical"
	// records BackfillProjection will have to recover.
	preStore := newStore(t, storeOpts{})

	type Entry struct {
		SKU      string `parquet:"sku"`
		Customer string `parquet:"customer"`
	}
	def := ProjectionDef[Rec]{
		Name:    "sku_idx",
		Columns: []string{"sku", "customer"},
		Of: func(r Rec) ([]string, error) {
			return []string{r.SKU, r.Customer}, nil
		},
	}

	historical := []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Ts: time.UnixMilli(100)},
		{Period: "2026-03-17", Customer: "def", SKU: "s1", Ts: time.UnixMilli(200)},
		{Period: "2026-03-18", Customer: "abc", SKU: "s2", Ts: time.UnixMilli(300)},
		{Period: "2026-04-01", Customer: "abc", SKU: "s3", Ts: time.UnixMilli(400)},
	}
	if _, err := preStore.Write(ctx, historical); err != nil {
		t.Fatalf("historical Write: %v", err)
	}

	// Phase 2: build a second store wired with the projection. Reuses
	// the same target (Bucket / Prefix) so subsequent writes share
	// the dataset with the historical writes.
	target := preStore.Target()
	store, err := NewWriter(WriterConfig[Rec]{
		Target: target,
		PartitionKeyOf: func(r Rec) string {
			return fmt.Sprintf("period=%s/customer=%s",
				r.Period, r.Customer)
		},
		Projections: []ProjectionDef[Rec]{def},
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	idx, err := NewProjectionReader(target,
		ProjectionLookupDef[Entry]{
			Name:    "sku_idx",
			Columns: []string{"sku", "customer"},
		})
	if err != nil {
		t.Fatalf("NewProjectionReader: %v", err)
	}

	// Write a post-registration record so we can verify
	// BackfillProjection produces the same marker as the live write
	// path (idempotent overlap).
	if _, err := store.Write(ctx, []Rec{
		{Period: "2026-04-01", Customer: "abc", SKU: "s3", Ts: time.UnixMilli(500)},
	}); err != nil {
		t.Fatalf("post-registration Write: %v", err)
	}

	time.Sleep(testSettleWindow + 100*time.Millisecond)

	// Before BackfillProjection: only the post-registration record is
	// visible.
	got, err := idx.Lookup(ctx, []string{"sku=*/customer=*"})
	if err != nil {
		t.Fatalf("pre-backfill Lookup: %v", err)
	}
	if len(got) != 1 || got[0].SKU != "s3" || got[0].Customer != "abc" {
		t.Errorf("pre-backfill: got %v, want just {s3, abc}", got)
	}

	// BackfillProjection with empty until covers everything.
	stats, err := BackfillProjection(
		ctx, target, def, []string{"*"}, time.Time{})
	if err != nil {
		t.Fatalf("BackfillProjection: %v", err)
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

	time.Sleep(testSettleWindow + 100*time.Millisecond)

	// After BackfillProjection: every distinct (sku, customer) is
	// visible.
	got, err = idx.Lookup(ctx, []string{"sku=*/customer=*"})
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
			t.Errorf("missing %+v after BackfillProjection", w)
		}
	}
	if len(got) != len(want) {
		t.Errorf("got %d distinct entries, want %d: %+v",
			len(got), len(want), got)
	}

	// Idempotency: a second BackfillProjection re-scans but the PUTs
	// are no-ops at the semantic level. We only check it doesn't
	// error and reports the same scan volume.
	stats2, err := BackfillProjection(
		ctx, target, def, []string{"*"}, time.Time{})
	if err != nil {
		t.Fatalf("second BackfillProjection: %v", err)
	}
	if stats2.DataObjects != stats.DataObjects {
		t.Errorf("second BackfillProjection DataObjects: got %d, want %d",
			stats2.DataObjects, stats.DataObjects)
	}

	// Pattern scoping: backfilling only the 2026-03-17 partition
	// covers 2 of the 5 objects.
	scoped, err := BackfillProjection(
		ctx, target, def,
		[]string{"period=2026-03-17/customer=*"},
		time.Time{})
	if err != nil {
		t.Fatalf("scoped BackfillProjection: %v", err)
	}
	if scoped.DataObjects != 2 {
		t.Errorf("scoped DataObjects: got %d, want 2",
			scoped.DataObjects)
	}
}

// TestBackfillProjection_UntilBound verifies the typical migration
// shape: the live writer "starts" at time T0, backfill covers
// only files with LastModified < T0 so the live path's markers
// and the backfill's don't overlap. We write two files with a
// gap between them, pass OffsetAt(midpoint) as until, and assert
// that only the earlier file is scanned.
func TestBackfillProjection_UntilBound(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{})

	// Early write — should be covered by the bounded backfill.
	if _, err := store.WriteWithKey(ctx,
		"period=2026-03-17/customer=early",
		[]Rec{{Period: "2026-03-17", Customer: "early", SKU: "s1",
			Ts: time.UnixMilli(1)}},
	); err != nil {
		t.Fatalf("early Write: %v", err)
	}

	// Wait so the until cutoff cleanly falls between the two
	// writes. S3's LastModified has second granularity on most
	// providers, so 1.1s guarantees a distinct boundary.
	time.Sleep(1100 * time.Millisecond)
	midpoint := time.Now()
	time.Sleep(1100 * time.Millisecond)

	// Late write — should be outside the bounded backfill.
	if _, err := store.WriteWithKey(ctx,
		"period=2026-03-17/customer=late",
		[]Rec{{Period: "2026-03-17", Customer: "late", SKU: "s2",
			Ts: time.UnixMilli(2)}},
	); err != nil {
		t.Fatalf("late Write: %v", err)
	}

	def := ProjectionDef[Rec]{
		Name:    "bounded_idx",
		Columns: []string{"sku", "customer"},
		Of: func(r Rec) ([]string, error) {
			return []string{r.SKU, r.Customer}, nil
		},
	}

	target := store.Target()

	stats, err := BackfillProjection(
		ctx, target, def, []string{"*"}, midpoint)
	if err != nil {
		t.Fatalf("BackfillProjection: %v", err)
	}
	if stats.DataObjects != 1 {
		t.Errorf("DataObjects: got %d, want 1 (only early write "+
			"should be below until)", stats.DataObjects)
	}
}

// TestBackfillProjection_MissingDataTolerant verifies the at-least-
// once posture when a data file disappears before backfill: the
// live files still get markers and BackfillProjection does NOT fail.
//
// MinIO's LIST is strongly consistent with DELETE, so the deleted
// file is fully absent from the subsequent LIST — the LIST-to-GET
// race window doesn't exist in this fixture. The skip-on-
// NoSuchKey + slog.Warn + missing-data-metric path is exercised
// by code review; what this test pins down is that backfill
// survives the partial-delete scenario without erroring.
func TestBackfillProjection_MissingDataTolerant(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{})

	// Two writes so one can be deleted (exercising the
	// after-the-fact skip path) while the other keeps the
	// backfill work non-trivial.
	r1, err := store.WriteWithKey(ctx,
		"period=2026-03-17/customer=gone",
		[]Rec{{Period: "2026-03-17", Customer: "gone", SKU: "s1"}})
	if err != nil {
		t.Fatalf("Write r1: %v", err)
	}
	if _, err := store.WriteWithKey(ctx,
		"period=2026-03-17/customer=live",
		[]Rec{{Period: "2026-03-17", Customer: "live", SKU: "s2"}},
	); err != nil {
		t.Fatalf("Write r2: %v", err)
	}

	if _, err := store.Target().S3Client().DeleteObject(ctx,
		&s3.DeleteObjectInput{
			Bucket: aws.String(store.Target().Bucket()),
			Key:    aws.String(r1.DataPath),
		}); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}

	def := ProjectionDef[Rec]{
		Name:    "missing_idx",
		Columns: []string{"sku", "customer"},
		Of: func(r Rec) ([]string, error) {
			return []string{r.SKU, r.Customer}, nil
		},
	}

	stats, err := BackfillProjection(
		ctx, store.Target(), def, []string{"*"}, time.Time{})
	if err != nil {
		t.Fatalf("BackfillProjection: %v", err)
	}

	// MinIO's LIST reflects the delete, so backfill sees only
	// the live file. Markers is 1 for that live file.
	if stats.DataObjects != 1 {
		t.Errorf("DataObjects: got %d, want 1 (LIST consistent "+
			"with delete)", stats.DataObjects)
	}
	if stats.Markers != 1 {
		t.Errorf("Markers: got %d, want 1 (live file marker)",
			stats.Markers)
	}
}

// TestMissingData_PollSkipsReadIsLISTConsistent pins down the
// post-cleanup-removal split between strict and tolerant read
// paths. We delete a data file directly, leaving its ref in
// place — operator-driven prune shape:
//
//   - Read is LIST-based and MinIO's LIST is strongly consistent
//     with DELETE, so the deleted file is absent from Read's
//     plan; only the surviving record comes back, no missing-
//     data signal fires.
//   - PollRecords walks the ref stream, so the ref to the
//     deleted file is still there. The data GET returns
//     NoSuchKey; PollRecords logs via slog.Warn, increments the
//     missing-data metric, and returns the surviving record
//     without erroring.
//
// What this test asserts: both paths return the surviving
// record, neither errors. The slog and metric side effects are
// exercised by code review — asserting them in an integration
// test would couple the test to the slog handler / OTel SDK and
// obscure the behavioural contract that matters here.
func TestMissingData_PollSkipsReadIsLISTConsistent(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	f.SeedTimingConfig(t, "store", testCommitTimeout, testMaxClockSkew)

	store, err := New(ctx, Config[Rec]{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		PartitionKeyOf: func(r Rec) string {
			return fmt.Sprintf("period=%s/customer=%s",
				r.Period, r.Customer)
		},
		ConsistencyControl: ConsistencyStrongGlobal,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

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

	// Operator-driven prune: delete the data file, leave the
	// ref in place. PollRecords' GET for this ref will see
	// NoSuchKey.
	if _, err := f.S3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(f.Bucket),
		Key:    aws.String(r1.DataPath),
	}); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}

	time.Sleep(testSettleWindow + 100*time.Millisecond)

	// Read is LIST-based and MinIO LIST reflects the delete,
	// so Read never tries to GET the missing file.
	got, err := store.Read(ctx, []string{"*"})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 1 || got[0].Value != 2 {
		t.Errorf("Read: got %+v, want single record with Value=2",
			got)
	}

	// PollRecords walks the ref stream and will hit NoSuchKey
	// on the deleted file; it must skip via slog + metric and
	// keep going.
	pollGot, _, err := store.PollRecords(ctx, "", 100)
	if err != nil {
		t.Fatalf("PollRecords: %v", err)
	}
	if len(pollGot) != 1 || pollGot[0].Value != 2 {
		t.Errorf("PollRecords: got %+v, want single record with "+
			"Value=2", pollGot)
	}
}

// TestPoll_SkipsMalformedRefs simulates an external tool — or a
// future binary version with a different ref schema — writing a
// ref-shaped object whose filename this binary's parseRefKey
// rejects. Poll must keep walking, surface every well-formed
// entry, and never error on the malformed one.
//
// Side effects (slog.Warn + s3store.read.malformed_refs increment)
// are exercised by code review, not asserted here — wiring the
// OTel SDK / a slog capture harness into an integration test would
// drown out the behavioural contract this test pins down (skip,
// don't fail). The metric being a counter means the inverse
// signal — "we never increment when a real ref shows up" — is
// already covered by every other Poll-based integration test.
func TestPoll_SkipsMalformedRefs(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{})

	// 1. Seed one valid ref so the malformed entry sits alongside
	// real data, and Poll has something concrete to return.
	if _, err := store.WriteWithKey(ctx,
		"period=2026-04-22/customer=alice", []Rec{{
			Period: "2026-04-22", Customer: "alice",
			SKU: "valid", Value: 1, Ts: time.UnixMilli(1),
		}}); err != nil {
		t.Fatalf("seed write: %v", err)
	}

	// 2. PUT a malformed ref directly under the ref prefix. The
	// filename has refTsMicros=0 so it sorts strictly before the
	// live-tip cutoffPrefix and isn't filtered out by Poll's
	// settle-window check; the body has no refSeparator (";"), so
	// parseRefKey's SplitN returns one part and surfaces an
	// "invalid ref key" error → the malformed-ref skip branch
	// fires.
	malformedKey := refPath(store.Target().Prefix()) + "/0-malformed.ref"
	if _, err := store.Target().S3Client().PutObject(ctx,
		&s3.PutObjectInput{
			Bucket: aws.String(store.Target().Bucket()),
			Key:    aws.String(malformedKey),
			Body:   strings.NewReader(""),
		}); err != nil {
		t.Fatalf("PutObject malformed ref: %v", err)
	}
	time.Sleep(testSettleWindow + 100*time.Millisecond)

	// 3. Poll: succeeds and returns only the valid entry. The
	// malformed ref is logged + metric'd + skipped.
	entries, _, err := store.Poll(ctx, "", 100)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("Poll got %d entries, want 1 (malformed ref must "+
			"be skipped, not surfaced)", len(entries))
	}
	if entries[0].Key != "period=2026-04-22/customer=alice" {
		t.Errorf("Poll entry key %q, want the seeded partition",
			entries[0].Key)
	}

	// 4. PollRecords: end-to-end — the GET pipeline runs only on
	// the surviving valid ref, so one record comes back.
	recs, _, err := store.PollRecords(ctx, "", 100)
	if err != nil {
		t.Fatalf("PollRecords: %v", err)
	}
	if len(recs) != 1 || recs[0].SKU != "valid" {
		t.Fatalf("PollRecords got %+v, want single 'valid' record "+
			"(malformed ref must not break the pipeline)", recs)
	}
}

// TestInsertedAtField_Populate covers the InsertedAtField hook:
// the writer populates a struct field with its wall-clock
// time.Now() before parquet encode, and Read / PollRecords
// surface that same value back. The field carries a real parquet
// tag so it's a first-class column — identical across every read
// path.
func TestInsertedAtField_Populate(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	f.SeedTimingConfig(t, "store", testCommitTimeout, testMaxClockSkew)

	type RecWithMeta struct {
		Period     string    `parquet:"period"`
		Customer   string    `parquet:"customer"`
		SKU        string    `parquet:"sku"`
		Ts         time.Time `parquet:"ts,timestamp(millisecond)"`
		InsertedAt time.Time `parquet:"inserted_at,timestamp(millisecond)"`
	}

	store, err := New[RecWithMeta](ctx, Config[RecWithMeta]{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		PartitionKeyOf: func(r RecWithMeta) string {
			return fmt.Sprintf("period=%s/customer=%s",
				r.Period, r.Customer)
		},
		ConsistencyControl: ConsistencyStrongGlobal,
		InsertedAtField:    "InsertedAt",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	before := time.Now()
	if _, err := store.Write(ctx, []RecWithMeta{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1",
			Ts: time.UnixMilli(100)},
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	after := time.Now()
	time.Sleep(testSettleWindow + 100*time.Millisecond)

	got, err := store.Read(ctx, []string{"*"})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d records, want 1", len(got))
	}
	// The populated InsertedAt is the writer's time.Now()
	// captured inside Write, so it's bracketed precisely by
	// before/after. 100 ms of tolerance covers scheduler jitter.
	ia := got[0].InsertedAt
	if ia.Before(before.Add(-100*time.Millisecond)) ||
		ia.After(after.Add(100*time.Millisecond)) {
		t.Errorf("InsertedAt=%v outside [%v, %v]",
			ia, before, after)
	}

	// Phase 1's promise: the InsertedAt returned from PollRecords
	// is the exact same column value Read produces. Previous
	// LastModified-based implementations drifted by the ref-PUT-
	// vs-data-PUT delta; the column sidesteps that entirely.
	polled, _, err := store.PollRecords(ctx, "", 100)
	if err != nil {
		t.Fatalf("PollRecords: %v", err)
	}
	if len(polled) != 1 {
		t.Fatalf("PollRecords: got %d, want 1", len(polled))
	}
	if !polled[0].InsertedAt.Equal(ia) {
		t.Errorf("PollRecords InsertedAt=%v != Read InsertedAt=%v "+
			"(column value must match exactly)",
			polled[0].InsertedAt, ia)
	}
}

// TestInsertedAtField_Validation covers the New()-time checks
// that protect users from configuring InsertedAtField wrong: no
// such field, wrong type, or a missing / "-" parquet tag (the
// field is now a real parquet column, so it must carry a
// non-empty, non-"-" tag).
func TestInsertedAtField_Validation(t *testing.T) {
	f := newFixture(t)
	f.SeedTimingConfig(t, "store", testCommitTimeout, testMaxClockSkew)
	ctx := t.Context()

	type RecIgnoredMeta struct {
		Period   string    `parquet:"period"`
		Customer string    `parquet:"customer"`
		SKU      string    `parquet:"sku"`
		Value    int64     `parquet:"value"`
		Ts       time.Time `parquet:"ts,timestamp(millisecond)"`
		Ignored  time.Time `parquet:"-"`
	}

	mkCfgRec := func(field string) Config[Rec] {
		return Config[Rec]{
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
	mkCfgIgnored := func(field string) Config[RecIgnoredMeta] {
		return Config[RecIgnoredMeta]{
			Bucket:            f.Bucket,
			Prefix:            "store",
			S3Client:          f.S3Client,
			PartitionKeyParts: []string{"period", "customer"},
			PartitionKeyOf: func(r RecIgnoredMeta) string {
				return "period=p/customer=c"
			},
			InsertedAtField: field,
		}
	}

	t.Run("no such field", func(t *testing.T) {
		_, err := New[Rec](ctx, mkCfgRec("Nonexistent"))
		if err == nil || !strings.Contains(err.Error(), "no such field") {
			t.Fatalf("want %q error, got %v", "no such field", err)
		}
	})
	t.Run("wrong type", func(t *testing.T) {
		// Period is string, not time.Time.
		_, err := New[Rec](ctx, mkCfgRec("Period"))
		if err == nil || !strings.Contains(err.Error(), "must be time.Time") {
			t.Fatalf("want %q error, got %v", "must be time.Time", err)
		}
	})
	t.Run("parquet dash tag rejected", func(t *testing.T) {
		// Ignored is time.Time but tagged parquet:"-" — rejected
		// because the value must be persisted as a real column.
		_, err := New[RecIgnoredMeta](ctx, mkCfgIgnored("Ignored"))
		if err == nil || !strings.Contains(err.Error(), "non-empty, non-\"-\" parquet tag") {
			t.Fatalf("want non-empty/non-\"-\" error, got %v", err)
		}
	})
}

// TestNewReaderFromStore_NarrowT covers the cross-T read path:
// a Store is constructed over a full-shape record type (with a
// heavy write-only column), and NewReaderFromStore produces a
// Reader[NarrowT] that decodes only the fields present on
// NarrowT. parquet-go's decode naturally skips unlisted columns,
// so the narrow Reader never even tries to materialise the heavy
// column. Also proves the shared wiring (Bucket, Prefix,
// PartitionKeyParts, S3Client) carries from Writer through
// NewReaderFromStore without the caller respecifying anything.
func TestNewReaderFromStore_NarrowT(t *testing.T) {
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

	f := newFixture(t)
	f.SeedTimingConfig(t, "store", testCommitTimeout, testMaxClockSkew)
	store, err := New[FullRec](ctx, Config[FullRec]{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		PartitionKeyOf: func(r FullRec) string {
			return fmt.Sprintf("period=%s/customer=%s",
				r.Period, r.Customer)
		},
		ConsistencyControl: ConsistencyStrongGlobal,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, err := store.Write(ctx, []FullRec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1",
			Value: 10, ProcessLog: "..heavy JSON blob.."},
		{Period: "2026-03-17", Customer: "def", SKU: "s2",
			Value: 20, ProcessLog: "..another blob.."},
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	view, err := NewReaderFromStore(
		store, ReaderConfig[NarrowRec]{})
	if err != nil {
		t.Fatalf("NewReaderFromStore: %v", err)
	}

	got, err := view.Read(ctx, []string{"*"})
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

// TestRead_NonCartesian proves Read covers an arbitrary
// tuple set, not just a Cartesian product. Writing to four
// (period, customer) tuples and asking for only two of them via
// a 2-element patterns slice must return just those records —
// something a single `|`-style pattern couldn't express without
// over-reading the cross product.
func TestRead_NonCartesian(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{})

	in := []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Value: 10},
		{Period: "2026-03-17", Customer: "def", SKU: "s2", Value: 20},
		{Period: "2026-03-18", Customer: "abc", SKU: "s3", Value: 30},
		{Period: "2026-03-18", Customer: "def", SKU: "s4", Value: 40},
	}
	if _, err := store.Write(ctx, in); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Pick the diagonal: (2026-03-17, abc) and (2026-03-18, def).
	// A Cartesian pattern would also return the off-diagonal
	// entries (10 and 40 only wanted; Cartesian would include
	// 20 and 30).
	got, err := store.Read(ctx, []string{
		"period=2026-03-17/customer=abc",
		"period=2026-03-18/customer=def",
	})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d records, want 2", len(got))
	}
	values := map[int64]bool{}
	for _, r := range got {
		values[r.Value] = true
	}
	if !values[10] || !values[40] {
		t.Errorf("got values %v, want {10, 40} only", values)
	}
	if values[20] || values[30] {
		t.Errorf("off-diagonal records leaked: %v", values)
	}
}

// TestRead_OverlapsDeduped proves overlapping patterns don't
// cause a parquet file to be fetched + decoded twice. A bare "*"
// plus a narrower pattern that it subsumes must still yield the
// single expected record set.
func TestRead_OverlapsDeduped(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{})

	if _, err := store.Write(ctx, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Value: 1},
		{Period: "2026-03-17", Customer: "def", SKU: "s2", Value: 2},
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := store.Read(ctx, []string{
		"*",
		"period=2026-03-17/customer=abc",
	})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	// 2 records, not 3 — overlap dedup collapses the redundant
	// listing of abc's parquet file.
	if len(got) != 2 {
		t.Errorf("got %d records, want 2 (overlap dedup)", len(got))
	}
}

// TestRead_EmptyAndBadPattern covers the two edge cases:
// an empty slice returns (nil, nil) without S3 traffic, and a
// malformed pattern fails with the offending index in the error.
func TestRead_EmptyAndBadPattern(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{})

	got, err := store.Read(ctx, nil)
	if err != nil {
		t.Errorf("Read(nil): %v", err)
	}
	if got != nil {
		t.Errorf("Read(nil): got %v, want nil", got)
	}

	_, err = store.Read(ctx, []string{
		"period=2026-03-17/customer=abc",
		"not-a-valid-pattern", // segment count wrong
	})
	if err == nil {
		t.Fatal("expected error for bad pattern, got nil")
	}
	if !strings.Contains(err.Error(), "pattern 1") {
		t.Errorf("error %q should identify pattern index 1", err)
	}
}

// TestRead_WithHistory guards that opts pass through to the
// dedup path: with dedup configured, Read + WithHistory()
// returns every record, and without WithHistory() returns one
// per entity.
func TestRead_WithHistory(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{
		entityKeyOf: func(r Rec) string { return r.SKU },
		versionOf:   func(r Rec) int64 { return r.Value },
	})

	key := "period=2026-03-17/customer=abc"
	for i := int64(0); i < 3; i++ {
		if _, err := store.WriteWithKey(ctx, key, []Rec{
			{Period: "2026-03-17", Customer: "abc", SKU: "s1", Value: i},
		}); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}

	// Read without opts: dedup kicks in → 1 record.
	deduped, err := store.Read(ctx, []string{key})
	if err != nil {
		t.Fatalf("Read (deduped): %v", err)
	}
	if len(deduped) != 1 {
		t.Errorf("deduped: got %d records, want 1", len(deduped))
	}

	// Read with WithHistory: all 3 records returned.
	full, err := store.Read(ctx,
		[]string{key}, WithHistory())
	if err != nil {
		t.Fatalf("Read (history): %v", err)
	}
	if len(full) != 3 {
		t.Errorf("history: got %d, want 3", len(full))
	}
}

// TestLookup_EmptyAndBadPattern mirrors
// TestRead_EmptyAndBadPattern at the Projection layer: empty
// slice is a no-op, malformed pattern surfaces the offending
// index.
func TestLookup_EmptyAndBadPattern(t *testing.T) {
	ctx := context.Background()

	type Entry struct {
		SKU      string `parquet:"sku"`
		Customer string `parquet:"customer"`
	}
	store := newStore(t, storeOpts{
		projections: []ProjectionDef[Rec]{{
			Name:    "empty_bad_idx",
			Columns: []string{"sku", "customer"},
			Of: func(r Rec) ([]string, error) {
				return []string{r.SKU, r.Customer}, nil
			},
		}},
	})

	idx, err := NewProjectionReader(store.Target(),
		ProjectionLookupDef[Entry]{
			Name:    "empty_bad_idx",
			Columns: []string{"sku", "customer"},
		})
	if err != nil {
		t.Fatalf("NewProjectionReader: %v", err)
	}

	got, err := idx.Lookup(ctx, nil)
	if err != nil {
		t.Errorf("Lookup(nil): %v", err)
	}
	if got != nil {
		t.Errorf("Lookup(nil): got %v, want nil", got)
	}

	_, err = idx.Lookup(ctx, []string{
		"sku=s1/customer=abc",
		"not-a-valid-pattern",
	})
	if err == nil {
		t.Fatal("expected error for bad pattern, got nil")
	}
	if !strings.Contains(err.Error(), "pattern 1") {
		t.Errorf("error %q should identify pattern index 1", err)
	}
}

// TestBackfillProjection_EmptyAndBadPattern covers the matching
// edge cases for the migration entry point.
func TestBackfillProjection_EmptyAndBadPattern(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{})

	def := ProjectionDef[Rec]{
		Name:    "empty_bad_backfill_idx",
		Columns: []string{"sku", "customer"},
		Of: func(r Rec) ([]string, error) {
			return []string{r.SKU, r.Customer}, nil
		},
	}
	target := store.Target()

	stats, err := BackfillProjection(
		ctx, target, def, nil, time.Time{})
	if err != nil {
		t.Errorf("BackfillProjection(nil): %v", err)
	}
	if stats != (BackfillStats{}) {
		t.Errorf("BackfillProjection(nil): got %+v, want zero stats",
			stats)
	}

	_, err = BackfillProjection(ctx, target, def, []string{
		"period=2026-03-17/customer=abc",
		"not-a-valid-pattern",
	}, time.Time{})
	if err == nil {
		t.Fatal("expected error for bad pattern, got nil")
	}
	if !strings.Contains(err.Error(), "pattern 1") {
		t.Errorf("error %q should identify pattern index 1", err)
	}
}

// TestLookup_NonCartesian mirrors TestRead_NonCartesian
// at the projection layer: pick a non-Cartesian tuple set of
// (sku, customer) pairs and verify only those markers come back.
func TestLookup_NonCartesian(t *testing.T) {
	ctx := context.Background()

	type Entry struct {
		SKU      string `parquet:"sku"`
		Customer string `parquet:"customer"`
	}
	store := newStore(t, storeOpts{
		projections: []ProjectionDef[Rec]{{
			Name:    "sku_customer_idx",
			Columns: []string{"sku", "customer"},
			Of: func(r Rec) ([]string, error) {
				return []string{r.SKU, r.Customer}, nil
			},
		}},
	})

	idx, err := NewProjectionReader(store.Target(),
		ProjectionLookupDef[Entry]{
			Name:    "sku_customer_idx",
			Columns: []string{"sku", "customer"},
		})
	if err != nil {
		t.Fatalf("NewProjectionReader: %v", err)
	}

	if _, err := store.Write(ctx, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1"},
		{Period: "2026-03-17", Customer: "def", SKU: "s2"},
		{Period: "2026-03-17", Customer: "abc", SKU: "s3"},
		{Period: "2026-03-17", Customer: "def", SKU: "s4"},
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	time.Sleep(testSettleWindow + 100*time.Millisecond)

	got, err := idx.Lookup(ctx, []string{
		"sku=s1/customer=abc",
		"sku=s4/customer=def",
	})
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d entries, want 2", len(got))
	}
	set := map[Entry]bool{}
	for _, e := range got {
		set[e] = true
	}
	want := []Entry{
		{SKU: "s1", Customer: "abc"},
		{SKU: "s4", Customer: "def"},
	}
	for _, w := range want {
		if !set[w] {
			t.Errorf("missing entry %+v", w)
		}
	}
	// Off-diagonal entries must NOT appear.
	for _, w := range []Entry{
		{SKU: "s1", Customer: "def"},
		{SKU: "s4", Customer: "abc"},
	} {
		if set[w] {
			t.Errorf("unexpected off-diagonal entry %+v", w)
		}
	}
}

// TestBackfillProjection_NonCartesian exercises the multi-pattern
// migration shape: write records across several partitions, then
// backfill only the partitions of interest via a patterns slice.
// The run covers exactly the selected partitions, and the union
// is deduplicated when patterns overlap.
func TestBackfillProjection_NonCartesian(t *testing.T) {
	ctx := context.Background()

	type Entry struct {
		SKU      string `parquet:"sku"`
		Customer string `parquet:"customer"`
	}
	def := ProjectionDef[Rec]{
		Name:    "many_idx",
		Columns: []string{"sku", "customer"},
		Of: func(r Rec) ([]string, error) {
			return []string{r.SKU, r.Customer}, nil
		},
	}

	// No-projection store for the historical writes: backfill must
	// run from a clean state so the test pins down what the
	// scoped patterns covered (vs. the live writer covering
	// everything).
	store := newStore(t, storeOpts{})

	historical := []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1"},
		{Period: "2026-03-17", Customer: "def", SKU: "s2"},
		{Period: "2026-03-18", Customer: "abc", SKU: "s3"},
		{Period: "2026-04-01", Customer: "abc", SKU: "s4"},
	}
	if _, err := store.Write(ctx, historical); err != nil {
		t.Fatalf("Write: %v", err)
	}

	idx, err := NewProjectionReader(store.Target(),
		ProjectionLookupDef[Entry]{
			Name:    "many_idx",
			Columns: []string{"sku", "customer"},
		})
	if err != nil {
		t.Fatalf("NewProjectionReader: %v", err)
	}

	// Backfill just the two March partitions via explicit patterns.
	// The April partition should NOT be covered.
	stats, err := BackfillProjection(ctx, store.Target(), def,
		[]string{
			"period=2026-03-17/customer=*",
			"period=2026-03-18/customer=*",
		},
		time.Time{})
	if err != nil {
		t.Fatalf("BackfillProjection: %v", err)
	}
	if stats.DataObjects != 3 {
		t.Errorf("DataObjects: got %d, want 3 (two March-17 + one "+
			"March-18; April skipped)", stats.DataObjects)
	}

	time.Sleep(testSettleWindow + 100*time.Millisecond)

	// Sanity: Lookup sees the March markers, NOT April.
	got, err := idx.Lookup(ctx, []string{"sku=*/customer=*"})
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	set := map[Entry]bool{}
	for _, e := range got {
		set[e] = true
	}
	if !set[(Entry{SKU: "s1", Customer: "abc"})] ||
		!set[(Entry{SKU: "s2", Customer: "def"})] ||
		!set[(Entry{SKU: "s3", Customer: "abc"})] {
		t.Errorf("March entries missing: got %v", set)
	}
	if set[(Entry{SKU: "s4", Customer: "abc"})] {
		t.Errorf("April entry (s4) should not be backfilled, got %v", set)
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

	got, err := store.Read(ctx, []string{"*"})
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

	got, err := store.Read(ctx, []string{key})
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
			got, err := store.Read(ctx, []string{tc.pattern})
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
		versionOf: func(r Rec) int64 {
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

	got, err := store.Read(ctx, []string{key})
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

// TestReplicaDedup_CollapsesSameEntityVersion proves Phase 1.5's
// core contract: two writes that produce the same (entity,
// version) pair — i.e. byte-identical replicas from a retry,
// zombie writer, or cross-node race — collapse to one record on
// read, regardless of WithHistory. Without this, retries surface
// as phantom "distinct versions" to the consumer.
func TestReplicaDedup_CollapsesSameEntityVersion(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{
		entityKeyOf: func(r Rec) string {
			return r.Customer + "|" + r.SKU
		},
		versionOf: func(r Rec) int64 {
			return r.Ts.UnixNano()
		},
	})

	key := "period=2026-03-17/customer=abc"
	same := []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1",
			Value: 10, Ts: time.UnixMilli(100)},
	}
	for i := 0; i < 2; i++ {
		if _, err := store.WriteWithKey(ctx, key, same); err != nil {
			t.Fatalf("write #%d: %v", i, err)
		}
	}

	deduped, err := store.Read(ctx, []string{key})
	if err != nil {
		t.Fatalf("Read (default): %v", err)
	}
	if len(deduped) != 1 {
		t.Errorf("default dedup: got %d records, want 1", len(deduped))
	}

	full, err := store.Read(ctx, []string{key}, WithHistory())
	if err != nil {
		t.Fatalf("Read (history): %v", err)
	}
	if len(full) != 1 {
		t.Errorf("WithHistory: got %d records, want 1 "+
			"(replicas must collapse even under WithHistory)",
			len(full))
	}
}

// TestReplicaDedup_PreservesDistinctVersionsWithHistory guards
// the other side of Phase 1.5: distinct versions of the same
// entity still flow through WithHistory. Only byte-identical
// replicas (same version) collapse; genuinely different versions
// never do.
func TestReplicaDedup_PreservesDistinctVersionsWithHistory(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{
		entityKeyOf: func(r Rec) string {
			return r.Customer + "|" + r.SKU
		},
		versionOf: func(r Rec) int64 {
			return r.Ts.UnixNano()
		},
	})

	key := "period=2026-03-17/customer=abc"
	for i := int64(1); i <= 3; i++ {
		if _, err := store.WriteWithKey(ctx, key, []Rec{
			{Period: "2026-03-17", Customer: "abc", SKU: "s1",
				Value: i, Ts: time.UnixMilli(i * 100)},
		}); err != nil {
			t.Fatalf("write ver=%d: %v", i, err)
		}
	}

	full, err := store.Read(ctx, []string{key}, WithHistory())
	if err != nil {
		t.Fatalf("Read (history): %v", err)
	}
	if len(full) != 3 {
		t.Errorf("WithHistory: got %d records, want 3 "+
			"(three distinct versions must survive)", len(full))
	}
}

// TestReadIter_ReplicaDedupWithHistory covers the ReadIter path
// through emitPartition: same (entity, version) replicas collapse
// even with WithHistory. Read's dedup path is covered by
// TestReplicaDedup_CollapsesSameEntityVersion — this test pins
// the iter path so the emitPartition refactor doesn't regress.
func TestReadIter_ReplicaDedupWithHistory(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{
		entityKeyOf: func(r Rec) string {
			return r.Customer + "|" + r.SKU
		},
		versionOf: func(r Rec) int64 {
			return r.Ts.UnixNano()
		},
	})

	key := "period=2026-03-17/customer=abc"
	same := []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1",
			Value: 10, Ts: time.UnixMilli(100)},
	}
	for i := 0; i < 2; i++ {
		if _, err := store.WriteWithKey(ctx, key, same); err != nil {
			t.Fatalf("write #%d: %v", i, err)
		}
	}

	var got []Rec
	for r, err := range store.ReadIter(ctx, []string{key}, WithHistory()) {
		if err != nil {
			t.Fatalf("ReadIter: %v", err)
		}
		got = append(got, r)
	}
	if len(got) != 1 {
		t.Errorf("WithHistory via ReadIter: got %d, want 1 "+
			"(replicas must collapse even under WithHistory)",
			len(got))
	}
}

// TestReadWithHistory opts out of dedup: every written record
// must appear in the result.
func TestReadWithHistory(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{
		entityKeyOf: func(r Rec) string { return r.SKU },
		versionOf:   func(r Rec) int64 { return r.Value },
	})

	key := "period=2026-03-17/customer=abc"
	for i := int64(0); i < 3; i++ {
		if _, err := store.WriteWithKey(ctx, key, []Rec{
			{Period: "2026-03-17", Customer: "abc", SKU: "s1", Value: i},
		}); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}

	deduped, err := store.Read(ctx, []string{key})
	if err != nil {
		t.Fatalf("Read (deduped): %v", err)
	}
	if len(deduped) != 1 {
		t.Errorf("deduped: got %d, want 1", len(deduped))
	}

	full, err := store.Read(ctx, []string{key}, WithHistory())
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

	time.Sleep(testSettleWindow + 100*time.Millisecond)

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
		if e.RowCount != 1 {
			t.Errorf("[%d] RowCount = %d, want 1 (one record per write)",
				i, e.RowCount)
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
	f := newFixture(t)
	f.SeedTimingConfig(t, "store", testCommitTimeout, testMaxClockSkew)

	wNarrow, err := New[RecNarrow](ctx, Config[RecNarrow]{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		PartitionKeyOf: func(r RecNarrow) string {
			return fmt.Sprintf("period=%s/customer=%s",
				r.Period, r.Customer)
		},
		ConsistencyControl: ConsistencyStrongGlobal,
	})
	if err != nil {
		t.Fatalf("New(RecNarrow): %v", err)
	}

	if _, err := wNarrow.Write(ctx, []RecNarrow{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Ts: time.UnixMilli(100)},
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	rWide, err := New[Rec](ctx, Config[Rec]{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		PartitionKeyOf: func(r Rec) string {
			return fmt.Sprintf("period=%s/customer=%s",
				r.Period, r.Customer)
		},
		ConsistencyControl: ConsistencyStrongGlobal,
	})
	if err != nil {
		t.Fatalf("New(Rec): %v", err)
	}

	got, err := rWide.Read(ctx, []string{"period=2026-03-17/customer=abc"})
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
// TestPollRecords pins the cursor-based contract: both versions
// of an entity flow through (CDC semantics — caller must see
// every change). WithHistory is accepted but is the default
// behavior on this path; latest-per-entity collapse is NOT
// offered, because per-batch latest is meaningless on a cursor.
// Use ReadRangeIter for snapshot semantics over a range.
func TestPollRecords(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{
		entityKeyOf: func(r Rec) string {
			return r.Customer + "|" + r.SKU
		},
		versionOf: func(r Rec) int64 {
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
	time.Sleep(testSettleWindow + 100*time.Millisecond)

	// Default: both versions of the same entity flow through.
	got, off, err := store.PollRecords(ctx, "", 100)
	if err != nil {
		t.Fatalf("PollRecords: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("default: got %d records, want 2 (CDC: no version collapse)", len(got))
	}
	seen := map[int64]bool{got[0].Value: true, got[1].Value: true}
	if !seen[1] || !seen[2] {
		t.Errorf("default: got values %v, want {1,2}",
			[]int64{got[0].Value, got[1].Value})
	}

	// PollRecords always runs replica-dedup; no IncludeHistory
	// knob on PollOption (type-enforced — WithHistory wouldn't
	// even compile here). Re-poll from the head to confirm the
	// same shape regardless.
	full, _, err := store.PollRecords(ctx, "", 100)
	if err != nil {
		t.Fatalf("PollRecords replay: %v", err)
	}
	if len(full) != 2 {
		t.Errorf("replay: got %d, want 2", len(full))
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
//
// Writes are spaced by 1.1s because under Phase 4 the ref
// filename's refTsMicros is the data file's server-stamped
// LastModified at *second* precision (HEAD's RFC 1123 format
// + the cross-source truncation in truncLMToSecond). Two writes
// landing in the same wall-clock second are indistinguishable
// at the offset level — the time window can't separate them.
// The 1.1s spacing guarantees each write lands in its own
// second.
func TestPollTimeWindow(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{})

	beforeFirst := time.Now()
	if _, err := store.WriteWithKey(ctx,
		"period=2026-03-17/customer=a",
		[]Rec{{Period: "2026-03-17", Customer: "a", SKU: "s1"}},
	); err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	time.Sleep(1100 * time.Millisecond)
	afterFirst := time.Now()
	time.Sleep(1100 * time.Millisecond)
	if _, err := store.WriteWithKey(ctx,
		"period=2026-03-17/customer=b",
		[]Rec{{Period: "2026-03-17", Customer: "b", SKU: "s2"}},
	); err != nil {
		t.Fatalf("Write 2: %v", err)
	}
	time.Sleep(1100 * time.Millisecond)
	beforeThird := time.Now()
	time.Sleep(1100 * time.Millisecond)
	if _, err := store.WriteWithKey(ctx,
		"period=2026-03-17/customer=c",
		[]Rec{{Period: "2026-03-17", Customer: "c", SKU: "s3"}},
	); err != nil {
		t.Fatalf("Write 3: %v", err)
	}
	time.Sleep(testSettleWindow + 100*time.Millisecond)

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
		WithUntilOffset(end))
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
		WithUntilOffset(offEnd))
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

// TestReadRangeIter exercises the streaming iter: a small
// stream of writes, then ReadRangeIter drains the full window
// via range. Also checks that zero time.Time bounds mean
// "stream head → live tip" and that an empty window terminates
// without error or yielded records.
//
// `before` is taken from the previous wall-clock second so it
// sorts strictly before any write's server-stamped refTsMicros
// (which is second-precision under Phase 4 — see truncLMToSecond).
// A `before` taken from the same second as the writes would
// land mid-second and the second-truncated refKeys would lex
// less than it, excluding every write from the window.
func TestReadRangeIter(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{})

	before := time.Now().Add(-1 * time.Second).Truncate(time.Second)
	for i := range 5 {
		if _, err := store.WriteWithKey(ctx,
			fmt.Sprintf("period=2026-03-17/customer=c%d", i),
			[]Rec{{Period: "2026-03-17", Customer: fmt.Sprintf("c%d", i),
				SKU: "s1", Value: int64(i)}},
		); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	time.Sleep(testSettleWindow + 100*time.Millisecond)
	after := time.Now()

	// Bounded window: all 5 records.
	var got []Rec
	for r, err := range store.ReadRangeIter(ctx, before, after) {
		if err != nil {
			t.Fatalf("ReadRangeIter: %v", err)
		}
		got = append(got, r)
	}
	if len(got) != 5 {
		t.Fatalf("got %d, want 5", len(got))
	}

	// Open bounds (zero-value time) read from stream head to live tip.
	var open []Rec
	for r, err := range store.ReadRangeIter(ctx, time.Time{}, time.Time{}) {
		if err != nil {
			t.Fatalf("ReadRangeIter open: %v", err)
		}
		open = append(open, r)
	}
	if len(open) != 5 {
		t.Errorf("open: got %d, want 5", len(open))
	}

	// Empty window yields nothing without error.
	var empty []Rec
	for r, err := range store.ReadRangeIter(ctx,
		before.Add(-time.Hour), before.Add(-time.Minute)) {
		if err != nil {
			t.Fatalf("ReadRangeIter empty: %v", err)
		}
		empty = append(empty, r)
	}
	if len(empty) != 0 {
		t.Errorf("empty: got %d, want 0", len(empty))
	}
}

// TestReadRangeIter_EarlyBreak proves the downloadAndDecodeIter break
// contract: breaking out of the range loop after the first
// record stops further yielding (and cancels in-flight downloads
// via the streaming pipeline's defer cancel). Note: the LIST walk
// runs upfront before any record yields, so this test does not
// claim "no further LIST" — it claims "no further records
// yielded after break".
func TestReadRangeIter_EarlyBreak(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{})

	for i := range 10 {
		if _, err := store.WriteWithKey(ctx,
			fmt.Sprintf("period=2026-03-17/customer=c%d", i),
			[]Rec{{Period: "2026-03-17", Customer: fmt.Sprintf("c%d", i),
				SKU: "s1", Value: int64(i)}},
		); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	time.Sleep(testSettleWindow + 100*time.Millisecond)

	// Break after first record. The closure must observe yield
	// returning false and stop — no further work, no error.
	count := 0
	for _, err := range store.ReadRangeIter(ctx, time.Time{}, time.Time{}) {
		if err != nil {
			t.Fatalf("ReadRangeIter: %v", err)
		}
		count++
		if count == 1 {
			break
		}
	}
	if count != 1 {
		t.Errorf("early-break: yielded %d records, want 1", count)
	}
}

// TestReadRangeIter_SnapshotsLiveTipCutoff guards the
// snapshot-at-entry contract for the zero-time `until` bound:
// writes that land AFTER the iter starts must not appear in the
// yielded records. Without the snapshot, sustained writes during
// the walk would keep the now-SettleWindow cutoff advancing and
// the loop could expose them — defeating the "single-pass over
// the stream as of this call" guarantee.
func TestReadRangeIter_SnapshotsLiveTipCutoff(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{})

	// Phase 1: pre-existing writes.
	for i := range 3 {
		if _, err := store.WriteWithKey(ctx,
			fmt.Sprintf("period=2026-03-17/customer=pre%d", i),
			[]Rec{{Period: "2026-03-17",
				Customer: fmt.Sprintf("pre%d", i),
				SKU:      "s1", Value: int64(i)}},
		); err != nil {
			t.Fatalf("Write pre %d: %v", i, err)
		}
	}
	// Wait past the settle window so the pre-writes are visible.
	time.Sleep(testSettleWindow + 100*time.Millisecond)

	// Phase 2: start the iter, then write more concurrently. The
	// concurrent writes are timed AFTER iter entry — the snapshot
	// must exclude them.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Small head-start so iter has definitely entered before
		// the post-writes land.
		time.Sleep(50 * time.Millisecond)
		for i := range 5 {
			if _, err := store.WriteWithKey(ctx,
				fmt.Sprintf("period=2026-03-17/customer=post%d", i),
				[]Rec{{Period: "2026-03-17",
					Customer: fmt.Sprintf("post%d", i),
					SKU:      "s1", Value: int64(100 + i)}},
			); err != nil {
				t.Errorf("Write post %d: %v", i, err)
				return
			}
		}
	}()

	// Drain — should only see the 3 pre-writes, not the 5 post-writes.
	got := map[string]bool{}
	for r, err := range store.ReadRangeIter(ctx, time.Time{}, time.Time{}) {
		if err != nil {
			t.Fatalf("ReadRangeIter: %v", err)
		}
		got[r.Customer] = true
	}
	wg.Wait()

	for i := range 3 {
		want := fmt.Sprintf("pre%d", i)
		if !got[want] {
			t.Errorf("expected %q in iter output, missing", want)
		}
	}
	for i := range 5 {
		bad := fmt.Sprintf("post%d", i)
		if got[bad] {
			t.Errorf("post-iter-entry write %q leaked into iter output", bad)
		}
	}
}

// LogField is a named int8 enum, mirroring the shape a
// go-enum generator would produce. Declared at package scope
// so Store[T] generic instantiation works at integration-test
// scope.
type LogField int8

const (
	LogFieldUnknown LogField = iota
	LogFieldPrimary
	LogFieldSecondary
)

// ParquetLog is the nested-struct payload: string + named int8
// enum + map, all carried inside ParquetRec.Logs.
type ParquetLog struct {
	Processor string            `parquet:"processor"`
	Field     LogField          `parquet:"field"`
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
// JSONB-style shape through the full s3store pipeline —
// Write encodes + puts to S3, Read lists + gets + decodes back
// into []ParquetRec. Guards that the parquet-go v0.29 small-int
// dispatch holds end-to-end for named int8 enums in nested
// structs, not just in the in-memory encode/decode unit test.
func TestWriteRead_NamedInt8EnumInNestedStruct(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	f.SeedTimingConfig(t, "store", testCommitTimeout, testMaxClockSkew)

	store, err := New[ParquetRec](ctx, Config[ParquetRec]{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		PartitionKeyOf: func(r ParquetRec) string {
			return fmt.Sprintf("period=%s/customer=%s",
				r.Period, r.Customer)
		},
		ConsistencyControl: ConsistencyStrongGlobal,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	in := []ParquetRec{{
		Period:   "2026-03-17",
		Customer: "abc",
		Ts:       time.UnixMilli(100),
		Logs: []ParquetLog{
			{
				Processor: "ingest",
				Field:     LogFieldPrimary,
				Attrs:     map[string]string{"stage": "raw"},
			},
			{
				Processor: "enrich",
				Field:     LogFieldSecondary,
				Attrs:     map[string]string{"model": "v2"},
			},
		},
	}}
	if _, err := store.Write(ctx, in); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := store.Read(ctx, []string{"period=2026-03-17/customer=abc"})
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
	_ = LogFieldUnknown
}

// TestWrite_ParallelResultsSorted guards the invariant that Write
// returns WriteResults in sorted-key order even when partitions
// complete out-of-order under parallel fan-out. Without slot-index
// preservation, the returned slice would leak completion order
// instead of the deterministic sorted-key order the doc promises.
func TestWrite_ParallelResultsSorted(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{})

	// 10 partitions exercises the partition fan-out path even though it
	// semaphore contention and scheduling variance across runs.
	periods := []string{
		"2026-03-01", "2026-03-02", "2026-03-03", "2026-03-04",
		"2026-03-05", "2026-03-06", "2026-03-07", "2026-03-08",
		"2026-03-09", "2026-03-10",
	}
	in := make([]Rec, 0, len(periods))
	want := make([]string, 0, len(periods))
	for i, p := range periods {
		in = append(in, Rec{
			Period: p, Customer: "abc", SKU: "s1",
			Value: int64(i),
		})
		want = append(want, fmt.Sprintf(
			"store/data/period=%s/customer=abc/", p))
	}

	results, err := store.Write(ctx, in)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if len(results) != len(periods) {
		t.Fatalf("results: got %d, want %d",
			len(results), len(periods))
	}
	for i, r := range results {
		if !strings.HasPrefix(r.DataPath, want[i]) {
			t.Errorf("result[%d] DataPath %q does not start with %q",
				i, r.DataPath, want[i])
		}
	}
}

// TestWrite_CallerCancelReturnsError guards that a pre-cancelled
// caller context surfaces as an error from Write rather than being
// swallowed into a (partial, nil) result. Without the parentCtx.Err()
// fallback, the cancel-filter would skip every goroutine's error and
// callers would mistake a no-op for success.
func TestWrite_CallerCancelReturnsError(t *testing.T) {
	store := newStore(t, storeOpts{})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	in := []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Value: 1},
		{Period: "2026-03-17", Customer: "def", SKU: "s2", Value: 2},
	}
	_, err := store.Write(ctx, in)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Write with cancelled ctx: got %v, want context.Canceled",
			err)
	}
}

// TestReadIter_PerPartitionDedup guards the iter contract: when
// EntityKeyOf is set and WithHistory is not, ReadIter dedups
// per-partition. Two writes to the same entity in the same
// partition must yield exactly one record (the newer Value).
// Mirrors TestDedupExplicit but for the iter path.
func TestReadIter_PerPartitionDedup(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{
		entityKeyOf: func(r Rec) string {
			return r.Customer + "|" + r.SKU
		},
		versionOf: func(r Rec) int64 {
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

	var got []Rec
	for r, err := range store.ReadIter(ctx, []string{key}) {
		if err != nil {
			t.Fatalf("ReadIter: %v", err)
		}
		got = append(got, r)
	}
	if len(got) != 1 {
		t.Fatalf("got %d records, want 1 (deduped)", len(got))
	}
	if got[0].Value != 99 {
		t.Errorf("got Value=%d, want 99 (newer Ts wins)", got[0].Value)
	}
}

// TestReadIter_WithHistory_Order guards that WithHistory disables
// dedup AND yields records in lex/insertion order within each
// partition. Two writes to the same partition produce two files,
// the second sorting after the first by tsMicros prefix; we must
// observe both records, the older write first.
func TestReadIter_WithHistory_Order(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{
		entityKeyOf: func(r Rec) string {
			return r.Customer + "|" + r.SKU
		},
		versionOf: func(r Rec) int64 { return r.Value },
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

	var values []int64
	for r, err := range store.ReadIter(ctx, []string{key}, WithHistory()) {
		if err != nil {
			t.Fatalf("ReadIter: %v", err)
		}
		values = append(values, r.Value)
	}
	if len(values) != 2 {
		t.Fatalf("got %d records, want 2 (history)", len(values))
	}
	if values[0] != 10 || values[1] != 99 {
		t.Errorf("got values %v, want [10 99] (insertion order)", values)
	}
}

// TestReadIter_EarlyBreak proves that breaking out of the
// for-range loop releases resources cleanly: the iterator's
// deferred cancel stops in-flight downloads and the next call
// completes normally. A goroutine leak or hang would surface as
// the second iteration timing out.
func TestReadIter_EarlyBreak(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{})

	in := make([]Rec, 30)
	for i := range in {
		in[i] = Rec{
			Period: "2026-03-17", Customer: "abc",
			SKU: fmt.Sprintf("sku-%02d", i), Value: int64(i),
		}
	}
	if _, err := store.WriteWithKey(ctx,
		"period=2026-03-17/customer=abc", in); err != nil {
		t.Fatalf("Write: %v", err)
	}

	count := 0
	for r, err := range store.ReadIter(ctx, []string{"*"}) {
		if err != nil {
			t.Fatalf("ReadIter: %v", err)
		}
		count++
		if r.SKU == "sku-05" {
			break
		}
	}
	if count == 0 {
		t.Fatal("got 0 records before break, expected at least 1")
	}

	// Second iteration must complete — proves the early-break
	// didn't leak goroutines or wedge S3 client state.
	count = 0
	for r, err := range store.ReadIter(ctx, []string{"*"}) {
		if err != nil {
			t.Fatalf("ReadIter (second pass): %v", err)
		}
		_ = r
		count++
	}
	if count != len(in) {
		t.Errorf("second pass got %d records, want %d", count, len(in))
	}
}

// TestReadIter_Empty guards that a pattern matching nothing
// produces an empty iterator (zero yields, no error) instead of
// either erroring or yielding a zero-value record.
func TestReadIter_Empty(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{})

	yields := 0
	for _, err := range store.ReadIter(ctx,
		[]string{"period=9999-01-01/customer=missing"}) {
		if err != nil {
			t.Fatalf("ReadIter: %v", err)
		}
		yields++
	}
	if yields != 0 {
		t.Errorf("got %d yields on empty match, want 0", yields)
	}
}

// TestReadIter_ReadAheadPartitions guards the pipelined path:
// setting a positive WithReadAheadPartitions must produce the
// same records (same content, same lex order) as the default
// strict-serial path — correctness first, speed second.
// Covers dedup, no-dedup, and early-break with a prefetch goroutine
// alive to make sure cancel cleanly stops the producer.
func TestReadIter_ReadAheadPartitions(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{
		entityKeyOf: func(r Rec) string {
			return r.Period + "|" + r.Customer + "|" + r.SKU
		},
		versionOf: func(r Rec) int64 { return r.Value },
	})

	in := []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Value: 1},
		{Period: "2026-03-18", Customer: "abc", SKU: "s1", Value: 2},
		{Period: "2026-03-19", Customer: "abc", SKU: "s1", Value: 3},
		{Period: "2026-03-20", Customer: "abc", SKU: "s1", Value: 4},
	}
	if _, err := store.Write(ctx, in); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Prefetching matches strict-serial on values and order.
	var serial, prefetched []int64
	for r, err := range store.ReadIter(ctx, []string{"*"}) {
		if err != nil {
			t.Fatalf("serial ReadIter: %v", err)
		}
		serial = append(serial, r.Value)
	}
	for r, err := range store.ReadIter(ctx, []string{"*"},
		WithReadAheadPartitions(3)) {
		if err != nil {
			t.Fatalf("prefetched ReadIter: %v", err)
		}
		prefetched = append(prefetched, r.Value)
	}
	if !reflect.DeepEqual(serial, prefetched) {
		t.Errorf("prefetch changed order/values: serial=%v prefetched=%v",
			serial, prefetched)
	}

	// Early-break with prefetch alive: producer goroutine must
	// exit via ctx.Done. A subsequent full-read must still succeed
	// (no leaked goroutines, no wedged S3 client).
	count := 0
	for _, err := range store.ReadIter(ctx, []string{"*"},
		WithReadAheadPartitions(3)) {
		if err != nil {
			t.Fatalf("ReadIter (break): %v", err)
		}
		count++
		if count == 2 {
			break
		}
	}
	count = 0
	for _, err := range store.ReadIter(ctx, []string{"*"},
		WithReadAheadPartitions(3)) {
		if err != nil {
			t.Fatalf("ReadIter (recovery): %v", err)
		}
		count++
	}
	if count != len(in) {
		t.Errorf("recovery pass got %d records, want %d",
			count, len(in))
	}
}

// TestReadIter_MultiPartition guards that ReadIter visits
// partitions in lex order and applies dedup per partition. Same
// (Customer, SKU) entity in two different periods produces two
// records (different partitions ⇒ different entities under the
// per-partition contract), in lex order of the partition key.
func TestReadIter_MultiPartition(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{
		entityKeyOf: func(r Rec) string {
			return r.Period + "|" + r.Customer + "|" + r.SKU
		},
		versionOf: func(r Rec) int64 { return r.Value },
	})

	if _, err := store.Write(ctx, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1", Value: 1},
		{Period: "2026-03-18", Customer: "abc", SKU: "s1", Value: 2},
		{Period: "2026-03-19", Customer: "abc", SKU: "s1", Value: 3},
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	var periods []string
	for r, err := range store.ReadIter(ctx, []string{"*"}) {
		if err != nil {
			t.Fatalf("ReadIter: %v", err)
		}
		periods = append(periods, r.Period)
	}
	want := []string{"2026-03-17", "2026-03-18", "2026-03-19"}
	if !reflect.DeepEqual(periods, want) {
		t.Errorf("partition order: got %v, want %v", periods, want)
	}
}

// TestInsertedAtField_PopulatedByWriter pins the Phase 1
// guarantee that InsertedAtField is populated by the writer at
// Write time, not derived from S3 LastModified at read time. We
// bracket the write with before/after timestamps and assert the
// column's value lies between them — the writer captures
// time.Now() inside Write, so the bracket is tight.
func TestInsertedAtField_PopulatedByWriter(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	f.SeedTimingConfig(t, "store", testCommitTimeout, testMaxClockSkew)

	type RecWithMeta struct {
		Period     string    `parquet:"period"`
		Customer   string    `parquet:"customer"`
		SKU        string    `parquet:"sku"`
		InsertedAt time.Time `parquet:"inserted_at,timestamp(millisecond)"`
	}

	store, err := New[RecWithMeta](ctx, Config[RecWithMeta]{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		PartitionKeyOf: func(r RecWithMeta) string {
			return fmt.Sprintf("period=%s/customer=%s",
				r.Period, r.Customer)
		},
		ConsistencyControl: ConsistencyStrongGlobal,
		InsertedAtField:    "InsertedAt",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	before := time.Now()
	if _, err := store.Write(ctx, []RecWithMeta{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1"},
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	after := time.Now()
	time.Sleep(testSettleWindow + 100*time.Millisecond)

	got, err := store.Read(ctx, []string{"*"})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d, want 1", len(got))
	}
	ia := got[0].InsertedAt
	if ia.Before(before.Add(-100*time.Millisecond)) ||
		ia.After(after.Add(100*time.Millisecond)) {
		t.Errorf("InsertedAt=%v outside [%v, %v] — writer "+
			"should have captured time.Now() inside the Write "+
			"call, so the value is bracketed precisely",
			ia, before, after)
	}
}

// TestInsertedAtField_ColumnIsAuthoritativeOverLastModified pins
// the Phase 1 contract that the column's value — not S3
// LastModified — is what Read surfaces. We write, sleep long
// enough that time.Now() diverges clearly from writeStartTime,
// and assert the decoded InsertedAt is close to writeStartTime
// (from the column) and NOT close to time.Now() (where
// LastModified-based sourcing would have landed if the reader
// were wrong).
func TestInsertedAtField_ColumnIsAuthoritativeOverLastModified(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	f.SeedTimingConfig(t, "store", testCommitTimeout, testMaxClockSkew)

	type RecWithMeta struct {
		Period     string    `parquet:"period"`
		Customer   string    `parquet:"customer"`
		SKU        string    `parquet:"sku"`
		InsertedAt time.Time `parquet:"inserted_at,timestamp(millisecond)"`
	}

	store, err := New[RecWithMeta](ctx, Config[RecWithMeta]{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		PartitionKeyOf: func(r RecWithMeta) string {
			return fmt.Sprintf("period=%s/customer=%s",
				r.Period, r.Customer)
		},
		ConsistencyControl: ConsistencyStrongGlobal,
		InsertedAtField:    "InsertedAt",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	writeStart := time.Now()
	if _, err := store.Write(ctx, []RecWithMeta{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1"},
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	writeEnd := time.Now()

	// Sleep long enough that now-time clearly differs from the
	// write instant — if the read path were pulling from
	// something time-of-read-ish (it shouldn't), the delta would
	// show up here. LastModified is second-granular on MinIO and
	// stamped around the writeStart anyway, so this test doesn't
	// exercise divergence between column and LastModified per se;
	// it exercises that the read path does NOT stamp its own
	// clock on the record.
	time.Sleep(2 * time.Second)
	readTime := time.Now()

	got, err := store.Read(ctx, []string{"*"})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d, want 1", len(got))
	}
	ia := got[0].InsertedAt
	if ia.Before(writeStart.Add(-100*time.Millisecond)) ||
		ia.After(writeEnd.Add(100*time.Millisecond)) {
		t.Errorf("InsertedAt=%v outside write bracket [%v, %v]",
			ia, writeStart, writeEnd)
	}
	// Guard: InsertedAt should be clearly earlier than readTime
	// by at least the sleep. If the reader were sourcing from a
	// fresh time.Now() this check would fail.
	if readTime.Sub(ia) < time.Second {
		t.Errorf("InsertedAt=%v is too close to readTime=%v "+
			"(delta %v < 1s) — reader must source from the "+
			"column, not a fresh clock",
			ia, readTime, readTime.Sub(ia))
	}
}

// TestSort_ByEntityKeyAndVersion covers the two-tier sort when
// both EntityKeyOf and VersionOf are configured: records arrive
// grouped by entity, ascending by version within each group.
func TestSort_ByEntityKeyAndVersion(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{
		entityKeyOf: func(r Rec) string { return r.SKU },
		versionOf:   func(r Rec) int64 { return r.Value },
	})

	// Interleaved entities + versions; write one record per call
	// so every record lands in its own file.
	writes := []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "b",
			Value: 2, Ts: time.UnixMilli(100)},
		{Period: "2026-03-17", Customer: "abc", SKU: "a",
			Value: 1, Ts: time.UnixMilli(200)},
		{Period: "2026-03-17", Customer: "abc", SKU: "b",
			Value: 1, Ts: time.UnixMilli(300)},
		{Period: "2026-03-17", Customer: "abc", SKU: "a",
			Value: 2, Ts: time.UnixMilli(400)},
	}
	for i, r := range writes {
		if _, err := store.Write(ctx, []Rec{r}); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	time.Sleep(testSettleWindow + 100*time.Millisecond)

	// WithHistory so sort can be observed without dedup folding.
	got, err := store.Read(ctx, []string{"*"}, WithHistory())
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	want := []struct {
		sku string
		v   int64
	}{
		{"a", 1}, {"a", 2}, {"b", 1}, {"b", 2},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d records, want %d", len(got), len(want))
	}
	for i, w := range want {
		if got[i].SKU != w.sku || got[i].Value != w.v {
			t.Errorf("[%d] got (sku=%q, v=%d), want (sku=%q, v=%d)",
				i, got[i].SKU, got[i].Value, w.sku, w.v)
		}
	}
}

// TestSort_LastModifiedFallback covers the no-EntityKeyOf branch:
// emission order is per-file chronological by LastModified, with
// a fileName tiebreak that fires only on identical LastModified.
// We use one-second MinIO precision to produce distinct times.
func TestSort_LastModifiedFallback(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{}) // no dedup, no EntityKeyOf

	// Each Write call produces its own data file with its own
	// LastModified. Sleep past one second between writes so
	// MinIO stamps distinct values.
	if _, err := store.Write(ctx, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1",
			Value: 1, Ts: time.UnixMilli(100)},
	}); err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	time.Sleep(1100 * time.Millisecond)
	if _, err := store.Write(ctx, []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1",
			Value: 2, Ts: time.UnixMilli(200)},
	}); err != nil {
		t.Fatalf("Write 2: %v", err)
	}
	time.Sleep(testSettleWindow + 100*time.Millisecond)

	got, err := store.Read(ctx, []string{"*"})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d records, want 2", len(got))
	}
	if got[0].Value != 1 || got[1].Value != 2 {
		t.Errorf("order wrong: got [%d, %d], want [1, 2]",
			got[0].Value, got[1].Value)
	}
}

// TestSort_AppliesToAllReadPaths checks Read, ReadIter, and
// PollRecords all emit the same deterministic order when the
// reader is configured with EntityKeyOf + VersionOf. This is
// Phase 1's "sort is the source of truth" contract.
func TestSort_AppliesToAllReadPaths(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{
		entityKeyOf: func(r Rec) string { return r.SKU },
		versionOf:   func(r Rec) int64 { return r.Value },
	})

	writes := []Rec{
		{Period: "2026-03-17", Customer: "abc", SKU: "b",
			Value: 2, Ts: time.UnixMilli(100)},
		{Period: "2026-03-17", Customer: "abc", SKU: "a",
			Value: 1, Ts: time.UnixMilli(200)},
		{Period: "2026-03-17", Customer: "abc", SKU: "a",
			Value: 2, Ts: time.UnixMilli(300)},
		{Period: "2026-03-17", Customer: "abc", SKU: "b",
			Value: 1, Ts: time.UnixMilli(400)},
	}
	for i, r := range writes {
		if _, err := store.Write(ctx, []Rec{r}); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	time.Sleep(testSettleWindow + 100*time.Millisecond)

	readGot, err := store.Read(ctx, []string{"*"}, WithHistory())
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	var iterGot []Rec
	for rec, err := range store.ReadIter(ctx, []string{"*"}, WithHistory()) {
		if err != nil {
			t.Fatalf("ReadIter: %v", err)
		}
		iterGot = append(iterGot, rec)
	}

	pollGot, _, err := store.PollRecords(ctx, "", 100)
	if err != nil {
		t.Fatalf("PollRecords: %v", err)
	}

	sameOrder := func(a, b []Rec) bool {
		if len(a) != len(b) {
			return false
		}
		for i := range a {
			if a[i].SKU != b[i].SKU || a[i].Value != b[i].Value {
				return false
			}
		}
		return true
	}
	if !sameOrder(readGot, iterGot) {
		t.Errorf("Read vs ReadIter order mismatch:\n read=%+v\n iter=%+v",
			readGot, iterGot)
	}
	if !sameOrder(readGot, pollGot) {
		t.Errorf("Read vs PollRecords order mismatch:\n read=%+v\n poll=%+v",
			readGot, pollGot)
	}
}

// newIdempotentStore builds a Store with idempotency-specific
// config: entity-key dedup so a near-concurrent retry overlap
// (two attempts of the same token whose upfront LISTs both miss
// the prior commit) collapses to one record at the reader layer.
// Used by the WithIdempotencyToken end-to-end tests below.
func newIdempotentStore(t *testing.T) *Store[Rec] {
	t.Helper()
	f := newFixture(t)
	f.SeedTimingConfig(t, "store", testCommitTimeout, testMaxClockSkew)
	store, err := New[Rec](t.Context(), Config[Rec]{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
		PartitionKeyOf: func(r Rec) string {
			return fmt.Sprintf("period=%s/customer=%s",
				r.Period, r.Customer)
		},
		// MinIO is in fact strongly consistent; the claim keeps
		// the upfront-LIST dedup gate linearized against prior
		// writes on a multi-site StorageGRID-style backend.
		ConsistencyControl: ConsistencyStrongGlobal,
		EntityKeyOf: func(r Rec) string {
			return r.Customer + "|" + r.SKU
		},
		VersionOf: func(r Rec) int64 {
			return r.Value
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return store
}

// TestWriteWithIdempotencyToken_FreshAndRetry exercises the
// upfront-LIST dedup gate end-to-end on MinIO:
//
//  1. A fresh write with a token writes a per-attempt triple
//     (data + ref + commit marker) at id = "{token}-{tsMicros}-
//     {shortID}".
//  2. A retry with the same token runs an upfront LIST under
//     {partition}/{token}-, finds the prior valid commit pair
//     ({.parquet} + {.commit} with marker.LM - data.LM <
//     CommitTimeout), reconstructs the WriteResult from the
//     LIST response — no body re-upload, no extra PUTs, same
//     DataPath / RefPath returned.
//  3. A Read sees exactly one record — the retry's upfront LIST
//     short-circuited before any new objects landed.
func TestWriteWithIdempotencyToken_FreshAndRetry(t *testing.T) {
	ctx := context.Background()
	store := newIdempotentStore(t)

	key := "period=2026-04-22/customer=alice"
	rec := []Rec{{
		Period: "2026-04-22", Customer: "alice",
		SKU: "sku1", Value: 42, Ts: time.UnixMilli(1),
	}}
	const token = "2026-04-22T10:15:00Z-batch42"

	// Fresh write.
	first, err := store.WriteWithKey(ctx, key, rec,
		WithIdempotencyToken(token))
	if err != nil {
		t.Fatalf("fresh write: %v", err)
	}
	if first == nil {
		t.Fatal("fresh write: nil result")
	}
	// DataPath has shape "{prefix}/data/{partition}/{token}-{tsMicros}-{shortID}.parquet"
	// — the per-attempt id under WithIdempotencyToken.
	wantPrefix := "/" + token + "-"
	if !strings.Contains(first.DataPath, wantPrefix) {
		t.Errorf("data path %q does not contain per-attempt token prefix %q",
			first.DataPath, wantPrefix)
	}

	// Retry with the same token. The upfront LIST under
	// {partition}/{token}- finds the prior valid commit
	// ({.parquet} + {.commit} pair with marker.LM - data.LM <
	// CommitTimeout) and returns its WriteResult unchanged — no
	// new PUTs.
	second, err := store.WriteWithKey(ctx, key, rec,
		WithIdempotencyToken(token))
	if err != nil {
		t.Fatalf("retry write: %v", err)
	}
	if second == nil {
		t.Fatal("retry write: nil result")
	}
	if second.DataPath != first.DataPath {
		t.Errorf("retry DataPath drift: fresh=%q retry=%q "+
			"(upfront-LIST should reconstruct the same path)",
			first.DataPath, second.DataPath)
	}
	if second.RefPath != first.RefPath {
		t.Errorf("retry RefPath drift: fresh=%q retry=%q",
			first.RefPath, second.RefPath)
	}

	// Read should see exactly one record — the idempotent retry
	// didn't land a duplicate. No SettleWindow sleep needed:
	// snapshot reads are read-after-write.
	got, err := store.Read(ctx, []string{key})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d records, want 1 (idempotent retry should not duplicate)",
			len(got))
	}
	if got[0].Value != 42 {
		t.Errorf("got Value=%d, want 42", got[0].Value)
	}
}

// TestWriteWithIdempotencyToken_RetryAfterFailedAttempt simulates
// a failed prior attempt (data + ref landed but the token-commit
// got externally deleted, mimicking "token-commit PUT failed" or
// "operator-driven sweeper reclaimed an orphan"). The retry's
// upfront HEAD on `<token>.commit` sees 404 and creates a fresh
// per-attempt path with a fresh attempt-id.
//
// Asserts the per-attempt-path uniqueness invariant: two attempts
// of the same token land at *different* data files (no overwrite
// of the prior attempt's parquet).
func TestWriteWithIdempotencyToken_RetryAfterFailedAttempt(t *testing.T) {
	ctx := context.Background()
	store := newIdempotentStore(t)

	key := "period=2026-04-22/customer=bob"
	rec := []Rec{{
		Period: "2026-04-22", Customer: "bob",
		SKU: "sku1", Value: 7, Ts: time.UnixMilli(1),
	}}
	const token = "job-2026-04-22-batchA"

	first, err := store.WriteWithKey(ctx, key, rec,
		WithIdempotencyToken(token))
	if err != nil {
		t.Fatalf("fresh write: %v", err)
	}

	// Simulate a failed prior attempt: delete the token-commit
	// out-of-band so the upfront HEAD on <token>.commit returns
	// 404. The retry must take the fresh-attempt path.
	firstID := strings.TrimSuffix(path.Base(first.DataPath), ".parquet")
	commitKey := tokenCommitKey(
		store.Target().Prefix()+"/data", key, token)
	if _, err := store.Target().S3Client().DeleteObject(ctx,
		&s3.DeleteObjectInput{
			Bucket: aws.String(store.Target().Bucket()),
			Key:    aws.String(commitKey),
		}); err != nil {
		t.Fatalf("DeleteObject token-commit: %v", err)
	}

	second, err := store.WriteWithKey(ctx, key, rec,
		WithIdempotencyToken(token))
	if err != nil {
		t.Fatalf("retry write: %v", err)
	}

	// Per-attempt-path uniqueness: the retry must not overwrite
	// the prior data file, must produce its own per-attempt path.
	if second.DataPath == first.DataPath {
		t.Errorf("retry reused failed attempt's DataPath %q — "+
			"per-attempt-path invariant broken (no overwrite "+
			"should ever happen)", first.DataPath)
	}
	secondID := strings.TrimSuffix(path.Base(second.DataPath), ".parquet")
	if !strings.HasPrefix(firstID, token+"-") ||
		!strings.HasPrefix(secondID, token+"-") {
		t.Errorf("ids %q / %q do not both carry token prefix",
			firstID, secondID)
	}

	// Reader-side dedup (EntityKeyOf+VersionOf on this fixture)
	// collapses the two attempts to one logical record on Read.
	got, err := store.Read(ctx, []string{key})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("got %d records, want 1 (reader dedup should "+
			"collapse the two per-attempt files to one)",
			len(got))
	}
}

// TestWriteWithIdempotencyToken_PerAttemptTriple verifies that a
// successful Write under WithIdempotencyToken lands all three
// objects that make up the commit: the per-attempt data file
// (.parquet at the partition under {token}-{attemptID}), the ref
// (in _ref/), and the token-commit marker (.commit at the
// partition under {token}). Unlike the per-attempt commit of the
// earlier design, the token-commit is shared across retries —
// it's the single atomic event that flips read visibility.
func TestWriteWithIdempotencyToken_PerAttemptTriple(t *testing.T) {
	ctx := context.Background()
	store := newIdempotentStore(t)

	key := "period=2026-04-22/customer=eve"
	rec := []Rec{{
		Period: "2026-04-22", Customer: "eve",
		SKU: "sku1", Value: 1, Ts: time.UnixMilli(1),
	}}
	const token = "job-2026-04-22-triple"

	wr, err := store.WriteWithKey(ctx, key, rec,
		WithIdempotencyToken(token))
	if err != nil {
		t.Fatalf("WriteWithKey: %v", err)
	}

	id := strings.TrimSuffix(path.Base(wr.DataPath), ".parquet")
	if !strings.HasPrefix(id, token+"-") {
		t.Errorf("id %q does not carry token prefix", id)
	}
	wantCommit := tokenCommitKey(
		store.Target().Prefix()+"/data", key, token)

	// Confirm all three objects exist on S3.
	for _, key := range []string{wr.DataPath, wantCommit, wr.RefPath} {
		_, err := store.Target().S3Client().HeadObject(ctx,
			&s3.HeadObjectInput{
				Bucket: aws.String(store.Target().Bucket()),
				Key:    aws.String(key),
			})
		if err != nil {
			t.Errorf("HeadObject(%q): %v (commit triple must "+
				"include data + ref + token-commit)", key, err)
		}
	}

	// Confirm the token-commit carries the user metadata that
	// reads consume to identify the canonical attempt and
	// reconstruct WriteResult on retry.
	hd, err := store.Target().S3Client().HeadObject(ctx,
		&s3.HeadObjectInput{
			Bucket: aws.String(store.Target().Bucket()),
			Key:    aws.String(wantCommit),
		})
	if err != nil {
		t.Fatalf("HeadObject(token-commit): %v", err)
	}
	if _, err := readTokenCommitMeta(hd.Metadata); err != nil {
		t.Errorf("token-commit metadata invalid: %v (got %v)",
			err, hd.Metadata)
	}
}

// TestWriteWithIdempotencyToken_SameTokenAcrossPartitions: the
// same token may be reused across distinct partition keys without
// colliding. The upfront LIST under {partition}/{token}- is
// scoped to one partition, so each partition's dedup runs
// independently. Two writes with the same token to different
// partitions both produce fresh per-attempt triples; a retry of
// either is dedup'd to its own partition's prior attempt.
func TestWriteWithIdempotencyToken_SameTokenAcrossPartitions(t *testing.T) {
	ctx := context.Background()
	store := newIdempotentStore(t)

	const token = "job-2026-04-22-multi"
	keyA := "period=2026-04-22/customer=alpha"
	keyB := "period=2026-04-22/customer=beta"
	recA := []Rec{{Period: "2026-04-22", Customer: "alpha",
		SKU: "s1", Value: 1, Ts: time.UnixMilli(1)}}
	recB := []Rec{{Period: "2026-04-22", Customer: "beta",
		SKU: "s2", Value: 2, Ts: time.UnixMilli(2)}}

	// Fresh writes: same token, different partitions. Both must
	// land their own data + ref (no cross-partition dedup).
	freshA, err := store.WriteWithKey(ctx, keyA, recA,
		WithIdempotencyToken(token))
	if err != nil {
		t.Fatalf("fresh A: %v", err)
	}
	freshB, err := store.WriteWithKey(ctx, keyB, recB,
		WithIdempotencyToken(token))
	if err != nil {
		t.Fatalf("fresh B: %v", err)
	}
	if freshA.RefPath == freshB.RefPath {
		t.Fatalf("partitions collided on RefPath: %q", freshA.RefPath)
	}
	if freshA.DataPath == freshB.DataPath {
		t.Fatalf("partitions collided on DataPath: %q", freshA.DataPath)
	}

	// Retry of A: must dedup to A's own ref, not B's.
	retryA, err := store.WriteWithKey(ctx, keyA, recA,
		WithIdempotencyToken(token))
	if err != nil {
		t.Fatalf("retry A: %v", err)
	}
	if retryA.RefPath != freshA.RefPath {
		t.Errorf("retry A returned wrong RefPath: got %q, want %q",
			retryA.RefPath, freshA.RefPath)
	}

	// Retry of B: must dedup to B's own ref.
	retryB, err := store.WriteWithKey(ctx, keyB, recB,
		WithIdempotencyToken(token))
	if err != nil {
		t.Fatalf("retry B: %v", err)
	}
	if retryB.RefPath != freshB.RefPath {
		t.Errorf("retry B returned wrong RefPath: got %q, want %q",
			retryB.RefPath, freshB.RefPath)
	}

	// End-to-end: both records visible, exactly one per partition.
	time.Sleep(testSettleWindow + 100*time.Millisecond)
	got, err := store.Read(ctx, []string{keyA, keyB})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d records, want 2 (one per partition)", len(got))
	}
}

// TestWriteWithIdempotencyToken_RejectsBadToken: tokens that fail
// ValidateIdempotencyToken surface their error at the Write call
// site without touching S3.
func TestWriteWithIdempotencyToken_RejectsBadToken(t *testing.T) {
	ctx := context.Background()
	store := newIdempotentStore(t)

	key := "period=2026-04-22/customer=bad"
	rec := []Rec{{
		Period: "2026-04-22", Customer: "bad",
		SKU: "sku1", Value: 1, Ts: time.UnixMilli(1),
	}}
	_, err := store.WriteWithKey(ctx, key, rec,
		WithIdempotencyToken("has/slash"))
	if err == nil {
		t.Fatal("want error for token with '/', got nil")
	}
}

// TestWriteWithIdempotencyTokenOf_PerPartitionToken: a single
// multi-partition Write derives a different token per partition
// via WithIdempotencyTokenOf. Each partition's retry is dedup'd
// against its own token's commit marker — so a retry of one
// partition returns the prior WriteResult unchanged, while a
// fresh different-token partition lands a fresh per-attempt
// triple in parallel.
func TestWriteWithIdempotencyTokenOf_PerPartitionToken(t *testing.T) {
	ctx := context.Background()
	store := newIdempotentStore(t)

	recs := []Rec{
		{Period: "2026-04-22", Customer: "alpha",
			SKU: "s1", Value: 1, Ts: time.UnixMilli(1)},
		{Period: "2026-04-22", Customer: "beta",
			SKU: "s2", Value: 2, Ts: time.UnixMilli(2)},
	}
	// One token per partition: encode the partition key into the
	// token so every partition gets its own distinct commit
	// marker. Realistic shape (caller usually has an outbox row
	// per partition).
	tokenOf := func(part []Rec) (string, error) {
		if len(part) == 0 {
			return "", fmt.Errorf("empty partition")
		}
		r := part[0]
		return fmt.Sprintf("job-2026-04-22-%s-%s",
			r.Period, r.Customer), nil
	}

	fresh, err := store.Write(ctx, recs, WithIdempotencyTokenOf(tokenOf))
	if err != nil {
		t.Fatalf("fresh Write: %v", err)
	}
	if len(fresh) != 2 {
		t.Fatalf("fresh: got %d results, want 2", len(fresh))
	}

	// Retry with the same closure: each partition's upfront HEAD
	// finds its own commit marker and returns the prior
	// WriteResult unchanged.
	retry, err := store.Write(ctx, recs, WithIdempotencyTokenOf(tokenOf))
	if err != nil {
		t.Fatalf("retry Write: %v", err)
	}
	if len(retry) != len(fresh) {
		t.Fatalf("retry: got %d results, want %d", len(retry), len(fresh))
	}
	for i := range fresh {
		if retry[i].RefPath != fresh[i].RefPath {
			t.Errorf("partition %d: retry RefPath = %q, want %q",
				i, retry[i].RefPath, fresh[i].RefPath)
		}
		if retry[i].DataPath != fresh[i].DataPath {
			t.Errorf("partition %d: retry DataPath = %q, want %q",
				i, retry[i].DataPath, fresh[i].DataPath)
		}
	}
	// Cross-partition isolation: alpha's token must differ from
	// beta's, so their commit markers don't collide.
	if fresh[0].DataPath == fresh[1].DataPath {
		t.Errorf("partitions collided on DataPath: %q",
			fresh[0].DataPath)
	}
}

// TestWriteWithIdempotencyTokenOf_PropagatesFnError: a non-nil
// error from the per-partition token closure aborts that
// partition's write. Under multi-partition fan-out it surfaces
// as a wrapped error; sibling partitions whose closure succeeded
// may have committed (partial-success contract).
func TestWriteWithIdempotencyTokenOf_PropagatesFnError(t *testing.T) {
	ctx := context.Background()
	store := newIdempotentStore(t)

	rec := []Rec{{
		Period: "2026-04-22", Customer: "errfn",
		SKU: "s1", Value: 1, Ts: time.UnixMilli(1),
	}}
	tokenOf := func(_ []Rec) (string, error) {
		return "", fmt.Errorf("caller-supplied failure")
	}
	_, err := store.WriteWithKey(ctx, "period=2026-04-22/customer=errfn",
		rec, WithIdempotencyTokenOf(tokenOf))
	if err == nil {
		t.Fatal("want non-nil error from token-fn failure")
	}
	if !strings.Contains(err.Error(), "caller-supplied failure") {
		t.Errorf("error %q did not include the fn's error message", err)
	}
}

// TestWriteWithIdempotencyTokenOf_RejectsBadToken: a closure that
// returns a token failing validateIdempotencyToken surfaces the
// validation error to the caller without touching S3.
func TestWriteWithIdempotencyTokenOf_RejectsBadToken(t *testing.T) {
	ctx := context.Background()
	store := newIdempotentStore(t)

	rec := []Rec{{
		Period: "2026-04-22", Customer: "badtok",
		SKU: "s1", Value: 1, Ts: time.UnixMilli(1),
	}}
	tokenOf := func(_ []Rec) (string, error) {
		return "has/slash", nil
	}
	_, err := store.WriteWithKey(ctx, "period=2026-04-22/customer=badtok",
		rec, WithIdempotencyTokenOf(tokenOf))
	if err == nil {
		t.Fatal("want error for token with '/', got nil")
	}
}

// TestIdempotentRead_ReadModifyWriteRetrySafe drives the full
// Phase 3b read-modify-write cycle end-to-end on MinIO:
//
//  1. Seed a partition with a baseline record (no token).
//  2. Attempt-1: Read the baseline with WithIdempotentRead — the
//     token has no files yet, so Read returns the baseline.
//  3. Attempt-1 writes a new record with WithIdempotencyToken
//     using the same token as the barrier read.
//  4. A zombie / subsequent writer pushes another record into the
//     same partition without the token.
//  5. Attempt-2 retries the same Read with the same token. It
//     must see the *baseline only* — the attempt-1 file is self-
//     excluded, and the zombie file has LastModified >= the
//     barrier so it is also excluded.
//
// This gives callers retry-safe read-modify-write: attempt-2 reads
// the same state attempt-1 saw, computes the same diff, writes the
// same bytes (idempotently, thanks to the token).
func TestIdempotentRead_ReadModifyWriteRetrySafe(t *testing.T) {
	ctx := context.Background()
	store := newIdempotentStore(t)

	key := "period=2026-04-22/customer=alice"
	partition := []Rec{{
		Period: "2026-04-22", Customer: "alice",
		SKU: "baseline", Value: 1, Ts: time.UnixMilli(1),
	}}

	// 1. Seed baseline.
	if _, err := store.WriteWithKey(ctx, key, partition); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	// Wait so S3 LastModified of subsequent writes is strictly
	// greater than the baseline's — the barrier comparison is
	// LastModified-based, and MinIO has whole-second granularity on
	// some versions.
	time.Sleep(1100 * time.Millisecond)

	const token = "2026-04-22T10:15:00Z-job-a"

	// 2. Attempt-1 reads with the barrier: no token matches yet, so
	// the full current state (baseline only) is returned.
	attempt1, err := store.Read(ctx, []string{key},
		WithIdempotentRead(token))
	if err != nil {
		t.Fatalf("attempt-1 Read: %v", err)
	}
	if len(attempt1) != 1 || attempt1[0].SKU != "baseline" {
		t.Fatalf("attempt-1 saw %+v, want [baseline]", attempt1)
	}

	// 3. Attempt-1 writes with the matching idempotency token.
	_, err = store.WriteWithKey(ctx, key, []Rec{{
		Period: "2026-04-22", Customer: "alice",
		SKU: "derived-a", Value: 100, Ts: time.UnixMilli(10),
	}}, WithIdempotencyToken(token))
	if err != nil {
		t.Fatalf("attempt-1 write: %v", err)
	}
	time.Sleep(1100 * time.Millisecond)

	// 4. A zombie writer / subsequent job lands another record in
	// the same partition without the token.
	if _, err := store.WriteWithKey(ctx, key, []Rec{{
		Period: "2026-04-22", Customer: "alice",
		SKU: "zombie", Value: 999, Ts: time.UnixMilli(20),
	}}); err != nil {
		t.Fatalf("zombie write: %v", err)
	}
	time.Sleep(testSettleWindow + 100*time.Millisecond)

	// 5. Attempt-2 retries the same Read with the same token. The
	// barrier excludes both the attempt-1 file (self-exclusion) and
	// the zombie file (LastModified >= barrier). Only the baseline
	// survives.
	attempt2, err := store.Read(ctx, []string{key},
		WithIdempotentRead(token))
	if err != nil {
		t.Fatalf("attempt-2 Read: %v", err)
	}
	if len(attempt2) != 1 || attempt2[0].SKU != "baseline" {
		t.Fatalf("attempt-2 saw %+v, want [baseline] "+
			"(barrier should exclude attempt-1 + zombie)", attempt2)
	}

	// Sanity: without the barrier, Read sees all three records.
	// The newIdempotentStore fixture has EntityKeyOf on
	// (customer, sku) with VersionOf = Value, so the three SKUs
	// are distinct entities and nothing dedups away.
	full, err := store.Read(ctx, []string{key})
	if err != nil {
		t.Fatalf("full Read: %v", err)
	}
	if len(full) != 3 {
		t.Fatalf("full Read saw %d records, want 3", len(full))
	}
}

// TestIdempotentRead_PerPartitionIsolation verifies the per-
// partition scope: a token written into partition A produces a
// barrier in A but leaves partition B unfiltered, so a cross-
// partition Read under the barrier still returns B's data in
// full.
func TestIdempotentRead_PerPartitionIsolation(t *testing.T) {
	ctx := context.Background()
	store := newIdempotentStore(t)

	keyA := "period=2026-04-22/customer=alice"
	keyB := "period=2026-04-22/customer=bob"

	// Seed both partitions.
	if _, err := store.WriteWithKey(ctx, keyA, []Rec{{
		Period: "2026-04-22", Customer: "alice",
		SKU: "a-baseline", Value: 1, Ts: time.UnixMilli(1),
	}}); err != nil {
		t.Fatalf("seed A: %v", err)
	}
	if _, err := store.WriteWithKey(ctx, keyB, []Rec{{
		Period: "2026-04-22", Customer: "bob",
		SKU: "b-baseline", Value: 1, Ts: time.UnixMilli(1),
	}}); err != nil {
		t.Fatalf("seed B: %v", err)
	}
	time.Sleep(1100 * time.Millisecond)

	const token = "2026-04-22T10:15:00Z-cross-partition"

	// Attempt-1 only touches partition A.
	if _, err := store.WriteWithKey(ctx, keyA, []Rec{{
		Period: "2026-04-22", Customer: "alice",
		SKU: "a-derived", Value: 100, Ts: time.UnixMilli(10),
	}}, WithIdempotencyToken(token)); err != nil {
		t.Fatalf("attempt-1 A write: %v", err)
	}
	time.Sleep(1100 * time.Millisecond)

	// Zombie adds data to B *after* the barrier on A.
	if _, err := store.WriteWithKey(ctx, keyB, []Rec{{
		Period: "2026-04-22", Customer: "bob",
		SKU: "b-late", Value: 200, Ts: time.UnixMilli(20),
	}}); err != nil {
		t.Fatalf("zombie B: %v", err)
	}
	time.Sleep(testSettleWindow + 100*time.Millisecond)

	// Read across both partitions with the barrier. A is filtered
	// (only baseline survives); B has no token file so its barrier
	// is absent — both B records pass through.
	got, err := store.Read(ctx, []string{"period=2026-04-22/customer=*"},
		WithIdempotentRead(token))
	if err != nil {
		t.Fatalf("barrier Read: %v", err)
	}

	// Expected: a-baseline, b-baseline, b-late. Attempt-1's A
	// file (a-derived) is excluded by the barrier.
	gotSKUs := make(map[string]bool, len(got))
	for _, r := range got {
		gotSKUs[r.SKU] = true
	}
	want := []string{"a-baseline", "b-baseline", "b-late"}
	if len(got) != len(want) {
		t.Fatalf("got %d records, want %d: skus=%v",
			len(got), len(want), gotSKUs)
	}
	for _, sku := range want {
		if !gotSKUs[sku] {
			t.Errorf("missing SKU %q (got %v)", sku, gotSKUs)
		}
	}
	if gotSKUs["a-derived"] {
		t.Error("attempt-1's A file should be excluded by barrier")
	}
}

// TestIdempotentRead_RejectsBadToken: the barrier token must pass
// ValidateIdempotencyToken — bad tokens surface at Read time with
// a clear error, before any S3 GET.
func TestIdempotentRead_RejectsBadToken(t *testing.T) {
	ctx := context.Background()
	store := newIdempotentStore(t)

	if _, err := store.Read(ctx, []string{"period=2026-04-22/customer=*"},
		WithIdempotentRead("has/slash")); err == nil {
		t.Fatal("want error for barrier token with '/', got nil")
	}
}

// TestCommitTimeout_BelowFloorRejected guards that NewS3Target
// rejects a persisted CommitTimeout below CommitTimeoutFloor
// (1 ms — strictly positive; zero would cause every write to
// exceed the timeout). Both negatives and zero hit the same
// "below the floor" path; this test covers a negative — see
// TestCommitTimeout_ZeroRejected for the zero case.
func TestCommitTimeout_BelowFloorRejected(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	f.seedDurationConfig(t, "store/_config/commit-timeout", -100*time.Millisecond)
	f.seedDurationConfig(t, "store/_config/max-clock-skew", testMaxClockSkew)

	_, err := NewS3Target(ctx, S3TargetConfig{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
	})
	if err == nil {
		t.Fatal("expected error for negative CommitTimeout, got nil")
	}
	if !strings.Contains(err.Error(), "floor") {
		t.Errorf("error %q should mention the floor", err)
	}
}

// TestCommitTimeout_ZeroRejected guards that CommitTimeout = 0s
// is rejected via the CommitTimeoutFloor (1 ms — strictly
// positive). Zero is not "unlimited" — it would cause every
// write to exceed the timeout (the elapsed wall-clock from
// refMicroTs to token-commit completion is always strictly
// positive). Now flows through the same "below the floor" path
// as negatives.
func TestCommitTimeout_ZeroRejected(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	f.seedDurationConfig(t, "store/_config/commit-timeout", 0)
	f.seedDurationConfig(t, "store/_config/max-clock-skew", testMaxClockSkew)

	_, err := NewS3Target(ctx, S3TargetConfig{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
	})
	if err == nil {
		t.Fatal("expected error for zero CommitTimeout, got nil")
	}
	if !strings.Contains(err.Error(), "floor") {
		t.Errorf("error %q should mention the floor", err)
	}
}

// TestMaxClockSkew_BelowFloorRejected guards that NewS3Target
// rejects a negative MaxClockSkew (the floor is zero). A negative
// skew is incoherent — refCutoff would round-trip into the future.
func TestMaxClockSkew_BelowFloorRejected(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	f.seedDurationConfig(t, "store/_config/commit-timeout", testCommitTimeout)
	f.seedDurationConfig(t, "store/_config/max-clock-skew", -1*time.Second)

	_, err := NewS3Target(ctx, S3TargetConfig{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
	})
	if err == nil {
		t.Fatal("expected error for negative MaxClockSkew, got nil")
	}
	if !strings.Contains(err.Error(), "floor") {
		t.Errorf("error %q should mention the floor", err)
	}
}

// TestCommitTimeout_MissingRejected guards that NewS3Target fails
// fast when only max-clock-skew is seeded. Production callers seed
// via the boto3 snippet (see README); the error surfaces with a
// hint pointing at that step and names the missing key.
func TestCommitTimeout_MissingRejected(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	// Only seed max-clock-skew; commit-timeout is missing.
	f.seedDurationConfig(t, "store/_config/max-clock-skew", testMaxClockSkew)

	_, err := NewS3Target(ctx, S3TargetConfig{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
	})
	if err == nil {
		t.Fatal("expected error for missing commit-timeout, got nil")
	}
	if !strings.Contains(err.Error(), "commit-timeout") {
		t.Errorf("error %q should name the missing config key", err)
	}
}

// TestMaxClockSkew_MissingRejected guards that NewS3Target fails
// TestSnapshotRead_UncommittedDataInvisible guards Phase 2's
// snapshot-read commit gate: a parquet whose `<token>.commit`
// marker is missing must not appear in any snapshot read path,
// even though the file itself is present in the partition LIST.
//
// Simulates the failed-mid-write shape by deleting the
// token-commit out-of-band after a successful Write. The data
// file persists (the library never deletes it) but every
// snapshot entry point — Read, ReadIter, ReadRangeIter,
// LookupCommit — must agree it is invisible.
func TestSnapshotRead_UncommittedDataInvisible(t *testing.T) {
	ctx := context.Background()
	store := newIdempotentStore(t)

	key := "period=2026-04-22/customer=ghost"
	rec := []Rec{{
		Period: "2026-04-22", Customer: "ghost",
		SKU: "vanish", Value: 1, Ts: time.UnixMilli(1),
	}}
	const token = "job-uncommitted-ghost"

	wr, err := store.WriteWithKey(ctx, key, rec,
		WithIdempotencyToken(token))
	if err != nil {
		t.Fatalf("WriteWithKey: %v", err)
	}

	// Sanity: the write was visible before the simulated outage.
	got, err := store.Read(ctx, []string{key})
	if err != nil {
		t.Fatalf("pre-delete Read: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("pre-delete Read: got %d, want 1", len(got))
	}

	// Simulate a failed-mid-write attempt: delete the token-commit
	// while leaving the data file. The orphan parquet now mimics
	// a writer that crashed between data PUT and token-commit PUT.
	commitKey := tokenCommitKey(
		store.Target().Prefix()+"/data", key, token)
	if _, err := store.Target().S3Client().DeleteObject(ctx,
		&s3.DeleteObjectInput{
			Bucket: aws.String(store.Target().Bucket()),
			Key:    aws.String(commitKey),
		}); err != nil {
		t.Fatalf("DeleteObject token-commit: %v", err)
	}

	// Read: gate must drop the parquet.
	got, err = store.Read(ctx, []string{key})
	if err != nil {
		t.Fatalf("post-delete Read: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("post-delete Read: got %d records, want 0 "+
			"(uncommitted parquet must be invisible): %+v",
			len(got), got)
	}

	// ReadIter: same gate, streaming path.
	var iterOut []Rec
	for r, err := range store.ReadIter(ctx, []string{key}) {
		if err != nil {
			t.Fatalf("ReadIter: %v", err)
		}
		iterOut = append(iterOut, r)
	}
	if len(iterOut) != 0 {
		t.Errorf("post-delete ReadIter: got %d records, want 0",
			len(iterOut))
	}

	// LookupCommit: must report ok=false now that the marker is gone.
	_, ok, err := store.LookupCommit(ctx, key, token)
	if err != nil {
		t.Fatalf("LookupCommit: %v", err)
	}
	if ok {
		t.Errorf("LookupCommit: ok=true, want false (token-commit deleted)")
	}

	// Reference data path stayed in place (the library never
	// deletes data); the LIST sees it but the gate hides it.
	_, err = store.Target().S3Client().HeadObject(ctx,
		&s3.HeadObjectInput{
			Bucket: aws.String(store.Target().Bucket()),
			Key:    aws.String(wr.DataPath),
		})
	if err != nil {
		t.Fatalf("data file unexpectedly absent: %v", err)
	}
}

// TestStreamPoll_UncommittedRefSkipped guards the stream-side
// commit gate: a ref whose `<token>.commit` is missing is
// transparently skipped by Poll, the consumer's offset still
// advances past it (no re-walking), and the next ref's commit
// is observed normally.
func TestStreamPoll_UncommittedRefSkipped(t *testing.T) {
	ctx := context.Background()
	store := newIdempotentStore(t)

	key := "period=2026-04-22/customer=stream"
	rec := func(value int64) Rec {
		return Rec{
			Period: "2026-04-22", Customer: "stream",
			SKU: "k", Value: value, Ts: time.UnixMilli(value),
		}
	}

	// Write A — token-commit will be deleted.
	const tokA = "job-stream-A"
	wrA, err := store.WriteWithKey(ctx, key, []Rec{rec(1)},
		WithIdempotencyToken(tokA))
	if err != nil {
		t.Fatalf("WriteWithKey A: %v", err)
	}
	// Write B — fully committed.
	const tokB = "job-stream-B"
	if _, err := store.WriteWithKey(ctx, key, []Rec{rec(2)},
		WithIdempotencyToken(tokB)); err != nil {
		t.Fatalf("WriteWithKey B: %v", err)
	}

	// Knock out A's token-commit so the corresponding ref
	// becomes a "failed write" the gate must skip.
	commitKeyA := tokenCommitKey(
		store.Target().Prefix()+"/data", key, tokA)
	if _, err := store.Target().S3Client().DeleteObject(ctx,
		&s3.DeleteObjectInput{
			Bucket: aws.String(store.Target().Bucket()),
			Key:    aws.String(commitKeyA),
		}); err != nil {
		t.Fatalf("DeleteObject token-commit A: %v", err)
	}

	// Wait past SettleWindow so the cutoff has passed both refs
	// (Poll only emits refs whose refMicroTs is past the cutoff).
	time.Sleep(testSettleWindow + 200*time.Millisecond)

	entries, _, err := store.Poll(ctx, "", 100)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	// Only the committed ref (write B) should surface; A's
	// uncommitted ref must be skipped.
	if len(entries) != 1 {
		t.Fatalf("got %d entries, want 1 (A skipped, B kept):\n %+v",
			len(entries), entries)
	}
	if entries[0].RefPath == string(wrA.Offset) {
		t.Errorf("uncommitted ref %q surfaced — gate failed",
			entries[0].RefPath)
	}
}

// TestLookupCommit_RoundTrip asserts that LookupCommit returns a
// WriteResult identical to the original Write's. Same DataPath,
// same RefPath, same Offset, and an InsertedAt that matches
// (the field is sourced from the token-commit's `insertedat`
// metadata = the original write's pre-encode wall-clock).
func TestLookupCommit_RoundTrip(t *testing.T) {
	ctx := context.Background()
	store := newIdempotentStore(t)

	key := "period=2026-04-22/customer=lookup"
	rec := []Rec{{
		Period: "2026-04-22", Customer: "lookup",
		SKU: "k", Value: 5, Ts: time.UnixMilli(5),
	}}
	const token = "job-lookup-A"

	wr, err := store.WriteWithKey(ctx, key, rec,
		WithIdempotencyToken(token))
	if err != nil {
		t.Fatalf("WriteWithKey: %v", err)
	}

	got, ok, err := store.LookupCommit(ctx, key, token)
	if err != nil {
		t.Fatalf("LookupCommit: %v", err)
	}
	if !ok {
		t.Fatalf("LookupCommit: ok=false, want true")
	}
	if got.DataPath != wr.DataPath {
		t.Errorf("DataPath = %q, want %q", got.DataPath, wr.DataPath)
	}
	if got.RefPath != wr.RefPath {
		t.Errorf("RefPath = %q, want %q", got.RefPath, wr.RefPath)
	}
	if got.Offset != wr.Offset {
		t.Errorf("Offset = %q, want %q", got.Offset, wr.Offset)
	}
	if !got.InsertedAt.Equal(wr.InsertedAt) {
		t.Errorf("InsertedAt = %v, want %v", got.InsertedAt, wr.InsertedAt)
	}
	if got.RowCount != wr.RowCount {
		t.Errorf("RowCount = %d, want %d", got.RowCount, wr.RowCount)
	}
	if wr.RowCount != int64(len(rec)) {
		t.Errorf("fresh-write RowCount = %d, want %d (len(records))",
			wr.RowCount, len(rec))
	}
}

// TestLookupCommit_MissingReturnsOK_False guards the not-found
// branch: a token that was never written returns (zero-value, false, nil)
// — distinct from a real error.
func TestLookupCommit_MissingReturnsOK_False(t *testing.T) {
	ctx := context.Background()
	store := newIdempotentStore(t)

	got, ok, err := store.LookupCommit(ctx,
		"period=2026-04-22/customer=ghost", "never-written")
	if err != nil {
		t.Fatalf("LookupCommit: %v", err)
	}
	if ok {
		t.Errorf("ok=true for unknown token, want false")
	}
	if got != (WriteResult{}) {
		t.Errorf("got %+v, want zero-value WriteResult", got)
	}
}

// TestLookupCommit_RejectsBadToken guards that LookupCommit
// runs the token validation up-front so callers fail loudly on
// garbage input instead of HEADing nonsensical keys.
func TestLookupCommit_RejectsBadToken(t *testing.T) {
	ctx := context.Background()
	store := newIdempotentStore(t)

	for _, bad := range []string{"", "tok/with/slash", "tok;semi"} {
		_, _, err := store.LookupCommit(ctx,
			"period=2026-04-22/customer=ghost", bad)
		if err == nil {
			t.Errorf("LookupCommit(%q) returned nil err, want validation error", bad)
		}
	}
}

// fast when only commit-timeout is seeded. Mirrors
// TestCommitTimeout_MissingRejected for the second knob.
func TestMaxClockSkew_MissingRejected(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	// Only seed commit-timeout; max-clock-skew is missing.
	f.seedDurationConfig(t, "store/_config/commit-timeout", testCommitTimeout)

	_, err := NewS3Target(ctx, S3TargetConfig{
		Bucket:            f.Bucket,
		Prefix:            "store",
		S3Client:          f.S3Client,
		PartitionKeyParts: []string{"period", "customer"},
	})
	if err == nil {
		t.Fatal("expected error for missing max-clock-skew, got nil")
	}
	if !strings.Contains(err.Error(), "max-clock-skew") {
		t.Errorf("error %q should name the missing config key", err)
	}
}
