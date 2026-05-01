//go:build integration

package s3store

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// newStoreOnFixture builds a Store on the given fixture under
// the given prefix. Used by the cross-store rejection tests
// where two Stores must share a bucket but differ in prefix.
func newStoreOnFixture(
	t *testing.T, f *fixture, prefix string,
) *Store[Rec] {
	t.Helper()
	f.SeedTimingConfig(t, prefix, testCommitTimeout, testMaxClockSkew)
	store, err := New[Rec](t.Context(), StoreConfig[Rec]{
		S3TargetConfig: S3TargetConfig{
			Bucket:             f.Bucket,
			Prefix:             prefix,
			S3Client:           f.S3Client,
			PartitionKeyParts:  []string{"period", "customer"},
			ConsistencyControl: ConsistencyStrongGlobal,
		},
		PartitionKeyOf: func(r Rec) string {
			return fmt.Sprintf("period=%s/customer=%s",
				r.Period, r.Customer)
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return store
}

// TestReadEntriesIter_BasicShape pins the happy path: PollRange
// resolves a set of entries, ReadEntriesIter decodes them and
// surfaces every record. Verifies the no-Poll-required flow that
// is the whole point of the new method.
func TestReadEntriesIter_BasicShape(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{})

	before := time.Now().Add(-1 * time.Second).Truncate(time.Second)
	for _, c := range []string{"a", "b", "c"} {
		key := fmt.Sprintf("period=2026-03-17/customer=%s", c)
		if _, err := store.WriteWithKey(ctx, key, []Rec{
			{Period: "2026-03-17", Customer: c, SKU: "s1", Value: 1},
			{Period: "2026-03-17", Customer: c, SKU: "s2", Value: 2},
		}); err != nil {
			t.Fatalf("WriteWithKey %s: %v", c, err)
		}
	}
	time.Sleep(testSettleWindow + 100*time.Millisecond)
	after := time.Now()

	entries, err := store.PollRange(ctx, before, after)
	if err != nil {
		t.Fatalf("PollRange: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("PollRange: got %d entries, want 3", len(entries))
	}

	var got []Rec
	for r, err := range store.ReadEntriesIter(ctx, entries) {
		if err != nil {
			t.Fatalf("ReadEntriesIter: %v", err)
		}
		got = append(got, r)
	}
	if len(got) != 6 {
		t.Errorf("got %d records, want 6", len(got))
	}
}

// TestReadEntriesIter_RejectsCrossStoreEntries pins the safety
// gate: entries resolved from one Store cannot be silently
// passed to a different Store's ReadEntriesIter. The first call
// to yield must surface a wrapped error and the iter must
// terminate without any S3 traffic.
func TestReadEntriesIter_RejectsCrossStoreEntries(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	storeA := newStoreOnFixture(t, f, "storeA")
	storeB := newStoreOnFixture(t, f, "storeB")

	before := time.Now().Add(-1 * time.Second).Truncate(time.Second)
	if _, err := storeA.WriteWithKey(ctx,
		"period=2026-03-17/customer=a",
		[]Rec{{Period: "2026-03-17", Customer: "a",
			SKU: "s1", Value: 1}},
	); err != nil {
		t.Fatalf("Write storeA: %v", err)
	}
	time.Sleep(testSettleWindow + 100*time.Millisecond)

	entriesA, err := storeA.PollRange(ctx, before, time.Now())
	if err != nil {
		t.Fatalf("PollRange storeA: %v", err)
	}
	if len(entriesA) == 0 {
		t.Fatal("PollRange storeA: got 0 entries")
	}

	// Pass storeA's entries to storeB — must reject upfront.
	count := 0
	var gotErr error
	for _, err := range storeB.ReadEntriesIter(ctx, entriesA) {
		count++
		if err != nil {
			gotErr = err
			break
		}
	}
	if gotErr == nil {
		t.Fatalf("ReadEntriesIter cross-store: got nil err, "+
			"want validation error after %d yields", count)
	}
	if !strings.Contains(gotErr.Error(),
		"entries from a different Store") {
		t.Errorf("ReadEntriesIter cross-store: error %q does "+
			"not mention cross-store mismatch", gotErr)
	}
	// Exactly one yield: the error.
	if count != 1 {
		t.Errorf("ReadEntriesIter cross-store: got %d yields, want 1",
			count)
	}
}

// TestReadEntriesIter_EmptyEntries verifies the nil/empty
// fast path yields nothing without error.
func TestReadEntriesIter_EmptyEntries(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{})

	count := 0
	for _, err := range store.ReadEntriesIter(ctx, nil) {
		if err != nil {
			t.Fatalf("ReadEntriesIter nil: %v", err)
		}
		count++
	}
	if count != 0 {
		t.Errorf("nil entries: got %d records, want 0", count)
	}

	count = 0
	for _, err := range store.ReadEntriesIter(ctx, []StreamEntry{}) {
		if err != nil {
			t.Fatalf("ReadEntriesIter empty: %v", err)
		}
		count++
	}
	if count != 0 {
		t.Errorf("empty entries: got %d records, want 0", count)
	}
}

// TestReadEntriesIter_TolerantOfMissingData pins the tolerant
// NoSuchKey contract: an operator-driven prune between PollRange
// and ReadEntriesIter must skip the missing file (logged + counted
// via s3store.read.missing_data) rather than fail the whole
// read. Mirrors the same property on ReadRangeIter.
func TestReadEntriesIter_TolerantOfMissingData(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{})

	before := time.Now().Add(-1 * time.Second).Truncate(time.Second)
	for _, c := range []string{"a", "b", "c"} {
		key := fmt.Sprintf("period=2026-03-17/customer=%s", c)
		if _, err := store.WriteWithKey(ctx, key, []Rec{
			{Period: "2026-03-17", Customer: c, SKU: "s1", Value: 1},
		}); err != nil {
			t.Fatalf("WriteWithKey %s: %v", c, err)
		}
	}
	time.Sleep(testSettleWindow + 100*time.Millisecond)

	entries, err := store.PollRange(ctx, before, time.Now())
	if err != nil {
		t.Fatalf("PollRange: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("PollRange: got %d entries, want 3", len(entries))
	}

	// Delete the middle data file — operator-prune simulation.
	pruned := entries[1].DataPath
	if _, err := store.Target().S3Client().DeleteObject(ctx,
		&s3.DeleteObjectInput{
			Bucket: aws.String(store.Target().Bucket()),
			Key:    aws.String(pruned),
		}); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}

	// ReadEntriesIter should skip the missing file, yielding
	// the records from the other two.
	var got []Rec
	for r, err := range store.ReadEntriesIter(ctx, entries) {
		if err != nil {
			t.Fatalf("ReadEntriesIter: tolerant path should not "+
				"surface NoSuchKey: %v", err)
		}
		got = append(got, r)
	}
	if len(got) != 2 {
		t.Errorf("got %d records, want 2 (one partition pruned)",
			len(got))
	}
}

// TestReadPartitionEntriesIter_BasicShape pins the
// partition-grouped variant's happy path: each entry surfaces as
// one HivePartition with its records in Rows.
func TestReadPartitionEntriesIter_BasicShape(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{})

	before := time.Now().Add(-1 * time.Second).Truncate(time.Second)
	wantKeys := []string{
		"period=2026-03-17/customer=a",
		"period=2026-03-17/customer=b",
		"period=2026-03-17/customer=c",
	}
	for _, c := range []string{"a", "b", "c"} {
		key := fmt.Sprintf("period=2026-03-17/customer=%s", c)
		if _, err := store.WriteWithKey(ctx, key, []Rec{
			{Period: "2026-03-17", Customer: c, SKU: "s1", Value: 1},
			{Period: "2026-03-17", Customer: c, SKU: "s2", Value: 2},
		}); err != nil {
			t.Fatalf("WriteWithKey %s: %v", c, err)
		}
	}
	time.Sleep(testSettleWindow + 100*time.Millisecond)

	entries, err := store.PollRange(ctx, before, time.Now())
	if err != nil {
		t.Fatalf("PollRange: %v", err)
	}

	var got []HivePartition[Rec]
	for p, err := range store.ReadPartitionEntriesIter(ctx, entries) {
		if err != nil {
			t.Fatalf("ReadPartitionEntriesIter: %v", err)
		}
		got = append(got, p)
	}
	if len(got) != 3 {
		t.Fatalf("got %d partitions, want 3", len(got))
	}
	gotKeys := make([]string, len(got))
	for i, p := range got {
		gotKeys[i] = p.Key
		if len(p.Rows) != 2 {
			t.Errorf("[%d] %d rows, want 2", i, len(p.Rows))
		}
	}
	if !slices.Equal(gotKeys, wantKeys) {
		t.Errorf("got keys %v, want %v (lex)", gotKeys, wantKeys)
	}
}

// TestReadPartitionEntriesIter_RejectsCrossStoreEntries pins the
// same safety gate on the partition-grouped variant.
func TestReadPartitionEntriesIter_RejectsCrossStoreEntries(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	storeA := newStoreOnFixture(t, f, "storeA")
	storeB := newStoreOnFixture(t, f, "storeB")

	before := time.Now().Add(-1 * time.Second).Truncate(time.Second)
	if _, err := storeA.WriteWithKey(ctx,
		"period=2026-03-17/customer=a",
		[]Rec{{Period: "2026-03-17", Customer: "a",
			SKU: "s1", Value: 1}},
	); err != nil {
		t.Fatalf("Write storeA: %v", err)
	}
	time.Sleep(testSettleWindow + 100*time.Millisecond)

	entriesA, err := storeA.PollRange(ctx, before, time.Now())
	if err != nil {
		t.Fatalf("PollRange storeA: %v", err)
	}

	var gotErr error
	for _, err := range storeB.ReadPartitionEntriesIter(ctx, entriesA) {
		if err != nil {
			gotErr = err
			break
		}
	}
	if gotErr == nil {
		t.Fatal("ReadPartitionEntriesIter cross-store: got nil err")
	}
	if !strings.Contains(gotErr.Error(),
		"entries from a different Store") {
		t.Errorf("got error %q, want cross-store mention", gotErr)
	}
}
