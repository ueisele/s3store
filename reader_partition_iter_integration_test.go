//go:build integration

package s3store

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"
)

// TestReadPartitionIter_GroupsByHivePartition writes records to
// three Hive partitions and verifies ReadPartitionIter yields one
// HivePartition per partition with the correct Key and Rows.
// Mirrors TestReadIter for shape but checks the partition-grouped
// emit instead of record-at-a-time.
func TestReadPartitionIter_GroupsByHivePartition(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{})

	// Three partitions, two records each.
	for _, c := range []string{"a", "b", "c"} {
		key := fmt.Sprintf("period=2026-03-17/customer=%s", c)
		if _, err := store.WriteWithKey(ctx, key, []Rec{
			{Period: "2026-03-17", Customer: c, SKU: "s1", Value: 1},
			{Period: "2026-03-17", Customer: c, SKU: "s2", Value: 2},
		}); err != nil {
			t.Fatalf("WriteWithKey %s: %v", c, err)
		}
	}

	var got []HivePartition[Rec]
	for p, err := range store.ReadPartitionIter(ctx, []string{"*"}) {
		if err != nil {
			t.Fatalf("ReadPartitionIter: %v", err)
		}
		got = append(got, p)
	}

	if len(got) != 3 {
		t.Fatalf("got %d partitions, want 3", len(got))
	}
	wantKeys := []string{
		"period=2026-03-17/customer=a",
		"period=2026-03-17/customer=b",
		"period=2026-03-17/customer=c",
	}
	for i, p := range got {
		if p.Key != wantKeys[i] {
			t.Errorf("[%d] Key=%q, want %q", i, p.Key, wantKeys[i])
		}
		if len(p.Rows) != 2 {
			t.Errorf("[%d] %d rows, want 2", i, len(p.Rows))
		}
	}
}

// TestReadPartitionIter_PerPartitionDedup mirrors
// TestReadIter_PerPartitionDedup: with EntityKeyOf set and no
// WithHistory, two writes to the same entity in the same partition
// must collapse to the latest record. The HivePartition.Rows for
// that partition holds exactly one record.
func TestReadPartitionIter_PerPartitionDedup(t *testing.T) {
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

	var got []HivePartition[Rec]
	for p, err := range store.ReadPartitionIter(ctx, []string{key}) {
		if err != nil {
			t.Fatalf("ReadPartitionIter: %v", err)
		}
		got = append(got, p)
	}
	if len(got) != 1 {
		t.Fatalf("got %d partitions, want 1", len(got))
	}
	if len(got[0].Rows) != 1 {
		t.Fatalf("got %d rows, want 1 (deduped)", len(got[0].Rows))
	}
	if got[0].Rows[0].Value != 99 {
		t.Errorf("got Value=%d, want 99 (newer Ts wins)",
			got[0].Rows[0].Value)
	}
}

// TestReadPartitionIter_WithHistory disables dedup; both writes
// surface inside a single HivePartition.Rows.
func TestReadPartitionIter_WithHistory(t *testing.T) {
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

	var got []HivePartition[Rec]
	for p, err := range store.ReadPartitionIter(
		ctx, []string{key}, WithHistory(),
	) {
		if err != nil {
			t.Fatalf("ReadPartitionIter: %v", err)
		}
		got = append(got, p)
	}
	if len(got) != 1 {
		t.Fatalf("got %d partitions, want 1", len(got))
	}
	if len(got[0].Rows) != 2 {
		t.Errorf("got %d rows, want 2 (history)", len(got[0].Rows))
	}
}

// TestReadPartitionIter_EmptyPatterns yields nothing without
// error when keyPatterns is empty.
func TestReadPartitionIter_EmptyPatterns(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{})

	count := 0
	for _, err := range store.ReadPartitionIter(ctx, nil) {
		if err != nil {
			t.Fatalf("ReadPartitionIter: %v", err)
		}
		count++
	}
	if count != 0 {
		t.Errorf("got %d partitions, want 0", count)
	}
}

// TestReadPartitionIter_EarlyBreak proves that breaking out of
// the for-range loop releases resources cleanly: the iterator's
// deferred cancel stops in-flight downloads and a follow-up
// iteration completes normally.
func TestReadPartitionIter_EarlyBreak(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{})

	for i := range 10 {
		key := fmt.Sprintf("period=2026-03-17/customer=c%d", i)
		if _, err := store.WriteWithKey(ctx, key, []Rec{
			{Period: "2026-03-17", Customer: fmt.Sprintf("c%d", i),
				SKU: "s1", Value: int64(i)},
		}); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}

	count := 0
	for _, err := range store.ReadPartitionIter(ctx, []string{"*"}) {
		if err != nil {
			t.Fatalf("ReadPartitionIter: %v", err)
		}
		count++
		if count == 3 {
			break
		}
	}
	if count != 3 {
		t.Fatalf("got %d, want exactly 3 before break", count)
	}

	// Second iteration must complete — no goroutine leak.
	count = 0
	for _, err := range store.ReadPartitionIter(ctx, []string{"*"}) {
		if err != nil {
			t.Fatalf("second ReadPartitionIter: %v", err)
		}
		count++
	}
	if count != 10 {
		t.Errorf("second pass got %d, want 10", count)
	}
}

// TestReadPartitionRangeIter_GroupsByHivePartition writes records
// across multiple partitions, then ReadPartitionRangeIter drains
// the full window and yields one HivePartition per Hive partition.
func TestReadPartitionRangeIter_GroupsByHivePartition(t *testing.T) {
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

	var got []HivePartition[Rec]
	for p, err := range store.ReadPartitionRangeIter(ctx, before, after) {
		if err != nil {
			t.Fatalf("ReadPartitionRangeIter: %v", err)
		}
		got = append(got, p)
	}
	if len(got) != 3 {
		t.Fatalf("got %d partitions, want 3", len(got))
	}
	wantKeys := []string{
		"period=2026-03-17/customer=a",
		"period=2026-03-17/customer=b",
		"period=2026-03-17/customer=c",
	}
	for i, p := range got {
		if p.Key != wantKeys[i] {
			t.Errorf("[%d] Key=%q, want %q", i, p.Key, wantKeys[i])
		}
		if len(p.Rows) != 2 {
			t.Errorf("[%d] %d rows, want 2", i, len(p.Rows))
		}
	}
}

// TestReadPartitionRangeIter_OpenBounds matches ReadRangeIter's
// open-bounds behavior: zero-value time on either side means
// stream-head / live-tip.
func TestReadPartitionRangeIter_OpenBounds(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{})

	for _, c := range []string{"a", "b"} {
		key := fmt.Sprintf("period=2026-03-17/customer=%s", c)
		if _, err := store.WriteWithKey(ctx, key, []Rec{
			{Period: "2026-03-17", Customer: c, SKU: "s1", Value: 1},
		}); err != nil {
			t.Fatalf("WriteWithKey %s: %v", c, err)
		}
	}
	time.Sleep(testSettleWindow + 100*time.Millisecond)

	var got []string
	for p, err := range store.ReadPartitionRangeIter(
		ctx, time.Time{}, time.Time{},
	) {
		if err != nil {
			t.Fatalf("ReadPartitionRangeIter: %v", err)
		}
		got = append(got, p.Key)
	}
	want := []string{
		"period=2026-03-17/customer=a",
		"period=2026-03-17/customer=b",
	}
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// TestReadPartitionRangeIter_EmptyWindow verifies an empty
// window yields nothing without error.
func TestReadPartitionRangeIter_EmptyWindow(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, storeOpts{})

	if _, err := store.WriteWithKey(ctx,
		"period=2026-03-17/customer=a",
		[]Rec{{Period: "2026-03-17", Customer: "a",
			SKU: "s1", Value: 1}},
	); err != nil {
		t.Fatalf("WriteWithKey: %v", err)
	}
	time.Sleep(testSettleWindow + 100*time.Millisecond)

	before := time.Now().Add(-time.Hour)
	until := before.Add(time.Minute)

	count := 0
	for _, err := range store.ReadPartitionRangeIter(ctx, before, until) {
		if err != nil {
			t.Fatalf("ReadPartitionRangeIter: %v", err)
		}
		count++
	}
	if count != 0 {
		t.Errorf("got %d partitions, want 0", count)
	}
}
