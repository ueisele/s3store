package s3parquet

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ueisele/s3store/internal/core"
)

// TestPollRefsAll_DrainsUntilEmpty guards the loop contract:
// pollRefsAll keeps calling its callback, advancing since to the
// next offset, until the callback returns an empty batch. The
// callback honours any upper bound — pollRefsAll treats "empty
// batch" as the sole termination signal.
func TestPollRefsAll_DrainsUntilEmpty(t *testing.T) {
	batches := [][]int{
		{1, 2},
		{3, 4},
		{5, 6},
		{},
	}
	offsets := []Offset{"o1", "o2", "o3", "o3"}

	var callCount int
	poll := func(
		_ context.Context, since Offset, max int32,
	) ([]int, Offset, error) {
		if max != pollAllBatch {
			t.Errorf("callback called with max=%d, want %d",
				max, pollAllBatch)
		}
		if callCount > 0 && since != offsets[callCount-1] {
			t.Errorf("call %d: since=%q, want %q",
				callCount, since, offsets[callCount-1])
		}
		b := batches[callCount]
		off := offsets[callCount]
		callCount++
		return b, off, nil
	}

	got, err := pollRefsAll(context.Background(), Offset(""), poll)
	if err != nil {
		t.Fatalf("pollRefsAll: %v", err)
	}

	want := []int{1, 2, 3, 4, 5, 6}
	if len(got) != len(want) {
		t.Fatalf("got %d items, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("got[%d]=%d, want %d", i, got[i], want[i])
		}
	}
	if callCount != len(batches) {
		t.Errorf("callback called %d times, want %d",
			callCount, len(batches))
	}
}

// TestPollRefsAll_PropagatesError guards that a callback error
// stops the loop and is returned unwrapped to the caller. Partial
// results accumulated before the error are discarded.
func TestPollRefsAll_PropagatesError(t *testing.T) {
	boom := errors.New("boom")
	var callCount int
	poll := func(
		_ context.Context, _ Offset, _ int32,
	) ([]int, Offset, error) {
		callCount++
		if callCount == 1 {
			return []int{1, 2}, "o1", nil
		}
		return nil, "", boom
	}

	got, err := pollRefsAll(context.Background(), Offset(""), poll)
	if !errors.Is(err, boom) {
		t.Errorf("got err=%v, want %v", err, boom)
	}
	if got != nil {
		t.Errorf("got %v, want nil on error", got)
	}
}

// TestPollRefsAll_EmptyFromStart handles the no-op case: callback
// returns empty immediately, pollRefsAll returns nil without
// looping.
func TestPollRefsAll_EmptyFromStart(t *testing.T) {
	var callCount int
	poll := func(
		_ context.Context, _ Offset, _ int32,
	) ([]int, Offset, error) {
		callCount++
		return nil, "", nil
	}

	got, err := pollRefsAll(context.Background(), Offset("since-x"), poll)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("got %v, want nil", got)
	}
	if callCount != 1 {
		t.Errorf("callback called %d times, want 1", callCount)
	}
}

// TestOffsetAtHelper guards the pure-computation part of the
// ref-stream helpers: offsetAt produces an offset that sorts
// before any ref written strictly after t and at-or-after any ref
// written at or before t.
func TestOffsetAtHelper(t *testing.T) {
	const refPath = "p/_stream/refs"
	const hiveKey = "period=X/customer=Y"

	now := time.UnixMicro(2_000_000_000_000_000)
	offset := offsetAt(refPath, now)

	earlier := core.EncodeRefKey(refPath,
		now.Add(-time.Second).UnixMicro(), "abcd1234",
		now.Add(-time.Second).UnixMicro(), hiveKey)
	if string(offset) <= earlier {
		t.Errorf("offset %q should sort after earlier ref %q",
			offset, earlier)
	}

	later := core.EncodeRefKey(refPath,
		now.Add(time.Second).UnixMicro(), "abcd1234",
		now.Add(time.Second).UnixMicro(), hiveKey)
	if string(offset) >= later {
		t.Errorf("offset %q should sort before later ref %q",
			offset, later)
	}
}

// listServer captures the Consistency-Control header on the
// incoming LIST and returns an empty (Contents-less)
// ListObjectsV2 response so pollRefs terminates cleanly after one
// page.
func listServer(
	t *testing.T, store *atomic.Value,
) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				w.WriteHeader(500)
				return
			}
			store.Store(r.Header.Get("Consistency-Control"))
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(xmlEmptyList))
		}))
	t.Cleanup(srv.Close)
	return srv
}

const xmlEmptyList = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
	<Name>bucket</Name>
	<Prefix>p/_stream/refs/</Prefix>
	<KeyCount>0</KeyCount>
	<MaxKeys>1000</MaxKeys>
	<IsTruncated>false</IsTruncated>
</ListBucketResult>`

func newTestS3Client(endpoint string) *s3.Client {
	return s3.NewFromConfig(aws.Config{
		Region: "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider(
			"k", "s", ""),
		Retryer: func() aws.Retryer { return aws.NopRetryer{} },
	}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
}

// TestPollRefs_ConsistencyControlHeaderSent verifies the
// ConsistencyControl field on refPollOpts surfaces as the
// Consistency-Control HTTP header on the LIST request. Required
// for StorageGRID strong-global to linearize the LIST with a
// concurrent ref PUT.
func TestPollRefs_ConsistencyControlHeaderSent(t *testing.T) {
	var sawHeader atomic.Value
	srv := listServer(t, &sawHeader)
	cli := newTestS3Client(srv.URL)

	_, _, err := pollRefs(context.Background(), cli, refPollOpts{
		Bucket:             "bucket",
		RefPath:            "p/_stream/refs",
		DataPath:           "p/data",
		MaxEntries:         10,
		ConsistencyControl: "strong-global",
	})
	if err != nil {
		t.Fatalf("pollRefs: %v", err)
	}
	got, _ := sawHeader.Load().(string)
	if got != "strong-global" {
		t.Errorf("Consistency-Control header = %q, want %q",
			got, "strong-global")
	}
}

// TestPollRefs_EmptyConsistencyOmitsHeader guards the fast-path:
// no header wired in means no header sent.
func TestPollRefs_EmptyConsistencyOmitsHeader(t *testing.T) {
	var sawHeader atomic.Value
	srv := listServer(t, &sawHeader)
	cli := newTestS3Client(srv.URL)

	_, _, err := pollRefs(context.Background(), cli, refPollOpts{
		Bucket:     "bucket",
		RefPath:    "p/_stream/refs",
		DataPath:   "p/data",
		MaxEntries: 10,
	})
	if err != nil {
		t.Fatalf("pollRefs: %v", err)
	}
	got, _ := sawHeader.Load().(string)
	if got != "" {
		t.Errorf("Consistency-Control header = %q, want empty", got)
	}
}
