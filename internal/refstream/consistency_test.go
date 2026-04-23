package refstream

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// listServer captures the Consistency-Control header on the
// incoming LIST and returns an empty (Contents-less) ListObjectsV2
// response so Poll terminates cleanly after one page.
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
			// Record whatever Consistency-Control the request
			// carried (may be empty).
			store.Store(r.Header.Get("Consistency-Control"))
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(xmlEmptyList))
		}))
	t.Cleanup(srv.Close)
	return srv
}

// xmlEmptyList is the minimal valid ListObjectsV2 response body
// with no matching keys — enough for the paginator to declare the
// page empty and Poll to return.
const xmlEmptyList = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
	<Name>bucket</Name>
	<Prefix>p/_stream/refs/</Prefix>
	<KeyCount>0</KeyCount>
	<MaxKeys>1000</MaxKeys>
	<IsTruncated>false</IsTruncated>
</ListBucketResult>`

// newTestClient returns an S3 client aimed at the given endpoint
// with SDK retries disabled so the test observes exactly the
// headers Poll's one LIST sends.
func newTestClient(endpoint string) *s3.Client {
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

// TestPoll_ConsistencyControlHeaderSent verifies the
// ConsistencyControl field on PollOpts surfaces as the
// Consistency-Control HTTP header on the LIST request. Required
// for StorageGRID strong-global to linearize the LIST with a
// concurrent ref PUT (otherwise Poll's cutoff can advance past a
// ref that's still propagating between metadata nodes).
func TestPoll_ConsistencyControlHeaderSent(t *testing.T) {
	var sawHeader atomic.Value
	srv := listServer(t, &sawHeader)
	cli := newTestClient(srv.URL)

	_, _, err := Poll(context.Background(), cli, PollOpts{
		Bucket:             "bucket",
		RefPath:            "p/_stream/refs",
		DataPath:           "p/data",
		MaxEntries:         10,
		ConsistencyControl: "strong-global",
	})
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	got, _ := sawHeader.Load().(string)
	if got != "strong-global" {
		t.Errorf("Consistency-Control header = %q, want %q",
			got, "strong-global")
	}
}

// TestPoll_EmptyConsistencyOmitsHeader guards the fast-path: no
// header wired in means no header sent — preserves the "AWS / MinIO
// sees no surprise header" contract.
func TestPoll_EmptyConsistencyOmitsHeader(t *testing.T) {
	var sawHeader atomic.Value
	srv := listServer(t, &sawHeader)
	cli := newTestClient(srv.URL)

	_, _, err := Poll(context.Background(), cli, PollOpts{
		Bucket:     "bucket",
		RefPath:    "p/_stream/refs",
		DataPath:   "p/data",
		MaxEntries: 10,
		// ConsistencyControl intentionally empty.
	})
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	got, _ := sawHeader.Load().(string)
	if got != "" {
		t.Errorf("Consistency-Control header = %q, want empty",
			got)
	}
}
