package s3store

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
	<Prefix>p/_ref/</Prefix>
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

// newTestReader constructs a Reader pointing at the given httptest
// endpoint with the given consistency level. Used by the
// header-on-the-wire tests that don't need a real S3.
func newTestReader(
	t *testing.T, endpoint string, consistency ConsistencyLevel,
) *Reader[int] {
	t.Helper()
	r, err := NewReader[int](ReaderConfig[int]{
		Target: newS3TargetSkipConfig(S3TargetConfig{
			Bucket:             "bucket",
			Prefix:             "p",
			S3Client:           newTestS3Client(endpoint),
			PartitionKeyParts:  []string{"period"},
			ConsistencyControl: consistency,
		}),
	})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	return r
}

// TestPoll_ConsistencyControlHeaderSent verifies the
// ConsistencyControl field on the Reader's config surfaces as the
// Consistency-Control HTTP header on the LIST request. Required
// for StorageGRID strong-global to linearize the LIST with a
// concurrent ref PUT.
func TestPoll_ConsistencyControlHeaderSent(t *testing.T) {
	var sawHeader atomic.Value
	srv := listServer(t, &sawHeader)
	r := newTestReader(t, srv.URL, ConsistencyStrongGlobal)

	_, _, err := r.Poll(context.Background(), Offset(""), 10)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	got, _ := sawHeader.Load().(string)
	if got != string(ConsistencyStrongGlobal) {
		t.Errorf("Consistency-Control header = %q, want %q",
			got, ConsistencyStrongGlobal)
	}
}

// TestPoll_EmptyConsistencyDefaultsToStrongGlobal guards that an
// unset ConsistencyControl on the config is substituted with the
// library's safe-multi-site default (strong-global) at target
// construction. AWS S3 / MinIO ignore the header so this is a
// no-op for those backends; on StorageGRID it ensures
// list-after-write converges across sites.
func TestPoll_EmptyConsistencyDefaultsToStrongGlobal(t *testing.T) {
	var sawHeader atomic.Value
	srv := listServer(t, &sawHeader)
	r := newTestReader(t, srv.URL, ConsistencyDefault)

	_, _, err := r.Poll(context.Background(), Offset(""), 10)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	got, _ := sawHeader.Load().(string)
	if got != string(ConsistencyStrongGlobal) {
		t.Errorf("Consistency-Control header = %q, want %q",
			got, ConsistencyStrongGlobal)
	}
}
