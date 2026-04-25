package s3parquet

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// stubBackoff replaces retryBackoff with zero-length sleeps so
// the tests don't wait 1.4s for an exhaustion case. Restored on
// cleanup so parallel tests don't observe the stub.
func stubBackoff(t *testing.T) {
	t.Helper()
	old := retryBackoff
	retryBackoff = [retryMaxAttempts - 1]time.Duration{0, 0, 0}
	t.Cleanup(func() { retryBackoff = old })
}

func TestRetry_SuccessFirstAttempt(t *testing.T) {
	stubBackoff(t)
	var calls atomic.Int32
	err := retry(context.Background(), func() error {
		calls.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("want nil err, got %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("want 1 call, got %d", got)
	}
}

func TestRetry_TransientThenSuccess(t *testing.T) {
	stubBackoff(t)
	var calls atomic.Int32
	transient := &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{
			Response: &http.Response{StatusCode: 503},
		},
	}
	err := retry(context.Background(), func() error {
		if calls.Add(1) < 3 {
			return transient
		}
		return nil
	})
	if err != nil {
		t.Fatalf("want nil err, got %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Errorf("want 3 calls (2 transient + 1 success), got %d", got)
	}
}

func TestRetry_NonTransientNoRetry(t *testing.T) {
	stubBackoff(t)
	var calls atomic.Int32
	notFound := &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{
			Response: &http.Response{StatusCode: 404},
		},
	}
	err := retry(context.Background(), func() error {
		calls.Add(1)
		return notFound
	})
	if err == nil {
		t.Fatal("want non-nil err")
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("want 1 call (no retry on 404), got %d", got)
	}
}

func TestRetry_ExhaustsBudget(t *testing.T) {
	stubBackoff(t)
	var calls atomic.Int32
	transient := &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{
			Response: &http.Response{StatusCode: 500},
		},
	}
	err := retry(context.Background(), func() error {
		calls.Add(1)
		return transient
	})
	if err == nil {
		t.Fatal("want non-nil err after exhaustion")
	}
	if got := calls.Load(); int(got) != retryMaxAttempts {
		t.Errorf("want %d calls, got %d", retryMaxAttempts, got)
	}
}

func TestRetry_ContextCancelledBetweenAttempts(t *testing.T) {
	// Real backoff here so the ctx-check in the sleep path fires.
	old := retryBackoff
	retryBackoff = [retryMaxAttempts - 1]time.Duration{
		50 * time.Millisecond, 0, 0,
	}
	t.Cleanup(func() { retryBackoff = old })

	ctx, cancel := context.WithCancel(context.Background())
	var calls atomic.Int32
	transient := &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{
			Response: &http.Response{StatusCode: 500},
		},
	}
	err := retry(ctx, func() error {
		calls.Add(1)
		// Cancel after the first attempt fails; the retry helper
		// should observe the cancellation during its backoff
		// sleep and bail out before issuing attempt 2.
		cancel()
		return transient
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("want 1 call (cancelled before retry), got %d", got)
	}
}

func TestIsTransientS3Error(t *testing.T) {
	stubBackoff(t)
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"context.Canceled", context.Canceled, false},
		{"context.DeadlineExceeded", context.DeadlineExceeded, false},
		{"http 500", respErr(500), true},
		{"http 502", respErr(502), true},
		{"http 503", respErr(503), true},
		{"http 429", respErr(429), true},
		{"http 404", respErr(404), false},
		{"http 403", respErr(403), false},
		{"http 412", respErr(412), false},
		{"http 400", respErr(400), false},
		// No HTTP response → transport/network error → retry.
		{"plain network error", &net.OpError{Op: "dial"}, true},
		{"generic error", errors.New("boom"), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isTransientS3Error(tc.err); got != tc.want {
				t.Errorf("isTransientS3Error(%v) = %v, want %v",
					tc.err, got, tc.want)
			}
		})
	}
}

func respErr(status int) error {
	return &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{
			Response: &http.Response{StatusCode: status},
		},
	}
}

// newTestTarget builds an S3Target pointed at the given httptest
// server. SDK retry is disabled via aws.NopRetryer so the tests
// observe only the package-level retry layer.
func newTestTarget(t *testing.T, endpoint string) S3Target {
	t.Helper()
	cli := s3.NewFromConfig(aws.Config{
		Region: "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider(
			"k", "s", ""),
		Retryer: func() aws.Retryer { return aws.NopRetryer{} },
	}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	return S3Target{Bucket: "bucket", S3Client: cli}
}

// statusServer returns an httptest server that responds with
// statuses[i] on the i-th request (clamped to the last entry
// for requests beyond len(statuses)). Counts requests so tests
// can assert the retry attempt count.
func statusServer(
	t *testing.T, statuses []int,
) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var count atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			i := int(count.Add(1)) - 1
			if i >= len(statuses) {
				i = len(statuses) - 1
			}
			w.WriteHeader(statuses[i])
		}))
	t.Cleanup(srv.Close)
	return srv, &count
}

func TestTarget_PutRetriesOn5xx(t *testing.T) {
	stubBackoff(t)
	srv, count := statusServer(t,
		[]int{503, 503, 200})
	tgt := newTestTarget(t, srv.URL)

	if err := tgt.put(context.Background(), "key",
		[]byte("x"), "application/octet-stream", ""); err != nil {
		t.Fatalf("put: %v", err)
	}
	if got := count.Load(); got != 3 {
		t.Errorf("want 3 requests, got %d", got)
	}
}

func TestTarget_PutRetriesOn429(t *testing.T) {
	stubBackoff(t)
	srv, count := statusServer(t, []int{429, 200})
	tgt := newTestTarget(t, srv.URL)

	if err := tgt.put(context.Background(), "key",
		[]byte("x"), "application/octet-stream", ""); err != nil {
		t.Fatalf("put: %v", err)
	}
	if got := count.Load(); got != 2 {
		t.Errorf("want 2 requests, got %d", got)
	}
}

func TestTarget_PutNoRetryOn404(t *testing.T) {
	stubBackoff(t)
	srv, count := statusServer(t, []int{404})
	tgt := newTestTarget(t, srv.URL)

	err := tgt.put(context.Background(), "key",
		[]byte("x"), "application/octet-stream", "")
	if err == nil {
		t.Fatal("want error on 404, got nil")
	}
	if got := count.Load(); got != 1 {
		t.Errorf("want 1 request (no retry on 404), got %d", got)
	}
}

func TestTarget_GetRetriesOn5xx(t *testing.T) {
	stubBackoff(t)
	body := "hello"
	var count atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			n := count.Add(1)
			if n < 3 {
				w.WriteHeader(500)
				return
			}
			w.Header().Set(
				"Content-Length", fmt.Sprintf("%d", len(body)))
			w.WriteHeader(200)
			_, _ = w.Write([]byte(body))
		}))
	t.Cleanup(srv.Close)
	tgt := newTestTarget(t, srv.URL)

	got, err := tgt.get(context.Background(), "key", "")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got) != body {
		t.Errorf("got %q, want %q", got, body)
	}
	if n := count.Load(); n != 3 {
		t.Errorf("want 3 requests, got %d", n)
	}
}
