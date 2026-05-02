package s3store

import (
	"context"
	"crypto/md5" //nolint:gosec // test mirrors target.go's integrity check
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// headServer returns an httptest server that distinguishes PUT
// from HEAD responses. putFn runs on PUT, headFn runs on HEAD —
// either may be nil for tests whose flow doesn't exercise that
// verb (an unexpected request through the nil branch falls
// through to 500 so the test still surfaces loudly). Both
// callbacks receive the request counter (1-indexed) for
// per-call branching.
func headServer(
	t *testing.T,
	putFn, headFn func(w http.ResponseWriter, r *http.Request, i int),
) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var total atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			i := int(total.Add(1))
			switch r.Method {
			case http.MethodPut:
				if putFn != nil {
					putFn(w, r, i)
					return
				}
				w.WriteHeader(500)
			case http.MethodHead:
				if headFn != nil {
					headFn(w, r, i)
					return
				}
				w.WriteHeader(500)
			default:
				w.WriteHeader(500)
			}
		}))
	t.Cleanup(srv.Close)
	return srv, &total
}

// TestConsistencyControl_HeaderSentOnPUT: a target configured
// with ConsistencyControl plumbs the value through as a
// Consistency-Control HTTP header on the outgoing PUT. Confirms
// the middleware attach point.
func TestConsistencyControl_HeaderSentOnPUT(t *testing.T) {
	stubBackoff(t)
	var sawHeader atomic.Value
	srv, _ := headServer(t,
		func(w http.ResponseWriter, r *http.Request, _ int) {
			sawHeader.Store(r.Header.Get("Consistency-Control"))
			w.WriteHeader(200)
		},
		nil)
	tgt := newTestTarget(t, srv.URL, ConsistencyStrongGlobal)

	err := tgt.put(context.Background(), "k",
		[]byte("x"), "application/octet-stream")
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	got, _ := sawHeader.Load().(string)
	if got != string(ConsistencyStrongGlobal) {
		t.Errorf("Consistency-Control header = %q, want %q",
			got, ConsistencyStrongGlobal)
	}
}

// TestConsistencyControl_DefaultsToStrongGlobal: when the level
// is ConsistencyDefault (empty) at config time, NewS3Target
// substitutes ConsistencyStrongGlobal so token-commit overwrites
// converge under sequential same-token retries on multi-site
// StorageGRID without operator intervention. AWS S3 and MinIO
// ignore the header so the substitution is a no-op for those
// backends.
func TestConsistencyControl_DefaultsToStrongGlobal(t *testing.T) {
	stubBackoff(t)
	var sawHeader atomic.Value
	srv, _ := headServer(t,
		func(w http.ResponseWriter, r *http.Request, _ int) {
			sawHeader.Store(r.Header.Get("Consistency-Control"))
			w.WriteHeader(200)
		},
		nil)
	tgt := newTestTarget(t, srv.URL)

	err := tgt.put(context.Background(), "k",
		[]byte("x"), "application/octet-stream")
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	got, _ := sawHeader.Load().(string)
	if got != string(ConsistencyStrongGlobal) {
		t.Errorf("Consistency-Control header = %q, want %q",
			got, ConsistencyStrongGlobal)
	}
}

// TestConsistencyControl_HeaderSentOnGET: reader-side GETs
// propagate the target's configured consistency level too,
// matching the writer. Uses the library's get() method directly.
func TestConsistencyControl_HeaderSentOnGET(t *testing.T) {
	stubBackoff(t)
	var sawHeader atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			sawHeader.Store(r.Header.Get("Consistency-Control"))
			w.Header().Set("Content-Length", "1")
			w.WriteHeader(200)
			_, _ = w.Write([]byte("x"))
		}))
	t.Cleanup(srv.Close)
	tgt := newTestTarget(t, srv.URL, ConsistencyStrongSite)

	_, err := tgt.get(context.Background(), "k")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	got, _ := sawHeader.Load().(string)
	if got != string(ConsistencyStrongSite) {
		t.Errorf("Consistency-Control header = %q, want %q",
			got, ConsistencyStrongSite)
	}
}

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
// observe only the package-level retry layer. Pass an optional
// ConsistencyLevel to bake it onto the target — only the
// consistency-header tests need this.
func newTestTarget(
	t *testing.T, endpoint string, consistency ...ConsistencyLevel,
) S3Target {
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
	cfg := S3TargetConfig{Bucket: "bucket", S3Client: cli}
	if len(consistency) > 0 {
		cfg.ConsistencyControl = consistency[0]
	}
	return newS3TargetSkipConfig(cfg)
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
		[]byte("x"), "application/octet-stream"); err != nil {
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
		[]byte("x"), "application/octet-stream"); err != nil {
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
		[]byte("x"), "application/octet-stream")
	if err == nil {
		t.Fatal("want error on 404, got nil")
	}
	if got := count.Load(); got != 1 {
		t.Errorf("want 1 request (no retry on 404), got %d", got)
	}
}

// TestVerifyPutObjectETag pins the branch table for the
// post-PUT integrity check: equal/unequal MD5, multipart marker,
// SSE-KMS / SSE-KMS-DSSE / SSE-C, missing ETag, nil response. The
// integration-tagged TestWriteWithKey_OuterRetryPreservesParquetBody
// covers the end-to-end "happy path produces matching ETag" case
// against MinIO; this unit test exercises the skip rules in
// isolation so a future refactor of the SSE handling can't silently
// turn the check off.
func TestVerifyPutObjectETag(t *testing.T) {
	body := []byte("the body that was supposed to land at S3")
	sum := md5.Sum(body) //nolint:gosec // mirrors target.go integrity check
	expectedHex := hex.EncodeToString(sum[:])
	wrongHex := hex.EncodeToString(make([]byte, 16)) // 32 zeros

	cases := []struct {
		name    string
		out     *s3.PutObjectOutput
		wantErr bool
	}{
		{
			name:    "nil response",
			out:     nil,
			wantErr: false,
		},
		{
			name:    "matching ETag",
			out:     &s3.PutObjectOutput{ETag: aws.String(`"` + expectedHex + `"`)},
			wantErr: false,
		},
		{
			name:    "matching ETag without quotes (some backends)",
			out:     &s3.PutObjectOutput{ETag: aws.String(expectedHex)},
			wantErr: false,
		},
		{
			name:    "matching ETag uppercase hex (case-insensitive compare)",
			out:     &s3.PutObjectOutput{ETag: aws.String(`"` + strings.ToUpper(expectedHex) + `"`)},
			wantErr: false,
		},
		{
			name:    "mismatched ETag — body did not land intact",
			out:     &s3.PutObjectOutput{ETag: aws.String(`"` + wrongHex + `"`)},
			wantErr: true,
		},
		{
			name:    "0-byte upload (the EOF-body bug shape)",
			out:     &s3.PutObjectOutput{ETag: aws.String(`"d41d8cd98f00b204e9800998ecf8427e"`)},
			wantErr: true,
		},
		{
			name: "SSE-KMS — skip (ETag opaque)",
			out: &s3.PutObjectOutput{
				ETag:                 aws.String(`"` + wrongHex + `"`),
				ServerSideEncryption: s3types.ServerSideEncryptionAwsKms,
			},
			wantErr: false,
		},
		{
			name: "SSE-KMS-DSSE — skip",
			out: &s3.PutObjectOutput{
				ETag:                 aws.String(`"` + wrongHex + `"`),
				ServerSideEncryption: s3types.ServerSideEncryptionAwsKmsDsse,
			},
			wantErr: false,
		},
		{
			name: "SSE-S3 (AES256) — verify (ETag is MD5 of plaintext)",
			out: &s3.PutObjectOutput{
				ETag:                 aws.String(`"` + expectedHex + `"`),
				ServerSideEncryption: s3types.ServerSideEncryptionAes256,
			},
			wantErr: false,
		},
		{
			name: "SSE-C — skip (ETag depends on customer key)",
			out: &s3.PutObjectOutput{
				ETag:                 aws.String(`"` + wrongHex + `"`),
				SSECustomerAlgorithm: aws.String("AES256"),
			},
			wantErr: false,
		},
		{
			name: "multipart ETag — skip",
			out: &s3.PutObjectOutput{
				ETag: aws.String(`"abcdef0123456789abcdef0123456789-3"`),
			},
			wantErr: false,
		},
		{
			name:    "empty ETag — skip (backend returned no ETag)",
			out:     &s3.PutObjectOutput{ETag: aws.String("")},
			wantErr: false,
		},
		{
			name:    "nil ETag string — skip",
			out:     &s3.PutObjectOutput{},
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := verifyPutObjectETag(tc.out, expectedHex)
			gotErr := err != nil
			if gotErr != tc.wantErr {
				t.Fatalf("verifyPutObjectETag err=%v, wantErr=%v",
					err, tc.wantErr)
			}
		})
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

	got, err := tgt.get(context.Background(), "key")
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
