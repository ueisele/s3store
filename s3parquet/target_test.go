package s3parquet

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// headServer returns an httptest server that distinguishes PUT
// from HEAD responses. putFn runs on PUT (including PUT-with-
// If-None-Match), headFn runs on HEAD. Both receive the request
// counter (1-indexed) for per-call branching. All other verbs
// fall through to 500 so buggy tests surface loudly.
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
				putFn(w, r, i)
			case http.MethodHead:
				headFn(w, r, i)
			case http.MethodDelete:
				w.WriteHeader(204)
			default:
				w.WriteHeader(500)
			}
		}))
	t.Cleanup(srv.Close)
	return srv, &total
}

// TestPutIfAbsent_Fresh_OK: a PUT that the backend accepts (200)
// returns nil, and the request carried the If-None-Match header.
func TestPutIfAbsent_Fresh_OK(t *testing.T) {
	stubBackoff(t)
	var sawHeader atomic.Bool
	srv, count := headServer(t,
		func(w http.ResponseWriter, r *http.Request, _ int) {
			if r.Header.Get("If-None-Match") == "*" {
				sawHeader.Store(true)
			}
			w.WriteHeader(200)
		},
		func(w http.ResponseWriter, _ *http.Request, _ int) {
			t.Errorf("unexpected HEAD on fresh-write path")
			w.WriteHeader(500)
		})
	tgt := newTestTarget(t, srv.URL)

	err := tgt.putIfAbsent(context.Background(), "k",
		[]byte("x"), "application/octet-stream", nil)
	if err != nil {
		t.Fatalf("putIfAbsent: %v", err)
	}
	if !sawHeader.Load() {
		t.Errorf("If-None-Match: * header not observed")
	}
	if got := count.Load(); got != 1 {
		t.Errorf("want 1 request, got %d", got)
	}
}

// TestPutIfAbsent_412: the direct If-None-Match rejection path.
// AWS and recent MinIO. One PUT, no HEAD.
func TestPutIfAbsent_412(t *testing.T) {
	stubBackoff(t)
	srv, count := headServer(t,
		func(w http.ResponseWriter, _ *http.Request, _ int) {
			w.WriteHeader(412)
		},
		func(w http.ResponseWriter, _ *http.Request, _ int) {
			t.Errorf("HEAD must not fire on 412 path")
			w.WriteHeader(500)
		})
	tgt := newTestTarget(t, srv.URL)

	err := tgt.putIfAbsent(context.Background(), "k",
		[]byte("x"), "application/octet-stream", nil)
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("want ErrAlreadyExists, got %v", err)
	}
	if got := count.Load(); got != 1 {
		t.Errorf("want 1 request, got %d", got)
	}
}

// TestPutIfAbsent_403ThenHeadFound: StorageGRID bucket-policy
// path. PUT → 403 AccessDenied, follow-up HEAD → 200, so the
// target reports ErrAlreadyExists. Two requests total.
func TestPutIfAbsent_403ThenHeadFound(t *testing.T) {
	stubBackoff(t)
	srv, count := headServer(t,
		func(w http.ResponseWriter, _ *http.Request, _ int) {
			w.WriteHeader(403)
		},
		func(w http.ResponseWriter, _ *http.Request, _ int) {
			w.WriteHeader(200)
		})
	tgt := newTestTarget(t, srv.URL)

	err := tgt.putIfAbsent(context.Background(), "k",
		[]byte("x"), "application/octet-stream", nil)
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("want ErrAlreadyExists, got %v", err)
	}
	if got := count.Load(); got != 2 {
		t.Errorf("want 2 requests (PUT + HEAD), got %d", got)
	}
}

// TestPutIfAbsent_403ThenHeadMissing: PUT → 403 but HEAD → 404,
// which means the 403 was a real permission error (not overwrite
// deny). The error surfaces unchanged, NOT as ErrAlreadyExists.
func TestPutIfAbsent_403ThenHeadMissing(t *testing.T) {
	stubBackoff(t)
	srv, count := headServer(t,
		func(w http.ResponseWriter, _ *http.Request, _ int) {
			w.WriteHeader(403)
		},
		func(w http.ResponseWriter, _ *http.Request, _ int) {
			w.WriteHeader(404)
		})
	tgt := newTestTarget(t, srv.URL)

	err := tgt.putIfAbsent(context.Background(), "k",
		[]byte("x"), "application/octet-stream", nil)
	if err == nil {
		t.Fatal("want non-nil error")
	}
	if errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("got ErrAlreadyExists; want real 403 error")
	}
	if got := count.Load(); got != 2 {
		t.Errorf("want 2 requests (PUT + HEAD), got %d", got)
	}
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

// TestConsistencyControl_EmptyOmitsHeader: when the level is
// ConsistencyDefault (empty), no header is sent — preserves the
// "AWS / MinIO sees no surprise header" contract.
func TestConsistencyControl_EmptyOmitsHeader(t *testing.T) {
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
	if got != "" {
		t.Errorf("Consistency-Control header = %q, want empty", got)
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
