package s3store

import (
	"fmt"
	"strings"
	"testing"
)

func TestWithIdempotencyToken(t *testing.T) {
	var o WriteOpts
	WithIdempotencyToken("tok-1")(&o)
	if o.IdempotencyToken != "tok-1" {
		t.Errorf("IdempotencyToken = %q, want %q",
			o.IdempotencyToken, "tok-1")
	}
}

func TestWithIdempotencyTokenOf_StoresFn(t *testing.T) {
	var o WriteOpts
	fn := func(_ []testRec) (string, error) { return "x", nil }
	WithIdempotencyTokenOf(fn)(&o)
	if o.IdempotencyTokenFn == nil {
		t.Fatal("IdempotencyTokenFn was not stored")
	}
	got, ok := o.IdempotencyTokenFn.(func([]testRec) (string, error))
	if !ok {
		t.Fatalf("IdempotencyTokenFn type = %T, want func([]testRec) (string, error)",
			o.IdempotencyTokenFn)
	}
	v, err := got(nil)
	if err != nil || v != "x" {
		t.Errorf("captured fn returned (%q, %v), want (%q, nil)", v, err, "x")
	}
}

func TestWithIdempotencyTokenOf_NilFnIsNoop(t *testing.T) {
	var o WriteOpts
	WithIdempotencyTokenOf[testRec](nil)(&o)
	if o.IdempotencyTokenFn != nil {
		t.Errorf("nil fn should be a no-op, got %T", o.IdempotencyTokenFn)
	}
}

// TestResolveWriteOpts_MutualExclusion guards that combining the
// static and per-partition idempotency options is rejected at
// resolution — without the check, the resolver would silently
// pick one (the fn-derived value would overwrite the static token)
// and obscure caller intent.
func TestResolveWriteOpts_MutualExclusion(t *testing.T) {
	fn := func(_ []testRec) (string, error) { return "x", nil }
	_, err := resolveWriteOpts([]WriteOption{
		WithIdempotencyToken("tok"),
		WithIdempotencyTokenOf(fn),
	}, []testRec(nil))
	if err == nil {
		t.Fatal("expected error from combining static + fn options")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("error %q did not mention mutual exclusion", err)
	}
}

func TestResolveWriteOpts_StaticOnly(t *testing.T) {
	o, err := resolveWriteOpts([]WriteOption{
		WithIdempotencyToken("tok-1"),
	}, []testRec(nil))
	if err != nil {
		t.Fatalf("resolveWriteOpts: %v", err)
	}
	if o.IdempotencyToken != "tok-1" {
		t.Errorf("IdempotencyToken = %q, want %q",
			o.IdempotencyToken, "tok-1")
	}
	if o.IdempotencyTokenFn != nil {
		t.Errorf("IdempotencyTokenFn should be nil, got %T",
			o.IdempotencyTokenFn)
	}
}

// TestResolveWriteOpts_FnInvoked: the per-partition fn runs
// inside resolveWriteOpts; its return value populates
// IdempotencyToken so downstream code reads a single resolved
// field regardless of which option the caller passed.
func TestResolveWriteOpts_FnInvoked(t *testing.T) {
	recs := []testRec{
		{Period: "p1", Customer: "alpha", Value: 1},
		{Period: "p1", Customer: "alpha", Value: 2},
	}
	var seen []testRec
	fn := func(part []testRec) (string, error) {
		seen = part
		return "tok-from-fn", nil
	}
	o, err := resolveWriteOpts([]WriteOption{
		WithIdempotencyTokenOf(fn),
	}, recs)
	if err != nil {
		t.Fatalf("resolveWriteOpts: %v", err)
	}
	if o.IdempotencyToken != "tok-from-fn" {
		t.Errorf("IdempotencyToken = %q, want %q",
			o.IdempotencyToken, "tok-from-fn")
	}
	if len(seen) != len(recs) {
		t.Fatalf("fn received %d records, want %d", len(seen), len(recs))
	}
}

// TestResolveWriteOpts_FnError: a non-nil error from the
// per-partition fn surfaces as a wrapped error so the caller can
// fail the partition's write loudly.
func TestResolveWriteOpts_FnError(t *testing.T) {
	fn := func(_ []testRec) (string, error) {
		return "", fmt.Errorf("caller-supplied failure")
	}
	_, err := resolveWriteOpts([]WriteOption{
		WithIdempotencyTokenOf(fn),
	}, []testRec(nil))
	if err == nil {
		t.Fatal("expected error from fn failure")
	}
	if !strings.Contains(err.Error(), "caller-supplied failure") {
		t.Errorf("error %q did not include the fn's error message", err)
	}
}

// TestResolveWriteOpts_FnReturnsInvalidToken: the fn-returned
// token is validated via validateIdempotencyToken just like the
// static branch. Bad tokens fail loudly without touching S3.
func TestResolveWriteOpts_FnReturnsInvalidToken(t *testing.T) {
	fn := func(_ []testRec) (string, error) {
		return "has/slash", nil
	}
	_, err := resolveWriteOpts([]WriteOption{
		WithIdempotencyTokenOf(fn),
	}, []testRec(nil))
	if err == nil {
		t.Fatal("expected error from invalid fn-returned token")
	}
}

// TestResolveWriteOpts_FnTypeMismatch: a closure typed for a
// different T than the writer's T surfaces a clear error at
// resolution time. The type-erased IdempotencyTokenFn (any) is
// asserted to func([]T) (string, error) for the writer's T —
// without this guard a typo'd generic instantiation would crash
// with a stale "interface conversion" message inside the writer.
func TestResolveWriteOpts_FnTypeMismatch(t *testing.T) {
	mismatchFn := func(_ []string) (string, error) { return "x", nil }
	var o WriteOpts
	o.IdempotencyTokenFn = mismatchFn
	_, err := resolveWriteOpts[testRec]([]WriteOption{
		func(w *WriteOpts) { *w = o },
	}, nil)
	if err == nil {
		t.Fatal("expected error from closure-T / writer-T mismatch")
	}
	if !strings.Contains(err.Error(), "does not") &&
		!strings.Contains(err.Error(), "match") {
		t.Errorf("error %q did not mention the type mismatch", err)
	}
}

func TestResolveWriteOpts_StaticInvalidToken(t *testing.T) {
	_, err := resolveWriteOpts([]WriteOption{
		WithIdempotencyToken("bad/token"),
	}, []testRec(nil))
	if err == nil {
		t.Fatal("expected error from invalid static token")
	}
}
