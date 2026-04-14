package s3store

import (
	"context"
	"testing"
)

// Read is a thin wrapper that delegates to Query with
// "SELECT * FROM <TableAlias>" and feeds the rows to
// scanAll / ScanFunc. All happy-path behavior (globs,
// dedup, schema evolution, value-glob matching) lives in
// Query and is exercised by integration_test.go — there's
// no pure helper to unit-test.
//
// The one thing that can be unit-tested without a real S3
// or DuckDB is error propagation from the pattern compiler:
// an invalid key pattern causes scanExprForPattern to
// return before Query touches s.db, so nil-db tests work.

func TestReadRejectsTruncatedPattern(t *testing.T) {
	s := newTestStore("period", "customer")
	_, err := s.Read(context.Background(), "period=2026-03-17")
	if err == nil {
		t.Error("expected error for truncated pattern")
	}
}

func TestReadRejectsWrongPartName(t *testing.T) {
	s := newTestStore("period", "customer")
	_, err := s.Read(context.Background(),
		"ustomer=abc/period=X")
	if err == nil {
		t.Error("expected error for mislabelled segment")
	}
}
