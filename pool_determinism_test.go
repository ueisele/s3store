package s3store

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// TestEncodeParquet_PooledMatchesFresh checks that pooling parquet
// writers via Reset produces byte-identical output to fresh
// construction for the same input. CLAUDE.md asserts deterministic
// parquet encoding ("same records + same codec produce byte-identical
// bytes") as a load-bearing invariant for WithIdempotencyToken
// retries — refactors must preserve it.
//
// Encodes the same record set three ways:
//  1. Fresh package-level encodeParquet (no pool).
//  2. First call through Writer.encodeParquet (pool empty → fresh
//     under the hood, but goes through the new code path).
//  3. Second call through the same Writer.encodeParquet (pool
//     populated → exercises the Reset-and-reuse path).
//
// All three must produce byte-identical output.
func TestEncodeParquet_PooledMatchesFresh(t *testing.T) {
	type rec struct {
		Period   string `parquet:"period"`
		Customer string `parquet:"customer"`
		SKU      string `parquet:"sku"`
		Payload  []byte `parquet:"payload"`
	}
	in := []rec{
		{Period: "2026-05-06", Customer: "acme", SKU: "alpha",
			Payload: []byte("first-payload-bytes")},
		{Period: "2026-05-06", Customer: "acme", SKU: "beta",
			Payload: []byte("second-payload-bytes")},
		{Period: "2026-05-06", Customer: "globex", SKU: "alpha",
			Payload: []byte("third-payload-bytes")},
	}

	fresh, err := encodeParquet(in, &parquet.Snappy)
	if err != nil {
		t.Fatalf("fresh encode: %v", err)
	}

	w := &Writer[rec]{
		compressionCodec: &parquet.Snappy,
	}
	first, err := w.encodeParquet(in)
	if err != nil {
		t.Fatalf("pooled encode #1: %v", err)
	}
	second, err := w.encodeParquet(in)
	if err != nil {
		t.Fatalf("pooled encode #2 (after Reset): %v", err)
	}

	if !bytes.Equal(fresh, first) {
		t.Errorf("pooled-first != fresh: lens %d vs %d",
			len(first), len(fresh))
	}
	if !bytes.Equal(fresh, second) {
		t.Errorf("pooled-second != fresh: lens %d vs %d "+
			"(Reset path leaks state)",
			len(second), len(fresh))
	}
	if !bytes.Equal(first, second) {
		t.Errorf("pooled-first != pooled-second: lens %d vs %d "+
			"(non-deterministic across Reset)",
			len(first), len(second))
	}
}
