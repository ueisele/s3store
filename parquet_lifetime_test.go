package s3store

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
)

// Phase 1 lifetime probe — does parquet-go's typed Read return
// fresh-allocated string / []byte values, or subslices into the
// reader's internal page / dictionary / decompression buffers?
//
// parquet-go v0.29 documents the answer on
// GenericReader[T].Read directly:
//
//	The returned values are safe to reuse across Read calls and do
//	not share memory with the reader's underlying page buffers.
//
// These tests pin that documented contract empirically so a future
// parquet-go upgrade that regresses it fails loudly.
//
// Note on API mismatch with the original task brief:
// GenericReader[T].Reset() takes NO io.Reader argument — it
// rewinds to the start of the SAME source, not "rebind to new
// file." So the per-file pool design ("Get → Reset(newFile) →
// Read → Put") in the original brief is not supported by
// parquet-go's typed reader API at all. The probes below test
// what the API actually allows: (1) values survive after the
// reader is closed, and (2) values from earlier Read calls
// survive subsequent Read calls within the same reader.

type lifetimeRec struct {
	ID    int64  `parquet:"id"`
	Str   string `parquet:"str"`
	Bytes []byte `parquet:"bytes"`
}

// makeLifetimeFile encodes n records prefixed with `prefix` so a
// later check can detect which file's bytes a corrupted string /
// byte field is pointing at.
func makeLifetimeFile(t *testing.T, prefix string, n int, codec compress.Codec) []byte {
	t.Helper()
	recs := make([]lifetimeRec, n)
	for i := range recs {
		recs[i] = lifetimeRec{
			ID:    int64(i),
			Str:   fmt.Sprintf("%s_STR_%06d", prefix, i),
			Bytes: fmt.Appendf(nil, "%s_BYTES_%06d", prefix, i),
		}
	}
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[lifetimeRec](
		&buf, parquet.Compression(codec))
	if _, err := w.Write(recs); err != nil {
		t.Fatalf("write %s: %v", prefix, err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close %s: %v", prefix, err)
	}
	return buf.Bytes()
}

func checkCorruption(
	t *testing.T, label string, recs []lifetimeRec, prefix string, baseIdx int,
) {
	t.Helper()
	const sampleLogs = 3
	corruptedStr := 0
	corruptedBytes := 0
	for i, rec := range recs {
		gIdx := baseIdx + i
		wantStr := fmt.Sprintf("%s_STR_%06d", prefix, gIdx)
		if rec.Str != wantStr {
			if corruptedStr < sampleLogs {
				t.Logf("%s: recs[%d].Str = %q, want %q",
					label, i, rec.Str, wantStr)
			}
			corruptedStr++
		}
		wantBytes := fmt.Appendf(nil, "%s_BYTES_%06d", prefix, gIdx)
		if !bytes.Equal(rec.Bytes, wantBytes) {
			if corruptedBytes < sampleLogs {
				t.Logf("%s: recs[%d].Bytes = %q, want %q",
					label, i, rec.Bytes, wantBytes)
			}
			corruptedBytes++
		}
	}
	if corruptedStr > 0 || corruptedBytes > 0 {
		t.Errorf("%s: corruption detected — Str %d/%d, Bytes %d/%d",
			label, corruptedStr, len(recs),
			corruptedBytes, len(recs))
	}
}

// Variant 1 — Values survive Close of the reader they came from.
//
// This is the lifetime that matters for the current codebase:
// reader_iter.go decodeParquet constructs a fresh GenericReader
// per file and `defer reader.Close()`s it; the decoded `recs` slice
// is appended into the partition's `out` slice, which is later
// shipped on decodedCh. If Close releases internal page buffers
// that the strings / []byte fields point into, every record on
// decodedCh would be silently corrupted.
func TestParquetReader_Lifetime_ValuesSurviveClose(t *testing.T) {
	const N = 5000
	cases := []struct {
		name  string
		codec compress.Codec
	}{
		{"snappy", &parquet.Snappy},
		{"uncompressed", &parquet.Uncompressed},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			file := makeLifetimeFile(t, "ONE", N, tc.codec)

			reader := parquet.NewGenericReader[lifetimeRec](
				bytes.NewReader(file))
			out := make([]lifetimeRec, N)
			n, err := reader.Read(out)
			if err != nil && err != io.EOF {
				t.Fatalf("read: %v", err)
			}
			if n != N {
				t.Fatalf("read: n=%d want %d", n, N)
			}

			if err := reader.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			// Stamp some additional churn through the runtime to
			// raise the chance that any released buffer is
			// reused for unrelated data before we check.
			scratch := make([]byte, 1<<20)
			for i := range scratch {
				scratch[i] = byte(i)
			}
			_ = scratch

			checkCorruption(t,
				"out (after reader.Close)",
				out, "ONE", 0)
		})
	}
}

// Variant 2 — Sequential Read calls within ONE file, no Reset.
//
// Read the file in many chunks (forcing parquet-go to cross
// page / row-group boundaries). After all reads complete, verify
// every chunk still matches the originals. Catches "page buffer
// reused for next page within one decode."
//
// This is the per-partition pool scenario (one reader, sequential
// decodes across the partition's files, Put once at end). If
// variant 1 fails but variant 2 passes, A is still viable but only
// at partition granularity.
func TestParquetReader_Lifetime_SequentialReadsOneFile(t *testing.T) {
	const N = 5000
	const chunkSize = 250
	cases := []struct {
		name  string
		codec compress.Codec
	}{
		{"snappy", &parquet.Snappy},
		{"uncompressed", &parquet.Uncompressed},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			file := makeLifetimeFile(t, "ONE", N, tc.codec)
			reader := parquet.NewGenericReader[lifetimeRec](
				bytes.NewReader(file))
			defer func() { _ = reader.Close() }()

			var chunks [][]lifetimeRec
			collected := 0
			for collected < N {
				chunk := make([]lifetimeRec, chunkSize)
				m, err := reader.Read(chunk)
				if err != nil && err != io.EOF {
					t.Fatalf("read at %d: %v", collected, err)
				}
				if m == 0 {
					break
				}
				chunks = append(chunks, chunk[:m])
				collected += m
			}
			if collected != N {
				t.Fatalf("collected=%d want %d", collected, N)
			}

			// All reads done. Now verify every chunk's values are
			// still intact — earlier chunks must survive later
			// page decodes against the same reader.
			base := 0
			for ci, chunk := range chunks {
				checkCorruption(t,
					fmt.Sprintf("chunk[%d] (after all %d reads done)",
						ci, len(chunks)),
					chunk, "ONE", base)
				base += len(chunk)
			}
		})
	}
}
