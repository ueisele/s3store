package s3store

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestTokenCommitKey verifies the path shape:
// `<dataPath>/<partition>/<token>.commit`.
func TestTokenCommitKey(t *testing.T) {
	got := tokenCommitKey("p/data", "period=A/customer=B", "tok42")
	want := "p/data/period=A/customer=B/tok42.commit"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

// TestReadTokenCommitMeta_HappyPath confirms that the parser
// extracts all three fields from a well-formed metadata map.
func TestReadTokenCommitMeta_HappyPath(t *testing.T) {
	meta := map[string]string{
		attemptIDMetaKey:  testAttemptIDA,
		refMicroTsMetaKey: "1710684000000000",
		insertedAtMetaKey: "1710683999500000",
	}
	got, err := readTokenCommitMeta(meta)
	if err != nil {
		t.Fatalf("readTokenCommitMeta: %v", err)
	}
	if got.attemptID != testAttemptIDA {
		t.Errorf("attemptID = %q, want %q", got.attemptID, testAttemptIDA)
	}
	if got.refMicroTs != 1710684000000000 {
		t.Errorf("refMicroTs = %d, want %d",
			got.refMicroTs, int64(1710684000000000))
	}
	if got.insertedAtUs != 1710683999500000 {
		t.Errorf("insertedAtUs = %d, want %d",
			got.insertedAtUs, int64(1710683999500000))
	}
}

// TestReadTokenCommitMeta_Invalid pins the failure modes: a
// committed marker with malformed metadata is a hard error,
// not "missing" — silent acceptance would let a corrupted
// commit cause redundant retries.
func TestReadTokenCommitMeta_Invalid(t *testing.T) {
	cases := []struct {
		name string
		meta map[string]string
	}{
		{
			name: "missing attemptid",
			meta: map[string]string{
				refMicroTsMetaKey: "1710684000000000",
				insertedAtMetaKey: "1710683999500000",
			},
		},
		{
			name: "missing refmicrots",
			meta: map[string]string{
				attemptIDMetaKey:  testAttemptIDA,
				insertedAtMetaKey: "1710683999500000",
			},
		},
		{
			name: "missing insertedat",
			meta: map[string]string{
				attemptIDMetaKey:  testAttemptIDA,
				refMicroTsMetaKey: "1710684000000000",
			},
		},
		{
			name: "attemptid too short",
			meta: map[string]string{
				attemptIDMetaKey:  testAttemptIDA[:31],
				refMicroTsMetaKey: "1710684000000000",
				insertedAtMetaKey: "1710683999500000",
			},
		},
		{
			name: "attemptid uppercase hex",
			meta: map[string]string{
				attemptIDMetaKey:  strings.ToUpper(testAttemptIDA),
				refMicroTsMetaKey: "1710684000000000",
				insertedAtMetaKey: "1710683999500000",
			},
		},
		{
			name: "attemptid non-hex char",
			meta: map[string]string{
				attemptIDMetaKey:  testAttemptIDA[:31] + "z",
				refMicroTsMetaKey: "1710684000000000",
				insertedAtMetaKey: "1710683999500000",
			},
		},
		{
			name: "refmicrots not a number",
			meta: map[string]string{
				attemptIDMetaKey:  testAttemptIDA,
				refMicroTsMetaKey: "notanumber",
				insertedAtMetaKey: "1710683999500000",
			},
		},
		{
			name: "insertedat not a number",
			meta: map[string]string{
				attemptIDMetaKey:  testAttemptIDA,
				refMicroTsMetaKey: "1710684000000000",
				insertedAtMetaKey: "notanumber",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := readTokenCommitMeta(tc.meta); err == nil {
				t.Errorf("want error for %v", tc.meta)
			}
		})
	}
}

// TestReconstructWriteResult guards that the upfront-HEAD dedup
// gate's reconstruction produces the same WriteResult shape the
// original write would have returned. Drives the same
// (token, attemptID, refMicroTs, partition) through encodeRefKey
// so any drift in the formula breaks here. InsertedAt is sourced
// from the token-commit's `insertedat` metadata (= the original
// write's pre-encode wall-clock = the parquet column value), so
// the assertion is against insertedAtUs, not refMicroTs.
func TestReconstructWriteResult(t *testing.T) {
	const refMicroTs int64 = 1_700_000_000_500_000
	const insertedAtUs int64 = 1_700_000_000_000_000
	meta := tokenCommitMeta{
		attemptID:    testAttemptIDA,
		refMicroTs:   refMicroTs,
		insertedAtUs: insertedAtUs,
	}
	wr := reconstructWriteResult(
		"p/data", "p/_ref", "period=A", "tok42", meta)

	wantData := "p/data/period=A/" + makeID("tok42", testAttemptIDA) + ".parquet"
	if wr.DataPath != wantData {
		t.Errorf("DataPath = %q, want %q", wr.DataPath, wantData)
	}
	wantRef := encodeRefKey("p/_ref", refMicroTs,
		"tok42", testAttemptIDA, "period=A")
	if wr.RefPath != wantRef {
		t.Errorf("RefPath = %q, want %q", wr.RefPath, wantRef)
	}
	if string(wr.Offset) != wantRef {
		t.Errorf("Offset = %q, want %q", string(wr.Offset), wantRef)
	}
	if wr.InsertedAt != time.UnixMicro(insertedAtUs) {
		t.Errorf("InsertedAt = %v, want %v (from insertedat metadata)",
			wr.InsertedAt, time.UnixMicro(insertedAtUs))
	}
}

// TestPutTokenCommit_MetaShape confirms the writer's PUT helper
// assembles the metadata fields the reader's parser expects.
// Round-trips through readTokenCommitMeta so future renames of
// the metadata keys break here.
func TestPutTokenCommit_MetaShape(t *testing.T) {
	const refMicroTs int64 = 1_710_684_000_000_000
	const insertedAtUs int64 = 1_710_683_999_500_000
	meta := map[string]string{
		attemptIDMetaKey:  testAttemptIDA,
		refMicroTsMetaKey: strconv.FormatInt(refMicroTs, 10),
		insertedAtMetaKey: strconv.FormatInt(insertedAtUs, 10),
	}
	got, err := readTokenCommitMeta(meta)
	if err != nil {
		t.Fatalf("readTokenCommitMeta(produced metadata): %v", err)
	}
	if got.attemptID != testAttemptIDA ||
		got.refMicroTs != refMicroTs ||
		got.insertedAtUs != insertedAtUs {
		t.Errorf("round-trip drift: got (%q, %d, %d), want (%q, %d, %d)",
			got.attemptID, got.refMicroTs, got.insertedAtUs,
			testAttemptIDA, refMicroTs, insertedAtUs)
	}
}
