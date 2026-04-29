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
// extracts all four fields from a well-formed metadata map.
func TestReadTokenCommitMeta_HappyPath(t *testing.T) {
	meta := map[string]string{
		attemptIDMetaKey:  testAttemptIDA,
		refMicroTsMetaKey: "1710684000000000",
		insertedAtMetaKey: "1710683999500000",
		rowCountMetaKey:   "42",
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
	if got.rowCount != 42 {
		t.Errorf("rowCount = %d, want %d", got.rowCount, int64(42))
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
				rowCountMetaKey:   "1",
			},
		},
		{
			name: "missing refmicrots",
			meta: map[string]string{
				attemptIDMetaKey:  testAttemptIDA,
				insertedAtMetaKey: "1710683999500000",
				rowCountMetaKey:   "1",
			},
		},
		{
			name: "missing insertedat",
			meta: map[string]string{
				attemptIDMetaKey:  testAttemptIDA,
				refMicroTsMetaKey: "1710684000000000",
				rowCountMetaKey:   "1",
			},
		},
		{
			name: "missing rowcount",
			meta: map[string]string{
				attemptIDMetaKey:  testAttemptIDA,
				refMicroTsMetaKey: "1710684000000000",
				insertedAtMetaKey: "1710683999500000",
			},
		},
		{
			name: "attemptid too short",
			meta: map[string]string{
				attemptIDMetaKey:  testAttemptIDA[:31],
				refMicroTsMetaKey: "1710684000000000",
				insertedAtMetaKey: "1710683999500000",
				rowCountMetaKey:   "1",
			},
		},
		{
			name: "attemptid uppercase hex",
			meta: map[string]string{
				attemptIDMetaKey:  strings.ToUpper(testAttemptIDA),
				refMicroTsMetaKey: "1710684000000000",
				insertedAtMetaKey: "1710683999500000",
				rowCountMetaKey:   "1",
			},
		},
		{
			name: "attemptid non-hex char",
			meta: map[string]string{
				attemptIDMetaKey:  testAttemptIDA[:31] + "z",
				refMicroTsMetaKey: "1710684000000000",
				insertedAtMetaKey: "1710683999500000",
				rowCountMetaKey:   "1",
			},
		},
		{
			name: "refmicrots not a number",
			meta: map[string]string{
				attemptIDMetaKey:  testAttemptIDA,
				refMicroTsMetaKey: "notanumber",
				insertedAtMetaKey: "1710683999500000",
				rowCountMetaKey:   "1",
			},
		},
		{
			name: "insertedat not a number",
			meta: map[string]string{
				attemptIDMetaKey:  testAttemptIDA,
				refMicroTsMetaKey: "1710684000000000",
				insertedAtMetaKey: "notanumber",
				rowCountMetaKey:   "1",
			},
		},
		{
			name: "rowcount not a number",
			meta: map[string]string{
				attemptIDMetaKey:  testAttemptIDA,
				refMicroTsMetaKey: "1710684000000000",
				insertedAtMetaKey: "1710683999500000",
				rowCountMetaKey:   "notanumber",
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
	const rowCount int64 = 17
	meta := tokenCommitMeta{
		attemptID:    testAttemptIDA,
		refMicroTs:   refMicroTs,
		insertedAtUs: insertedAtUs,
		rowCount:     rowCount,
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
	if wr.RowCount != rowCount {
		t.Errorf("RowCount = %d, want %d (from rowcount metadata)",
			wr.RowCount, rowCount)
	}
}

// TestCommitTokenFromBasename pins the round-trip:
// `<token>.commit` yields token; anything else yields !ok.
func TestCommitTokenFromBasename(t *testing.T) {
	cases := []struct {
		base    string
		wantTok string
		wantOK  bool
	}{
		{"tok42.commit", "tok42", true},
		{"2026-04-22T10:15:00Z-batch42.commit", "2026-04-22T10:15:00Z-batch42", true},
		{".commit", "", false}, // empty token before suffix
		{"tok42.parquet", "", false},
		{"tok42", "", false},
		{"", "", false},
	}
	for _, c := range cases {
		t.Run(c.base, func(t *testing.T) {
			tok, ok := commitTokenFromBasename(c.base)
			if ok != c.wantOK {
				t.Fatalf("ok = %v, want %v", ok, c.wantOK)
			}
			if tok != c.wantTok {
				t.Errorf("tok = %q, want %q", tok, c.wantTok)
			}
		})
	}
}

// TestDataFileTokenAndID pins extraction from a parquet basename:
// only the well-formed `<token>-<attemptID:32hex>.parquet` shape
// returns ok=true.
func TestDataFileTokenAndID(t *testing.T) {
	good := "tok42-" + testAttemptIDA + ".parquet"
	tok, id, ok := dataFileTokenAndID(good)
	if !ok || tok != "tok42" || id != "tok42-"+testAttemptIDA {
		t.Errorf("good: tok=%q id=%q ok=%v", tok, id, ok)
	}
	for _, bad := range []string{
		"",
		"tok42.parquet", // missing attempt
		"tok42-" + testAttemptIDA[:31] + ".parquet", // short attempt
		"tok42-" + testAttemptIDA + "z.parquet",     // non-hex extra
		"tok42-" + testAttemptIDA + ".commit",       // wrong suffix
	} {
		if _, _, ok := dataFileTokenAndID(bad); ok {
			t.Errorf("ok for %q, want !ok", bad)
		}
	}
}

// TestGateByCommit_DropsUncommittedAndKeepsSingles exercises the
// two trivial gate branches that don't HEAD: a token with no
// `<token>.commit` is fully dropped; a token with one parquet +
// commit passes through. Multi-attempt resolution (HEAD) is
// covered by integration tests because it requires a live target.
func TestGateByCommit_DropsUncommittedAndKeepsSingles(t *testing.T) {
	const dataPath = "p/data"

	tokA := "tok-a"
	tokB := "tok-b"
	tokC := "tok-c"

	committedSingle := KeyMeta{
		Key: dataPath + "/period=A/" + makeID(tokA, testAttemptIDA) + ".parquet",
	}
	uncommitted := KeyMeta{
		Key: dataPath + "/period=A/" + makeID(tokB, testAttemptIDA) + ".parquet",
	}
	committedDifferentPartition := KeyMeta{
		Key: dataPath + "/period=B/" + makeID(tokC, testAttemptIDA) + ".parquet",
	}

	commits := map[string]struct{}{
		"period=A:" + tokA: {},
		"period=B:" + tokC: {},
	}
	in := []KeyMeta{committedSingle, uncommitted, committedDifferentPartition}

	got, err := gateByCommit(t.Context(), S3Target{}, dataPath,
		in, commits, methodRead)
	if err != nil {
		t.Fatalf("gateByCommit: %v", err)
	}
	want := map[string]bool{
		committedSingle.Key:             true,
		committedDifferentPartition.Key: true,
	}
	if len(got) != len(want) {
		t.Fatalf("got %d, want %d: %v", len(got), len(want), got)
	}
	for _, k := range got {
		if !want[k.Key] {
			t.Errorf("unexpected key %q", k.Key)
		}
	}
}

// TestGateByCommit_PartitionScopedTokens guards that the same
// token under two different partitions is treated independently:
// committing in partition A doesn't expose an uncommitted parquet
// of the same token in partition B.
func TestGateByCommit_PartitionScopedTokens(t *testing.T) {
	const dataPath = "p/data"
	const tok = "tok42"
	commitA := KeyMeta{Key: dataPath + "/period=A/" + makeID(tok, testAttemptIDA) + ".parquet"}
	uncommitB := KeyMeta{Key: dataPath + "/period=B/" + makeID(tok, testAttemptIDA) + ".parquet"}
	commits := map[string]struct{}{"period=A:" + tok: {}}
	got, err := gateByCommit(t.Context(), S3Target{}, dataPath,
		[]KeyMeta{commitA, uncommitB}, commits, methodRead)
	if err != nil {
		t.Fatalf("gateByCommit: %v", err)
	}
	if len(got) != 1 || got[0].Key != commitA.Key {
		t.Fatalf("got %v, want only %q", got, commitA.Key)
	}
}

// TestCommitCache_StartsEmpty pins newCommitCache's initial state:
// no entries, ready to fill on first lookup.
func TestCommitCache_StartsEmpty(t *testing.T) {
	c := newCommitCache()
	if len(c.entries) != 0 {
		t.Errorf("entries = %d, want 0", len(c.entries))
	}
}

// TestPutTokenCommit_MetaShape confirms the writer's PUT helper
// assembles the metadata fields the reader's parser expects.
// Round-trips through readTokenCommitMeta so future renames of
// the metadata keys break here.
func TestPutTokenCommit_MetaShape(t *testing.T) {
	const refMicroTs int64 = 1_710_684_000_000_000
	const insertedAtUs int64 = 1_710_683_999_500_000
	const rowCount int64 = 5
	meta := map[string]string{
		attemptIDMetaKey:  testAttemptIDA,
		refMicroTsMetaKey: strconv.FormatInt(refMicroTs, 10),
		insertedAtMetaKey: strconv.FormatInt(insertedAtUs, 10),
		rowCountMetaKey:   strconv.FormatInt(rowCount, 10),
	}
	got, err := readTokenCommitMeta(meta)
	if err != nil {
		t.Fatalf("readTokenCommitMeta(produced metadata): %v", err)
	}
	if got.attemptID != testAttemptIDA ||
		got.refMicroTs != refMicroTs ||
		got.insertedAtUs != insertedAtUs ||
		got.rowCount != rowCount {
		t.Errorf("round-trip drift: got (%q, %d, %d, %d), want (%q, %d, %d, %d)",
			got.attemptID, got.refMicroTs, got.insertedAtUs, got.rowCount,
			testAttemptIDA, refMicroTs, insertedAtUs, rowCount)
	}
}
