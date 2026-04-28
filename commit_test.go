package s3store

import (
	"fmt"
	"testing"
	"time"
)

// TestIsCommitValid pins down the timeliness check at the
// boundaries: equal LMs are valid (zero-gap commit), gap ==
// CommitTimeout is invalid (strict <), and either zero LM is
// invalid. Both inputs are S3-server-stamped so a negative gap
// (marker-before-data) is impossible by construction — not
// covered.
func TestIsCommitValid(t *testing.T) {
	base := time.UnixMicro(1_700_000_000_000_000)
	cases := []struct {
		name          string
		dataLM        time.Time
		markerLM      time.Time
		commitTimeout time.Duration
		want          bool
	}{
		{"zero gap", base, base, time.Second, true},
		{"sub-timeout gap", base, base.Add(500 * time.Millisecond), time.Second, true},
		{"exactly-at-timeout invalid", base, base.Add(time.Second), time.Second, false},
		{"over-timeout invalid", base, base.Add(2 * time.Second), time.Second, false},
		{"zero dataLM invalid", time.Time{}, base, time.Second, false},
		{"zero markerLM invalid", base, time.Time{}, time.Second, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isCommitValid(tc.dataLM, tc.markerLM, tc.commitTimeout)
			if got != tc.want {
				t.Errorf("isCommitValid(%v, %v, %v) = %v, want %v",
					tc.dataLM, tc.markerLM, tc.commitTimeout, got, tc.want)
			}
		})
	}
}

// TestParseDataOrCommitKey verifies the tiny shape parser the
// LIST-pair primitive uses to bucket entries by id + extension.
func TestParseDataOrCommitKey(t *testing.T) {
	cases := []struct {
		key         string
		wantID      string
		wantParquet bool
		wantCommit  bool
	}{
		{"prefix/data/period=A/id1.parquet", "id1", true, false},
		{"prefix/data/period=A/id1.commit", "id1", false, true},
		{"prefix/data/period=A/something.txt", "", false, false},
		{"id-with-dashes-1700000000000000-deadbeef.parquet",
			"id-with-dashes-1700000000000000-deadbeef", true, false},
	}
	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			id, p, c := parseDataOrCommitKey(tc.key)
			if id != tc.wantID || p != tc.wantParquet || c != tc.wantCommit {
				t.Errorf("got (%q, %v, %v), want (%q, %v, %v)",
					id, p, c, tc.wantID, tc.wantParquet, tc.wantCommit)
			}
		})
	}
}

// TestCommitMarkerKey verifies the sibling-of-parquet shape.
func TestCommitMarkerKey(t *testing.T) {
	got := commitMarkerKey("p/data", "period=A/customer=B", "id1")
	want := "p/data/period=A/customer=B/id1.commit"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

// TestReconstructWriteResult guards that the upfront-LIST dedup
// gate's reconstruction produces exactly the WriteResult the
// original write would have returned. Drives the same id /
// dataLM / partition through encodeRefKey so any drift in the
// formula breaks here.
func TestReconstructWriteResult(t *testing.T) {
	dataLM := time.UnixMicro(1_700_000_000_500_000)
	tsMicros := int64(1_700_000_000_000_000)
	id := fmt.Sprintf("tok42-%d-deadbeef", tsMicros)
	dataKey := "p/data/period=A/" + id + ".parquet"

	ci := commitInfo{
		id:        id,
		dataKey:   dataKey,
		dataLM:    dataLM,
		markerKey: "p/data/period=A/" + id + ".commit",
		markerLM:  dataLM.Add(50 * time.Millisecond),
	}

	wr, err := reconstructWriteResult("p/_ref", ci, "period=A")
	if err != nil {
		t.Fatalf("reconstructWriteResult: %v", err)
	}
	if wr.DataPath != dataKey {
		t.Errorf("DataPath = %q, want %q", wr.DataPath, dataKey)
	}
	if wr.InsertedAt != time.UnixMicro(tsMicros) {
		t.Errorf("InsertedAt = %v, want %v",
			wr.InsertedAt, time.UnixMicro(tsMicros))
	}
	wantRef := encodeRefKey("p/_ref", dataLM.UnixMicro(),
		tsMicros, "deadbeef", "tok42", "period=A")
	if wr.RefPath != wantRef {
		t.Errorf("RefPath = %q, want %q", wr.RefPath, wantRef)
	}
	if string(wr.Offset) != wantRef {
		t.Errorf("Offset = %q, want %q", string(wr.Offset), wantRef)
	}
}
