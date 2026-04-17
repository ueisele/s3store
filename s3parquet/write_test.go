package s3parquet

import "testing"

// TestGroupByKey guards that groupByKey partitions records by
// PartitionKeyOf and preserves every record in its group.
func TestGroupByKey(t *testing.T) {
	s := &Store[testRec]{cfg: Config[testRec]{
		PartitionKeyParts: []string{"period", "customer"},
		PartitionKeyOf: func(r testRec) string {
			return "period=" + r.Period + "/customer=" + r.Customer
		},
	}}

	records := []testRec{
		{Period: "p1", Customer: "a", Value: 1},
		{Period: "p1", Customer: "a", Value: 2},
		{Period: "p1", Customer: "b", Value: 3},
		{Period: "p2", Customer: "a", Value: 4},
	}
	got := s.groupByKey(records)

	if len(got) != 3 {
		t.Fatalf("got %d groups, want 3", len(got))
	}
	cases := []struct {
		key  string
		want int
	}{
		{"period=p1/customer=a", 2},
		{"period=p1/customer=b", 1},
		{"period=p2/customer=a", 1},
	}
	for _, tc := range cases {
		if n := len(got[tc.key]); n != tc.want {
			t.Errorf("group %q: got %d records, want %d",
				tc.key, n, tc.want)
		}
	}
}

// TestEncodeParquet guards the write-side encode path: the
// produced byte stream has the parquet magic markers and
// decodes back to the same records.
func TestEncodeParquet(t *testing.T) {
	in := []testRec{
		{Period: "p1", Customer: "a", Value: 1},
		{Period: "p1", Customer: "b", Value: 2},
		{Period: "p2", Customer: "c", Value: 3},
	}
	data, err := encodeParquet(in)
	if err != nil {
		t.Fatalf("encodeParquet: %v", err)
	}
	if len(data) < 8 ||
		string(data[:4]) != "PAR1" ||
		string(data[len(data)-4:]) != "PAR1" {
		t.Fatalf("not a valid parquet file: len=%d", len(data))
	}

	out, err := decodeParquet[testRec](data)
	if err != nil {
		t.Fatalf("decodeParquet: %v", err)
	}
	if len(out) != len(in) {
		t.Fatalf("got %d rows, want %d", len(out), len(in))
	}
	for i := range in {
		if out[i] != in[i] {
			t.Errorf("row %d: got %+v, want %+v", i, out[i], in[i])
		}
	}
}
