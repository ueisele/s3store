package s3store

import (
	"testing"
)

func TestValidateKey(t *testing.T) {
	s := newTestStore("period", "customer")

	cases := []struct {
		name    string
		key     string
		wantErr bool
	}{
		{"valid simple", "period=2026-03-17/customer=abc", false},
		{"valid with hyphen in value", "period=X/customer=foo-bar", false},
		{"valid with equals in value", "period=a=b/customer=abc", false},

		{"empty key", "", true},
		{"wrong order", "customer=abc/period=2026-03-17", true},
		{"missing second segment", "period=2026-03-17", true},
		{"extra segment", "period=X/customer=Y/extra=Z", true},
		{"empty first value", "period=/customer=abc", true},
		{"empty second value", "period=X/customer=", true},
		{"wrong first part name", "ustomer=abc/period=X", true},
		{"wrong second part name", "period=X/extra=Y", true},
		{"trailing slash", "period=X/customer=Y/", true},
		{"leading slash", "/period=X/customer=Y", true},
		{"missing equals in first", "period_X/customer=Y", true},
		{"missing equals in second", "period=X/customer_Y", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := s.validateKey(tc.key)
			if tc.wantErr && err == nil {
				t.Errorf("validateKey(%q): expected error, got nil", tc.key)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("validateKey(%q): unexpected error: %v", tc.key, err)
			}
		})
	}
}

func TestGroupByKey(t *testing.T) {
	s := newTestStore("period", "customer")
	s.cfg.KeyFunc = func(r testRecord) string {
		return "period=" + r.Period + "/customer=" + r.Customer
	}

	records := []testRecord{
		{Customer: "a", Period: "p1", Value: 1},
		{Customer: "a", Period: "p1", Value: 2},
		{Customer: "b", Period: "p1", Value: 3},
		{Customer: "a", Period: "p2", Value: 4},
	}

	grouped := s.groupByKey(records)
	if len(grouped) != 3 {
		t.Fatalf("expected 3 groups, got %d: %v", len(grouped), keys(grouped))
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
		if got := len(grouped[tc.key]); got != tc.want {
			t.Errorf("group %q: got %d records, want %d", tc.key, got, tc.want)
		}
	}
}

func TestEncodeParquet(t *testing.T) {
	s := newTestStore()
	records := []testRecord{
		{Customer: "a", Period: "p1", Value: 1},
		{Customer: "b", Period: "p1", Value: 2},
	}
	got, err := s.encodeParquet(records)
	if err != nil {
		t.Fatalf("encodeParquet: %v", err)
	}
	if len(got) == 0 {
		t.Error("encodeParquet returned empty buffer")
	}
	// Parquet files start with the magic bytes "PAR1".
	if len(got) < 4 || string(got[:4]) != "PAR1" {
		t.Errorf("buffer does not start with PAR1 magic: %x", got[:min(4, len(got))])
	}
}

func keys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
