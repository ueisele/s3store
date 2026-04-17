package s3parquet

import "testing"

func TestBuildReadPlan_ListPrefix(t *testing.T) {
	keyParts := []string{"period", "customer"}
	const dataPath = "pre/data"

	cases := []struct {
		name    string
		pattern string
		want    string
	}{
		{"match all empty", "", "pre/data/"},
		{"match all star", "*", "pre/data/"},
		{"exact key", "period=2026-03-17/customer=abc",
			"pre/data/period=2026-03-17/customer=abc/"},
		{"trailing star", "period=2026-03-*/customer=abc", "pre/data/"},
		{"whole segment at head", "*/customer=abc", "pre/data/"},
		{"whole segment at tail", "period=X/*", "pre/data/period=X/"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := buildReadPlan(tc.pattern, dataPath, keyParts)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if plan.ListPrefix != tc.want {
				t.Errorf("ListPrefix = %q, want %q",
					plan.ListPrefix, tc.want)
			}
		})
	}
}

func TestBuildReadPlan_Match(t *testing.T) {
	keyParts := []string{"period", "customer"}
	const dataPath = "pre/data"

	cases := []struct {
		name    string
		pattern string
		hiveKey string
		want    bool
	}{
		{"exact match", "period=X/customer=Y", "period=X/customer=Y", true},
		{"exact mismatch customer", "period=X/customer=Y", "period=X/customer=Z", false},
		{"trailing star match", "period=2026-03-*/customer=abc", "period=2026-03-17/customer=abc", true},
		{"trailing star miss", "period=2026-03-*/customer=abc", "period=2026-04-01/customer=abc", false},
		{"trailing star customer mismatch", "period=2026-03-*/customer=abc", "period=2026-03-17/customer=xyz", false},
		{"whole segment head", "*/customer=abc", "period=anything/customer=abc", true},
		{"whole segment head mismatch tail", "*/customer=abc", "period=anything/customer=nope", false},
		{"match all", "*", "anything/anything", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := buildReadPlan(tc.pattern, dataPath, keyParts)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := plan.Match(tc.hiveKey); got != tc.want {
				t.Errorf("Match(%q) under pattern %q = %v, want %v",
					tc.hiveKey, tc.pattern, got, tc.want)
			}
		})
	}
}

func TestBuildReadPlan_RejectsInvalidPattern(t *testing.T) {
	keyParts := []string{"period", "customer"}
	cases := []string{
		"period=*-17/customer=abc",      // leading star
		"period=2026-*-17/customer=abc", // middle star
		"period=X",                      // wrong segment count
		"period={a,b}/customer=abc",     // alternation
	}
	for _, p := range cases {
		t.Run(p, func(t *testing.T) {
			if _, err := buildReadPlan(p, "pre/data", keyParts); err == nil {
				t.Errorf("buildReadPlan(%q): expected error", p)
			}
		})
	}
}

func TestHiveKeyOfDataFile(t *testing.T) {
	cases := []struct {
		name    string
		s3Key   string
		dp      string
		want    string
		wantOK  bool
	}{
		{"standard", "pre/data/period=X/customer=Y/abcd1234.parquet",
			"pre/data", "period=X/customer=Y", true},
		{"non-parquet", "pre/data/period=X/customer=Y/not-parquet",
			"pre/data", "", false},
		{"outside dataPath", "elsewhere/abcd1234.parquet",
			"pre/data", "", false},
		{"root-level parquet under data", "pre/data/abcd1234.parquet",
			"pre/data", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := hiveKeyOfDataFile(tc.s3Key, tc.dp)
			if ok != tc.wantOK {
				t.Errorf("ok = %v, want %v", ok, tc.wantOK)
			}
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}
