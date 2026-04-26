package s3parquet

import (
	"testing"

	"github.com/ueisele/s3store/internal/core"
)

func TestBuildReadPlan_ListPrefix(t *testing.T) {
	partitionKeyParts := []string{"period", "customer"}
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
		{"trailing star extends prefix",
			"period=2026-03-*/customer=abc",
			"pre/data/period=2026-03-"},
		{"whole segment at head", "*/customer=abc", "pre/data/"},
		{"whole segment at tail", "period=X/*", "pre/data/period=X/"},
		{"range narrows to common prefix",
			"period=2026-03-01..2026-04-01/customer=abc",
			"pre/data/period=2026-0"},
		{"range no common prefix",
			"period=a..z/customer=abc",
			"pre/data/"},
		{"range unbounded upper",
			"period=2026-03-01../customer=abc",
			"pre/data/"},
		{"range after exact segment",
			"period=X/customer=abc..abz",
			"pre/data/period=X/customer=ab"},
		{"range after exact segment no common prefix",
			"period=X/customer=a..m",
			"pre/data/period=X/"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plans, err := core.BuildReadPlans(
				[]string{tc.pattern}, dataPath, partitionKeyParts)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if plans[0].ListPrefix != tc.want {
				t.Errorf("ListPrefix = %q, want %q",
					plans[0].ListPrefix, tc.want)
			}
		})
	}
}

func TestBuildReadPlan_Match(t *testing.T) {
	partitionKeyParts := []string{"period", "customer"}
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
		{"range inside", "period=2026-03-01..2026-04-01/customer=abc",
			"period=2026-03-15/customer=abc", true},
		{"range lower bound inclusive",
			"period=2026-03-01..2026-04-01/customer=abc",
			"period=2026-03-01/customer=abc", true},
		{"range upper bound exclusive",
			"period=2026-03-01..2026-04-01/customer=abc",
			"period=2026-04-01/customer=abc", false},
		{"range below",
			"period=2026-03-01..2026-04-01/customer=abc",
			"period=2026-02-28/customer=abc", false},
		{"range above",
			"period=2026-03-01..2026-04-01/customer=abc",
			"period=2026-04-02/customer=abc", false},
		{"range unbounded upper",
			"period=2026-03-01../customer=abc",
			"period=2030-01-01/customer=abc", true},
		{"range unbounded lower",
			"period=..2026-04-01/customer=abc",
			"period=2020-01-01/customer=abc", true},
		{"range mismatched other segment",
			"period=2026-03-01..2026-04-01/customer=abc",
			"period=2026-03-15/customer=xyz", false},
		{"range + prefix both match",
			"period=2026-03-01..2026-04-01/customer=ab*",
			"period=2026-03-15/customer=abc", true},
		{"range + prefix range fails",
			"period=2026-03-01..2026-04-01/customer=ab*",
			"period=2026-04-15/customer=abc", false},
		{"range + prefix prefix fails",
			"period=2026-03-01..2026-04-01/customer=ab*",
			"period=2026-03-15/customer=xyz", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plans, err := core.BuildReadPlans(
				[]string{tc.pattern}, dataPath, partitionKeyParts)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := plans[0].Match(tc.hiveKey); got != tc.want {
				t.Errorf("Match(%q) under pattern %q = %v, want %v",
					tc.hiveKey, tc.pattern, got, tc.want)
			}
		})
	}
}

func TestBuildReadPlans_RejectsInvalidPattern(t *testing.T) {
	partitionKeyParts := []string{"period", "customer"}
	cases := []string{
		"period=*-17/customer=abc",      // leading star
		"period=2026-*-17/customer=abc", // middle star
		"period=X",                      // wrong segment count
		"period={a,b}/customer=abc",     // alternation
	}
	for _, p := range cases {
		t.Run(p, func(t *testing.T) {
			if _, err := core.BuildReadPlans(
				[]string{p}, "pre/data", partitionKeyParts); err == nil {
				t.Errorf("core.BuildReadPlans(%q): expected error", p)
			}
		})
	}
}

func TestHiveKeyOfDataFile(t *testing.T) {
	cases := []struct {
		name   string
		s3Key  string
		dp     string
		want   string
		wantOK bool
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
			got, ok := core.HiveKeyOfDataFile(tc.s3Key, tc.dp)
			if ok != tc.wantOK {
				t.Errorf("ok = %v, want %v", ok, tc.wantOK)
			}
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}
