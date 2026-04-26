package s3store

import (
	"reflect"
	"strings"
	"testing"
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
			plans, err := buildReadPlans(
				[]string{tc.pattern}, dataPath, partitionKeyParts)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if plans[0].listPrefix != tc.want {
				t.Errorf("listPrefix = %q, want %q",
					plans[0].listPrefix, tc.want)
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
			plans, err := buildReadPlans(
				[]string{tc.pattern}, dataPath, partitionKeyParts)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := plans[0].match(tc.hiveKey); got != tc.want {
				t.Errorf("match(%q) under pattern %q = %v, want %v",
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
			if _, err := buildReadPlans(
				[]string{p}, "pre/data", partitionKeyParts); err == nil {
				t.Errorf("buildReadPlans(%q): expected error", p)
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

func TestParseKeyPattern(t *testing.T) {
	parts := []string{"period", "customer"}
	cases := []struct {
		name    string
		pattern string
		want    []segment
	}{
		{"match all empty", "", nil},
		{"match all star", "*", nil},
		{"whole-segment wild head", "*/customer=abc",
			[]segment{
				{kind: segWildAll, keyPart: "period"},
				{kind: segExact, keyPart: "customer", value: "abc"},
			}},
		{"two exacts", "period=X/customer=Y",
			[]segment{
				{kind: segExact, keyPart: "period", value: "X"},
				{kind: segExact, keyPart: "customer", value: "Y"},
			}},
		{"trailing star", "period=2026-03-*/customer=abc",
			[]segment{
				{kind: segPrefix, keyPart: "period", value: "2026-03-"},
				{kind: segExact, keyPart: "customer", value: "abc"},
			}},
		{"range both sides", "period=2026-03-01..2026-04-01/customer=abc",
			[]segment{
				{kind: segRange, keyPart: "period",
					value: "2026-03-01", toValue: "2026-04-01"},
				{kind: segExact, keyPart: "customer", value: "abc"},
			}},
		{"range unbounded upper", "period=2026-03-01../customer=abc",
			[]segment{
				{kind: segRange, keyPart: "period",
					value: "2026-03-01", toValue: ""},
				{kind: segExact, keyPart: "customer", value: "abc"},
			}},
		{"range unbounded lower", "period=..2026-04-01/customer=abc",
			[]segment{
				{kind: segRange, keyPart: "period",
					value: "", toValue: "2026-04-01"},
				{kind: segExact, keyPart: "customer", value: "abc"},
			}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseKeyPattern(tc.pattern, parts)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("\ngot  %+v\nwant %+v", got, tc.want)
			}
		})
	}
}

func TestParseKeyPattern_SurfacesValidationErrors(t *testing.T) {
	parts := []string{"period", "customer"}
	bad := []string{
		"period=2026-*-17/customer=abc",
		"period=../customer=abc",
		"period=a..b..c/customer=abc",
		"period=X",
	}
	for _, p := range bad {
		t.Run(p, func(t *testing.T) {
			if _, err := parseKeyPattern(p, parts); err == nil {
				t.Errorf("parseKeyPattern(%q): expected error", p)
			}
		})
	}
}

func TestCommonPrefix(t *testing.T) {
	cases := []struct {
		a, b, want string
	}{
		{"2026-03-01", "2026-04-01", "2026-0"},
		{"2026-03-01", "2026-03-15", "2026-03-"},
		{"a", "z", ""},
		{"abc", "abc", "abc"},
		{"", "abc", ""},
		{"abc", "", ""},
		{"", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.a+"|"+tc.b, func(t *testing.T) {
			if got := commonPrefix(tc.a, tc.b); got != tc.want {
				t.Errorf("commonPrefix(%q, %q) = %q, want %q",
					tc.a, tc.b, got, tc.want)
			}
		})
	}
}

func TestValidateHivePartitionValue(t *testing.T) {
	cases := []struct {
		name, value string
		wantErr     bool
	}{
		{"plain", "abc", false},
		{"with equals", "a=b=c", false},
		{"with dash", "a-b-c", false},
		{"with underscore", "a_b_c", false},
		{"unicode", "日本", false},
		{"empty", "", true},
		{"slash", "a/b", true},
		{"double dot", "a..b", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateHivePartitionValue(tc.value)
			if tc.wantErr != (err != nil) {
				t.Errorf("value=%q: wantErr=%v got %v",
					tc.value, tc.wantErr, err)
			}
		})
	}
}

func TestValidatePartitionKeyParts(t *testing.T) {
	cases := []struct {
		name    string
		parts   []string
		wantSub string // empty = want nil
	}{
		{"valid", []string{"period", "customer"}, ""},
		{"empty slice", nil, "PartitionKeyParts is required"},
		{"empty string", []string{"period", ""}, "is empty"},
		{"contains equals", []string{"period", "cust=bad"}, "must not contain"},
		{"contains slash", []string{"per/iod", "customer"}, "must not contain"},
		{"duplicate", []string{"period", "period"}, "is duplicated"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validatePartitionKeyParts(tc.parts)
			if tc.wantSub == "" {
				if err != nil {
					t.Errorf("expected nil, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantSub)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("error %q did not contain %q",
					err.Error(), tc.wantSub)
			}
		})
	}
}

func TestValidateKeyPattern(t *testing.T) {
	partitionKeyParts := []string{"period", "customer"}
	cases := []struct {
		name    string
		pattern string
		wantErr bool
	}{
		{"empty matches all", "", false},
		{"star matches all", "*", false},
		{"exact key", "period=2026-03-17/customer=abc", false},
		{"trailing star in value", "period=2026-03-*/customer=abc", false},
		{"whole-segment star first", "*/customer=abc", false},
		{"whole-segment star second", "period=X/*", false},
		{"both whole-segment", "*/*", false},

		{"range both sides", "period=2026-03-01..2026-04-01/customer=abc", false},
		{"range only lower", "period=2026-03-01../customer=abc", false},
		{"range only upper", "period=..2026-04-01/customer=abc", false},
		{"range equal ends", "period=2026-03-01..2026-03-01/customer=abc", false},
		{"range plus wildcard tail", "period=2026-03-01..2026-04-01/*", false},

		{"wrong segment count", "period=2026-03-17", true},
		{"extra segment", "period=X/customer=Y/extra=Z", true},
		{"wrong part name", "ustomer=abc/period=X", true},
		{"leading star in value", "period=*-17/customer=abc", true},
		{"middle star in value", "period=2026-*-17/customer=abc", true},
		{"multiple stars in value", "period=2026-*-*/customer=abc", true},
		{"char class", "period=[0-9]/customer=abc", true},
		{"question mark", "period=2026-03-??/customer=abc", true},
		{"alternation", "period={2026,2027}/customer=abc", true},
		{"range empty both", "period=../customer=abc", true},
		{"range reversed", "period=2026-04-01..2026-03-01/customer=abc", true},
		{"range triple dots", "period=a..b..c/customer=abc", true},
		{"range star in endpoint", "period=2026-*..2026-04/customer=abc", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateKeyPattern(tc.pattern, partitionKeyParts)
			if tc.wantErr {
				if err == nil {
					t.Errorf("expected error for %q, got nil", tc.pattern)
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error for %q: %v", tc.pattern, err)
			}
		})
	}
}
