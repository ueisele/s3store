package core

import (
	"reflect"
	"testing"
)

func TestParseKeyPattern(t *testing.T) {
	parts := []string{"period", "customer"}
	cases := []struct {
		name    string
		pattern string
		want    []Segment
	}{
		{"match all empty", "", nil},
		{"match all star", "*", nil},
		{"whole-segment wild head", "*/customer=abc",
			[]Segment{
				{Kind: SegWildAll, KeyPart: "period"},
				{Kind: SegExact, KeyPart: "customer", Value: "abc"},
			}},
		{"two exacts", "period=X/customer=Y",
			[]Segment{
				{Kind: SegExact, KeyPart: "period", Value: "X"},
				{Kind: SegExact, KeyPart: "customer", Value: "Y"},
			}},
		{"trailing star", "period=2026-03-*/customer=abc",
			[]Segment{
				{Kind: SegPrefix, KeyPart: "period", Value: "2026-03-"},
				{Kind: SegExact, KeyPart: "customer", Value: "abc"},
			}},
		{"range both sides", "period=2026-03-01..2026-04-01/customer=abc",
			[]Segment{
				{Kind: SegRange, KeyPart: "period",
					Value: "2026-03-01", ToValue: "2026-04-01"},
				{Kind: SegExact, KeyPart: "customer", Value: "abc"},
			}},
		{"range unbounded upper", "period=2026-03-01../customer=abc",
			[]Segment{
				{Kind: SegRange, KeyPart: "period",
					Value: "2026-03-01", ToValue: ""},
				{Kind: SegExact, KeyPart: "customer", Value: "abc"},
			}},
		{"range unbounded lower", "period=..2026-04-01/customer=abc",
			[]Segment{
				{Kind: SegRange, KeyPart: "period",
					Value: "", ToValue: "2026-04-01"},
				{Kind: SegExact, KeyPart: "customer", Value: "abc"},
			}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseKeyPattern(tc.pattern, parts)
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
			if _, err := ParseKeyPattern(p, parts); err == nil {
				t.Errorf("ParseKeyPattern(%q): expected error", p)
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
			if got := CommonPrefix(tc.a, tc.b); got != tc.want {
				t.Errorf("CommonPrefix(%q, %q) = %q, want %q",
					tc.a, tc.b, got, tc.want)
			}
		})
	}
}
