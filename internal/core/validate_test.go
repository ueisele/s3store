package core

import (
	"strings"
	"testing"
)

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
			err := ValidatePartitionKeyParts(tc.parts)
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

		{"wrong segment count", "period=2026-03-17", true},
		{"extra segment", "period=X/customer=Y/extra=Z", true},
		{"wrong part name", "ustomer=abc/period=X", true},
		{"leading star in value", "period=*-17/customer=abc", true},
		{"middle star in value", "period=2026-*-17/customer=abc", true},
		{"multiple stars in value", "period=2026-*-*/customer=abc", true},
		{"char class", "period=[0-9]/customer=abc", true},
		{"question mark", "period=2026-03-??/customer=abc", true},
		{"alternation", "period={2026,2027}/customer=abc", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateKeyPattern(tc.pattern, partitionKeyParts)
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
