package s3store

import (
	"strings"
	"testing"
)

func TestValidateIdempotencyToken(t *testing.T) {
	cases := []struct {
		name    string
		token   string
		wantErr bool
	}{
		{"ok simple", "batch42", false},
		{"ok with dashes", "2026-04-22T10:15:00Z-batch42", false},
		{"ok with digits", "job.1234567890", false},
		{"ok max length", strings.Repeat("a", 200), false},
		{"empty", "", true},
		{"too long", strings.Repeat("a", 201), true},
		{"contains slash", "ns/job42", true},
		{"contains semicolon", "job;42", true},
		{"contains dotdot", "job..42", true},
		{"contains space", "job 42", true},
		{"contains tab", "job\t42", true},
		{"contains null", "job\x0042", true},
		{"contains unicode", "jöb42", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateIdempotencyToken(tc.token)
			if tc.wantErr && err == nil {
				t.Errorf("want error for %q, got nil", tc.token)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("want nil error for %q, got %v",
					tc.token, err)
			}
		})
	}
}
