package s3sql

import (
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestDeriveDuckDBS3Settings_Empty(t *testing.T) {
	c := s3.New(s3.Options{})
	got := deriveDuckDBS3Settings(c)
	if len(got) != 0 {
		t.Errorf("expected empty settings for default options, got %v", got)
	}
}

func TestDeriveDuckDBS3Settings_MinIOStyle(t *testing.T) {
	c := s3.New(s3.Options{
		BaseEndpoint: aws.String("http://minio:9000"),
		Region:       "us-east-1",
		UsePathStyle: true,
	})
	got := deriveDuckDBS3Settings(c)

	want := []string{
		"SET s3_endpoint='minio:9000'",
		"SET s3_use_ssl=false",
		"SET s3_region='us-east-1'",
		"SET s3_url_style='path'",
	}
	if !slices.Equal(got, want) {
		t.Errorf("\ngot:  %v\nwant: %v", got, want)
	}
}

func TestDeriveDuckDBS3Settings_HTTPSKeepsSSL(t *testing.T) {
	c := s3.New(s3.Options{
		BaseEndpoint: aws.String("https://s3.example.com"),
		Region:       "eu-west-1",
	})
	got := deriveDuckDBS3Settings(c)

	// HTTPS endpoint: no s3_use_ssl override emitted; path-style
	// is false so no s3_url_style either.
	for _, stmt := range got {
		if stmt == "SET s3_use_ssl=false" {
			t.Errorf("unexpected s3_use_ssl=false for https endpoint")
		}
		if stmt == "SET s3_url_style='path'" {
			t.Errorf("unexpected s3_url_style='path' when UsePathStyle is false")
		}
	}

	// Must include endpoint + region.
	wantEndpoint := "SET s3_endpoint='s3.example.com'"
	wantRegion := "SET s3_region='eu-west-1'"
	if !slices.Contains(got, wantEndpoint) {
		t.Errorf("missing %q in %v", wantEndpoint, got)
	}
	if !slices.Contains(got, wantRegion) {
		t.Errorf("missing %q in %v", wantRegion, got)
	}
}

func TestSQLEscape(t *testing.T) {
	cases := []struct{ in, want string }{
		{"", ""},
		{"simple", "simple"},
		{"o'brien", "o''brien"},
		{"''", "''''"},
	}
	for _, tc := range cases {
		if got := sqlEscape(tc.in); got != tc.want {
			t.Errorf("sqlEscape(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
