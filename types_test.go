package s3store

import (
	"slices"
	"strings"
	"testing"
	"time"
)

func TestConfigSettleWindowDefault(t *testing.T) {
	var c Config[testRecord]
	if got := c.settleWindow(); got != 5*time.Second {
		t.Errorf("default: got %v, want 5s", got)
	}
}

func TestConfigSettleWindowOverride(t *testing.T) {
	c := Config[testRecord]{SettleWindow: 12 * time.Second}
	if got := c.settleWindow(); got != 12*time.Second {
		t.Errorf("override: got %v, want 12s", got)
	}
}

func TestWithHistory(t *testing.T) {
	var o queryOpts
	WithHistory()(&o)
	if !o.includeHistory {
		t.Error("WithHistory didn't set includeHistory=true")
	}
}

func TestDuckDBSettingsSQLNoEndpoint(t *testing.T) {
	stmts := duckDBSettingsSQL("")
	if len(stmts) == 0 {
		t.Fatal("expected at least one statement")
	}
	for _, stmt := range stmts {
		if stmt == "" {
			t.Error("empty statement in output")
		}
	}
	if slices.ContainsFunc(stmts, func(s string) bool {
		return strings.Contains(s, "s3_endpoint")
	}) {
		t.Errorf("unexpected s3_endpoint setting: %v", stmts)
	}
}

func TestDuckDBSettingsSQLWithEndpoint(t *testing.T) {
	stmts := duckDBSettingsSQL("minio:9000")
	if !slices.ContainsFunc(stmts, func(s string) bool {
		return strings.Contains(s, "s3_endpoint='minio:9000'")
	}) {
		t.Errorf("expected s3_endpoint='minio:9000' in %v", stmts)
	}
}

// TestDuckDBSettingsSQLEnablesObjectCache guards the #10
// performance fix: the parquet-footer cache must be enabled so
// the schema-introspection LIMIT 0 query and the main query
// don't double-read metadata.
func TestDuckDBSettingsSQLEnablesObjectCache(t *testing.T) {
	stmts := duckDBSettingsSQL("")
	if !slices.ContainsFunc(stmts, func(s string) bool {
		return strings.Contains(s, "enable_object_cache=true")
	}) {
		t.Errorf("expected enable_object_cache=true in %v", stmts)
	}
}

// TestDuckDBSettingsSQLPathStyle guards that path-style
// addressing is always set — required for MinIO and for
// virtual-hosted buckets with dots in their names.
func TestDuckDBSettingsSQLPathStyle(t *testing.T) {
	stmts := duckDBSettingsSQL("")
	if !slices.ContainsFunc(stmts, func(s string) bool {
		return strings.Contains(s, "s3_url_style='path'")
	}) {
		t.Errorf("expected s3_url_style='path' in %v", stmts)
	}
}
