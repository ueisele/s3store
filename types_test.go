package s3store

import (
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

func TestWithCompaction(t *testing.T) {
	var o pollOpts
	WithCompaction()(&o)
	if !o.compacted {
		t.Error("WithCompaction didn't set compacted=true")
	}
}

func TestDuckDBSettingsSQLNoEndpoint(t *testing.T) {
	stmts := duckDBSettingsSQL("")
	if len(stmts) == 0 {
		t.Fatal("expected at least one statement")
	}
	for _, stmt := range stmts {
		if stmt == "" {
			t.Error("empty statement in duckDBSettingsSQL output")
		}
	}
	for _, stmt := range stmts {
		if contains(stmt, "s3_endpoint") {
			t.Errorf("unexpected s3_endpoint setting: %q", stmt)
		}
	}
}

func TestDuckDBSettingsSQLWithEndpoint(t *testing.T) {
	stmts := duckDBSettingsSQL("minio:9000")
	found := false
	for _, stmt := range stmts {
		if contains(stmt, "s3_endpoint='minio:9000'") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected s3_endpoint in output, got %v", stmts)
	}
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
