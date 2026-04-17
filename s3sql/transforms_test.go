package s3sql

import (
	"slices"
	"strings"
	"testing"
)

// TestScanExprForPattern guards the base scan expression
// emitted for a validated key pattern: a read_parquet call
// with hive_partitioning + union_by_name, over an S3 glob
// single-quoted for DuckDB.
func TestScanExprForPattern(t *testing.T) {
	s := newTestStore()

	cases := []struct {
		name    string
		pattern string
		want    string
		wantErr bool
	}{
		{
			name:    "exact key",
			pattern: "period=2026-03-17/customer=abc",
			want: "SELECT * FROM read_parquet(" +
				"'s3://b/p/data/period=2026-03-17/customer=abc/*.parquet', " +
				"hive_partitioning=true, union_by_name=true)",
		},
		{
			name:    "match all",
			pattern: "*",
			want: "SELECT * FROM read_parquet(" +
				"'s3://b/p/data/**/*.parquet', " +
				"hive_partitioning=true, union_by_name=true)",
		},
		{
			name:    "partition value with apostrophe is SQL-quoted",
			pattern: "period=X/customer=o'brien",
			want: "SELECT * FROM read_parquet(" +
				"'s3://b/p/data/period=X/customer=o''brien/*.parquet', " +
				"hive_partitioning=true, union_by_name=true)",
		},
		{
			name:    "truncated pattern errors",
			pattern: "period=X",
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := s.scanExprForPattern(tc.pattern)
			if tc.wantErr {
				if err == nil {
					t.Errorf("expected error, got %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("\ngot  %q\nwant %q", got, tc.want)
			}
		})
	}
}

// TestBuildColumnTransformsAllCases is the comprehensive
// table-driven test for buildColumnTransforms: every combination
// of ColumnAliases and ColumnDefaults against every combination
// of existing-columns sets it has to handle.
func TestBuildColumnTransformsAllCases(t *testing.T) {
	cases := []struct {
		name          string
		aliases       map[string][]string
		defaults      map[string]string
		existingCols  map[string]bool
		wantReplaces  []string
		wantAdditions []string
		wantExcludes  []string
	}{
		{
			name: "nothing configured",
		},
		{
			name:         "default on existing column uses REPLACE",
			defaults:     map[string]string{"currency": "'EUR'"},
			existingCols: map[string]bool{"currency": true},
			wantReplaces: []string{"COALESCE(currency, 'EUR') AS currency"},
		},
		{
			name:          "default on missing column becomes addition",
			defaults:      map[string]string{"currency": "'EUR'"},
			existingCols:  map[string]bool{},
			wantAdditions: []string{"'EUR' AS currency"},
		},
		{
			name: "alias with new name present uses REPLACE + excludes olds",
			aliases: map[string][]string{
				"cost": {"price", "unit_price"},
			},
			existingCols: map[string]bool{
				"cost":       true,
				"price":      true,
				"unit_price": true,
			},
			wantReplaces: []string{
				"COALESCE(cost, price, unit_price) AS cost",
			},
			wantExcludes: []string{"price", "unit_price"},
		},
		{
			name: "alias with new name missing, all olds present: addition + excludes",
			aliases: map[string][]string{
				"cost": {"price", "unit_price"},
			},
			existingCols: map[string]bool{
				"price":      true,
				"unit_price": true,
			},
			wantAdditions: []string{
				"COALESCE(price, unit_price) AS cost",
			},
			wantExcludes: []string{"price", "unit_price"},
		},
		{
			name: "alias with new name missing, some olds missing",
			aliases: map[string][]string{
				"cost": {"price", "unit_price"},
			},
			existingCols: map[string]bool{
				"price": true,
			},
			wantAdditions: []string{
				"COALESCE(price) AS cost",
			},
			wantExcludes: []string{"price"},
		},
		{
			name: "alias with nothing present falls back to NULL",
			aliases: map[string][]string{
				"cost": {"price"},
			},
			existingCols:  map[string]bool{},
			wantAdditions: []string{"NULL AS cost"},
		},
		{
			name: "alias filters missing olds from COALESCE",
			aliases: map[string][]string{
				"cost": {"price", "unit_price"},
			},
			existingCols: map[string]bool{
				"cost":  true,
				"price": true,
			},
			wantReplaces: []string{
				"COALESCE(cost, price) AS cost",
			},
			wantExcludes: []string{"price"},
		},
		{
			name: "alias shadows default on same column name",
			aliases: map[string][]string{
				"cost": {"price"},
			},
			defaults: map[string]string{
				"cost": "0",
			},
			existingCols: map[string]bool{
				"cost":  true,
				"price": true,
			},
			wantReplaces: []string{
				"COALESCE(cost, price) AS cost",
			},
			wantExcludes: []string{"price"},
		},
		{
			name: "multiple defaults emit in sorted order",
			defaults: map[string]string{
				"z": "0", "a": "1", "m": "2",
			},
			existingCols: map[string]bool{
				"a": true, "m": true, "z": true,
			},
			wantReplaces: []string{
				"COALESCE(a, 1) AS a",
				"COALESCE(m, 2) AS m",
				"COALESCE(z, 0) AS z",
			},
		},
		{
			// Two aliases absorbing the same old column must
			// not produce EXCLUDE (value, value) — DuckDB would
			// reject that at plan time.
			name: "shared old name across aliases deduplicates EXCLUDE",
			aliases: map[string][]string{
				"amount": {"value"},
				"cost":   {"value"},
			},
			existingCols: map[string]bool{"value": true},
			wantAdditions: []string{
				"COALESCE(value) AS amount",
				"COALESCE(value) AS cost",
			},
			wantExcludes: []string{"value"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestStore()
			s.cfg.ColumnAliases = tc.aliases
			s.cfg.ColumnDefaults = tc.defaults

			replaces, additions, excludes := s.buildColumnTransforms(tc.existingCols)

			if !slicesEqualNilEmpty(replaces, tc.wantReplaces) {
				t.Errorf("replaces:\n got  %v\n want %v", replaces, tc.wantReplaces)
			}
			if !slicesEqualNilEmpty(additions, tc.wantAdditions) {
				t.Errorf("additions:\n got  %v\n want %v", additions, tc.wantAdditions)
			}
			if !slicesEqualNilEmpty(excludes, tc.wantExcludes) {
				t.Errorf("excludes:\n got  %v\n want %v", excludes, tc.wantExcludes)
			}
		})
	}
}

// TestBuildColumnTransformsDeterministic guards that the output
// order is stable across invocations — required for plan-cache
// hit rates, diffable logs, and snapshot-style tests.
func TestBuildColumnTransformsDeterministic(t *testing.T) {
	s := newTestStore()
	s.cfg.ColumnDefaults = map[string]string{
		"zeta": "0", "alpha": "1", "mike": "2", "bravo": "3",
	}
	existing := map[string]bool{
		"alpha": true, "bravo": true, "mike": true, "zeta": true,
	}

	first, _, _ := s.buildColumnTransforms(existing)
	for range 50 {
		next, _, _ := s.buildColumnTransforms(existing)
		if !slices.Equal(first, next) {
			t.Fatalf("non-deterministic output:\n %v\n vs\n %v", first, next)
		}
	}

	want := []string{
		"COALESCE(alpha, 1) AS alpha",
		"COALESCE(bravo, 3) AS bravo",
		"COALESCE(mike, 2) AS mike",
		"COALESCE(zeta, 0) AS zeta",
	}
	if !slices.Equal(first, want) {
		t.Errorf("sort order:\n got  %v\n want %v", first, want)
	}
}

// TestWrapScanExprShapes covers the full space of SQL shapes
// wrapScanExpr emits: bare scan, dedup-only, transforms-only,
// transforms + dedup, REPLACE vs. addition paths, and the
// EXCLUDE dedup across aliased olds.
func TestWrapScanExprShapes(t *testing.T) {
	scan := "SELECT * FROM read_parquet('s3://x/**/*.parquet', " +
		"hive_partitioning=true, union_by_name=true)"
	// newTestStore's TableAlias is "t"; user SQL references it.
	userSQL := "SELECT * FROM t"

	cases := []struct {
		name         string
		versionCol   string
		aliases      map[string][]string
		defaults     map[string]string
		existingCols map[string]bool
		history      bool
		wantContains []string
		wantMissing  []string
	}{
		{
			name:         "no transforms, no dedup",
			history:      true,
			wantContains: []string{"WITH t AS", scan, userSQL},
			wantMissing:  []string{"_raw", "REPLACE", "QUALIFY"},
		},
		{
			name:       "no transforms, dedup enabled",
			versionCol: "ts",
			wantContains: []string{
				"QUALIFY ROW_NUMBER()",
				"PARTITION BY period, customer",
				"ORDER BY ts DESC",
			},
			wantMissing: []string{"_raw", "REPLACE"},
		},
		{
			name:         "default on existing col uses REPLACE",
			defaults:     map[string]string{"currency": "'EUR'"},
			existingCols: map[string]bool{"currency": true},
			history:      true,
			wantContains: []string{
				"_raw AS",
				"SELECT * REPLACE (COALESCE(currency, 'EUR') AS currency) FROM _raw",
			},
		},
		{
			name:         "default on missing col appended",
			defaults:     map[string]string{"currency": "'EUR'"},
			existingCols: map[string]bool{},
			history:      true,
			wantContains: []string{
				"_raw AS",
				"SELECT *, 'EUR' AS currency FROM _raw",
			},
			wantMissing: []string{"REPLACE"},
		},
		{
			name: "mixed replaces, additions, and EXCLUDE",
			aliases: map[string][]string{
				"cost": {"price"},
			},
			defaults: map[string]string{
				"currency": "'EUR'",
			},
			existingCols: map[string]bool{
				"currency": true,
				"price":    true,
				// cost missing → addition; price → EXCLUDEd
			},
			history: true,
			wantContains: []string{
				"* EXCLUDE (price)",
				"REPLACE (COALESCE(currency, 'EUR') AS currency)",
				"COALESCE(price) AS cost",
			},
			wantMissing: []string{"QUALIFY"},
		},
		{
			name: "alias with existing new name uses REPLACE + EXCLUDE",
			aliases: map[string][]string{
				"cost": {"price", "unit_price"},
			},
			existingCols: map[string]bool{
				"cost":       true,
				"price":      true,
				"unit_price": true,
			},
			history: true,
			wantContains: []string{
				"* EXCLUDE (price, unit_price) REPLACE " +
					"(COALESCE(cost, price, unit_price) AS cost)",
			},
			wantMissing: []string{"QUALIFY"},
		},
		{
			name:       "dedup + transforms combine correctly",
			versionCol: "ts",
			aliases: map[string][]string{
				"cost": {"price"},
			},
			existingCols: map[string]bool{
				"price": true,
				// cost missing → addition, price excluded
			},
			wantContains: []string{
				"* EXCLUDE (price)",
				"COALESCE(price) AS cost",
				"QUALIFY ROW_NUMBER()",
				"PARTITION BY period, customer",
				"ORDER BY ts DESC",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestStore()
			s.cfg.VersionColumn = tc.versionCol
			s.cfg.ColumnAliases = tc.aliases
			s.cfg.ColumnDefaults = tc.defaults

			got := s.wrapScanExpr(scan, userSQL, tc.existingCols, tc.history)
			for _, want := range tc.wantContains {
				if !strings.Contains(got, want) {
					t.Errorf("missing %q in output:\n%s", want, got)
				}
			}
			for _, missing := range tc.wantMissing {
				if strings.Contains(got, missing) {
					t.Errorf("unexpected %q in output:\n%s", missing, got)
				}
			}
		})
	}
}

// TestDedupColumnsCustom guards that the dedup key follows
// DeduplicateBy when set — this feeds directly into the
// PARTITION BY clause in the QUALIFY wrapScanExpr emits.
func TestDedupColumnsCustom(t *testing.T) {
	s := newTestStore()
	s.cfg.VersionColumn = "ts"
	s.cfg.DeduplicateBy = []string{"instance_id", "sku"}

	got := s.wrapScanExpr("BASE", "SELECT 1", nil, false)
	if !strings.Contains(got, "PARTITION BY instance_id, sku") {
		t.Errorf("PARTITION BY didn't follow DeduplicateBy:\n%s", got)
	}
}

// slicesEqualNilEmpty treats nil and empty slices as equal so
// tests don't have to repeat `[]string{}` everywhere.
func slicesEqualNilEmpty(a, b []string) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	return slices.Equal(a, b)
}
