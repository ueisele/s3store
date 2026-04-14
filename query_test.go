package s3store

import (
	"slices"
	"strings"
	"testing"
)

func TestBuildColumnTransforms(t *testing.T) {
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
				// unit_price missing
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
			// Default for "cost" is skipped because an alias exists for it.
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

func TestBuildColumnTransformsIsDeterministic(t *testing.T) {
	s := newTestStore()
	s.cfg.ColumnDefaults = map[string]string{
		"zeta": "0", "alpha": "1", "mike": "2", "bravo": "3",
	}
	existing := map[string]bool{
		"alpha": true, "bravo": true, "mike": true, "zeta": true,
	}

	first, _, _ := s.buildColumnTransforms(existing)
	for i := 0; i < 50; i++ {
		next, _, _ := s.buildColumnTransforms(existing)
		if !slices.Equal(first, next) {
			t.Fatalf("non-deterministic output:\n %v\n vs\n %v", first, next)
		}
	}

	// And it's alphabetically sorted.
	wantOrder := []string{
		"COALESCE(alpha, 1) AS alpha",
		"COALESCE(bravo, 3) AS bravo",
		"COALESCE(mike, 2) AS mike",
		"COALESCE(zeta, 0) AS zeta",
	}
	if !slices.Equal(first, wantOrder) {
		t.Errorf("sort order:\n got  %v\n want %v", first, wantOrder)
	}
}

func TestWrapScanExprShapes(t *testing.T) {
	scan := "SELECT * FROM read_parquet('s3://x/**/*.parquet', " +
		"hive_partitioning=true, union_by_name=true)"
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
			name:         "no transforms, dedup enabled",
			versionCol:   "ts",
			wantContains: []string{"QUALIFY ROW_NUMBER()", "PARTITION BY period, customer", "ORDER BY ts DESC"},
			wantMissing:  []string{"_raw", "REPLACE"},
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
				// cost missing
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
			name: "dedup + transforms combine correctly",
			versionCol: "ts",
			aliases: map[string][]string{
				"cost": {"price"},
			},
			existingCols: map[string]bool{
				"price": true,
				// cost missing -> addition, price excluded
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
			s := newTestStore("period", "customer")
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

// slicesEqualNilEmpty treats nil and empty slices as equal so
// tests don't have to repeat `[]string{}` everywhere.
func slicesEqualNilEmpty(a, b []string) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	return slices.Equal(a, b)
}
