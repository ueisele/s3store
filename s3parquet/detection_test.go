package s3parquet

import "testing"

// TestDuplicateWriteDetection_FactoryReturnTypes asserts each
// factory returns a non-nil DuplicateWriteDetection value of the
// sealed-internal concrete type. Concrete-kind correctness is
// exercised via TestDuplicateWriteDetection_AsConcrete below —
// this test narrowly guards that the factory shape hasn't drifted
// (e.g. returning interface{} or a nil zero-value).
func TestDuplicateWriteDetection_FactoryReturnTypes(t *testing.T) {
	if v := DuplicateWriteDetectionByOverwritePrevention(); v == nil {
		t.Errorf("DuplicateWriteDetectionByOverwritePrevention() = nil")
	}
	if v := DuplicateWriteDetectionByHEAD(); v == nil {
		t.Errorf("DuplicateWriteDetectionByHEAD() = nil")
	}
	if v := DuplicateWriteDetectionByProbe(true); v == nil {
		t.Errorf("DuplicateWriteDetectionByProbe(true) = nil")
	}
	if v := DuplicateWriteDetectionByProbe(false); v == nil {
		t.Errorf("DuplicateWriteDetectionByProbe(false) = nil")
	}
}

// TestDuplicateWriteDetection_AsConcrete confirms that asConcrete
// unwraps each factory into the expected kind and deleteScratch
// value. Nil input resolves to the documented default (probe,
// delete scratch).
func TestDuplicateWriteDetection_AsConcrete(t *testing.T) {
	cases := []struct {
		name       string
		input      DuplicateWriteDetection
		wantKind   detectionKind
		wantDelete bool
	}{
		{
			"nil defaults to probe+delete",
			nil,
			detectKindProbe, true,
		},
		{
			"overwrite-prevention",
			DuplicateWriteDetectionByOverwritePrevention(),
			detectKindOverwritePrevention, false,
		},
		{
			"head",
			DuplicateWriteDetectionByHEAD(),
			detectKindHEAD, false,
		},
		{
			"probe true",
			DuplicateWriteDetectionByProbe(true),
			detectKindProbe, true,
		},
		{
			"probe false",
			DuplicateWriteDetectionByProbe(false),
			detectKindProbe, false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := asConcrete(tc.input)
			if got.kind != tc.wantKind {
				t.Errorf("kind = %d, want %d", got.kind, tc.wantKind)
			}
			if got.deleteScratch != tc.wantDelete {
				t.Errorf("deleteScratch = %v, want %v",
					got.deleteScratch, tc.wantDelete)
			}
		})
	}
}

// TestConsistencyLevel_IsKnown guards the enum closure: every
// named constant must report known, and unknown values (typos,
// forward-compat values) must report unknown so NewWriter /
// NewReader can log the warning without blocking the call.
func TestConsistencyLevel_IsKnown(t *testing.T) {
	known := []ConsistencyLevel{
		ConsistencyDefault,
		ConsistencyAll,
		ConsistencyStrongGlobal,
		ConsistencyStrongSite,
		ConsistencyReadAfterNewWrite,
		ConsistencyAvailable,
	}
	for _, c := range known {
		if !c.IsKnown() {
			t.Errorf("ConsistencyLevel(%q).IsKnown() = false, want true", c)
		}
	}
	unknown := []ConsistencyLevel{
		"STRONG",      // uppercase — real value is "strong-global"
		"future",      // hypothetical forward-compat
		"all-the-way", // typo
	}
	for _, c := range unknown {
		if c.IsKnown() {
			t.Errorf("ConsistencyLevel(%q).IsKnown() = true, want false", c)
		}
	}
}
