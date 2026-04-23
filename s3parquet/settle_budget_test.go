package s3parquet

import (
	"testing"
	"time"
)

// TestRefPutBudget guards the consistency → ref-PUT budget
// mapping. Strong levels get the full SettleWindow (LIST sees new
// refs immediately, zero propagation reserve). Everything else —
// default, read-after-new-write, available, unknown — gets half
// the window so the other half covers LIST propagation on weakly-
// consistent backends.
func TestRefPutBudget(t *testing.T) {
	const settle = 6 * time.Second

	strong := []ConsistencyLevel{
		ConsistencyStrongGlobal,
		ConsistencyStrongSite,
		ConsistencyAll,
	}
	for _, c := range strong {
		t.Run(string(c), func(t *testing.T) {
			if got := refPutBudget(c, settle); got != settle {
				t.Errorf("strong level %q got %v, want %v",
					c, got, settle)
			}
		})
	}

	weak := []ConsistencyLevel{
		ConsistencyDefault,
		ConsistencyReadAfterNewWrite,
		ConsistencyAvailable,
		ConsistencyLevel("unknown-future-value"),
	}
	for _, c := range weak {
		t.Run(string(c), func(t *testing.T) {
			want := settle / 2
			if got := refPutBudget(c, settle); got != want {
				t.Errorf("weak level %q got %v, want %v",
					c, got, want)
			}
		})
	}
}

// TestRefPutBudget_ZeroSettleWindow guards the extreme case: a
// zero SettleWindow (read-to-live-tip mode) produces a zero budget
// on both paths. Callers using this mode accept the "no settle"
// contract and don't need a PUT deadline; the write path still
// completes but the caller can't expect Poll-visibility guarantees.
func TestRefPutBudget_ZeroSettleWindow(t *testing.T) {
	if got := refPutBudget(ConsistencyStrongGlobal, 0); got != 0 {
		t.Errorf("strong zero: got %v, want 0", got)
	}
	if got := refPutBudget(ConsistencyDefault, 0); got != 0 {
		t.Errorf("weak zero: got %v, want 0", got)
	}
}
