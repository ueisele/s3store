package s3parquet

import (
	"testing"
	"time"
)

// TestRefPutBudget guards the ref-PUT budget formula: always
// half of SettleWindow, independent of ConsistencyControl. The
// reserved half covers HEAD time on the post-PUT disambiguation
// plus any LIST propagation between the node that accepted the
// PUT and a concurrent Poll. A uniform settle/2 is simpler and
// strictly safer than the old strong-vs-weak split, which handed
// strong-consistency writers the full SettleWindow and forced
// unnecessary recovery on borderline-slow PUTs that actually
// landed in budget.
func TestRefPutBudget(t *testing.T) {
	const settle = 6 * time.Second
	want := settle / 2
	if got := refPutBudget(settle); got != want {
		t.Errorf("refPutBudget(%v) = %v, want %v", settle, got, want)
	}
}

// TestRefPutBudget_ZeroSettleWindow guards the extreme case: a
// zero SettleWindow (read-to-live-tip mode) produces a zero
// budget. Callers using that mode accept the "no settle" contract
// and don't need a PUT deadline; the write path still completes
// but the caller can't expect Poll-visibility guarantees.
func TestRefPutBudget_ZeroSettleWindow(t *testing.T) {
	if got := refPutBudget(0); got != 0 {
		t.Errorf("refPutBudget(0) = %v, want 0", got)
	}
}
