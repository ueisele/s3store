package s3sql

import (
	"testing"

	"github.com/ueisele/s3store/internal/core"
)

// TestWithHistory guards that the re-exported WithHistory
// produces the same QueryOpts state as core.WithHistory. The
// re-export exists so callers who only import s3sql never need
// to reach into internal/core.
func TestWithHistory(t *testing.T) {
	var o core.QueryOpts
	WithHistory()(&o)
	if !o.IncludeHistory {
		t.Error("WithHistory didn't set IncludeHistory=true")
	}
}
