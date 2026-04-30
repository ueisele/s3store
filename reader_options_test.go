package s3store

import "testing"

func TestWithHistory(t *testing.T) {
	var o readOpts
	WithHistory()(&o)
	if !o.includeHistory {
		t.Error("WithHistory() didn't set includeHistory=true")
	}
}

func TestReadOptsApply(t *testing.T) {
	var o readOpts
	o.apply(WithHistory())
	if !o.includeHistory {
		t.Error("apply(WithHistory()) didn't set includeHistory=true")
	}
}

func TestReadOptsApplyNoopOnZero(t *testing.T) {
	var o readOpts
	o.apply()
	if o.includeHistory {
		t.Error("apply() with no options should leave includeHistory=false")
	}
}
