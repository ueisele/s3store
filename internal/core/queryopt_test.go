package core

import "testing"

func TestWithHistory(t *testing.T) {
	var o QueryOpts
	WithHistory()(&o)
	if !o.IncludeHistory {
		t.Error("WithHistory() didn't set IncludeHistory=true")
	}
}

func TestQueryOptsApply(t *testing.T) {
	var o QueryOpts
	o.Apply(WithHistory())
	if !o.IncludeHistory {
		t.Error("Apply(WithHistory()) didn't set IncludeHistory=true")
	}
}

func TestQueryOptsApplyNoopOnZero(t *testing.T) {
	var o QueryOpts
	o.Apply()
	if o.IncludeHistory {
		t.Error("Apply() with no options should leave IncludeHistory=false")
	}
}
