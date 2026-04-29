package s3store

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"
)

// TestMain installs a slog handler that drops the
// "CommitTimeout is below the advisory floor" warning so the
// test suite (which uses sub-advisory CommitTimeout values by
// design — testCommitTimeout = 2s in integration_test.go to
// keep settle-window sleeps short) doesn't emit the warning on
// every NewS3Target call. Other slog records pass through to
// the default text handler unchanged, so genuine warnings
// (missing data files, malformed refs, etc.) still surface.
//
// No build tag — the same filter applies whether `go test`
// runs the unit-only suite or the `-tags=integration` suite.
func TestMain(m *testing.M) {
	base := slog.NewTextHandler(os.Stderr, nil)
	slog.SetDefault(slog.New(filterAdvisoryHandler{base: base}))
	os.Exit(m.Run())
}

// filterAdvisoryHandler wraps a slog.Handler and drops records
// whose message is the CommitTimeout-below-advisory warning.
// Every other record is delegated to the wrapped handler
// unchanged.
type filterAdvisoryHandler struct {
	base slog.Handler
}

const advisoryWarningPrefix = "s3store: CommitTimeout is below the advisory floor"

func (h filterAdvisoryHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.base.Enabled(ctx, level)
}

func (h filterAdvisoryHandler) Handle(ctx context.Context, r slog.Record) error {
	if strings.HasPrefix(r.Message, advisoryWarningPrefix) {
		return nil
	}
	return h.base.Handle(ctx, r)
}

func (h filterAdvisoryHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return filterAdvisoryHandler{base: h.base.WithAttrs(attrs)}
}

func (h filterAdvisoryHandler) WithGroup(name string) slog.Handler {
	return filterAdvisoryHandler{base: h.base.WithGroup(name)}
}
