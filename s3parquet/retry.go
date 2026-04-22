package s3parquet

import (
	"context"
	"errors"
	"time"

	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// Transient-error retry policy applied by S3Target's helpers
// around every S3 call (put / get / head / delete / list). The
// AWS SDK runs its own retryer under us; this is an outer layer
// that survives an exhausted SDK budget and keeps behaviour
// predictable when callers plug in aws.NopRetryer or a custom
// retryer. Budget is deliberately small — four attempts over
// ~1.4s — so one flaky call doesn't delay a Write by minutes.
const retryMaxAttempts = 4 // 1 initial + 3 retries

var retryBackoff = [retryMaxAttempts - 1]time.Duration{
	200 * time.Millisecond,
	400 * time.Millisecond,
	800 * time.Millisecond,
}

// retry runs fn up to retryMaxAttempts times on transient S3
// failures, sleeping retryBackoff[i] before retry i+1. Returns
// as soon as fn succeeds, returns a non-retryable error, or
// ctx is cancelled.
func retry(ctx context.Context, fn func() error) error {
	var err error
	for attempt := 0; attempt < retryMaxAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(retryBackoff[attempt-1]):
			}
		}
		err = fn()
		if err == nil {
			return nil
		}
		if !isTransientS3Error(err) {
			return err
		}
	}
	return err
}

// isTransientS3Error reports whether err is likely to succeed
// on retry: HTTP 5xx, HTTP 429 (SlowDown), or transport-layer
// errors without an HTTP response (DNS / TCP / TLS / connection
// reset). Context cancellation or deadline expiry is never
// retryable — the caller already decided to stop.
func isTransientS3Error(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if respErr, ok := errors.AsType[*smithyhttp.ResponseError](err); ok {
		status := respErr.HTTPStatusCode()
		return status >= 500 || status == 429
	}
	// No HTTP response attached — transport-layer error. Retry.
	return true
}
