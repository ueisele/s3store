package core

import (
	"context"

	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// AddHeaderMiddleware returns an AWS SDK v2 APIOption that installs
// a Build-step middleware writing the given HTTP header into every
// request passing through. Registered at the Build step (after
// Serialize, before Finalize) so it runs once per operation
// regardless of retries, and sees the assembled smithyhttp.Request.
//
// Shared between the write path (Consistency-Control on
// correctness-critical PUT / GET / LIST) and the ref-stream Poll
// path (same header on LIST-after-write to keep StorageGRID
// strong-consistency guarantees from drifting between writer and
// consumer).
func AddHeaderMiddleware(
	header, value string,
) func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		return stack.Build.Add(middleware.BuildMiddlewareFunc(
			"s3store.addHeader."+header,
			func(
				ctx context.Context, in middleware.BuildInput,
				next middleware.BuildHandler,
			) (middleware.BuildOutput, middleware.Metadata, error) {
				if req, ok := in.Request.(*smithyhttp.Request); ok {
					req.Header.Set(header, value)
				}
				return next.HandleBuild(ctx, in)
			}), middleware.After)
	}
}
