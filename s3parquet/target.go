package s3parquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/ueisele/s3store/internal/core"
)

// ErrAlreadyExists is the sentinel returned by putIfAbsent and
// headThenPut when the target key is already present at the
// destination. Signals the write path that the current attempt is
// a retry of a previously-persisted logical write; callers scope-
// LIST the ref stream to decide whether the ref also needs
// re-emission.
var ErrAlreadyExists = errors.New("s3parquet: object already exists")

// s3CallOpt configures a single S3 target call. Used to thread
// optional per-operation headers (currently Consistency-Control)
// through put / get / head / list without widening method
// signatures with scalar parameters that are empty on every call
// today. Unexported — callers use the `with...` helpers below.
type s3CallOpt func(*s3CallOpts)

// s3CallOpts is the resolved per-call option set.
type s3CallOpts struct {
	// consistencyControl is the value for the Consistency-Control
	// HTTP header when non-empty; unknown to AWS S3 and MinIO
	// (ignored), honoured by NetApp StorageGRID.
	consistencyControl string
}

// applyS3CallOpts folds a variadic s3CallOpt chain into an
// s3CallOpts value.
func applyS3CallOpts(opts []s3CallOpt) s3CallOpts {
	var o s3CallOpts
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// withConsistencyControl sets the Consistency-Control header on
// the target call. Zero-length values (ConsistencyDefault) become
// no-ops so callers can forward their config value unconditionally
// without guarding on emptiness at every site.
func withConsistencyControl(level ConsistencyLevel) s3CallOpt {
	return func(o *s3CallOpts) {
		o.consistencyControl = string(level)
	}
}

// apiOptionsForOpts returns the aws.Config.APIOptions slice the
// current call should pass to the SDK. Today it contains at most
// one middleware — the Consistency-Control header setter — and
// returns nil when no header is requested.
func apiOptionsForOpts(o s3CallOpts) []func(*middleware.Stack) error {
	if o.consistencyControl == "" {
		return nil
	}
	return []func(*middleware.Stack) error{
		core.AddHeaderMiddleware(
			"Consistency-Control", o.consistencyControl),
	}
}

// S3Target is the untyped handle to an s3parquet dataset. Holds
// the S3 wiring and partitioning metadata shared by Writer,
// Reader, and Index — anything a caller needs to address the
// dataset that is independent of the record type T.
//
// Embedded in WriterConfig and ReaderConfig (via the Target
// field) so the five S3-wiring fields live in exactly one place.
// Also surfaced on Writer[T]/Reader[T]/Store[T] via .Target() so
// read-only tools (NewIndex, BackfillIndex) can address the same
// dataset without carrying T through their call graph.
type S3Target struct {
	// Bucket is the S3 bucket name.
	Bucket string

	// Prefix is the key prefix under which data/ref/index files
	// live. Must be non-empty — a bare bucket root would collide
	// with any other tenant of the bucket.
	Prefix string

	// S3Client is the AWS SDK v2 client to use. Endpoint, region,
	// credentials, and path-style setting are used as-is.
	S3Client *s3.Client

	// PartitionKeyParts declares the Hive-partition key segments in
	// the order they appear in the S3 path. Read/Write key
	// patterns are validated against this order.
	PartitionKeyParts []string

	// SettleWindow is how far behind the live tip Poll,
	// PollRecords, and Index.Lookup read, and the total budget the
	// ref PUT must fit inside (the ref-PUT timeout is SettleWindow
	// / 2; see refPutBudget). Keeps readers consistent with near-
	// tip writers whose refs may not yet be visible in S3 LIST.
	//
	// Default (zero value): 5s. Zero is treated as "use library
	// default", not "disable settle" — consumer correctness and
	// ref-PUT budgeting both depend on a non-zero value, so there
	// is no disabled mode. Set explicitly if you want a different
	// window (e.g. 30s on a slow backend, 500ms for low-latency
	// testing).
	SettleWindow time.Duration

	// DisableRefStream opts the dataset out of writing stream ref
	// files under <Prefix>/_stream/refs/. Saves one S3 PUT per
	// distinct partition key touched by a Write (Write groups
	// records by key and calls WriteWithKey once per group, each
	// of which issues one ref PUT without this flag). Read /
	// Query / Lookup / BackfillIndex are unaffected; Poll /
	// PollRecords / PollRecordsAll return ErrRefStreamDisabled.
	// OffsetAt still works (pure timestamp encoding — no S3
	// dependency).
	//
	// Irreversible per write: data written with this flag set has
	// no refs, so flipping the flag back does not retroactively
	// make Poll see historical writes. Set only for datasets that
	// are read purely via Read / Query.
	DisableRefStream bool
}

// NewS3Target constructs an S3Target with required fields. Use
// this in read-only services (Lookup) and migration jobs
// (BackfillIndex) that don't want to build a full Reader[T] or
// Writer[T]. SettleWindow defaults to 5s — override on the
// returned value if needed.
func NewS3Target(
	bucket, prefix string,
	cli *s3.Client,
	partitionKeyParts []string,
) S3Target {
	return S3Target{
		Bucket:            bucket,
		Prefix:            prefix,
		S3Client:          cli,
		PartitionKeyParts: partitionKeyParts,
	}
}

// Validate runs the full check for constructors that operate on
// partitioned data: Bucket, Prefix, S3Client, PartitionKeyParts.
// Used by NewWriter, NewReader, BackfillIndex, and the s3sql
// reader — anything that reads/writes data files keyed by
// partition. Exported so s3sql's NewReader can reuse the same
// check without duplicating the messages.
func (t S3Target) Validate() error {
	if err := t.ValidateLookup(); err != nil {
		return err
	}
	return core.ValidatePartitionKeyParts(t.PartitionKeyParts)
}

// ValidateLookup is the reduced check for constructors that
// only LIST / GET / PUT under a known prefix (no partition-key
// predicates): Bucket, Prefix, S3Client. Used by NewIndex —
// Lookup walks the <Prefix>/_index/<name>/ subtree, which is
// keyed by the index's own Columns, not the Target's
// PartitionKeyParts. A read-only analytics service can pass a
// minimally-populated S3Target and still build a working Index.
// Exported alongside Validate for symmetry.
func (t S3Target) ValidateLookup() error {
	if t.Bucket == "" {
		return fmt.Errorf("s3parquet: Bucket is required")
	}
	if t.Prefix == "" {
		return fmt.Errorf("s3parquet: Prefix is required")
	}
	if t.S3Client == nil {
		return fmt.Errorf("s3parquet: S3Client is required")
	}
	return nil
}

// EffectiveSettleWindow returns the configured SettleWindow, or
// 5s when it's unset (zero value). Zero is deliberately mapped
// to the default rather than "disabled" — a zero window would
// collapse both Poll's cutoff and the ref-PUT budget, which has
// no valid use case (consumers and writers would both skip
// their consistency safeguards).
//
// Exported so both s3parquet and s3sql read the same resolved
// value from a shared S3Target — callers that want the raw
// configured value can read .SettleWindow directly.
func (t S3Target) EffectiveSettleWindow() time.Duration {
	if t.SettleWindow > 0 {
		return t.SettleWindow
	}
	return 5 * time.Second
}

// get downloads a single object into memory. Used by the read
// path (Read, PollRecords) and by BackfillIndex when scanning
// historical parquet data.
func (t S3Target) get(
	ctx context.Context, key string, opts ...s3CallOpt,
) ([]byte, error) {
	callOpts := applyS3CallOpts(opts)
	apiOpts := apiOptionsForOpts(callOpts)
	var data []byte
	err := retry(ctx, func() error {
		resp, err := t.S3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(t.Bucket),
			Key:    aws.String(key),
		}, func(o *s3.Options) {
			o.APIOptions = append(o.APIOptions, apiOpts...)
		})
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		data, err = io.ReadAll(resp.Body)
		return err
	})
	return data, err
}

// put uploads data under key. Used by the write path (parquet +
// ref + markers) and by BackfillIndex for retroactive marker
// emission.
func (t S3Target) put(
	ctx context.Context, key string, data []byte, contentType string,
	opts ...s3CallOpt,
) error {
	callOpts := applyS3CallOpts(opts)
	apiOpts := apiOptionsForOpts(callOpts)
	return retry(ctx, func() error {
		_, err := t.S3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(t.Bucket),
			Key:         aws.String(key),
			Body:        bytes.NewReader(data),
			ContentType: aws.String(contentType),
		}, func(o *s3.Options) {
			o.APIOptions = append(o.APIOptions, apiOpts...)
		})
		return err
	})
}

// putWithMeta is put with user-supplied S3 object metadata.
// The AWS SDK transforms each entry into an x-amz-meta-<key>
// HTTP header; values are preserved verbatim. Used by the data
// PUT to stamp x-amz-meta-created-at so external tooling can
// see the writer's wall-clock time alongside the in-file
// InsertedAtField column. Marker and ref PUTs stay on the
// plain put — they don't carry writer metadata.
func (t S3Target) putWithMeta(
	ctx context.Context, key string, data []byte,
	contentType string, meta map[string]string,
	opts ...s3CallOpt,
) error {
	callOpts := applyS3CallOpts(opts)
	apiOpts := apiOptionsForOpts(callOpts)
	return retry(ctx, func() error {
		_, err := t.S3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(t.Bucket),
			Key:         aws.String(key),
			Body:        bytes.NewReader(data),
			ContentType: aws.String(contentType),
			Metadata:    meta,
		}, func(o *s3.Options) {
			o.APIOptions = append(o.APIOptions, apiOpts...)
		})
		return err
	})
}

// putIfAbsent PUTs data at key with an If-None-Match: * header so
// the PUT is rejected by the backend when the object already
// exists. Returns ErrAlreadyExists on:
//
//   - HTTP 412 PreconditionFailed — direct If-None-Match rejection
//     (AWS S3, recent MinIO).
//   - HTTP 403 AccessDenied followed by a HEAD that finds the
//     object — StorageGRID path where a bucket policy denies
//     s3:PutOverwriteObject. A 403 whose follow-up HEAD returns
//     404 or 403 is a real permission error and surfaces
//     unchanged so callers don't mask it.
//
// Any other error propagates as-is. On success returns nil with
// the object written. meta is attached as x-amz-meta-<k> headers
// when non-nil; mirrors putWithMeta for the idempotent data PUT.
func (t S3Target) putIfAbsent(
	ctx context.Context, key string, data []byte,
	contentType string, meta map[string]string,
	opts ...s3CallOpt,
) error {
	callOpts := applyS3CallOpts(opts)
	apiOpts := apiOptionsForOpts(callOpts)
	putErr := retry(ctx, func() error {
		_, err := t.S3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(t.Bucket),
			Key:         aws.String(key),
			Body:        bytes.NewReader(data),
			ContentType: aws.String(contentType),
			Metadata:    meta,
			IfNoneMatch: aws.String("*"),
		}, func(o *s3.Options) {
			o.APIOptions = append(o.APIOptions, apiOpts...)
		})
		return err
	})
	if putErr == nil {
		return nil
	}
	// 412 PreconditionFailed: direct If-None-Match rejection. Any
	// HTTP response error carrying that status lands us here —
	// smithy wraps it in *smithyhttp.ResponseError, and retry()
	// classifies 412 as non-transient so we see it on the first
	// attempt.
	if status, ok := httpStatusOf(putErr); ok {
		switch status {
		case 412:
			return ErrAlreadyExists
		case 403:
			// Could be overwrite-deny (StorageGRID bucket policy)
			// or a real permission error. Disambiguate via HEAD.
			//
			// The HEAD uses the same APIOptions so it carries the
			// same Consistency-Control header — NetApp's rule is
			// that paired PUT and follow-up operations must match
			// on consistency.
			ok, headErr := t.exists(ctx, key, opts...)
			if headErr == nil && ok {
				return ErrAlreadyExists
			}
			// Either HEAD failed (surface the HEAD error, still a
			// real failure) or the object genuinely doesn't exist
			// — the 403 is a permission problem. Either way, don't
			// mask the error as "already exists".
			return putErr
		}
	}
	return putErr
}

// headThenPut is the pre-flight-HEAD write path used when the
// backend has no overwrite-prevention mechanism. HEAD first;
// if the object exists returns ErrAlreadyExists without writing
// (so the parquet body is never re-uploaded on retry), otherwise
// PUTs data and returns the PUT error (or nil).
func (t S3Target) headThenPut(
	ctx context.Context, key string, data []byte,
	contentType string, meta map[string]string,
	opts ...s3CallOpt,
) error {
	existsAlready, err := t.exists(ctx, key, opts...)
	if err != nil {
		return err
	}
	if existsAlready {
		return ErrAlreadyExists
	}
	return t.putWithMeta(ctx, key, data, contentType, meta, opts...)
}

// exists reports whether an object exists, mapping S3's NotFound
// to (false, nil) so callers can distinguish "missing" from real
// failures without pattern-matching the error at every site.
func (t S3Target) exists(
	ctx context.Context, key string, opts ...s3CallOpt,
) (bool, error) {
	callOpts := applyS3CallOpts(opts)
	apiOpts := apiOptionsForOpts(callOpts)
	err := retry(ctx, func() error {
		_, err := t.S3Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(t.Bucket),
			Key:    aws.String(key),
		}, func(o *s3.Options) {
			o.APIOptions = append(o.APIOptions, apiOpts...)
		})
		return err
	})
	if err == nil {
		return true, nil
	}
	if _, ok := errors.AsType[*s3types.NotFound](err); ok {
		return false, nil
	}
	return false, err
}

// del removes an object. Used on the write-cleanup paths.
func (t S3Target) del(ctx context.Context, key string) error {
	return retry(ctx, func() error {
		_, err := t.S3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(t.Bucket),
			Key:    aws.String(key),
		})
		return err
	})
}

// size returns the object's content length. Used by the
// row-group-filtered read path to size the io.ReaderAt parquet-
// go opens the file through.
func (t S3Target) size(
	ctx context.Context, key string, opts ...s3CallOpt,
) (int64, error) {
	callOpts := applyS3CallOpts(opts)
	apiOpts := apiOptionsForOpts(callOpts)
	var resp *s3.HeadObjectOutput
	err := retry(ctx, func() error {
		var err error
		resp, err = t.S3Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(t.Bucket),
			Key:    aws.String(key),
		}, func(o *s3.Options) {
			o.APIOptions = append(o.APIOptions, apiOpts...)
		})
		return err
	})
	if err != nil {
		return 0, err
	}
	if resp.ContentLength == nil {
		return 0, fmt.Errorf(
			"s3parquet: HEAD %s returned no ContentLength", key)
	}
	return *resp.ContentLength, nil
}

// getRange issues a ranged GET for bytes [start, end) of key.
// Returns the body bytes. Used as the ReadAt transport for the
// row-group-filtered parquet reader.
func (t S3Target) getRange(
	ctx context.Context, key string, start, end int64,
	opts ...s3CallOpt,
) ([]byte, error) {
	if end <= start {
		return nil, nil
	}
	callOpts := applyS3CallOpts(opts)
	apiOpts := apiOptionsForOpts(callOpts)
	var data []byte
	err := retry(ctx, func() error {
		resp, err := t.S3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(t.Bucket),
			Key:    aws.String(key),
			Range:  aws.String(fmt.Sprintf("bytes=%d-%d", start, end-1)),
		}, func(o *s3.Options) {
			o.APIOptions = append(o.APIOptions, apiOpts...)
		})
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		data, err = io.ReadAll(resp.Body)
		return err
	})
	return data, err
}

// list returns a paginator over objects under a prefix. Callers
// iterate via HasMorePages + listPage; no in-memory accumulation
// here — large prefixes must stream.
func (t S3Target) list(prefix string) *s3.ListObjectsV2Paginator {
	return s3.NewListObjectsV2Paginator(
		t.S3Client, &s3.ListObjectsV2Input{
			Bucket: aws.String(t.Bucket),
			Prefix: aws.String(prefix),
		})
}

// listRange returns a paginator over objects in the lexical
// string range [startAfter, endInclusive]. Used by the
// idempotent-retry path: bounded LIST on the ref stream scoped
// to [now - MaxRetryAge, now] keeps the scan cost independent of
// stream length. StartAfter is exclusive on the S3 side, which
// is fine — the caller pads the lower bound into a tsMicros
// prefix that never matches a real ref key directly.
func (t S3Target) listRange(
	prefix, startAfter string,
) *s3.ListObjectsV2Paginator {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(t.Bucket),
		Prefix: aws.String(prefix),
	}
	if startAfter != "" {
		input.StartAfter = aws.String(startAfter)
	}
	return s3.NewListObjectsV2Paginator(t.S3Client, input)
}

// listPage fetches the next page from p, wrapping NextPage in
// the standard transient-error retry loop. A failed NextPage
// does not advance the paginator's continuation token, so
// retrying re-requests the same page cleanly.
func (t S3Target) listPage(
	ctx context.Context, p *s3.ListObjectsV2Paginator,
	opts ...s3CallOpt,
) (*s3.ListObjectsV2Output, error) {
	callOpts := applyS3CallOpts(opts)
	apiOpts := apiOptionsForOpts(callOpts)
	var out *s3.ListObjectsV2Output
	err := retry(ctx, func() error {
		var err error
		out, err = p.NextPage(ctx, func(o *s3.Options) {
			o.APIOptions = append(o.APIOptions, apiOpts...)
		})
		return err
	})
	return out, err
}

// httpStatusOf extracts the HTTP status code from a smithy-wrapped
// S3 error. Returns (0, false) when err carries no HTTP response
// (transport-level failure or non-SDK error).
func httpStatusOf(err error) (int, bool) {
	if err == nil {
		return 0, false
	}
	if respErr, ok := errors.AsType[*smithyhttp.ResponseError](err); ok {
		return respErr.HTTPStatusCode(), true
	}
	return 0, false
}

// probeOverwritePrevention tests whether the backend rejects
// re-PUTs to an existing key. Uses putIfAbsent for both the seed
// and the follow-up so the "capability detected" signal is the
// same ErrAlreadyExists sentinel in every case — including the
// one where scratch persists from a prior run with deleteScratch
// false, which a plain-PUT seed would fail outright on StorageGRID
// deployments with a deny-overwrite policy scoped to _probe/*.
//
// Two sequential putIfAbsent calls, short-circuiting as soon as
// capability is proven:
//
//   - Call 1 returns ErrAlreadyExists: scratch already existed and
//     the backend rejected our overwrite → capability confirmed,
//     no call 2 needed. Typical on restart when deleteScratch was
//     false.
//   - Call 1 succeeds: scratch was absent, this PUT created it.
//     Call 2 attempts a second write to the same key; if the
//     backend rejects it → capability confirmed. If call 2 also
//     succeeds → no overwrite prevention, writer falls back to
//     HEAD-before-PUT.
//
// On StorageGRID the If-None-Match header is ignored; the bucket
// policy denying s3:PutOverwriteObject produces the 403 that
// putIfAbsent maps to ErrAlreadyExists via the follow-up HEAD. On
// AWS / MinIO the header itself produces a 412. Both collapse to
// the same sentinel, so this probe doesn't need to distinguish
// between the two mechanisms.
//
// deleteScratch controls whether the scratch object is removed
// after detection. Set to false on deployments where the writer
// lacks DELETE permission on {Prefix}/_probe/ — the scratch stays
// at the stable key, next restart's call 1 trivially detects the
// capability.
//
// Invoked from NewWriter under a 10s deadline. Any non-
// ErrAlreadyExists error from either call surfaces verbatim so a
// misconfigured endpoint fails construction rather than silently
// mis-classifying the backend.
func (t S3Target) probeOverwritePrevention(
	ctx context.Context, deleteScratch bool,
	opts ...s3CallOpt,
) (bool, error) {
	key := t.Prefix + "/_probe/overwrite-prevention"
	body := []byte("s3store probe — safe to delete")

	firstErr := t.putIfAbsent(
		ctx, key, body, "text/plain", nil, opts...)
	overwritePreventionActive := errors.Is(firstErr, ErrAlreadyExists)
	if !overwritePreventionActive {
		if firstErr != nil {
			return false, fmt.Errorf(
				"s3parquet: probe seed PUT %s: %w", key, firstErr)
		}
		// Seed succeeded (scratch was absent). A second putIfAbsent
		// decides capability: rejection → overwrite prevention
		// active; silent success → not active.
		secondErr := t.putIfAbsent(
			ctx, key, body, "text/plain", nil, opts...)
		overwritePreventionActive = errors.Is(
			secondErr, ErrAlreadyExists)
		if !overwritePreventionActive && secondErr != nil {
			return false, fmt.Errorf(
				"s3parquet: probe PUT %s: %w", key, secondErr)
		}
	}

	if deleteScratch {
		if err := t.del(ctx, key); err != nil {
			// Probe-scratch delete failing is annoying but not
			// fatal — capability is already detected. Surface
			// via the returned error so the caller sees it once
			// at NewWriter, not on every write.
			return overwritePreventionActive, fmt.Errorf(
				"s3parquet: probe scratch cleanup %s: %w "+
					"(capability detected: %v)",
				key, err, overwritePreventionActive)
		}
	}
	return overwritePreventionActive, nil
}
