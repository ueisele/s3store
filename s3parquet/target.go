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

// ErrAlreadyExists is the sentinel returned by putIfAbsent when
// the target key is already present at the destination. Signals
// the write path that the current attempt is a retry of a
// previously-persisted logical write; callers scope-LIST the ref
// stream to decide whether the ref also needs re-emission.
var ErrAlreadyExists = errors.New("s3parquet: object already exists")

// consistencyAPIOpts returns the SDK APIOptions slice that
// installs the Consistency-Control header for an S3 call. Empty
// level → nil → no header, which is the correct behaviour on
// AWS S3 / MinIO; on NetApp StorageGRID callers pass the
// configured ConsistencyLevel through their target methods.
func consistencyAPIOpts(level ConsistencyLevel) []func(*middleware.Stack) error {
	if level == "" {
		return nil
	}
	return []func(*middleware.Stack) error{
		core.AddHeaderMiddleware(
			"Consistency-Control", string(level)),
	}
}

// S3TargetConfig is the user-facing config for an s3parquet
// dataset — pure data, struct-literal-friendly. Convert to a
// live S3Target via NewS3Target before passing to a Writer/Reader/
// Index/BackfillIndex.
//
// Embedded indirectly via WriterConfig.Target / ReaderConfig.Target
// (which carry an S3Target — the live form) so the four S3-wiring
// fields plus knobs live in exactly one place. Surfaced on
// Writer/Reader/Store via .Target() so read-only tools (NewIndex,
// BackfillIndex) can address the same dataset without carrying T
// through their call graph.
type S3TargetConfig struct {
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

	// SettleWindow is how far behind the live tip Poll and
	// PollRecords read, and the total budget the ref PUT must fit
	// inside (the ref-PUT timeout is SettleWindow / 2; see
	// refPutBudget). Keeps readers consistent with near-tip writers
	// whose refs may not yet be visible in S3 LIST.
	//
	// Does not apply to Index.Lookup: marker visibility is
	// delegated to the storage layer via ConsistencyControl, so a
	// read-after-write Lookup works without any settle delay on
	// strong-consistent backends.
	//
	// Default (zero value): 5s. Zero is treated as "use library
	// default", not "disable settle" — Poll correctness and
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

	// MaxInflightRequests caps the number of S3 requests a single
	// constructed S3Target may have outstanding at once. Enforced
	// by a semaphore inside S3Target — every PUT/GET/HEAD/LIST
	// acquires one slot before issuing and releases on completion,
	// so the cap holds across every fan-out axis (partitions,
	// files, patterns, markers) without per-axis tuning.
	//
	// Zero → default (8). The cap is per S3Target: one Writer +
	// one Reader sharing the same constructed S3Target share the
	// cap; two Targets do not.
	//
	// The AWS SDK v2's default HTTP transport leaves
	// MaxConnsPerHost unlimited (Go default 0) and sets
	// MaxIdleConnsPerHost to 100, so this library cap is what
	// bounds parallelism for stock-configured clients — no
	// transport tuning is needed at the defaults. Only if you've
	// explicitly set a non-zero MaxConnsPerHost on your
	// *s3.Client's transport does it need to be >=
	// MaxInflightRequests, otherwise excess requests queue at the
	// transport layer instead of running in parallel.
	MaxInflightRequests int
}

// EffectiveSettleWindow returns the configured SettleWindow, or
// 5s when it's unset (zero value). Zero is deliberately mapped
// to the default rather than "disabled" — a zero window would
// collapse both Poll's cutoff and the ref-PUT budget, which has
// no valid use case (consumers and writers would both skip
// their consistency safeguards).
func (c S3TargetConfig) EffectiveSettleWindow() time.Duration {
	if c.SettleWindow > 0 {
		return c.SettleWindow
	}
	return 5 * time.Second
}

// EffectiveMaxInflightRequests returns the configured
// MaxInflightRequests, or 8 when unset.
func (c S3TargetConfig) EffectiveMaxInflightRequests() int {
	if c.MaxInflightRequests > 0 {
		return c.MaxInflightRequests
	}
	return 8
}

// Validate runs the full check for constructors that operate on
// partitioned data: Bucket, Prefix, S3Client, PartitionKeyParts.
// Used by NewWriter, NewReader, BackfillIndex, and the s3sql
// reader — anything that reads/writes data files keyed by
// partition.
func (c S3TargetConfig) Validate() error {
	if err := c.ValidateLookup(); err != nil {
		return err
	}
	return core.ValidatePartitionKeyParts(c.PartitionKeyParts)
}

// ValidateLookup is the reduced check for constructors that
// only LIST / GET / PUT under a known prefix (no partition-key
// predicates): Bucket, Prefix, S3Client. Used by NewIndex —
// Lookup walks the <Prefix>/_index/<name>/ subtree, which is
// keyed by the index's own Columns, not the config's
// PartitionKeyParts. A read-only analytics service can pass a
// minimally-populated S3TargetConfig and still build a working
// Index.
func (c S3TargetConfig) ValidateLookup() error {
	if c.Bucket == "" {
		return fmt.Errorf("s3parquet: Bucket is required")
	}
	if c.Prefix == "" {
		return fmt.Errorf("s3parquet: Prefix is required")
	}
	if c.S3Client == nil {
		return fmt.Errorf("s3parquet: S3Client is required")
	}
	return nil
}

// S3Target is the constructed live handle to an s3parquet
// dataset. Built once from an S3TargetConfig via NewS3Target;
// all S3 operations go through this type so the per-target
// MaxInflightRequests semaphore caps net in-flight requests
// regardless of fan-out axis.
//
// Pass the same S3Target value to WriterConfig.Target and
// ReaderConfig.Target so the Writer and Reader share the same
// semaphore. The struct is copied by value but every field is a
// reference type (chan, pointer, slice header) so copies share
// the underlying state.
//
// Fields are unexported and immutable after construction:
// callers read via the accessor methods. Re-construct with a
// fresh NewS3Target if you need to change MaxInflightRequests
// or any other field.
type S3Target struct {
	cfg S3TargetConfig
	sem chan struct{}
}

// NewS3Target constructs a live S3Target from config, allocating
// the shared in-flight semaphore. Performs no field validation —
// downstream constructors (NewWriter, NewReader, NewIndex,
// BackfillIndex) call Validate / ValidateLookup as appropriate.
func NewS3Target(cfg S3TargetConfig) S3Target {
	return S3Target{
		cfg: cfg,
		sem: make(chan struct{}, cfg.EffectiveMaxInflightRequests()),
	}
}

// Config returns a copy of the S3TargetConfig the target was
// built from. Use for introspection or passing to tools that
// expect the config form.
func (t S3Target) Config() S3TargetConfig { return t.cfg }

// Bucket returns the S3 bucket name.
func (t S3Target) Bucket() string { return t.cfg.Bucket }

// Prefix returns the dataset's key prefix.
func (t S3Target) Prefix() string { return t.cfg.Prefix }

// S3Client returns the configured AWS SDK v2 client.
func (t S3Target) S3Client() *s3.Client { return t.cfg.S3Client }

// PartitionKeyParts returns the configured Hive-partition keys.
func (t S3Target) PartitionKeyParts() []string { return t.cfg.PartitionKeyParts }

// DisableRefStream reports whether the dataset is configured to
// skip ref-stream emission.
func (t S3Target) DisableRefStream() bool { return t.cfg.DisableRefStream }

// EffectiveSettleWindow forwards to S3TargetConfig.EffectiveSettleWindow.
func (t S3Target) EffectiveSettleWindow() time.Duration {
	return t.cfg.EffectiveSettleWindow()
}

// EffectiveMaxInflightRequests forwards to
// S3TargetConfig.EffectiveMaxInflightRequests.
func (t S3Target) EffectiveMaxInflightRequests() int {
	return t.cfg.EffectiveMaxInflightRequests()
}

// Validate forwards to S3TargetConfig.Validate.
func (t S3Target) Validate() error { return t.cfg.Validate() }

// ValidateLookup forwards to S3TargetConfig.ValidateLookup.
func (t S3Target) ValidateLookup() error { return t.cfg.ValidateLookup() }

// acquire blocks until a semaphore slot is available or ctx is
// cancelled. Paired with release in defer. Every S3 method on
// S3Target acquires before issuing the request so net in-flight
// is capped at MaxInflightRequests regardless of how many
// goroutines call concurrently.
func (t S3Target) acquire(ctx context.Context) error {
	select {
	case t.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// release returns a slot to the semaphore. Paired with acquire.
func (t S3Target) release() { <-t.sem }

// get downloads a single object into memory. Used by the read
// path (Read, PollRecords) and by BackfillIndex when scanning
// historical parquet data.
func (t S3Target) get(
	ctx context.Context, key string, consistency ConsistencyLevel,
) ([]byte, error) {
	if err := t.acquire(ctx); err != nil {
		return nil, err
	}
	defer t.release()
	apiOpts := consistencyAPIOpts(consistency)
	var data []byte
	err := retry(ctx, func() error {
		resp, err := t.cfg.S3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(t.cfg.Bucket),
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
	consistency ConsistencyLevel,
) error {
	if err := t.acquire(ctx); err != nil {
		return err
	}
	defer t.release()
	apiOpts := consistencyAPIOpts(consistency)
	return retry(ctx, func() error {
		_, err := t.cfg.S3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(t.cfg.Bucket),
			Key:         aws.String(key),
			Body:        bytes.NewReader(data),
			ContentType: aws.String(contentType),
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
// when non-nil — used by the data PUT to stamp
// x-amz-meta-created-at so external tooling sees the writer's
// wall-clock alongside the in-file InsertedAtField column.
func (t S3Target) putIfAbsent(
	ctx context.Context, key string, data []byte,
	contentType string, meta map[string]string,
	consistency ConsistencyLevel,
) error {
	if err := t.acquire(ctx); err != nil {
		return err
	}
	defer t.release()
	apiOpts := consistencyAPIOpts(consistency)
	putErr := retry(ctx, func() error {
		_, err := t.cfg.S3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(t.cfg.Bucket),
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
			// The HEAD reuses the same consistency level so it
			// pairs with the PUT under NetApp's "same consistency
			// for paired operations" rule.
			//
			// existsLocked reads the same semaphore slot we hold
			// (we're inside the acquire/release pair) — calling
			// the public exists() would deadlock waiting for our
			// own slot.
			ok, headErr := t.existsLocked(ctx, key, consistency)
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

// existsLocked is the slot-already-held variant of exists. Called
// from putIfAbsent's 403 branch where the caller is already inside
// an acquire/release pair — re-acquiring would deadlock when the
// semaphore is sized to 1 (or saturated by the writer's other
// concurrent calls).
func (t S3Target) existsLocked(
	ctx context.Context, key string, consistency ConsistencyLevel,
) (bool, error) {
	apiOpts := consistencyAPIOpts(consistency)
	err := retry(ctx, func() error {
		_, err := t.cfg.S3Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(t.cfg.Bucket),
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
	if err := t.acquire(ctx); err != nil {
		return err
	}
	defer t.release()
	return retry(ctx, func() error {
		_, err := t.cfg.S3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(t.cfg.Bucket),
			Key:    aws.String(key),
		})
		return err
	})
}

// size returns the object's content length. Used by the
// row-group-filtered read path to size the io.ReaderAt parquet-
// go opens the file through.
func (t S3Target) size(
	ctx context.Context, key string, consistency ConsistencyLevel,
) (int64, error) {
	if err := t.acquire(ctx); err != nil {
		return 0, err
	}
	defer t.release()
	apiOpts := consistencyAPIOpts(consistency)
	var resp *s3.HeadObjectOutput
	err := retry(ctx, func() error {
		var err error
		resp, err = t.cfg.S3Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(t.cfg.Bucket),
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
	consistency ConsistencyLevel,
) ([]byte, error) {
	if end <= start {
		return nil, nil
	}
	if err := t.acquire(ctx); err != nil {
		return nil, err
	}
	defer t.release()
	apiOpts := consistencyAPIOpts(consistency)
	var data []byte
	err := retry(ctx, func() error {
		resp, err := t.cfg.S3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(t.cfg.Bucket),
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
// here — large prefixes must stream. The paginator itself does
// no I/O — listPage is what acquires the semaphore on each page
// fetch.
func (t S3Target) list(prefix string) *s3.ListObjectsV2Paginator {
	return s3.NewListObjectsV2Paginator(
		t.cfg.S3Client, &s3.ListObjectsV2Input{
			Bucket: aws.String(t.cfg.Bucket),
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
		Bucket: aws.String(t.cfg.Bucket),
		Prefix: aws.String(prefix),
	}
	if startAfter != "" {
		input.StartAfter = aws.String(startAfter)
	}
	return s3.NewListObjectsV2Paginator(t.cfg.S3Client, input)
}

// listPage fetches the next page from p, wrapping NextPage in
// the standard transient-error retry loop. Acquires a semaphore
// slot for the duration of the fetch. A failed NextPage does not
// advance the paginator's continuation token, so retrying
// re-requests the same page cleanly.
func (t S3Target) listPage(
	ctx context.Context, p *s3.ListObjectsV2Paginator,
	consistency ConsistencyLevel,
) (*s3.ListObjectsV2Output, error) {
	if err := t.acquire(ctx); err != nil {
		return nil, err
	}
	defer t.release()
	apiOpts := consistencyAPIOpts(consistency)
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
