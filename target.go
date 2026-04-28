package s3store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"go.opentelemetry.io/otel/metric"
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

// ErrAlreadyExists is the sentinel returned by putIfAbsent when
// the target key is already present at the destination. Phase 4's
// per-attempt-paths design no longer routes data PUTs through
// putIfAbsent (every attempt id is unique by construction), so
// the write path doesn't observe this sentinel today; callers
// that build their own conditional PUTs against an S3Target may
// still encounter it.
var ErrAlreadyExists = errors.New("s3store: object already exists")

// consistencyHeader is the HTTP header name routed through every
// correctness-critical S3 call when ConsistencyControl is set.
// NetApp StorageGRID interprets the value to override the
// bucket's default consistency; AWS S3 and MinIO ignore the
// header entirely.
const consistencyHeader = "Consistency-Control"

// consistencyAPIOpts returns the SDK APIOptions slice that
// installs the Consistency-Control header for an S3 call. Empty
// level → nil → no header, which is the correct behaviour on
// AWS S3 / MinIO; on NetApp StorageGRID the target's configured
// ConsistencyLevel is sent on every routed call.
//
// Registered at the Build step (after Serialize, before Finalize)
// so it runs once per operation regardless of retries, and sees
// the assembled smithyhttp.Request.
func consistencyAPIOpts(level ConsistencyLevel) []func(*middleware.Stack) error {
	if level == "" {
		return nil
	}
	value := string(level)
	return []func(*middleware.Stack) error{
		func(stack *middleware.Stack) error {
			return stack.Build.Add(middleware.BuildMiddlewareFunc(
				"s3parquet.addHeader."+consistencyHeader,
				func(
					ctx context.Context, in middleware.BuildInput,
					next middleware.BuildHandler,
				) (middleware.BuildOutput, middleware.Metadata, error) {
					if req, ok := in.Request.(*smithyhttp.Request); ok {
						req.Header.Set(consistencyHeader, value)
					}
					return next.HandleBuild(ctx, in)
				}), middleware.After)
		},
	}
}

// S3TargetConfig is the user-facing config for an s3parquet
// dataset — pure data, struct-literal-friendly. Convert to a
// live S3Target via NewS3Target before passing to a Writer/Reader/
// ProjectionReader/BackfillProjection.
//
// Embedded indirectly via WriterConfig.Target / ReaderConfig.Target
// (which carry an S3Target — the live form) so the four S3-wiring
// fields plus knobs live in exactly one place. Surfaced on
// Writer/Reader/Store via .Target() so read-only tools
// (NewProjectionReader, BackfillProjection) can address the same dataset
// without carrying T through their call graph.
type S3TargetConfig struct {
	// Bucket is the S3 bucket name.
	Bucket string

	// Prefix is the key prefix under which data/ref/projection files
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

	// MaxInflightRequests caps the number of S3 requests a single
	// constructed S3Target may have outstanding at once. Enforced
	// by a semaphore inside S3Target — every PUT/GET/HEAD/LIST
	// acquires one slot before issuing and releases on completion,
	// so the cap holds across every fan-out axis (partitions,
	// files, patterns, markers) without per-axis tuning.
	//
	// Zero → default (32). The cap is per S3Target: one Writer +
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

	// ConsistencyControl sets the Consistency-Control HTTP header
	// applied to every correctness-critical S3 operation routed
	// through this target — data PUTs (per-attempt-path, never
	// overwriting), ref PUTs, token-commit PUTs, projection
	// marker PUTs, data / config GETs, the upfront-dedup HEAD on
	// `<token>.commit`, and every LIST (partition LIST on the
	// read path, projection-marker LIST in ProjectionReader.Lookup,
	// ref-stream LIST in Poll/PollRecords/ReadRangeIter).
	//
	// Zero value (ConsistencyDefault) is substituted at
	// construction with ConsistencyStrongGlobal — the safe
	// multi-site choice that lets sequential same-token
	// retries observe a prior token-commit overwrite without
	// surfacing as a redundant PUT. Single-site deployments can
	// downgrade to ConsistencyStrongSite explicitly when the
	// per-call cost matters. AWS S3 and MinIO ignore the header
	// entirely; the substitution is a no-op for those backends.
	// See the README's "Consistency levels × S3 operations"
	// section for the full matrix.
	//
	// Setting the level on the target rather than on the Writer /
	// Reader / Projection configs enforces NetApp's "same
	// consistency for paired operations" rule by construction:
	// every operation routed through this target uses one and the
	// same value.
	ConsistencyControl ConsistencyLevel

	// MeterProvider, when set, supplies the OTel meter used to
	// record S3 op latencies, library method durations, semaphore
	// wait/inflight depth, fan-out spans, and bytes/records/files
	// counters. Zero value falls back to otel.GetMeterProvider() —
	// users who configure OTel globally pick up metrics without
	// touching this field; users who don't get the OTel global
	// no-op (zero overhead).
	//
	// Bucket and Prefix from this config plus ConsistencyControl
	// (when non-empty) are baked into every observation as
	// constant attributes (s3store.bucket / s3store.prefix /
	// s3store.consistency_level), so multi-Target deployments
	// can be distinguished without per-call overhead.
	MeterProvider metric.MeterProvider
}

// commitTimeoutConfigKey is the object key under which the
// dataset's CommitTimeout is persisted, relative to the target's
// Prefix. NewS3Target GETs this object once at construction; the
// body is a Go time.Duration string ("2s", "5s", ...). With the
// timeliness check dropped, CommitTimeout is now a writer-local
// elapsed bound: writes whose elapsed time from refMicroTs
// (captured just before the ref PUT) to token-commit completion
// exceeds it increment s3store.write.commit_after_timeout (the
// commit still lands; the metric flags that the SettleWindow
// tuned for this value may not yet have included the write in
// the stream window). The reader's SettleWindow remains derived
// from CommitTimeout + MaxClockSkew so the writer's expected
// envelope and the reader's emit cutoff stay paired by
// construction. Pre-ref work (parquet encoding, marker PUTs,
// data PUT — the last scaling with payload size) is excluded
// from the budget: only the ref-LIST-visible → token-commit-
// visible interval can put the SettleWindow contract at risk.
const commitTimeoutConfigKey = "_config/commit-timeout"

// maxClockSkewConfigKey is the object key under which the
// dataset's MaxClockSkew is persisted, relative to the target's
// Prefix. NewS3Target GETs this object once at construction; the
// body is a Go time.Duration string ("0s", "500ms", "5s", ...). It
// encodes the operator's assumed bound on writer↔reader
// wall-clock divergence (refMicroTs in the ref filename is now
// writer-stamped, so this skew is the one the reader's refCutoff
// has to absorb via the derived SettleWindow). The writer doesn't
// read it.
const maxClockSkewConfigKey = "_config/max-clock-skew"

// CommitTimeoutFloor is the lower bound enforced by
// loadDurationConfig: values strictly less than this are rejected
// with a "below the floor" error. Set to 1 ms — strictly positive
// (zero is rejected as "would cause every write to exceed the
// timeout") and small enough that test deployments can pick
// sub-second values without further plumbing. Production
// deployments should pick a value well above
// CommitTimeoutAdvisory: loadTimingConfig emits a slog.Warn
// when the configured value is below the advisory floor (the
// retry envelope of the ref-PUT + token-commit-PUT pair).
//
// The historical 1 s floor existed because HTTP-date
// `Last-Modified` is second-precision and the dropped timeliness
// check could not resolve sub-second gaps; with LastModified out
// of the protocol, the writer's local elapsed bound is honest at
// any strictly positive value.
const CommitTimeoutFloor = time.Millisecond

// CommitTimeoutAdvisory is the soft floor below which
// loadTimingConfig emits a slog.Warn at construction. It bounds
// the retry envelope for the two PUTs that participate in the
// SettleWindow contract (ref + token-commit): each PUT can retry
// up to retryMaxAttempts (4) with backoffs 200ms / 400ms / 800ms
// (sum 1.4 s sleep per call), so two PUTs in worst case spend
// ~2.8 s in retry sleep alone, plus actual request time. 6 s
// gives ~2× safety on that retry envelope. Operators on
// tightly-clocked dev/test deployments can set CommitTimeout
// below this and ignore the warning; production deployments
// should size CommitTimeout above the advisory so transient S3
// retries cannot cause the SettleWindow contract to be missed.
const CommitTimeoutAdvisory = 6 * time.Second

// MaxClockSkewFloor is the minimum MaxClockSkew value the library
// accepts. Zero is a valid claim on tightly-clocked deployments
// (NTP-synced nodes typically run within milliseconds of each
// other); negative would be incoherent.
const MaxClockSkewFloor = time.Duration(0)

// EffectiveMaxInflightRequests returns the configured
// MaxInflightRequests, or 32 when unset. 32 utilises typical S3
// backends (~50 ms request latency → ~640 req/s sustained per
// Target) while staying comfortably below per-project concurrency
// caps published by managed S3 vendors (typically several hundred
// concurrent operations). Tune lower for small MinIO setups or
// very large parquet files (memory cost = 32 × largest parquet
// body).
func (c S3TargetConfig) EffectiveMaxInflightRequests() int {
	if c.MaxInflightRequests > 0 {
		return c.MaxInflightRequests
	}
	return 32
}

// Validate runs the full check for constructors that operate on
// partitioned data: Bucket, Prefix, S3Client, PartitionKeyParts.
// Used by NewWriter, NewReader, BackfillProjection — anything that
// reads/writes data files keyed by partition.
func (c S3TargetConfig) Validate() error {
	if err := c.ValidateLookup(); err != nil {
		return err
	}
	return validatePartitionKeyParts(c.PartitionKeyParts)
}

// ValidateLookup is the reduced check for constructors that
// only LIST / GET / PUT under a known prefix (no partition-key
// predicates): Bucket, Prefix, S3Client. Used by NewProjectionReader
// — Lookup walks the <Prefix>/_projection/<name>/ subtree, which is
// keyed by the projection's own Columns, not the config's
// PartitionKeyParts. A read-only analytics service can pass a
// minimally-populated S3TargetConfig and still build a working
// ProjectionReader.
func (c S3TargetConfig) ValidateLookup() error {
	if c.Bucket == "" {
		return fmt.Errorf("s3store: Bucket is required")
	}
	if c.Prefix == "" {
		return fmt.Errorf("s3store: Prefix is required")
	}
	if c.S3Client == nil {
		return fmt.Errorf("s3store: S3Client is required")
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
	cfg           S3TargetConfig
	commitTimeout time.Duration
	maxClockSkew  time.Duration
	sem           chan struct{}
	metrics       *metrics
}

// NewS3Target constructs a live S3Target from config. Calls
// cfg.ValidateLookup (Bucket, Prefix, S3Client) up-front so the
// GETs that follow have the wiring they need, then GETs the
// persisted timing-config objects at <Prefix>/_config/commit-timeout
// and <Prefix>/_config/max-clock-skew, parses each as a Go
// time.Duration string, and rejects values below the configured
// floors (CommitTimeoutFloor = 0, MaxClockSkewFloor = 0 — only
// negatives are nonsensical) before stamping the values (and the
// derived SettleWindow) on the Target. Construction fails when
// ValidateLookup fails or when either object is missing,
// unparseable, or negative — operators must seed the dataset's
// prefix before any process can construct a Target against it
// (see README's "Initializing a new dataset").
//
// Does not call Validate (PartitionKeyParts) — that's a Writer /
// Reader concern and is checked by NewWriter / NewReader, not by
// every Target consumer (NewProjectionReader is read-only and
// doesn't need PartitionKeyParts).
//
// Logs a warning when ConsistencyControl is non-empty but doesn't
// match a named ConsistencyLevel constant — typo guard with no
// effect on behaviour (the value is still sent verbatim).
func NewS3Target(ctx context.Context, cfg S3TargetConfig) (S3Target, error) {
	if err := cfg.ValidateLookup(); err != nil {
		return S3Target{}, err
	}
	t := newS3TargetSkipConfig(cfg)
	commitTimeout, maxClockSkew, err := loadTimingConfig(ctx, t)
	if err != nil {
		return S3Target{}, err
	}
	t.commitTimeout = commitTimeout
	t.maxClockSkew = maxClockSkew
	return t, nil
}

// newS3TargetSkipConfig allocates an S3Target without GETing the
// persisted timing-config objects. Used internally by NewS3Target
// (which then loads the values) and by tests that don't want a
// live S3 dependency. Stamps the configured floors as the
// resolved timing values so code reading them gets a non-negative
// result before timing config is loaded.
//
// Substitutes the zero ConsistencyControl with the library's
// safe-multi-site default (strong-global) so token-commit
// overwrites converge under sequential same-token retries
// without operator intervention. Single-site deployments can
// pick strong-site explicitly when per-call cost matters.
func newS3TargetSkipConfig(cfg S3TargetConfig) S3Target {
	if cfg.ConsistencyControl == "" {
		cfg.ConsistencyControl = ConsistencyStrongGlobal
	}
	warnIfUnknownConsistency(cfg.ConsistencyControl, "S3TargetConfig")
	return S3Target{
		cfg:           cfg,
		commitTimeout: CommitTimeoutFloor,
		maxClockSkew:  MaxClockSkewFloor,
		sem:           make(chan struct{}, cfg.EffectiveMaxInflightRequests()),
		metrics: newMetrics(
			cfg.MeterProvider, cfg.Bucket, cfg.Prefix,
			cfg.ConsistencyControl),
	}
}

// loadTimingConfig GETs <Prefix>/_config/commit-timeout and
// <Prefix>/_config/max-clock-skew, parses each body as a Go
// time.Duration string, and rejects values below their floors.
// CommitTimeoutFloor (1 ms) is the strict-positive floor that
// keeps zero out of the protocol; CommitTimeoutAdvisory (6 s) is
// a soft floor — values below it pass construction but emit a
// slog.Warn so operators see when a configured CommitTimeout is
// below the retry envelope of the two SettleWindow-relevant PUTs
// (ref + token-commit). Surfaces a hint at the operator's
// seeding step when either object is missing.
func loadTimingConfig(
	ctx context.Context, t S3Target,
) (commitTimeout, maxClockSkew time.Duration, _ error) {
	commitTimeout, err := loadDurationConfig(
		ctx, t, commitTimeoutConfigKey, CommitTimeoutFloor)
	if err != nil {
		return 0, 0, err
	}
	if commitTimeout < CommitTimeoutAdvisory {
		slog.Warn(
			"s3store: CommitTimeout is below the advisory floor — "+
				"transient S3 retries on the ref-PUT or "+
				"token-commit-PUT could exceed CommitTimeout and "+
				"cause stream readers to skip the write; the "+
				"advisory floor leaves ~2× safety on the worst-"+
				"case retry envelope of the two PUTs (~2.8 s of "+
				"backoff sleep across 4 attempts each). Acceptable "+
				"for tightly-clocked dev/test deployments; "+
				"production should size above the advisory.",
			"bucket", t.cfg.Bucket,
			"prefix", t.cfg.Prefix,
			"commitTimeout", commitTimeout,
			"advisoryFloor", CommitTimeoutAdvisory)
	}
	maxClockSkew, err = loadDurationConfig(
		ctx, t, maxClockSkewConfigKey, MaxClockSkewFloor)
	if err != nil {
		return 0, 0, err
	}
	return commitTimeout, maxClockSkew, nil
}

// loadDurationConfig GETs <Prefix>/<key>, parses the body as a Go
// time.Duration string, and rejects values below floor. Returns a
// hint at the seeding step when the object is missing.
func loadDurationConfig(
	ctx context.Context, t S3Target,
	relKey string, floor time.Duration,
) (time.Duration, error) {
	key := t.cfg.Prefix + "/" + relKey
	body, err := t.get(ctx, key)
	if err != nil {
		if _, notFound := errors.AsType[*s3types.NoSuchKey](err); notFound {
			return 0, fmt.Errorf(
				"s3store: %s/%s missing — seed the dataset's "+
					"timing config before constructing a Target "+
					"(see README \"Initializing a new dataset\")",
				t.cfg.Bucket, key)
		}
		return 0, fmt.Errorf(
			"s3store: get %s/%s: %w", t.cfg.Bucket, key, err)
	}
	raw := strings.TrimSpace(string(body))
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf(
			"s3store: %s/%s body %q: parse as time.Duration: %w",
			t.cfg.Bucket, key, raw, err)
	}
	if value < floor {
		return 0, fmt.Errorf(
			"s3store: %s/%s body %q resolves to %s, below the "+
				"%s floor",
			t.cfg.Bucket, key, raw, value, floor)
	}
	return value, nil
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

// ConsistencyControl returns the configured Consistency-Control
// header value applied to every routed S3 call.
func (t S3Target) ConsistencyControl() ConsistencyLevel {
	return t.cfg.ConsistencyControl
}

// CommitTimeout returns the value resolved at construction time
// from <Prefix>/_config/commit-timeout. Pure accessor — no I/O.
// Bounds the writer's ref-PUT-to-token-commit-completion budget
// for one Write: past CommitTimeout (measured from refMicroTs,
// captured just before the ref PUT, to the moment the
// token-commit PUT returns), the s3store.write.commit_after_timeout
// counter increments and Write returns an error (the commit still
// lands; the metric flags that the reader's SettleWindow tuned
// for this CommitTimeout may not yet have included this write in
// the stream window). Pre-ref work — parquet encoding, marker
// PUTs, data PUT — is deliberately outside the budget; only the
// ref-LIST-visible → token-commit-visible window can put the
// SettleWindow contract at risk.
func (t S3Target) CommitTimeout() time.Duration {
	return t.commitTimeout
}

// MaxClockSkew returns the value resolved at construction time
// from <Prefix>/_config/max-clock-skew. Pure accessor — no I/O.
// Operator's assumed bound on writer↔reader wall-clock divergence
// (refMicroTs in the ref filename is writer-stamped, so this is
// the skew SettleWindow has to absorb). Consumed by the reader's
// refCutoff via SettleWindow.
func (t S3Target) MaxClockSkew() time.Duration {
	return t.maxClockSkew
}

// SettleWindow returns the derived sum CommitTimeout + MaxClockSkew.
// Used by Poll's `refCutoff = now - SettleWindow`. Sized so the
// cutoff cannot overtake refs whose token-commit is still being
// written: CommitTimeout bounds the writer's ref-PUT +
// token-commit-PUT envelope (measured from refMicroTs onward;
// pre-ref work is outside the budget by design), MaxClockSkew
// bounds the writer↔reader wall-clock divergence applied to the
// writer-stamped refMicroTs. Pure accessor — no I/O.
func (t S3Target) SettleWindow() time.Duration {
	return t.commitTimeout + t.maxClockSkew
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
	sc := t.metrics.semaphoreScope(ctx)
	defer sc.cleanup()
	select {
	case t.sem <- struct{}{}:
		sc.recordAcquired()
		return nil
	case <-ctx.Done():
		sc.recordCanceled()
		return ctx.Err()
	}
}

// release returns a slot to the semaphore. Paired with acquire.
func (t S3Target) release() {
	<-t.sem
	t.metrics.recordReleased()
}

// get downloads a single object into memory. Used by the read
// path (Read, PollRecords) and by BackfillProjection when scanning
// historical parquet data. Carries the target's
// ConsistencyControl on every call.
func (t S3Target) get(
	ctx context.Context, key string,
) (_ []byte, err error) {
	scope := t.metrics.s3OpScope(ctx, s3OpGet)
	defer scope.end(&err)
	if err = t.acquire(ctx); err != nil {
		return nil, err
	}
	defer t.release()
	apiOpts := consistencyAPIOpts(t.cfg.ConsistencyControl)
	var data []byte
	err = retry(ctx, func() error {
		scope.incAttempts()
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
	if err == nil {
		scope.setRespBytes(int64(len(data)))
	}
	return data, err
}

// put uploads data under key. Used by the write path (parquet +
// ref + markers) and by BackfillProjection for retroactive marker
// emission. Carries the target's ConsistencyControl on every
// call.
func (t S3Target) put(
	ctx context.Context, key string, data []byte, contentType string,
) (err error) {
	return t.putWithMeta(ctx, key, data, contentType, nil)
}

// putWithMeta uploads data under key, attaching meta as
// x-amz-meta-<k> user-metadata headers. Used by the commit-marker
// PUT to stamp the data file's server-stamped LastModified into
// the marker so the change-stream read path can apply the
// timeliness check with a single per-ref HEAD on the marker
// (no second HEAD on the data file). Carries the target's
// ConsistencyControl on every call.
//
// meta=nil is equivalent to put — no metadata sent.
func (t S3Target) putWithMeta(
	ctx context.Context, key string, data []byte,
	contentType string, meta map[string]string,
) (err error) {
	scope := t.metrics.s3OpScope(ctx, s3OpPut)
	scope.setReqBytes(int64(len(data)))
	defer scope.end(&err)
	if err = t.acquire(ctx); err != nil {
		return err
	}
	defer t.release()
	apiOpts := consistencyAPIOpts(t.cfg.ConsistencyControl)
	err = retry(ctx, func() error {
		scope.incAttempts()
		_, err := t.cfg.S3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(t.cfg.Bucket),
			Key:         aws.String(key),
			Body:        bytes.NewReader(data),
			ContentType: aws.String(contentType),
			Metadata:    meta,
		}, func(o *s3.Options) {
			o.APIOptions = append(o.APIOptions, apiOpts...)
		})
		return err
	})
	return err
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
) (err error) {
	scope := t.metrics.s3OpScope(ctx, s3OpPut)
	scope.setReqBytes(int64(len(data)))
	// Safety-net defer for early-return paths (acquire failure).
	// Idempotent: a no-op once we manually call end() below after
	// the PUT, so the disambiguation HEAD on the 403 branch
	// doesn't inflate the PUT duration histogram.
	defer scope.end(&err)
	if err = t.acquire(ctx); err != nil {
		return err
	}
	defer t.release()
	apiOpts := consistencyAPIOpts(t.cfg.ConsistencyControl)
	putErr := retry(ctx, func() error {
		scope.incAttempts()
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
	// End the PUT scope here with the raw S3 outcome — any
	// follow-up HEAD on the 403 branch must not pollute the PUT
	// duration histogram. The deferred end() above becomes a no-op.
	scope.end(&putErr)
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
			ok, headErr := t.existsLocked(ctx, key)
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

// head HEADs key and returns its server-stamped LastModified plus
// user-defined metadata (the x-amz-meta-* headers, surfaced by the
// SDK with lowercase keys). Used by the write path's post-PUT
// verification (post-data HEAD captures data.LM as refTsMicros;
// post-marker HEAD captures marker.LM and reads the dataLM
// metadata for the timeliness check) and by the change-stream
// read path (per-ref HEAD on the marker reads marker.LM + dataLM
// metadata). Carries the target's ConsistencyControl so the HEAD
// pairs with its preceding PUT under NetApp's "same level" rule.
func (t S3Target) head(
	ctx context.Context, key string,
) (lastModified time.Time, meta map[string]string, err error) {
	scope := t.metrics.s3OpScope(ctx, s3OpHead)
	defer scope.end(&err)
	if err = t.acquire(ctx); err != nil {
		return time.Time{}, nil, err
	}
	defer t.release()
	apiOpts := consistencyAPIOpts(t.cfg.ConsistencyControl)
	err = retry(ctx, func() error {
		scope.incAttempts()
		resp, err := t.cfg.S3Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(t.cfg.Bucket),
			Key:    aws.String(key),
		}, func(o *s3.Options) {
			o.APIOptions = append(o.APIOptions, apiOpts...)
		})
		if err != nil {
			return err
		}
		lastModified = aws.ToTime(resp.LastModified)
		meta = resp.Metadata
		return nil
	})
	return lastModified, meta, err
}

// existsLocked is the slot-already-held variant of exists. Called
// from putIfAbsent's 403 branch where the caller is already inside
// an acquire/release pair — re-acquiring would deadlock when the
// semaphore is sized to 1 (or saturated by the writer's other
// concurrent calls). Carries the target's ConsistencyControl so
// the HEAD pairs with the PUT under NetApp's "same level" rule.
func (t S3Target) existsLocked(
	ctx context.Context, key string,
) (bool, error) {
	scope := t.metrics.s3OpScope(ctx, s3OpHead)
	// s3Err is the raw outcome of the HEAD round-trip — including
	// NotFound, which is a real S3 4xx response. The s3.head
	// instrument records what S3 actually did (latency + outcome
	// breakdown), so a 404 lands as outcome=error/error.type=not_found
	// even though the caller-facing return is (false, nil). Splitting
	// the metric-level truth from the caller-level abstraction needs
	// a separate variable — a named-return err would be overwritten
	// by the explicit `return false, nil` below before the defer runs.
	var s3Err error
	defer func() { scope.end(&s3Err) }()
	apiOpts := consistencyAPIOpts(t.cfg.ConsistencyControl)
	s3Err = retry(ctx, func() error {
		scope.incAttempts()
		_, err := t.cfg.S3Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(t.cfg.Bucket),
			Key:    aws.String(key),
		}, func(o *s3.Options) {
			o.APIOptions = append(o.APIOptions, apiOpts...)
		})
		return err
	})
	if s3Err == nil {
		return true, nil
	}
	if _, ok := errors.AsType[*s3types.NotFound](s3Err); ok {
		return false, nil
	}
	return false, s3Err
}

// listPage fetches the next page from p, wrapping NextPage in
// the standard transient-error retry loop. Acquires a semaphore
// slot for the duration of the fetch. A failed NextPage does not
// advance the paginator's continuation token, so retrying
// re-requests the same page cleanly.
//
// Internal building block of listEach — no current direct caller
// outside this file.
func (t S3Target) listPage(
	ctx context.Context, p *s3.ListObjectsV2Paginator,
) (_ *s3.ListObjectsV2Output, err error) {
	scope := t.metrics.s3OpScope(ctx, s3OpList)
	defer scope.end(&err)
	if err = t.acquire(ctx); err != nil {
		return nil, err
	}
	defer t.release()
	apiOpts := consistencyAPIOpts(t.cfg.ConsistencyControl)
	var out *s3.ListObjectsV2Output
	err = retry(ctx, func() error {
		scope.incAttempts()
		var err error
		out, err = p.NextPage(ctx, func(o *s3.Options) {
			o.APIOptions = append(o.APIOptions, apiOpts...)
		})
		return err
	})
	return out, err
}

// listEach iterates objects under prefix in S3 lex order,
// invoking fn for each. fn returns cont=false to stop early; an
// fn error short-circuits and surfaces unwrapped to the caller.
// Each page fetch goes through listPage (semaphore + retry +
// Consistency-Control header).
//
// startAfter is the S3 StartAfter (exclusive lower bound; ""
// starts at the prefix's lex head). pageSize caps MaxKeys per
// request (0 leaves it at the S3 default of 1000); useful when
// the caller knows it only needs a few hundred objects so a
// single page round-trip suffices.
//
// Single LIST primitive across the library — partition LIST,
// projection-marker LIST, ref-stream LIST (Poll), and the
// upfront-LIST dedup gate under {partition}/{token}- on
// idempotent writes all funnel through here so the
// "semaphore + retry + consistency header" wrapping is
// implemented once. Carries the target's ConsistencyControl on
// every page fetch.
func (t S3Target) listEach(
	ctx context.Context,
	prefix, startAfter string,
	pageSize int32,
	fn func(s3types.Object) (cont bool, err error),
) error {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(t.cfg.Bucket),
		Prefix: aws.String(prefix),
	}
	if startAfter != "" {
		input.StartAfter = aws.String(startAfter)
	}
	if pageSize > 0 {
		input.MaxKeys = aws.Int32(pageSize)
	}
	paginator := s3.NewListObjectsV2Paginator(t.cfg.S3Client, input)
	for paginator.HasMorePages() {
		page, err := t.listPage(ctx, paginator)
		if err != nil {
			return err
		}
		for _, obj := range page.Contents {
			cont, err := fn(obj)
			if err != nil {
				return err
			}
			if !cont {
				return nil
			}
		}
	}
	return nil
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
