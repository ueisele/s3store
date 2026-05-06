package s3store

import (
	"bytes"
	"context"
	"crypto/md5" //nolint:gosec // ETag integrity check, not a cryptographic primitive
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
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
// around every S3 call (put / get / head / list). The
// AWS SDK runs its own retryer under us; this is an outer layer
// that survives an exhausted SDK budget and keeps behaviour
// predictable when callers plug in aws.NopRetryer or a custom
// retryer. Budget is sized to keep flaky calls bounded — five
// attempts over 1.4-2.4 s of randomised backoff sleep — without
// blowing the SettleWindow contract for the two PUTs that
// participate in it (ref + token-commit, the orphan-creating
// pair). The retry count is the orphan-rate hedge: at typical
// per-attempt failure rates a single retry suffices, but
// sustained SlowDown bursts during the no-cancel commit window
// benefit from the wider envelope.
const retryMaxAttempts = 5 // 1 initial + 4 retries

// retryBackoffRange is the [min, max) sleep window for the i-th
// retry. Each retry samples uniformly within the range, which
// breaks correlation between concurrently failing callers: a
// burst of SlowDowns from the same prefix no longer translates
// into a synchronised retry storm against that same prefix on
// the next round. The schedule grows from 100-300 ms through
// 500-800 ms across the first three retries, then plateaus at
// 500-800 ms for the fourth — capping per-attempt sleep keeps
// the cumulative envelope manageable while still leaving each
// retry on its own random offset.
type retryBackoffRange struct {
	min, max time.Duration
}

// pick returns a uniformly-random duration in [min, max).
// Falls back to min when the range is empty (test stubs use
// {0, 0} to skip the sleep entirely).
func (r retryBackoffRange) pick() time.Duration {
	span := r.max - r.min
	if span <= 0 {
		return r.min
	}
	return r.min + time.Duration(rand.Int64N(int64(span)))
}

var retryBackoff = [retryMaxAttempts - 1]retryBackoffRange{
	{100 * time.Millisecond, 300 * time.Millisecond},
	{300 * time.Millisecond, 500 * time.Millisecond},
	{500 * time.Millisecond, 800 * time.Millisecond},
	{500 * time.Millisecond, 800 * time.Millisecond},
}

// retry runs fn up to retryMaxAttempts times on transient S3
// failures, sleeping retryBackoff[i].pick() before retry i+1
// (a uniformly-random duration drawn from the i-th window —
// see retryBackoffRange's jitter rationale). Returns as soon as
// fn succeeds, returns a non-retryable error, or ctx is
// cancelled.
//
// op labels the wrapping S3 operation ("get" / "put" / "head" /
// "list") for log lines; same string the caller's s3OpScope
// carries on the metric side.
//
// Transient errors that will be retried are logged at WARN. A
// transient error that succeeds on the next attempt is otherwise
// invisible to operators — the call returns nil and the only
// trace is s3store.s3.request.count{attempts!="1"}, which says
// "a retry happened" but not what actually failed. Logging the
// err on the masked attempts surfaces the underlying cause
// (timeout, 5xx body, ETag-integrity-check mismatch) so log
// search and dashboards agree on what's happening. The terminal
// transient error (final attempt failed transient too) is NOT
// logged here — the caller receives the error and handles
// surfacing.
func retry(
	ctx context.Context, op string, scope *s3OpScope,
	fn func() error,
) error {
	var err error
	for attempt := 0; attempt < retryMaxAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(retryBackoff[attempt-1].pick()):
			}
		}
		err = fn()
		if err == nil {
			return nil
		}
		if !isTransientS3Error(err) {
			return err
		}
		// Transient failure. Record it on s3TransientCount so
		// dashboards see masked retry causes (the call may still
		// succeed below, in which case s3Count's terminal
		// outcome=success would otherwise hide the cause). Fires
		// for both non-terminal and terminal transients — the
		// terminal one isn't double-counted because s3Count's
		// terminal observation classifies it once on its own
		// outcome=error label, while this metric counts the
		// underlying transient events directly.
		scope.recordTransient(err)
		// Log only when budget remains so the caller's terminal
		// error surface isn't duplicated by a log line; final-
		// attempt failures are the caller's to report.
		if attempt < retryMaxAttempts-1 {
			slog.Warn("s3store: transient error, retrying",
				"op", op,
				"attempt", attempt+1,
				"max_attempts", retryMaxAttempts,
				"err", err)
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
// MaterializedViewReader/BackfillMaterializedView.
//
// Embedded indirectly via WriterConfig.Target / ReaderConfig.Target
// (which carry an S3Target — the live form) so the four S3-wiring
// fields plus knobs live in exactly one place. Surfaced on
// Writer/Reader/Store via .Target() so read-only tools
// (NewMaterializedViewReader, BackfillMaterializedView) can address the same dataset
// without carrying T through their call graph.
type S3TargetConfig struct {
	// Bucket is the S3 bucket name.
	Bucket string

	// Prefix is the key prefix under which data/ref/matview files
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
	// overwriting), ref PUTs, token-commit PUTs, matview marker
	// PUTs, data / config GETs, the upfront-dedup HEAD on
	// `<token>.commit`, and every LIST (partition LIST on the
	// read path, matview-marker LIST in MaterializedViewReader.Lookup,
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
	// Reader / MaterializedView configs enforces NetApp's "same
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
// up to retryMaxAttempts (5) with jittered backoffs drawn from
// 100-300 / 300-500 / 500-800 / 500-800 ms (worst-case sum
// 2.4 s sleep per call), so two PUTs in worst case spend ~4.8 s
// in retry sleep alone, plus actual request time. 8 s gives
// ~1.67× safety on that retry envelope. Operators on tightly-
// clocked dev/test deployments can set CommitTimeout below this
// and ignore the warning; production deployments should size
// CommitTimeout above the advisory so transient S3 retries
// cannot cause the SettleWindow contract to be missed.
const CommitTimeoutAdvisory = 8 * time.Second

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
// Used by NewWriter, NewReader, BackfillMaterializedView —
// anything that reads/writes data files keyed by partition.
func (c S3TargetConfig) Validate() error {
	if err := c.ValidateLookup(); err != nil {
		return err
	}
	return validatePartitionKeyParts(c.PartitionKeyParts)
}

// ValidateLookup is the reduced check for constructors that
// only LIST / GET / PUT under a known prefix (no partition-key
// predicates): Bucket, Prefix, S3Client. Used by
// NewMaterializedViewReader — Lookup walks the
// <Prefix>/_matview/<name>/ subtree, which is keyed by the view's
// own Columns, not the config's PartitionKeyParts. A read-only
// analytics service can pass a minimally-populated S3TargetConfig
// and still build a working MaterializedViewReader.
func (c S3TargetConfig) ValidateLookup() error {
	if c.Bucket == "" {
		return errors.New("Bucket is required")
	}
	if c.Prefix == "" {
		return errors.New("Prefix is required")
	}
	if c.S3Client == nil {
		return errors.New("S3Client is required")
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
// every Target consumer (NewMaterializedViewReader is read-only and
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
// keeps zero out of the protocol; CommitTimeoutAdvisory (8 s)
// is a soft floor — values below it pass construction but emit
// a slog.Warn so operators see when a configured CommitTimeout
// is below the retry envelope of the two SettleWindow-relevant
// PUTs (ref + token-commit). Surfaces a hint at the operator's
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
				"advisory floor leaves ~1.67× safety on the worst-"+
				"case retry envelope of the two PUTs (~4.8 s of "+
				"jittered backoff sleep across 5 attempts each). "+
				"Acceptable for tightly-clocked dev/test "+
				"deployments; production should size above the "+
				"advisory.",
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
				"%s/%s missing — seed the dataset's "+
					"timing config before constructing a Target "+
					"(see README \"Initializing a new dataset\")",
				t.cfg.Bucket, key)
		}
		return 0, fmt.Errorf(
			"get %s/%s: %w", t.cfg.Bucket, key, err)
	}
	raw := strings.TrimSpace(string(body))
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf(
			"%s/%s body %q: parse as time.Duration: %w",
			t.cfg.Bucket, key, raw, err)
	}
	if value < floor {
		return 0, fmt.Errorf(
			"%s/%s body %q resolves to %s, below the %s floor",
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
// path (Read, PollRecords) and by BackfillMaterializedView when scanning
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
	err = retry(ctx, string(s3OpGet), scope, func() error {
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
// ref + markers) and by BackfillMaterializedView for retroactive marker
// emission. Carries the target's ConsistencyControl on every
// call.
func (t S3Target) put(
	ctx context.Context, key string, data []byte, contentType string,
) (err error) {
	return t.putWithMeta(ctx, key, data, contentType, nil)
}

// putWithMeta uploads data under key, attaching meta as
// x-amz-meta-<k> user-metadata headers. Used by the token-commit
// PUT (see putTokenCommit) to stamp the canonical attempt-id,
// refMicroTs, and writer-stamped insertedAt onto the marker so a
// same-token retry's upfront HEAD can reconstruct the original
// WriteResult without a second round trip. Carries the target's
// ConsistencyControl on every call.
//
// meta=nil is equivalent to put — no metadata sent.
func (t S3Target) putWithMeta(
	ctx context.Context, key string, data []byte,
	contentType string, meta map[string]string,
) (err error) {
	return t.putWithMetaCond(ctx, key, data, contentType, meta, false)
}

// putWithMetaCond is putWithMeta with an optional
// "fail-if-exists" precondition (`If-None-Match: *`). Used by the
// optimistic-commit write path to surface "prior commit landed"
// as a 412 PreconditionFailed (or a backend's bucket-policy 403)
// without an upfront HEAD round-trip.
//
// On AWS S3 (since Nov 2024) and recent MinIO, the precondition
// is enforced atomically server-side. On StorageGRID a bucket
// policy denying s3:PutOverwriteObject on the data/<...>.commit
// subtree gives the same effect (post-completion deny — fine for
// our use case where concurrent same-token writes are out of
// contract). On older backends that ignore both, the PUT
// overwrites silently — the optimistic-commit option documents
// this requirement.
func (t S3Target) putWithMetaCond(
	ctx context.Context, key string, data []byte,
	contentType string, meta map[string]string,
	failIfExists bool,
) (err error) {
	scope := t.metrics.s3OpScope(ctx, s3OpPut)
	scope.setReqBytes(int64(len(data)))
	defer scope.end(&err)
	if err = t.acquire(ctx); err != nil {
		return err
	}
	defer t.release()
	apiOpts := consistencyAPIOpts(t.cfg.ConsistencyControl)
	// Compute the expected MD5 once outside the retry loop —
	// every iteration uploads the same byte slice. Used by
	// verifyPutObjectETag to detect "PUT reported success but the
	// body that reached S3 is not the body we asked the SDK to
	// send." Defense-in-depth against the EOF-body bug fixed in
	// this same function (and against any future shape with the
	// same observable: caller-side reader state corrupted, proxy
	// truncated bytes, backend lost bytes).
	expectedMD5 := md5.Sum(data) //nolint:gosec // integrity-only
	expectedETagHex := hex.EncodeToString(expectedMD5[:])
	err = retry(ctx, string(s3OpPut), scope, func() error {
		scope.incAttempts()
		// Build a fresh PutObjectInput every iteration so each
		// attempt's Body is a *bytes.Reader at position 0. Reusing
		// one input across the outer retry loop is unsafe: the
		// previous attempt's HTTP transport read the underlying
		// reader to EOF, the SDK does not seek seekable bodies back
		// to 0 across top-level invocations (it only rewinds
		// between attempts within one PutObject's own retry), and
		// the SDK's content-length middleware reads Body.Len() at
		// the start of the next invocation — which is 0 once at
		// EOF. The result is a successful PUT that ships zero bytes
		// over the wire and overwrites a prior good upload with an
		// empty object (ETag d41d8cd9...). Per-iteration
		// construction keeps the data slice captured in the
		// closure but rebuilds a fresh reader on every attempt.
		input := &s3.PutObjectInput{
			Bucket:      aws.String(t.cfg.Bucket),
			Key:         aws.String(key),
			Body:        bytes.NewReader(data),
			ContentType: aws.String(contentType),
			Metadata:    meta,
		}
		if failIfExists {
			input.IfNoneMatch = aws.String("*")
		}
		out, err := t.cfg.S3Client.PutObject(ctx, input,
			func(o *s3.Options) {
				o.APIOptions = append(o.APIOptions, apiOpts...)
			})
		if err != nil {
			return err
		}
		return verifyPutObjectETag(out, expectedETagHex)
	})
	return err
}

// verifyPutObjectETag compares the PutObject response's ETag to the
// MD5 of the body s3store asked the SDK to send. They match for
// every PUT s3store issues today (single-part PutObject, no SSE-C,
// no SSE-KMS — SSE-S3 / AES256 still ETags as MD5(plaintext)), so a
// mismatch means the body that landed on S3 is not the body we
// passed in.
//
// Skips the comparison whenever the response carries a marker that
// breaks the ETag-equals-MD5 equality:
//
//   - SSE-KMS / SSE-KMS-DSSE: ETag is opaque, not MD5.
//   - SSE-C: ETag depends on the customer key.
//   - Multipart ETag (`<hex>-<N>`): hash-of-part-hashes, not MD5.
//   - Backend returned no ETag: nothing to compare against.
//
// Returns a plain error on mismatch (no smithyhttp.Response wrapped)
// so isTransientS3Error treats it as a transport-layer failure and
// the outer retry() re-uploads with a fresh *bytes.Reader. That
// recovers automatically from the EOF-body shape; for a persistent
// mismatch (proxy / backend bug) the retry budget is bounded and
// the final error surfaces to the caller with full diagnostics.
func verifyPutObjectETag(
	out *s3.PutObjectOutput, expectedETagHex string,
) error {
	if out == nil {
		return nil
	}
	switch out.ServerSideEncryption {
	case s3types.ServerSideEncryptionAwsKms,
		s3types.ServerSideEncryptionAwsKmsDsse:
		return nil
	}
	if aws.ToString(out.SSECustomerAlgorithm) != "" {
		return nil
	}
	gotETag := strings.Trim(aws.ToString(out.ETag), `"`)
	if gotETag == "" {
		return nil
	}
	if strings.Contains(gotETag, "-") {
		return nil
	}
	if !strings.EqualFold(gotETag, expectedETagHex) {
		return fmt.Errorf(
			"PUT body integrity check failed: response ETag %q "+
				"!= expected MD5 %q (body bytes did not reach S3 "+
				"intact — possible client-side reader corruption, "+
				"proxy truncation, or backend data loss)",
			gotETag, expectedETagHex)
	}
	return nil
}

// head HEADs key and returns its user-defined metadata (the
// x-amz-meta-* headers, surfaced by the SDK with lowercase keys).
// Carries the target's ConsistencyControl so the HEAD pairs with
// its preceding PUT under NetApp's "same consistency for paired
// operations" rule. Today's only caller is the token-commit HEAD
// on the read- and retry-paths (see headTokenCommit).
//
// 404 propagates as the SDK's *s3types.NotFound; callers
// distinguish "missing" from "transport error" themselves
// (headTokenCommit converts NotFound to ok=false).
func (t S3Target) head(
	ctx context.Context, key string,
) (meta map[string]string, err error) {
	scope := t.metrics.s3OpScope(ctx, s3OpHead)
	defer scope.end(&err)
	if err = t.acquire(ctx); err != nil {
		return nil, err
	}
	defer t.release()
	apiOpts := consistencyAPIOpts(t.cfg.ConsistencyControl)
	err = retry(ctx, string(s3OpHead), scope, func() error {
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
		meta = resp.Metadata
		return nil
	})
	return meta, err
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
	err = retry(ctx, string(s3OpList), scope, func() error {
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
// matview-marker LIST, ref-stream LIST (Poll), and the
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
