package s3store

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"time"

	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// instrumentationName is the OTel meter scope. Used as
// resource.scope.name on every emitted observation so backends
// can filter to "everything from this library".
const instrumentationName = "github.com/ueisele/s3store"

// s3OpKind names a low-level S3 operation. Used as the
// s3store.operation attribute on every s3store.s3.* observation.
// Typed enum so a caller passing the wrong literal fails at
// compile time. Internal — users never touch the metrics path
// directly.
type s3OpKind string

const (
	s3OpPut  s3OpKind = "put"
	s3OpGet  s3OpKind = "get"
	s3OpHead s3OpKind = "head"
	s3OpList s3OpKind = "list"
)

// methodKind names a public library method. Used as the
// s3store.method attribute on every s3store.method.* observation.
// Write and WriteWithKey are recorded separately even though Write
// dispatches to WriteWithKey internally — Write's per-partition
// dispatch goes through writeWithKeyResolved (no scope), so each
// public entry point yields exactly one observation.
type methodKind string

const (
	methodWrite                    methodKind = "write"
	methodWriteWithKey             methodKind = "write_with_key"
	methodRestampRef               methodKind = "restamp_ref"
	methodRead                     methodKind = "read"
	methodReadIter                 methodKind = "read_iter"
	methodReadPartitionIter        methodKind = "read_partition_iter"
	methodReadRangeIter            methodKind = "read_range_iter"
	methodReadPartitionRangeIter   methodKind = "read_partition_range_iter"
	methodReadEntriesIter          methodKind = "read_entries_iter"
	methodReadPartitionEntriesIter methodKind = "read_partition_entries_iter"
	methodPoll                     methodKind = "poll"
	methodPollRecords              methodKind = "poll_records"
	methodLookup                   methodKind = "lookup"
	methodLookupCommit             methodKind = "lookup_commit"
	methodBackfill                 methodKind = "backfill"
)

// Attribute keys. Kept as constants so callers cannot misspell and
// so the keys appear once per dimension in code search.
const (
	attrKeyBucket           = "s3store.bucket"
	attrKeyPrefix           = "s3store.prefix"
	attrKeyConsistency      = "s3store.consistency_level"
	attrKeyOperation        = "s3store.operation"
	attrKeyMethod           = "s3store.method"
	attrKeyOutcome          = "s3store.outcome"
	attrKeyErrorType        = "error.type"
	attrKeyAttempts         = "s3store.attempts"
	attrKeyAttempt          = "s3store.attempt"
	outcomeSuccess          = "success"
	outcomeError            = "error"
	outcomeCanceled         = "canceled"
	errTypeCanceled         = "canceled"
	errTypePreconditionFail = "precondition_failed"
	errTypeNotFound         = "not_found"
	errTypeSlowDown         = "slowdown"
	errTypeServer           = "server"
	errTypeClient           = "client"
	errTypeTransport        = "transport"
	errTypeOther            = "other"
)

// metrics owns every OTel instrument the library records into and
// the constant attribute set baked in at construction (bucket,
// prefix, consistency level when set). One *metrics per S3Target;
// Writer / Reader / ProjectionReader inherit it via the Target
// they hold.
//
// Internal — users configure observability by setting
// S3TargetConfig.MeterProvider; the rest of this struct is the
// library's instrumentation surface, not a public API. A nil
// *metrics is never produced; newMetrics resolves a nil
// MeterProvider to otel.GetMeterProvider() (global default —
// no-op when OTel isn't configured).
type metrics struct {
	// constSet is the pre-built attribute set of bucket / prefix /
	// (consistency) — frozen at construction. Combined with per-call
	// attrs via two MeasurementOptions on every record/add call so
	// the constants don't re-allocate or re-sort per observation.
	constSet attribute.Set

	// constSetOpt is metric.WithAttributeSet(constSet) memoised so
	// every record call passes the same MeasurementOption value
	// without rebuilding it.
	constSetOpt metric.MeasurementOption

	// Target / semaphore.
	semWaiting      metric.Int64UpDownCounter
	semInflight     metric.Int64UpDownCounter
	semWaitDuration metric.Float64Histogram
	semAcquires     metric.Int64Counter

	// Fan-out (generic concurrency primitive — not target state).
	fanoutWorkers metric.Int64Histogram
	fanoutItems   metric.Int64Histogram

	// S3 op level. Retry visibility lives on s3Count via the
	// attempts label (1..retryMaxAttempts) — see prewarm() for
	// why a counter beats a histogram for "did this call retry":
	// rate() on a labelled counter detects single retry events,
	// whereas a histogram bucketed at [1..retryMaxAttempts]
	// interpolates P95 of mostly-1 observations to ~0.95
	// (meaningless to operators) and makes the rare
	// retry-success case invisible.
	s3Duration       metric.Float64Histogram
	s3Count          metric.Int64Counter
	s3TransientCount metric.Int64Counter
	s3ReqBytes       metric.Int64Histogram
	s3RespBytes      metric.Int64Histogram

	// Library method level.
	methodDuration            metric.Float64Histogram
	methodCalls               metric.Int64Counter
	writeRecords              metric.Int64Histogram
	writePartitions           metric.Int64Histogram
	writeBytes                metric.Int64Histogram
	readRecords               metric.Int64Histogram
	readPartitions            metric.Int64Histogram
	readBytes                 metric.Int64Histogram
	readFiles                 metric.Int64Histogram
	readMissingData           metric.Int64Counter
	readMalformedRefs         metric.Int64Counter
	readCommitHead            metric.Int64Counter
	readCommitHeadHit         metric.Int64Counter
	writeCommitAfterTO        metric.Int64Counter
	writeOptimisticCollisions metric.Int64Counter

	// Iter pipeline internals (downloadAndDecodeIter): bottleneck
	// signals for the streamState's body-slot pool and byte-budget
	// reservation, plus per-partition decode duration. Surfaced
	// only when a wait actually fired and ended in success — the
	// canceled path is shutdown noise, not a saturation signal.
	iterBodySlotWait        metric.Float64Histogram
	iterBodySlotExhausted   metric.Int64Counter
	iterByteBudgetWait      metric.Float64Histogram
	iterByteBudgetExhausted metric.Int64Counter
	iterDecodeDuration      metric.Float64Histogram
	iterStallCount          metric.Int64Counter
}

// newMetrics constructs all instruments under the OTel meter
// scope. Falls back to otel.GetMeterProvider() when mp is nil so
// users who configure OTel globally pick up metrics for free.
//
// Errors building any instrument fall back to the noop provider's
// instrument silently — instrument creation only fails on
// programmer error (duplicate name with mismatched type) and we
// don't want that to break NewS3Target.
func newMetrics(
	mp metric.MeterProvider, bucket, prefix string,
	consistency ConsistencyLevel,
) *metrics {
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	meter := mp.Meter(instrumentationName)

	m := &metrics{}
	constAttrs := []attribute.KeyValue{
		attribute.String(attrKeyBucket, bucket),
		attribute.String(attrKeyPrefix, prefix),
	}
	if consistency != "" {
		constAttrs = append(constAttrs,
			attribute.String(attrKeyConsistency, string(consistency)))
	}
	m.constSet = attribute.NewSet(constAttrs...)
	m.constSetOpt = metric.WithAttributeSet(m.constSet)

	// Shared bucket boundaries. The OTel SDK default boundaries
	// (`[0, 5, 10, 25, 50, 75, 100, ...]`) are sized for milliseconds
	// and small counts; for our second-scale durations and
	// kilobyte-to-gigabyte payloads the entire useful range collapses
	// into the first bucket and `histogram_quantile` returns
	// interpolation artifacts (everything reads as ~4.75 in the [0, 5]
	// bucket). Explicit boundaries below cover the realistic range
	// for each metric family with enough resolution that P50/P95/P99
	// reflect actual observations rather than bucket geometry.
	//
	// durationBuckets covers ~5ms (typical S3 HEAD on AWS) to 30s
	// (a long Write or LIST under contention). Mirrors the shape
	// OpenTelemetry's HTTP semantic-convention guidance recommends,
	// extended to 30s for our heavier method calls.
	durationBuckets := []float64{
		0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30,
	}
	// shortWaitBuckets is for in-process wait durations (semaphore,
	// body-slot pool, byte-budget reservation). Most observations
	// are sub-millisecond when the system isn't saturated; only
	// extends to 5s.
	shortWaitBuckets := []float64{
		0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5,
	}
	// byteBuckets covers parquet body sizes from a few KB through
	// 1GB outliers. 4× jumps generally, with an extra boundary at
	// 32MB to give 2× resolution around the typical workload's
	// upper end (most data files in production land between 1KB
	// and ~20MB) so P95 / P99 don't interpolate across a full
	// decade of size.
	//
	// Zero-byte PUTs (markers, refs, token-commit) are filtered
	// out before recording — see s3OpScope.end() — so the lower
	// bound starts at 1KB without losing signal.
	byteBuckets := []float64{
		1024, 4096, 16384, 65536, 262144, 1048576,
		4194304, 16777216, 33554432, 134217728, 1073741824,
	}
	// recordCountBuckets covers per-call record counts from single-
	// row writes through million-row batches. Real per-partition
	// distributions for typical CDC workloads are heavily skewed
	// toward the 10-1000 band (P50 ≈ 75, P95 ≈ 1000, P99 ≈ 5000)
	// with a long thin tail to ~1M, so the boundaries are dense
	// 2-2.5× jumps from 10-1000 and stretch out to 5× past 10000
	// for outlier resolution without inflating cardinality.
	recordCountBuckets := []float64{
		1, 10, 25, 50, 100, 250, 500, 1000,
		2500, 10000, 50000, 250000, 1000000,
	}

	// Helpers — discard error to keep newMetrics infallible. The
	// OTel SDK only errors on duplicate-name-with-different-kind,
	// which is a static bug.
	mustHist := func(
		name, desc, unit string,
		extra ...metric.Float64HistogramOption,
	) metric.Float64Histogram {
		opts := []metric.Float64HistogramOption{
			metric.WithDescription(desc), metric.WithUnit(unit),
		}
		opts = append(opts, extra...)
		h, _ := meter.Float64Histogram(name, opts...)
		return h
	}
	mustHistInt := func(
		name, desc, unit string,
		extra ...metric.Int64HistogramOption,
	) metric.Int64Histogram {
		opts := []metric.Int64HistogramOption{
			metric.WithDescription(desc), metric.WithUnit(unit),
		}
		opts = append(opts, extra...)
		h, _ := meter.Int64Histogram(name, opts...)
		return h
	}
	mustCounter := func(name, desc, unit string) metric.Int64Counter {
		c, _ := meter.Int64Counter(name,
			metric.WithDescription(desc), metric.WithUnit(unit))
		return c
	}
	mustUpDown := func(name, desc, unit string) metric.Int64UpDownCounter {
		c, _ := meter.Int64UpDownCounter(name,
			metric.WithDescription(desc), metric.WithUnit(unit))
		return c
	}

	// Target / semaphore.
	m.semWaiting = mustUpDown(
		"s3store.target.waiting",
		"Goroutines currently blocked acquiring the per-Target in-flight semaphore",
		"{goroutine}")
	m.semInflight = mustUpDown(
		"s3store.target.inflight",
		"S3 requests currently holding a Target semaphore slot",
		"{request}")
	m.semWaitDuration = mustHist(
		"s3store.target.semaphore.wait.duration",
		"Time spent waiting in acquire() before a slot became available",
		"s",
		metric.WithExplicitBucketBoundaries(shortWaitBuckets...))
	m.semAcquires = mustCounter(
		"s3store.target.semaphore.acquires",
		"Total semaphore acquire attempts, labelled by outcome",
		"{acquire}")

	// Fan-out — independent of target state, lives in its own
	// namespace so dashboards can chart concurrency separately
	// from the target-bound semaphore gauges.
	m.fanoutWorkers = mustHistInt(
		"s3store.fanout.workers",
		"Worker goroutines spawned per fan-out call",
		"{goroutine}",
		// Bounded by MaxInflightRequests (default 32). Powers of
		// two cover the common configurations without forcing
		// histogram_quantile to interpolate inside one giant bucket.
		metric.WithExplicitBucketBoundaries(1, 2, 4, 8, 16, 32, 64, 128))
	m.fanoutItems = mustHistInt(
		"s3store.fanout.items",
		"Items dispatched per fan-out call",
		"{item}",
		metric.WithExplicitBucketBoundaries(
			1, 2, 5, 10, 25, 50, 100, 250, 1000, 5000))

	// S3 op level.
	m.s3Duration = mustHist(
		"s3store.s3.request.duration",
		"Wall-clock duration of one outer S3 wrapper call (acquire + retry + release)",
		"s",
		metric.WithExplicitBucketBoundaries(durationBuckets...))
	m.s3Count = mustCounter(
		"s3store.s3.request.count",
		"Total S3 wrapper calls, labelled by operation, outcome, "+
			"error.type (when applicable), and attempts (the outer "+
			"retry() iteration count, 0..retryMaxAttempts; 0 means "+
			"the call exited before the retry loop ran, e.g. "+
			"semaphore acquire failed)",
		"{request}")
	m.s3TransientCount = mustCounter(
		"s3store.s3.transient_error.count",
		"Total transient S3 errors observed inside retry() — HTTP "+
			"5xx, 429 SlowDown, and transport-layer failures (DNS / "+
			"TCP / TLS / connection reset). Incremented on every "+
			"transient failure regardless of whether a retry follows "+
			"or it's the final attempt that exhausted the budget. "+
			"Closes the visibility gap on s3.request.count, which "+
			"only carries error.type for terminal errors — masked "+
			"retries (call eventually succeeded) showed up only as "+
			"attempts>1 with no clue about the underlying cause. "+
			"Labels: operation, error.type (slowdown / server / "+
			"client / transport / ...), attempt (1..retryMaxAttempts, "+
			"the index of the failed attempt — not the call's final "+
			"attempt count, which is what s3store.attempts on "+
			"s3.request.count carries).",
		"{event}")
	m.s3ReqBytes = mustHistInt(
		"s3store.s3.request.body.size",
		"Request body size for outbound S3 PUTs",
		"By",
		metric.WithExplicitBucketBoundaries(byteBuckets...))
	m.s3RespBytes = mustHistInt(
		"s3store.s3.response.body.size",
		"Response body size for inbound S3 GETs",
		"By",
		metric.WithExplicitBucketBoundaries(byteBuckets...))

	// Library method level.
	m.methodDuration = mustHist(
		"s3store.method.duration",
		"Wall-clock duration of one public library method call",
		"s",
		metric.WithExplicitBucketBoundaries(durationBuckets...))
	m.methodCalls = mustCounter(
		"s3store.method.calls",
		"Total library method calls, labelled by method and outcome",
		"{call}")
	m.writeRecords = mustHistInt(
		"s3store.write.records",
		"Records per Write/WriteWithKey call",
		"{record}",
		metric.WithExplicitBucketBoundaries(recordCountBuckets...))
	m.writePartitions = mustHistInt(
		"s3store.write.partitions",
		"Distinct partition keys per Write call (always 1 for WriteWithKey)",
		"{partition}",
		metric.WithExplicitBucketBoundaries(
			1, 2, 5, 10, 25, 50, 100, 500))
	m.writeBytes = mustHistInt(
		"s3store.write.bytes",
		"Total parquet body bytes uploaded per Write/WriteWithKey call",
		"By",
		metric.WithExplicitBucketBoundaries(byteBuckets...))
	m.readRecords = mustHistInt(
		"s3store.read.records",
		"Records returned per read-side method call",
		"{record}",
		metric.WithExplicitBucketBoundaries(recordCountBuckets...))
	m.readPartitions = mustHistInt(
		"s3store.read.partitions",
		"Distinct Hive partitions touched per read-side method call (every method that funnels through the iter pipeline: Read / ReadIter / ReadPartitionIter / ReadRangeIter / ReadPartitionRangeIter / ReadEntriesIter / ReadPartitionEntriesIter / PollRecords). Not recorded for Poll / Lookup / LookupCommit / BackfillProjection.",
		"{partition}",
		metric.WithExplicitBucketBoundaries(
			1, 2, 5, 10, 25, 50, 100, 500))
	m.readBytes = mustHistInt(
		"s3store.read.bytes",
		"Total parquet body bytes downloaded per read-side method call",
		"By",
		metric.WithExplicitBucketBoundaries(byteBuckets...))
	m.readFiles = mustHistInt(
		"s3store.read.files",
		"Data files materialised per read-side method call",
		"{file}",
		metric.WithExplicitBucketBoundaries(
			1, 5, 10, 25, 50, 100, 500, 1000, 5000))
	m.readMissingData = mustCounter(
		"s3store.read.missing_data",
		"Data-file GETs that returned NoSuchKey on a tolerant read path (PollRecords / ReadRangeIter / ReadPartitionRangeIter / ReadEntriesIter / ReadPartitionEntriesIter / BackfillProjection). Strict paths (Read / ReadIter / ReadPartitionIter) fail instead of recording.",
		"{event}")
	m.readMalformedRefs = mustCounter(
		"s3store.read.malformed_refs",
		"Ref objects whose filename failed to parse during a LIST on the ref stream (Poll / PollRecords / ReadRangeIter / ReadPartitionRangeIter). Skipped after a slog.Warn — symmetric with read.missing_data on the data side.",
		"{event}")
	m.writeCommitAfterTO = mustCounter(
		"s3store.write.commit_after_timeout",
		"Writes whose elapsed time from refMicroTs (captured just before the ref PUT) to gate-visibility completion exceeded CommitTimeout. Fires from Write/WriteWithKey on token-commit-PUT completion (the gate flips when the commit becomes visible) and from RestampRef on ref-PUT completion (the commit gate is already satisfied; ref-LIST-visibility is the only contract-relevant interval). The write/restamp landed durably either way; signals that a SettleWindow tuned for this CommitTimeout may not yet have included it in the stream window. Pre-ref work (parquet encoding, marker PUTs, data PUT) is outside the budget by design.",
		"{event}")
	m.writeOptimisticCollisions = mustCounter(
		"s3store.write.optimistic_commit.collisions",
		"WithOptimisticCommit writes whose conditional commit PUT was rejected because a prior <token>.commit already existed (412 PreconditionFailed or 403 AccessDenied from a bucket-policy deny). The write recovers via a HEAD round-trip and returns the prior WriteResult; the orphan parquet+ref from this attempt are invisible to readers via the commit gate. Increments on the retry-found-prior-commit path that the option trades extra orphans for skipping the upfront HEAD on every fresh write.",
		"{event}")
	m.readCommitHead = mustCounter(
		"s3store.read.commit_head",
		"HEAD requests issued by the read paths against `<token>.commit` to gate visibility (snapshot reads on multi-attempt tokens; one per ref on stream reads after per-poll cache misses).",
		"{request}")
	m.readCommitHeadHit = mustCounter(
		"s3store.read.commit_head_cache_hit",
		"Stream-read commit-gate lookups satisfied by the per-poll (partition, token) → committed cache, collapsing repeat HEADs across refs that share a token within one Poll cycle.",
		"{event}")

	// Iter pipeline internals.
	m.iterBodySlotWait = mustHist(
		"s3store.read.iter.body_slot.wait.duration",
		"Time downloaders spent blocked acquiring a body-slot in the iter pipeline (recorded only when a wait actually occurred)",
		"s",
		metric.WithExplicitBucketBoundaries(shortWaitBuckets...))
	m.iterBodySlotExhausted = mustCounter(
		"s3store.read.iter.body_slot.exhausted",
		"Times the iter pipeline's body-slot pool was full and a downloader had to wait",
		"{event}")
	m.iterByteBudgetWait = mustHist(
		"s3store.read.iter.byte_budget.wait.duration",
		"Time the decoder spent blocked reserving uncompressed bytes against ReadAheadBytes (recorded only when a wait actually occurred)",
		"s",
		metric.WithExplicitBucketBoundaries(shortWaitBuckets...))
	m.iterByteBudgetExhausted = mustCounter(
		"s3store.read.iter.byte_budget.exhausted",
		"Times the iter pipeline's byte budget was full and the decoder had to wait",
		"{event}")
	m.iterDecodeDuration = mustHist(
		"s3store.read.iter.partition.decode.duration",
		"Wall-clock parquet decode time per partition (excludes byte-budget wait)",
		"s",
		metric.WithExplicitBucketBoundaries(durationBuckets...))
	m.iterStallCount = mustCounter(
		"s3store.read.iter.stall.count",
		"Times the iter pipeline made no forward progress (markComplete or slot release) within the watchdog window — pure observer, the watchdog does not cancel the pipeline. Indicates a deadlock (library bug) or a slow consumer (heavy yield-side processing).",
		"{event}")

	// Materialise rare-event counter series at zero so rate() can
	// catch their first non-zero observation. context.Background is
	// fine here — the pre-warm Add(0, ...) calls are not bound to
	// any caller's lifetime and the noop provider's Add is a no-op.
	m.prewarm(context.Background())

	return m
}

// callOpts builds the [constSet, perCall] MeasurementOption pair
// every record/add call passes. constSetOpt is pre-built once at
// construction; the per-call opt only carries the small variable
// set (operation/method/outcome/error.type), so no slice allocation
// is needed for the constants on the hot path.
//
// extras is passed through metric.WithAttributes which calls
// attribute.NewSet internally — unavoidable for the variable bit,
// but it's now a small set (≤ 3 entries) instead of constants+vars.
func (m *metrics) callOpts(extras ...attribute.KeyValue) (metric.MeasurementOption, metric.MeasurementOption) {
	return m.constSetOpt, metric.WithAttributes(extras...)
}

// prewarm materialises rare-event counter series at value 0 so
// rate-based queries can detect their first non-zero observation.
//
// Without prewarm, a counter series only appears in the scrape
// after its first Add() call. Prometheus's rate() / increase()
// treat the first-ever sample as the baseline — so the very
// first commit-after-timeout, the very first retry-success, the
// very first malformed-ref are silently absorbed into "rate from
// zero" rather than reported. Cheap defence: each Add(0, ...)
// emits the series at the next scrape and does not alter
// downstream histograms (which can't be pre-warmed at all since
// recording any value bumps a bucket).
//
// Called once per Target during construction. nil-safe so the
// noop provider path stays a no-op.
//
// Coverage:
//
//   - Incident counters (commit_after_timeout, optimistic
//     collisions, missing data, malformed refs, body-slot
//     exhaustion, byte-budget exhaustion). Single series each
//     under the Target's constant attribute set.
//   - s3.request.count for (operation × outcome=success ×
//     attempts ∈ "1..retryMaxAttempts"). Pre-warming the rare
//     retry-success combos (attempts > 1) is the headline win;
//     pre-warming attempts="1" is harmless and keeps the loop
//     uniform.
//   - target.semaphore.acquires for outcome=canceled — the rare
//     side of acquire (the success side fires on every call and
//     pre-warms naturally).
//
// Skipped on purpose:
//
//   - Histograms — Record(0, ...) bumps a bucket, so there's no
//     way to materialise an empty histogram bucket series.
//   - error.type-bearing combos (s3.request.count outcome=error,
//     method.calls outcome=error). error.type is dynamic across
//     8 declared values; pre-warming a no-error.type variant
//     creates a different label set than real observations carry.
//     Aggregate queries (sum without error.type) still work; per
//     error.type queries lose only their first sample, matching
//     pre-existing behaviour.
//   - method.calls success/canceled — methodCalls fires often
//     enough on real workloads that pre-warm doesn't pay; the
//     failure modes that do pay (errors) are blocked by the
//     error.type concern above.
func (m *metrics) prewarm(ctx context.Context) {
	if m == nil {
		return
	}

	// Incident counters — single series per Target (constSet only).
	incidents := []metric.Int64Counter{
		m.writeCommitAfterTO,
		m.writeOptimisticCollisions,
		m.readMissingData,
		m.readMalformedRefs,
		m.iterBodySlotExhausted,
		m.iterByteBudgetExhausted,
	}
	for _, c := range incidents {
		c.Add(ctx, 0, m.constSetOpt)
	}

	// s3.request.count — every (op × outcome=success × attempts).
	// retryMaxAttempts lives in target.go; same package, accessed
	// directly. The 1..N range covers happy-path (attempts="1")
	// and the rare retry-success combos (attempts="2..N") that
	// are the actual reason this method exists.
	for _, op := range []s3OpKind{s3OpGet, s3OpPut, s3OpHead, s3OpList} {
		for a := 1; a <= retryMaxAttempts; a++ {
			cs, vs := m.callOpts(
				attribute.String(attrKeyOperation, string(op)),
				attribute.String(attrKeyOutcome, outcomeSuccess),
				attribute.String(attrKeyAttempts, strconv.Itoa(a)),
			)
			m.s3Count.Add(ctx, 0, cs, vs)
		}
	}

	// target.semaphore.acquires — outcome=canceled. The success
	// side fires on every acquire and pre-warms itself.
	cs, vs := m.callOpts(
		attribute.String(attrKeyOutcome, outcomeCanceled),
	)
	m.semAcquires.Add(ctx, 0, cs, vs)
}

// semaphoreScope tracks one acquire-side observation cycle: from
// the moment a goroutine starts waiting on the semaphore through
// its terminal outcome (acquired or canceled). On creation it
// increments the waiting gauge and captures start time; the
// terminal call (acquired/canceled) decrements the gauge, records
// wait duration, and increments the per-outcome counter.
//
// Release stays separate as metrics.semaphoreReleased — release
// happens after the work is done and far away from the acquire
// site, so a scope object would have to be threaded through every
// S3 wrapper. Keeping release as a standalone method matches that
// reality without inventing a side-channel.
type semaphoreScope struct {
	m         *metrics
	ctx       context.Context
	start     time.Time
	waitEnded bool // true once acquired/canceled/cleanup recorded the wait phase
}

// semaphoreScope begins observing one acquire attempt. Returns a
// scope; the caller invokes acquired() or canceled() in the
// matching select arm. Always pair with `defer sc.cleanup()` so a
// panic between scope creation and the select arm doesn't leak the
// waiting gauge.
func (m *metrics) semaphoreScope(ctx context.Context) *semaphoreScope {
	if m == nil {
		return &semaphoreScope{}
	}
	m.semWaiting.Add(ctx, 1, m.constSetOpt)
	return &semaphoreScope{m: m, ctx: ctx, start: time.Now()}
}

// endWait decrements the waiting gauge and records wait duration.
// Idempotent — subsequent calls are no-ops, so cleanup() can run
// as a defer safety-net even when acquired/canceled already fired.
func (s *semaphoreScope) endWait() {
	if s.m == nil || s.waitEnded {
		return
	}
	s.waitEnded = true
	s.m.semWaiting.Add(s.ctx, -1, s.m.constSetOpt)
	s.m.semWaitDuration.Record(s.ctx, time.Since(s.start).Seconds(), s.m.constSetOpt)
}

// recordAcquired reports a successful acquire: waiting--, wait
// duration, inflight++, acquires{outcome=success}.
func (s *semaphoreScope) recordAcquired() {
	if s.m == nil {
		return
	}
	s.endWait()
	s.m.semInflight.Add(s.ctx, 1, s.m.constSetOpt)
	cs, vs := s.m.callOpts(attribute.String(attrKeyOutcome, outcomeSuccess))
	s.m.semAcquires.Add(s.ctx, 1, cs, vs)
}

// recordCanceled reports a cancelled acquire: waiting--, wait
// duration, acquires{outcome=canceled}. Inflight is NOT bumped —
// no slot was held.
func (s *semaphoreScope) recordCanceled() {
	if s.m == nil {
		return
	}
	s.endWait()
	cs, vs := s.m.callOpts(attribute.String(attrKeyOutcome, outcomeCanceled))
	s.m.semAcquires.Add(s.ctx, 1, cs, vs)
}

// cleanup is the panic-safety defer pair for semaphoreScope. It
// undoes the waiting++ that semaphoreScope() did, in case neither
// acquired() nor canceled() ran (a panic between scope creation
// and the select arm). Idempotent — no-op when endWait already
// fired.
func (s *semaphoreScope) cleanup() {
	if s.m == nil || s.waitEnded {
		return
	}
	s.waitEnded = true
	s.m.semWaiting.Add(s.ctx, -1, s.m.constSetOpt)
}

// recordReleased decrements the inflight gauge. Called from
// S3Target.release, which has no caller ctx — uses
// context.Background() (release is not cancellable; OTel
// UpDownCounter.Add requires a ctx).
func (m *metrics) recordReleased() {
	if m == nil {
		return
	}
	m.semInflight.Add(context.Background(), -1, m.constSetOpt)
}

// recordFanout records one fan-out dispatch's items + worker
// counts. Called from inside fanOut once it knows the actual
// worker count, and from the iter pipeline (which spawns workers
// directly with a known statically-bounded worker count).
//
// Skipped when items == 0 — that's a no-work path, not a fan-out
// dispatch, and recording (0, 0) would clutter the histograms with
// non-fanout events.
func (m *metrics) recordFanout(ctx context.Context, items, workers int) {
	if m == nil || items == 0 {
		return
	}
	m.fanoutItems.Record(ctx, int64(items), m.constSetOpt)
	m.fanoutWorkers.Record(ctx, int64(workers), m.constSetOpt)
}

// recordIterBodySlotWait reports one acquireBodySlot call that
// blocked and ended in a successful acquire. Records wait duration
// and increments the exhausted counter.
//
// Cancel-during-wait is intentionally NOT recorded — that path
// fires only during shutdown races, where a near-zero duration
// would drown out the saturation signal callers actually want to
// see. Caller must only invoke this when the wait actually fired
// (cond.Wait at least once) AND the acquire succeeded.
func (m *metrics) recordIterBodySlotWait(
	ctx context.Context, dur time.Duration,
) {
	if m == nil {
		return
	}
	m.iterBodySlotWait.Record(ctx, dur.Seconds(), m.constSetOpt)
	m.iterBodySlotExhausted.Add(ctx, 1, m.constSetOpt)
}

// recordIterByteBudgetWait reports one reserveBytes call that
// blocked and ended in a successful reservation. Same shape as
// recordIterBodySlotWait — cancel path is not recorded.
func (m *metrics) recordIterByteBudgetWait(
	ctx context.Context, dur time.Duration,
) {
	if m == nil {
		return
	}
	m.iterByteBudgetWait.Record(ctx, dur.Seconds(), m.constSetOpt)
	m.iterByteBudgetExhausted.Add(ctx, 1, m.constSetOpt)
}

// recordIterDecodeDuration reports one partition's parquet decode
// wall-clock time. Recorded regardless of decode outcome — decode
// time is meaningful even on the error path so operators see how
// much time the decoder spent before failing.
func (m *metrics) recordIterDecodeDuration(ctx context.Context, dur time.Duration) {
	if m == nil {
		return
	}
	m.iterDecodeDuration.Record(ctx, dur.Seconds(), m.constSetOpt)
}

// recordIterStall reports one watchdog tick that observed a
// pipeline with no forward progress (markComplete or slot
// release) within the threshold window. Carries the public
// method as an attribute so operators can attribute stalls to a
// specific entry point (Read / ReadIter / ReadPartitionIter /
// PollRecords / etc.).
//
// Pure observer — the caller logs a slog.Warn alongside the
// counter increment but does not cancel the pipeline.
func (m *metrics) recordIterStall(ctx context.Context, method methodKind) {
	if m == nil {
		return
	}
	cs, vs := m.callOpts(attribute.String(attrKeyMethod, string(method)))
	m.iterStallCount.Add(ctx, 1, cs, vs)
}

// s3OpScope tracks one outer S3 wrapper call. Created via
// s3OpStart, finalised via end in a defer. Captures duration,
// outcome, attempts, and (for put/get) request/response body
// sizes. Holding the scope as a pointer lets the deferred end
// read the eventual error via &err.
//
// end is idempotent — call it manually when you need the duration
// to stop before the function returns and the matching defer
// becomes a no-op.
type s3OpScope struct {
	m         *metrics
	ctx       context.Context
	op        s3OpKind
	start     time.Time
	attempts  int
	reqBytes  int64
	respBytes int64
	done      bool
}

// s3OpScope begins observing one outer S3 wrapper call (one
// S3Target.put / get / head / listPage). Returns a scope; defer
// scope.end(&err) at the call site so duration + outcome are
// recorded on every exit path.
func (m *metrics) s3OpScope(ctx context.Context, op s3OpKind) *s3OpScope {
	if m == nil {
		return &s3OpScope{}
	}
	return &s3OpScope{m: m, ctx: ctx, op: op, start: time.Now()}
}

// incAttempts increments the per-attempt counter. Invoked from
// inside the caller's retry loop so the s3.request.attempts
// histogram captures the actual attempt count, not just
// success/fail.
func (s *s3OpScope) incAttempts() {
	if s.m == nil {
		return
	}
	s.attempts++
}

// recordTransient increments s3TransientCount for the failed
// attempt currently being unwound. Called by retry() right
// after isTransientS3Error(err) returns true — both for
// non-terminal failures (next retry coming) and the terminal
// transient (budget exhausted). Bookkeeping for non-transient
// errors stays on s3Count alone; recording them here would
// double-count, since classifyError would surface the same
// error.type on the call's terminal s3Count observation.
//
// nil-safe on the receiver and on s.m so retry()'s test calls
// (which pass scope=nil) are no-ops.
func (s *s3OpScope) recordTransient(err error) {
	if s == nil || s.m == nil || err == nil {
		return
	}
	_, errType := classifyError(err)
	if errType == "" {
		return
	}
	cs, vs := s.m.callOpts(
		attribute.String(attrKeyOperation, string(s.op)),
		attribute.String(attrKeyErrorType, errType),
		attribute.String(attrKeyAttempt, strconv.Itoa(s.attempts)))
	s.m.s3TransientCount.Add(s.ctx, 1, cs, vs)
}

// setReqBytes records the size of the outgoing request body.
// PUT-only — GET / HEAD / DELETE / LIST callers don't invoke it.
// Set semantics: called exactly once per scope.
func (s *s3OpScope) setReqBytes(n int64) {
	if s.m == nil {
		return
	}
	s.reqBytes = n
}

// setRespBytes records the size of the inbound response body.
// GET-only — other operations don't invoke it. Set semantics:
// called exactly once per scope.
func (s *s3OpScope) setRespBytes(n int64) {
	if s.m == nil {
		return
	}
	s.respBytes = n
}

// end records the scope's terminal observations: duration, per-op
// count, attempt count, and (when set) request/response body
// sizes. errPtr is read at call time so the surrounding function's
// named return error drives outcome classification.
//
// Idempotent: subsequent calls are no-ops. This lets callers end
// early (excluding follow-up work from the duration metric) while
// keeping the standard `defer scope.end(&err)` as a safety net.
func (s *s3OpScope) end(errPtr *error) {
	if s.m == nil || s.done {
		return
	}
	s.done = true
	var err error
	if errPtr != nil {
		err = *errPtr
	}
	outcome, errType := classifyError(err)
	opAttr := attribute.String(attrKeyOperation, string(s.op))
	outcomeAttr := attribute.String(attrKeyOutcome, outcome)
	// Duration carries (op, outcome [, errType]) — distribution
	// per terminal classification. Attempts breakdown lives on
	// s3Count instead so retry rate is queryable as
	// rate(s3_count{attempts!="1"}) / rate(s3_count[...]).
	var dcs, dvs metric.MeasurementOption
	if errType != "" {
		dcs, dvs = s.m.callOpts(opAttr, outcomeAttr,
			attribute.String(attrKeyErrorType, errType))
	} else {
		dcs, dvs = s.m.callOpts(opAttr, outcomeAttr)
	}
	s.m.s3Duration.Record(
		s.ctx, time.Since(s.start).Seconds(), dcs, dvs)

	// Count carries the same labels plus attempts. attempts="0"
	// means the retry loop never ran (acquire failed before fn
	// could fire); attempts ∈ "1..retryMaxAttempts" otherwise.
	// prewarm() materialises the (op × outcome=success ×
	// attempts ∈ "1..retryMaxAttempts") combos at zero so the
	// rare retry-success series exist before their first real
	// observation.
	attemptsAttr := attribute.String(
		attrKeyAttempts, strconv.Itoa(s.attempts))
	var ccs, cvs metric.MeasurementOption
	if errType != "" {
		ccs, cvs = s.m.callOpts(opAttr, outcomeAttr, attemptsAttr,
			attribute.String(attrKeyErrorType, errType))
	} else {
		ccs, cvs = s.m.callOpts(opAttr, outcomeAttr, attemptsAttr)
	}
	s.m.s3Count.Add(s.ctx, 1, ccs, cvs)

	if s.reqBytes > 0 {
		// Body-size histograms are tagged only by operation — they
		// describe payload distribution per op, not per-outcome.
		bcs, bvs := s.m.callOpts(opAttr)
		s.m.s3ReqBytes.Record(s.ctx, s.reqBytes, bcs, bvs)
	}
	if s.respBytes > 0 {
		bcs, bvs := s.m.callOpts(opAttr)
		s.m.s3RespBytes.Record(s.ctx, s.respBytes, bcs, bvs)
	}
}

// methodScope tracks one public library method call. Records
// duration, outcome, and method-specific aggregates (records,
// partitions, bytes, files) accumulated through the call.
//
// Streaming methods (ReadIter / ReadRangeIter / ReadEntriesIter
// / ReadPartitionIter / ReadPartitionRangeIter /
// ReadPartitionEntriesIter) park the scope on the iterator and
// call end in the iter's deferred cancel block — the totals
// reflect everything actually drained, not just what fit in the
// first batch.
//
// Aggregates fire on every outcome (success and error alike) — but
// callers must only addX(n) for work that actually committed
// (records pushed to S3 / yielded to the consumer / files visited).
// Partial-success paths thus record what genuinely happened, while
// total-failure paths record zeros (which the >0 guard skips).
//
// Counters are atomic.Int64 because Write fans partitions out
// concurrently and each successful partition increments the scope.
//
// end is idempotent.
type methodScope struct {
	m          *metrics
	ctx        context.Context
	method     methodKind
	start      time.Time
	records    atomic.Int64
	partitions atomic.Int64
	bytes      atomic.Int64
	files      atomic.Int64
	done       bool
}

// methodScope begins observing one library method call. Returns a
// scope; defer scope.end(&err) at the call site.
func (m *metrics) methodScope(ctx context.Context, method methodKind) *methodScope {
	if m == nil {
		return &methodScope{}
	}
	return &methodScope{m: m, ctx: ctx, method: method, start: time.Now()}
}

// addRecords increments the count of records that the method
// committed (pushed for writes, yielded for reads). Safe for
// concurrent use — Write's per-partition fan-out goroutines call
// it after each successful partition. Only call on commit; the
// total-failure path then records zero, and partial-success
// records what actually landed.
func (s *methodScope) addRecords(n int64) {
	if s.m == nil || n <= 0 {
		return
	}
	s.records.Add(n)
}

// addPartitions increments the count of distinct partition keys
// touched by the call. WriteWithKey adds 1 on commit; Write adds
// 1 per successful partition. Read paths going through the iter
// pipeline (downloadAndDecodeIter) add len(parts) once at start.
func (s *methodScope) addPartitions(n int64) {
	if s.m == nil || n <= 0 {
		return
	}
	s.partitions.Add(n)
}

// addBytes increments the parquet body byte total for the method
// (uploaded for writes, downloaded for reads). Safe for
// concurrent use.
func (s *methodScope) addBytes(n int64) {
	if s.m == nil || n <= 0 {
		return
	}
	s.bytes.Add(n)
}

// addFiles increments the count of data files materialised by a
// read-side method. Safe for concurrent use — read pipelines
// fan downloads out to multiple goroutines.
func (s *methodScope) addFiles(n int64) {
	if s.m == nil || n <= 0 {
		return
	}
	s.files.Add(n)
}

// recordMissingData increments the missing-data counter for one
// NoSuchKey skip on a tolerant read path (PollRecords /
// ReadRangeIter / BackfillProjection). Reuses the scope's method as
// the attribute so dashboards can split by which path produced
// the skip — no caller plumbing.
func (s *methodScope) recordMissingData() {
	if s.m == nil {
		return
	}
	cs, vs := s.m.callOpts(attribute.String(attrKeyMethod, string(s.method)))
	s.m.readMissingData.Add(s.ctx, 1, cs, vs)
}

// recordMalformedRefs increments the malformed-refs counter for one
// ref object whose filename failed to parse during a LIST on the
// ref stream. Symmetric with recordMissingData on the data side —
// silent skips are an operability hazard, the counter makes the
// drift visible. Carries the scope's method so dashboards can tell
// which entry point surfaced the malformed ref.
func (s *methodScope) recordMalformedRefs() {
	if s.m == nil {
		return
	}
	cs, vs := s.m.callOpts(attribute.String(attrKeyMethod, string(s.method)))
	s.m.readMalformedRefs.Add(s.ctx, 1, cs, vs)
}

// recordReadCommitHead increments the commit_head counter for one
// HEAD on a `<token>.commit` issued by a read-path gate. Carries
// the caller's method (snapshot read vs stream poll) so dashboards
// can split by entry point.
func (m *metrics) recordReadCommitHead(ctx context.Context, method methodKind) {
	if m == nil {
		return
	}
	cs, vs := m.callOpts(attribute.String(attrKeyMethod, string(method)))
	m.readCommitHead.Add(ctx, 1, cs, vs)
}

// recordReadCommitHeadCacheHit increments the
// commit_head_cache_hit counter for one stream-poll gate lookup
// satisfied by the per-poll cache. Carries the caller's method so
// dashboards see hit-rate per entry point.
func (m *metrics) recordReadCommitHeadCacheHit(ctx context.Context, method methodKind) {
	if m == nil {
		return
	}
	cs, vs := m.callOpts(attribute.String(attrKeyMethod, string(method)))
	m.readCommitHeadHit.Add(ctx, 1, cs, vs)
}

// recordCommitAfterTimeout increments the commit_after_timeout
// counter for one Write whose elapsed time from refMicroTs (just
// before the ref PUT) to token-commit-PUT completion exceeded
// CommitTimeout. The commit landed; this is an observability
// signal that a SettleWindow tuned to the CommitTimeout may not
// yet have included this write in the stream window. Pre-ref
// work (parquet encoding, marker PUTs, data PUT) is outside the
// budget by design — only the ref-LIST-visible →
// token-commit-visible window can put the SettleWindow contract
// at risk. No scope dimension — Write is the only emitter.
func (m *metrics) recordCommitAfterTimeout(ctx context.Context) {
	if m == nil {
		return
	}
	m.writeCommitAfterTO.Add(ctx, 1, m.constSetOpt)
}

// recordOptimisticCommitCollision increments the
// optimistic_commit.collisions counter for one
// WithOptimisticCommit write whose conditional commit PUT was
// rejected because a prior <token>.commit already existed (412
// PreconditionFailed, or 403 AccessDenied on bucket-policy
// backends). The write recovers via a HEAD round-trip on the
// commit marker and returns the prior WriteResult — the orphan
// parquet + ref this attempt left behind are invisible to readers
// via the commit gate. Operators can chart this counter to see
// how often optimistic-commit's "trade upfront HEAD for orphan-
// on-collision" trade actually fires; if the rate climbs past
// ~5% the option becomes a net loss vs. the upfront-HEAD path.
// No scope dimension — only the write path emits.
func (m *metrics) recordOptimisticCommitCollision(ctx context.Context) {
	if m == nil {
		return
	}
	m.writeOptimisticCollisions.Add(ctx, 1, m.constSetOpt)
}

// end records the scope's terminal observations: duration,
// per-method count, and any non-zero aggregates. errPtr is read
// at call time so the surrounding function's named return error
// drives outcome classification.
//
// Aggregates record on every outcome — the values reflect what
// actually committed (callers only addX for committed work), so
// partial-success error paths still surface their progress. The
// >0 guard prevents zero-valued samples from polluting
// histograms on total-failure paths.
//
// Idempotent: subsequent calls are no-ops.
func (s *methodScope) end(errPtr *error) {
	if s.m == nil || s.done {
		return
	}
	s.done = true
	var err error
	if errPtr != nil {
		err = *errPtr
	}
	outcome, errType := classifyError(err)
	methodAttr := attribute.String(attrKeyMethod, string(s.method))
	outcomeAttr := attribute.String(attrKeyOutcome, outcome)
	var cs, vs metric.MeasurementOption
	if errType != "" {
		cs, vs = s.m.callOpts(methodAttr, outcomeAttr,
			attribute.String(attrKeyErrorType, errType))
	} else {
		cs, vs = s.m.callOpts(methodAttr, outcomeAttr)
	}
	s.m.methodDuration.Record(s.ctx, time.Since(s.start).Seconds(), cs, vs)
	s.m.methodCalls.Add(s.ctx, 1, cs, vs)

	mcs, mvs := s.m.callOpts(methodAttr)
	records := s.records.Load()
	partitions := s.partitions.Load()
	bytesN := s.bytes.Load()
	files := s.files.Load()
	switch s.method {
	case methodWrite, methodWriteWithKey:
		if records > 0 {
			s.m.writeRecords.Record(s.ctx, records, mcs, mvs)
		}
		if partitions > 0 {
			s.m.writePartitions.Record(s.ctx, partitions, mcs, mvs)
		}
		if bytesN > 0 {
			s.m.writeBytes.Record(s.ctx, bytesN, mcs, mvs)
		}
	case methodRead, methodReadIter, methodReadPartitionIter,
		methodReadRangeIter, methodReadPartitionRangeIter,
		methodReadEntriesIter, methodReadPartitionEntriesIter,
		methodPollRecords, methodPoll, methodLookup,
		methodLookupCommit, methodBackfill:
		if records > 0 {
			s.m.readRecords.Record(s.ctx, records, mcs, mvs)
		}
		if partitions > 0 {
			s.m.readPartitions.Record(s.ctx, partitions, mcs, mvs)
		}
		if bytesN > 0 {
			s.m.readBytes.Record(s.ctx, bytesN, mcs, mvs)
		}
		if files > 0 {
			s.m.readFiles.Record(s.ctx, files, mcs, mvs)
		}
	}
}

// classifyError maps an error into (outcome, error.type) for the
// outcome / error.type attributes. Buckets HTTP and SDK errors
// into a fixed enumeration so cardinality stays bounded.
//
// Returns ("", "") for a nil err so the success path emits
// outcome=success without an error.type label.
func classifyError(err error) (outcome, errType string) {
	if err == nil {
		return outcomeSuccess, ""
	}
	if errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return outcomeCanceled, errTypeCanceled
	}
	if _, ok := errors.AsType[*s3types.NoSuchKey](err); ok {
		return outcomeError, errTypeNotFound
	}
	if _, ok := errors.AsType[*s3types.NotFound](err); ok {
		return outcomeError, errTypeNotFound
	}
	if respErr, ok := errors.AsType[*smithyhttp.ResponseError](err); ok {
		// API error code takes precedence over HTTP status. AWS S3
		// can return SlowDown as HTTP 503 (not always 429), so a
		// status-only switch buckets the throttle as
		// errTypeServer and dashboards lose the actual cause. The
		// SDK exposes the protocol-level error code via
		// smithy.APIError; check it first when a smithy ResponseError
		// is in the chain.
		if apiErr, ok := errors.AsType[smithy.APIError](err); ok {
			switch apiErr.ErrorCode() {
			case "SlowDown":
				return outcomeError, errTypeSlowDown
			}
		}
		status := respErr.HTTPStatusCode()
		switch {
		case status == 412:
			return outcomeError, errTypePreconditionFail
		case status == 429:
			return outcomeError, errTypeSlowDown
		case status >= 500:
			return outcomeError, errTypeServer
		case status >= 400:
			return outcomeError, errTypeClient
		default:
			return outcomeError, errTypeOther
		}
	}
	// No HTTP response attached and not a known sentinel —
	// transport-level (DNS / TCP / TLS / connection reset).
	return outcomeError, errTypeTransport
}
