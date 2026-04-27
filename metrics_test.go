package s3store

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// newTestMetrics builds a metrics wired to a freshly-allocated
// ManualReader, returning both so tests can record + collect.
// All test scenarios use bucket "b" / prefix "p" so the constant
// attribute set is predictable.
func newTestMetrics(t *testing.T, consistency ConsistencyLevel) (*metrics, *metric.ManualReader) {
	t.Helper()
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	return newMetrics(mp, "b", "p", consistency), reader
}

func collectMetrics(t *testing.T, reader *metric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	return rm
}

// findMetric returns the named metric from the s3store scope, or
// nil if it wasn't recorded yet (zero observations don't appear
// in the export).
func findMetric(rm metricdata.ResourceMetrics, name string) *metricdata.Metrics {
	for _, sm := range rm.ScopeMetrics {
		if sm.Scope.Name != instrumentationName {
			continue
		}
		for i := range sm.Metrics {
			if sm.Metrics[i].Name == name {
				return &sm.Metrics[i]
			}
		}
	}
	return nil
}

// hasAttr reports whether the attribute set contains key=value.
func hasAttr(set attribute.Set, key, value string) bool {
	v, ok := set.Value(attribute.Key(key))
	return ok && v.AsString() == value
}

func TestClassifyError(t *testing.T) {
	preconditionErr := &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{
			Response: &http.Response{StatusCode: 412},
		},
		Err: &smithy.GenericAPIError{Code: "PreconditionFailed"},
	}
	slowDownErr := &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{
			Response: &http.Response{StatusCode: 429},
		},
		Err: &smithy.GenericAPIError{Code: "SlowDown"},
	}
	serverErr := &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{
			Response: &http.Response{StatusCode: 503},
		},
		Err: &smithy.GenericAPIError{Code: "InternalError"},
	}
	clientErr := &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{
			Response: &http.Response{StatusCode: 400},
		},
		Err: &smithy.GenericAPIError{Code: "BadRequest"},
	}

	cases := []struct {
		name        string
		err         error
		wantOutcome string
		wantErrType string
	}{
		{"nil success", nil, outcomeSuccess, ""},
		{"context canceled", context.Canceled, outcomeCanceled, errTypeCanceled},
		{"deadline", context.DeadlineExceeded, outcomeCanceled, errTypeCanceled},
		{"already exists sentinel", ErrAlreadyExists, outcomeError, errTypePreconditionFail},
		{"NoSuchKey", &s3types.NoSuchKey{}, outcomeError, errTypeNotFound},
		{"NotFound", &s3types.NotFound{}, outcomeError, errTypeNotFound},
		{"412 precondition", preconditionErr, outcomeError, errTypePreconditionFail},
		{"429 slowdown", slowDownErr, outcomeError, errTypeSlowDown},
		{"5xx server", serverErr, outcomeError, errTypeServer},
		{"4xx client", clientErr, outcomeError, errTypeClient},
		{"transport", errors.New("dial tcp: i/o timeout"), outcomeError, errTypeTransport},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotOutcome, gotErrType := classifyError(c.err)
			if gotOutcome != c.wantOutcome {
				t.Errorf("outcome: got %q, want %q", gotOutcome, c.wantOutcome)
			}
			if gotErrType != c.wantErrType {
				t.Errorf("errType: got %q, want %q", gotErrType, c.wantErrType)
			}
		})
	}
}

func TestNewMetrics_NilProviderUsesGlobal(t *testing.T) {
	// Nil provider must not panic — newMetrics falls back to
	// otel.GetMeterProvider() which is the global no-op until the
	// user configures one.
	m := newMetrics(nil, "b", "p", "")
	if m == nil {
		t.Fatal("newMetrics returned nil")
	}
	// Smoke-test a few methods so the no-op instruments don't
	// crash on use.
	ctx := context.Background()
	sc := m.semaphoreScope(ctx)
	sc.recordAcquired()
	m.recordReleased()
	m.recordFanout(ctx, 4, 2)
}

func TestMetrics_ConstantAttributes(t *testing.T) {
	m, reader := newTestMetrics(t, ConsistencyStrongGlobal)
	ctx := context.Background()

	// Drive one observation of every constant attribute by running
	// a successful semaphore cycle.
	sc := m.semaphoreScope(ctx)
	sc.recordAcquired()
	m.recordReleased()

	rm := collectMetrics(t, reader)

	acquires := findMetric(rm, "s3store.target.semaphore.acquires")
	if acquires == nil {
		t.Fatal("acquires metric not recorded")
	}
	sum, ok := acquires.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("expected Sum[int64], got %T", acquires.Data)
	}
	if len(sum.DataPoints) == 0 {
		t.Fatal("no datapoints recorded")
	}
	dp := sum.DataPoints[0]
	for _, want := range [][2]string{
		{attrKeyBucket, "b"},
		{attrKeyPrefix, "p"},
		{attrKeyConsistency, string(ConsistencyStrongGlobal)},
		{attrKeyOutcome, outcomeSuccess},
	} {
		if !hasAttr(dp.Attributes, want[0], want[1]) {
			t.Errorf("missing attribute %s=%s; got %v",
				want[0], want[1], dp.Attributes.ToSlice())
		}
	}
}

func TestMetrics_NoConsistencyLabelWhenUnset(t *testing.T) {
	m, reader := newTestMetrics(t, "")
	ctx := context.Background()
	sc := m.semaphoreScope(ctx)
	sc.recordAcquired()
	m.recordReleased()

	rm := collectMetrics(t, reader)
	acquires := findMetric(rm, "s3store.target.semaphore.acquires")
	if acquires == nil {
		t.Fatal("acquires metric not recorded")
	}
	sum := acquires.Data.(metricdata.Sum[int64])
	dp := sum.DataPoints[0]
	if _, ok := dp.Attributes.Value(attrKeyConsistency); ok {
		t.Errorf("consistency attribute should be absent when unset; got %v",
			dp.Attributes.ToSlice())
	}
}

func TestMetrics_S3OpScope_RecordsRequestBytes(t *testing.T) {
	m, reader := newTestMetrics(t, "")
	ctx := context.Background()

	scope := m.s3OpScope(ctx, s3OpPut)
	scope.setReqBytes(1024)
	scope.incAttempts()
	scope.incAttempts()
	var err error
	scope.end(&err)

	rm := collectMetrics(t, reader)

	dur := findMetric(rm, "s3store.s3.request.duration")
	if dur == nil {
		t.Fatal("duration not recorded")
	}
	hist := dur.Data.(metricdata.Histogram[float64])
	if got := hist.DataPoints[0].Count; got != 1 {
		t.Errorf("duration count: got %d, want 1", got)
	}
	if !hasAttr(hist.DataPoints[0].Attributes, attrKeyOperation, "put") {
		t.Error("missing operation=put on duration")
	}
	if !hasAttr(hist.DataPoints[0].Attributes, attrKeyOutcome, outcomeSuccess) {
		t.Error("missing outcome=success on duration")
	}

	attempts := findMetric(rm, "s3store.s3.request.attempts")
	if attempts == nil {
		t.Fatal("attempts not recorded")
	}
	attHist := attempts.Data.(metricdata.Histogram[int64])
	if got := attHist.DataPoints[0].Sum; got != 2 {
		t.Errorf("attempts sum: got %d, want 2", got)
	}

	bodyBytes := findMetric(rm, "s3store.s3.request.body.size")
	if bodyBytes == nil {
		t.Fatal("body size not recorded")
	}
	bytesHist := bodyBytes.Data.(metricdata.Histogram[int64])
	if got := bytesHist.DataPoints[0].Sum; got != 1024 {
		t.Errorf("body size: got %d, want 1024", got)
	}
}

func TestMetrics_S3OpScope_ErrorClassification(t *testing.T) {
	m, reader := newTestMetrics(t, "")
	ctx := context.Background()

	scope := m.s3OpScope(ctx, s3OpPut)
	err := ErrAlreadyExists
	scope.end(&err)

	rm := collectMetrics(t, reader)
	count := findMetric(rm, "s3store.s3.request.count")
	if count == nil {
		t.Fatal("count not recorded")
	}
	sum := count.Data.(metricdata.Sum[int64])
	dp := sum.DataPoints[0]
	if !hasAttr(dp.Attributes, attrKeyOutcome, outcomeError) {
		t.Error("expected outcome=error")
	}
	if !hasAttr(dp.Attributes, attrKeyErrorType, errTypePreconditionFail) {
		t.Errorf("expected error.type=precondition_failed; got %v",
			dp.Attributes.ToSlice())
	}
}

func TestMetrics_MethodScope_RecordsAggregates(t *testing.T) {
	m, reader := newTestMetrics(t, "")
	ctx := context.Background()

	sc := m.methodScope(ctx, methodWrite)
	sc.addRecords(500)
	sc.addPartitions(3)
	sc.addBytes(1024 * 1024)
	var err error
	sc.end(&err)

	rm := collectMetrics(t, reader)

	for name, want := range map[string]int64{
		"s3store.write.records":    500,
		"s3store.write.partitions": 3,
		"s3store.write.bytes":      1024 * 1024,
	} {
		hist := findMetric(rm, name)
		if hist == nil {
			t.Fatalf("%s not recorded", name)
		}
		h := hist.Data.(metricdata.Histogram[int64])
		if got := h.DataPoints[0].Sum; got != want {
			t.Errorf("%s sum: got %d, want %d", name, got, want)
		}
	}
}

func TestMetrics_MethodScope_TotalFailureSkipsAggregates(t *testing.T) {
	// Total-failure path: nothing committed before the error
	// fired, so no addX(...) calls happened. The aggregates stay
	// at zero and the >0 guard inside end() skips them — only
	// duration + method.calls{outcome=error} fire.
	m, reader := newTestMetrics(t, "")
	ctx := context.Background()

	sc := m.methodScope(ctx, methodRead)
	err := errors.New("synthetic")
	sc.end(&err)

	rm := collectMetrics(t, reader)
	if h := findMetric(rm, "s3store.read.records"); h != nil {
		t.Errorf("read.records should not record on zero count; got %+v", h)
	}
	count := findMetric(rm, "s3store.method.calls")
	if count == nil {
		t.Fatal("method.calls not recorded")
	}
	sum := count.Data.(metricdata.Sum[int64])
	if !hasAttr(sum.DataPoints[0].Attributes, attrKeyOutcome, outcomeError) {
		t.Error("expected outcome=error on method.calls")
	}
}

func TestMetrics_MethodScope_PartialSuccessRecordsCommitted(t *testing.T) {
	// Partial-success error path: some work committed (caller
	// called addX with the committed counts) before the error
	// fired. Aggregates fire with the committed values; outcome
	// on the count/duration is still error so error rate is
	// computable.
	m, reader := newTestMetrics(t, "")
	ctx := context.Background()

	sc := m.methodScope(ctx, methodWrite)
	// Caller's per-partition closure committed 3 partitions /
	// 250 records / 4096 bytes before the fourth partition
	// errored.
	sc.addPartitions(3)
	sc.addRecords(250)
	sc.addBytes(4096)
	err := errors.New("partition 4 failed")
	sc.end(&err)

	rm := collectMetrics(t, reader)

	// Aggregates record committed work even on error.
	for name, want := range map[string]int64{
		"s3store.write.records":    250,
		"s3store.write.partitions": 3,
		"s3store.write.bytes":      4096,
	} {
		mt := findMetric(rm, name)
		if mt == nil {
			t.Errorf("%s should have recorded committed work; missing", name)
			continue
		}
		h := mt.Data.(metricdata.Histogram[int64])
		if got := h.DataPoints[0].Sum; got != want {
			t.Errorf("%s sum: got %d, want %d", name, got, want)
		}
	}
	// And the call counter still records error outcome.
	count := findMetric(rm, "s3store.method.calls")
	if count == nil {
		t.Fatal("method.calls not recorded")
	}
	sum := count.Data.(metricdata.Sum[int64])
	if !hasAttr(sum.DataPoints[0].Attributes, attrKeyOutcome, outcomeError) {
		t.Error("expected outcome=error on method.calls")
	}
}

func TestMetrics_MethodScope_AddXIsAdditive(t *testing.T) {
	// Multiple addRecords(n) calls accumulate (additive
	// semantics) — Write's per-partition fan-out depends on this.
	m, reader := newTestMetrics(t, "")
	ctx := context.Background()
	sc := m.methodScope(ctx, methodWrite)
	sc.addRecords(100)
	sc.addRecords(200)
	sc.addRecords(50)
	var err error
	sc.end(&err)

	rm := collectMetrics(t, reader)
	mt := findMetric(rm, "s3store.write.records")
	if mt == nil {
		t.Fatal("write.records not recorded")
	}
	h := mt.Data.(metricdata.Histogram[int64])
	if got := h.DataPoints[0].Sum; got != 350 {
		t.Errorf("write.records sum: got %d, want 350", got)
	}
}

func TestMetrics_RecordFanout(t *testing.T) {
	m, reader := newTestMetrics(t, "")
	ctx := context.Background()
	m.recordFanout(ctx, 17, 5)

	rm := collectMetrics(t, reader)
	for name, wantSum := range map[string]int64{
		"s3store.fanout.items":   17,
		"s3store.fanout.workers": 5,
	} {
		mt := findMetric(rm, name)
		if mt == nil {
			t.Fatalf("%s not recorded", name)
		}
		h := mt.Data.(metricdata.Histogram[int64])
		if got := h.DataPoints[0].Sum; got != wantSum {
			t.Errorf("%s sum: got %d, want %d", name, got, wantSum)
		}
	}
}

func TestMetrics_RecordIterBodySlotWait(t *testing.T) {
	// Body-slot wait records both a duration histogram and an
	// exhausted counter. Caller invokes only on successful
	// acquires (cancel-during-wait is intentionally not recorded),
	// so the histogram never carries zero-duration samples.
	m, reader := newTestMetrics(t, "")
	ctx := context.Background()
	m.recordIterBodySlotWait(ctx, 25*time.Millisecond)
	m.recordIterBodySlotWait(ctx, 75*time.Millisecond)

	rm := collectMetrics(t, reader)

	dur := findMetric(rm, "s3store.read.iter.body_slot.wait.duration")
	if dur == nil {
		t.Fatal("body_slot.wait.duration not recorded")
	}
	hist := dur.Data.(metricdata.Histogram[float64])
	if len(hist.DataPoints) != 1 {
		t.Fatalf("expected one datapoint; got %d", len(hist.DataPoints))
	}
	if got := hist.DataPoints[0].Count; got != 2 {
		t.Errorf("count: got %d, want 2", got)
	}
	if got := hist.DataPoints[0].Sum; got < 0.099 || got > 0.101 {
		t.Errorf("sum (s): got %f, want ~0.1", got)
	}

	ex := findMetric(rm, "s3store.read.iter.body_slot.exhausted")
	if ex == nil {
		t.Fatal("body_slot.exhausted not recorded")
	}
	exSum := ex.Data.(metricdata.Sum[int64])
	var totalEx int64
	for _, dp := range exSum.DataPoints {
		totalEx += dp.Value
	}
	if totalEx != 2 {
		t.Errorf("exhausted total: got %d, want 2", totalEx)
	}
}

func TestMetrics_RecordIterByteBudgetWait(t *testing.T) {
	m, reader := newTestMetrics(t, "")
	ctx := context.Background()
	m.recordIterByteBudgetWait(ctx, 50*time.Millisecond)

	rm := collectMetrics(t, reader)
	dur := findMetric(rm, "s3store.read.iter.byte_budget.wait.duration")
	if dur == nil {
		t.Fatal("byte_budget.wait.duration not recorded")
	}
	hist := dur.Data.(metricdata.Histogram[float64])
	if got := hist.DataPoints[0].Count; got != 1 {
		t.Errorf("duration count: got %d, want 1", got)
	}
	ex := findMetric(rm, "s3store.read.iter.byte_budget.exhausted")
	if ex == nil {
		t.Fatal("byte_budget.exhausted not recorded")
	}
	if got := ex.Data.(metricdata.Sum[int64]).DataPoints[0].Value; got != 1 {
		t.Errorf("exhausted: got %d, want 1", got)
	}
}

func TestMetrics_RecordIterDecodeDuration(t *testing.T) {
	m, reader := newTestMetrics(t, "")
	ctx := context.Background()
	m.recordIterDecodeDuration(ctx, 200*time.Millisecond)
	m.recordIterDecodeDuration(ctx, 300*time.Millisecond)

	rm := collectMetrics(t, reader)
	dur := findMetric(rm, "s3store.read.iter.partition.decode.duration")
	if dur == nil {
		t.Fatal("decode.duration not recorded")
	}
	hist := dur.Data.(metricdata.Histogram[float64])
	if got := hist.DataPoints[0].Count; got != 2 {
		t.Errorf("count: got %d, want 2", got)
	}
	if got := hist.DataPoints[0].Sum; got < 0.499 || got > 0.501 {
		t.Errorf("sum (s): got %f, want ~0.5", got)
	}
}

// TestFanOut_RecordsThroughItself verifies that fanOut self-instruments
// — the truth lives inside fanOut, so a call site that just hands it
// (items, concurrency, *metrics) gets the correct worker count
// recorded automatically.
func TestFanOut_RecordsThroughItself(t *testing.T) {
	m, reader := newTestMetrics(t, "")
	// 5 items at concurrency=2 should yield workers=2.
	if err := fanOut(context.Background(), []int{1, 2, 3, 4, 5}, 2, m,
		func(_ context.Context, _ int, _ int) error { return nil },
	); err != nil {
		t.Fatalf("fanOut: %v", err)
	}
	rm := collectMetrics(t, reader)
	items := findMetric(rm, "s3store.fanout.items")
	workers := findMetric(rm, "s3store.fanout.workers")
	if items == nil || workers == nil {
		t.Fatal("fanout metrics not recorded")
	}
	itemsSum := items.Data.(metricdata.Histogram[int64]).DataPoints[0].Sum
	workersSum := workers.Data.(metricdata.Histogram[int64]).DataPoints[0].Sum
	if itemsSum != 5 {
		t.Errorf("items: got %d, want 5", itemsSum)
	}
	if workersSum != 2 {
		t.Errorf("workers: got %d, want 2", workersSum)
	}
}
