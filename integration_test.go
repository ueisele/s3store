//go:build integration

package s3store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go"
	tcminio "github.com/testcontainers/testcontainers-go/modules/minio"
)

// Shared MinIO fixture: started once in TestMain, reused by
// every integration test via unique prefixes for isolation.
var (
	minioHostPort string
	minioUser     string
	minioPass     string
	s3Client      *s3.Client
	bucketName    = "s3store-it"
	prefixCounter atomic.Int64
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	// Disable testcontainers' ryuk reaper sidecar — it fails
	// to bind its port on recent Docker Desktop versions, and
	// we already clean up the MinIO container via defer below.
	os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")

	container, err := tcminio.Run(ctx, "minio/minio:latest")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start MinIO: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		_ = testcontainers.TerminateContainer(container)
	}()

	connURL, err := container.ConnectionString(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get MinIO endpoint: %v\n", err)
		os.Exit(1)
	}
	minioHostPort = connURL // host:port

	minioUser = container.Username
	minioPass = container.Password

	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				minioUser, minioPass, "")),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to build AWS config: %v\n", err)
		os.Exit(1)
	}
	s3Client = s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://" + minioHostPort)
		o.UsePathStyle = true
	})

	if _, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	}); err != nil {
		fmt.Fprintf(os.Stderr, "failed to create bucket: %v\n", err)
		os.Exit(1)
	}

	os.Exit(m.Run())
}

// IntRecord is the record type used by integration tests.
// Lives outside store_test.go's testRecord so we can use
// different tag names for schema-evolution scenarios.
type IntRecord struct {
	Period   string    `parquet:"period"`
	Customer string    `parquet:"customer"`
	SKU      string    `parquet:"sku"`
	Amount   float64   `parquet:"amount"`
	Currency string    `parquet:"currency"`
	Ts       time.Time `parquet:"ts,timestamp(millisecond)"`
}

func scanIntRecord(rows *sql.Rows) (IntRecord, error) {
	var r IntRecord
	err := rows.Scan(
		&r.Period, &r.Customer, &r.SKU,
		&r.Amount, &r.Currency, &r.Ts)
	return r, err
}

// newStore creates a Store on a unique prefix so tests don't
// collide in the shared bucket. `dedupBy` can be empty to
// accept the default (KeyParts-based) dedup.
func newStore(
	t *testing.T, versionCol string, dedupBy ...string,
) *Store[IntRecord] {
	t.Helper()
	prefix := fmt.Sprintf("it-%d-%d",
		time.Now().UnixNano(), prefixCounter.Add(1))

	store, err := New[IntRecord](Config[IntRecord]{
		Bucket:        bucketName,
		Prefix:        prefix,
		S3Client:      s3Client,
		KeyParts:      []string{"period", "customer"},
		TableAlias:    "records",
		VersionColumn: versionCol,
		DeduplicateBy: dedupBy,
		SettleWindow:  10 * time.Millisecond,
		KeyFunc: func(r IntRecord) string {
			return fmt.Sprintf(
				"period=%s/customer=%s",
				r.Period, r.Customer)
		},
		ScanFunc:   scanIntRecord,
		S3Endpoint: minioHostPort,
		ExtraInitSQL: []string{
			"SET s3_use_ssl=false",
			fmt.Sprintf("SET s3_access_key_id='%s'", minioUser),
			fmt.Sprintf("SET s3_secret_access_key='%s'", minioPass),
			"SET s3_region='us-east-1'",
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestIntegration_WriteAndRead(t *testing.T) {
	ctx := context.Background()
	// Dedup by (period, customer, sku) so multiple SKUs in one
	// partition stay as distinct rows.
	store := newStore(t, "ts", "period", "customer", "sku")

	now := time.Now().UTC().Truncate(time.Millisecond)
	records := []IntRecord{
		{Period: "2026-03-17", Customer: "alpha", SKU: "x", Amount: 10, Currency: "EUR", Ts: now},
		{Period: "2026-03-17", Customer: "alpha", SKU: "y", Amount: 20, Currency: "EUR", Ts: now},
		{Period: "2026-03-17", Customer: "beta", SKU: "x", Amount: 5, Currency: "EUR", Ts: now},
		{Period: "2026-03-18", Customer: "alpha", SKU: "x", Amount: 30, Currency: "EUR", Ts: now},
	}
	if _, err := store.Write(ctx, records); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Exact key
	got, err := store.Read(ctx, "period=2026-03-17/customer=alpha")
	if err != nil {
		t.Fatalf("Read exact: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("exact key: got %d records, want 2", len(got))
	}

	// Glob across customers
	got, err = store.Read(ctx, "period=2026-03-17/*")
	if err != nil {
		t.Fatalf("Read glob customers: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("period glob: got %d records, want 3", len(got))
	}

	// Glob across periods
	got, err = store.Read(ctx, "period=2026-03-*/customer=alpha")
	if err != nil {
		t.Fatalf("Read glob periods: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("period value-glob: got %d records, want 3", len(got))
	}

	// Everything
	got, err = store.Read(ctx, "*")
	if err != nil {
		t.Fatalf("Read all: %v", err)
	}
	if len(got) != 4 {
		t.Errorf("read all: got %d records, want 4", len(got))
	}
}

func TestIntegration_ReadDeduplication(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, "ts")

	now := time.Now().UTC().Truncate(time.Millisecond)

	// First version
	if _, err := store.Write(ctx, []IntRecord{
		{Period: "2026-03-17", Customer: "alpha", SKU: "x", Amount: 10, Currency: "EUR", Ts: now},
	}); err != nil {
		t.Fatalf("Write v1: %v", err)
	}

	// Recalculation, newer ts
	if _, err := store.Write(ctx, []IntRecord{
		{Period: "2026-03-17", Customer: "alpha", SKU: "x", Amount: 99, Currency: "EUR", Ts: now.Add(1 * time.Second)},
	}); err != nil {
		t.Fatalf("Write v2: %v", err)
	}

	got, err := store.Read(ctx, "period=2026-03-17/customer=alpha")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("dedup: got %d records, want 1", len(got))
	}
	if got[0].Amount != 99 {
		t.Errorf("dedup: got amount %v, want 99 (newest)", got[0].Amount)
	}
}

func TestIntegration_StreamRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, "ts")

	now := time.Now().UTC().Truncate(time.Millisecond)
	input := []IntRecord{
		{Period: "2026-03-17", Customer: "alpha", SKU: "x", Amount: 10, Currency: "EUR", Ts: now},
		{Period: "2026-03-17", Customer: "beta", SKU: "y", Amount: 20, Currency: "EUR", Ts: now},
	}
	if _, err := store.Write(ctx, input); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Settle window is tiny, but still needs to pass.
	time.Sleep(50 * time.Millisecond)

	entries, _, err := store.Poll(ctx, "", 100)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(entries) != 2 {
		t.Errorf("Poll: got %d entries, want 2", len(entries))
	}

	// Stream mode (default): every record in file order, no dedup.
	records, _, err := store.PollRecords(ctx, "", 100)
	if err != nil {
		t.Fatalf("PollRecords stream: %v", err)
	}
	if len(records) != 2 {
		t.Errorf("PollRecords stream: got %d records, want 2", len(records))
	}
}

func TestIntegration_PollMaxEntries(t *testing.T) {
	// Regression for #1: maxEntries must be a true cap on
	// total records returned, not just the per-page size.
	ctx := context.Background()
	store := newStore(t, "ts")

	now := time.Now().UTC().Truncate(time.Millisecond)
	batch := make([]IntRecord, 0, 25)
	for i := 0; i < 25; i++ {
		batch = append(batch, IntRecord{
			Period:   "2026-03-17",
			Customer: fmt.Sprintf("c%03d", i),
			SKU:      "x",
			Amount:   float64(i),
			Currency: "EUR",
			Ts:       now,
		})
	}
	if _, err := store.Write(ctx, batch); err != nil {
		t.Fatalf("Write: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	entries, _, err := store.Poll(ctx, "", 5)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(entries) != 5 {
		t.Errorf("Poll maxEntries: got %d, want 5 (cap violated)", len(entries))
	}
}

func TestIntegration_PollRejectsMaxZero(t *testing.T) {
	// Regression for #1: maxEntries <= 0 errors explicitly.
	ctx := context.Background()
	store := newStore(t, "ts")

	if _, _, err := store.Poll(ctx, "", 0); err == nil {
		t.Error("Poll(..., 0): expected error")
	}
}

func TestIntegration_PollRecordsCompacted(t *testing.T) {
	// Regression for #6: WithCompaction dedupes latest-per-key
	// within the batch.
	ctx := context.Background()
	store := newStore(t, "ts")

	now := time.Now().UTC().Truncate(time.Millisecond)
	// Two versions of the same key in the same batch.
	if _, err := store.Write(ctx, []IntRecord{
		{Period: "2026-03-17", Customer: "alpha", SKU: "x", Amount: 10, Currency: "EUR", Ts: now},
	}); err != nil {
		t.Fatalf("Write v1: %v", err)
	}
	if _, err := store.Write(ctx, []IntRecord{
		{Period: "2026-03-17", Customer: "alpha", SKU: "x", Amount: 99, Currency: "EUR", Ts: now.Add(1 * time.Second)},
	}); err != nil {
		t.Fatalf("Write v2: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// Stream mode returns both.
	stream, _, err := store.PollRecords(ctx, "", 100)
	if err != nil {
		t.Fatalf("PollRecords stream: %v", err)
	}
	if len(stream) != 2 {
		t.Errorf("stream mode: got %d records, want 2", len(stream))
	}

	// Compacted mode returns only the latest version.
	compact, _, err := store.PollRecords(ctx, "", 100, WithCompaction())
	if err != nil {
		t.Fatalf("PollRecords compacted: %v", err)
	}
	if len(compact) != 1 {
		t.Fatalf("compacted: got %d records, want 1", len(compact))
	}
	if compact[0].Amount != 99 {
		t.Errorf("compacted: got amount %v, want 99", compact[0].Amount)
	}
}

func TestIntegration_CompactionRequiresVersionColumn(t *testing.T) {
	// Regression for #6: WithCompaction errors when
	// VersionColumn is not set, instead of silently no-oping.
	ctx := context.Background()
	store := newStore(t, "") // no version column

	_, _, err := store.PollRecords(ctx, "", 100, WithCompaction())
	if err == nil {
		t.Error("expected error for WithCompaction without VersionColumn")
	}
}

func TestIntegration_SchemaEvolution_DefaultOnMissingColumn(t *testing.T) {
	// Regression for #7: ColumnDefault on a column that no
	// file has yet must materialize the column with the
	// default literal (not fail at plan time).
	ctx := context.Background()

	type v1 struct {
		Period   string    `parquet:"period"`
		Customer string    `parquet:"customer"`
		Amount   float64   `parquet:"amount"`
		Ts       time.Time `parquet:"ts,timestamp(millisecond)"`
	}
	// Scan order matches the SQL output, not the struct
	// field order: additions for missing columns ('EUR' AS
	// currency) are appended at the end of SELECT *.
	type v2 struct {
		Period   string
		Customer string
		Amount   float64
		Ts       time.Time
		Currency string
	}

	prefix := fmt.Sprintf("it-evo-%d", time.Now().UnixNano())

	commonConfig := func(prefix string) []string {
		return []string{
			"SET s3_use_ssl=false",
			fmt.Sprintf("SET s3_access_key_id='%s'", minioUser),
			fmt.Sprintf("SET s3_secret_access_key='%s'", minioPass),
			"SET s3_region='us-east-1'",
		}
	}

	// Write a v1 file (no currency column).
	writer, err := New[v1](Config[v1]{
		Bucket:     bucketName,
		Prefix:     prefix,
		S3Client:   s3Client,
		KeyParts:   []string{"period", "customer"},
		TableAlias: "t",
		KeyFunc: func(r v1) string {
			return fmt.Sprintf("period=%s/customer=%s",
				r.Period, r.Customer)
		},
		ScanFunc: func(rows *sql.Rows) (v1, error) {
			var r v1
			return r, rows.Scan(&r.Period, &r.Customer, &r.Amount, &r.Ts)
		},
		S3Endpoint:   minioHostPort,
		ExtraInitSQL: commonConfig(prefix),
	})
	if err != nil {
		t.Fatalf("New v1: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	if _, err := writer.Write(ctx, []v1{
		{Period: "2026-03-17", Customer: "alpha", Amount: 10, Ts: now},
	}); err != nil {
		t.Fatalf("Write v1: %v", err)
	}
	_ = writer.Close()

	// Read as v2 with a ColumnDefault for the missing column.
	reader, err := New[v2](Config[v2]{
		Bucket:     bucketName,
		Prefix:     prefix,
		S3Client:   s3Client,
		KeyParts:   []string{"period", "customer"},
		TableAlias: "t",
		ColumnDefaults: map[string]string{
			"currency": "'EUR'",
		},
		KeyFunc: func(r v2) string {
			return fmt.Sprintf("period=%s/customer=%s",
				r.Period, r.Customer)
		},
		ScanFunc: func(rows *sql.Rows) (v2, error) {
			var r v2
			return r, rows.Scan(
				&r.Period, &r.Customer, &r.Amount, &r.Ts, &r.Currency)
		},
		S3Endpoint:   minioHostPort,
		ExtraInitSQL: commonConfig(prefix),
	})
	if err != nil {
		t.Fatalf("New v2: %v", err)
	}
	defer reader.Close()

	got, err := reader.Read(ctx, "period=2026-03-17/customer=alpha")
	if err != nil {
		t.Fatalf("Read v2: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d records, want 1", len(got))
	}
	if got[0].Currency != "EUR" {
		t.Errorf("currency: got %q, want EUR (default)", got[0].Currency)
	}
}

func TestIntegration_SchemaEvolution_AliasChain(t *testing.T) {
	// ColumnAliases: new file has `amount`, older files might
	// have `value`. Reader wants a single `amount` column.
	ctx := context.Background()

	type recOld struct {
		Period   string    `parquet:"period"`
		Customer string    `parquet:"customer"`
		Value    float64   `parquet:"value"`
		Ts       time.Time `parquet:"ts,timestamp(millisecond)"`
	}
	// Scan order matches the SQL output: `value` is excluded
	// by the alias transform, `amount` is appended at the end.
	type recNew struct {
		Period   string
		Customer string
		Ts       time.Time
		Amount   float64
	}

	prefix := fmt.Sprintf("it-alias-%d", time.Now().UnixNano())

	writerOld, err := New[recOld](Config[recOld]{
		Bucket:     bucketName,
		Prefix:     prefix,
		S3Client:   s3Client,
		KeyParts:   []string{"period", "customer"},
		TableAlias: "t",
		KeyFunc: func(r recOld) string {
			return fmt.Sprintf("period=%s/customer=%s",
				r.Period, r.Customer)
		},
		ScanFunc: func(rows *sql.Rows) (recOld, error) {
			var r recOld
			return r, rows.Scan(&r.Period, &r.Customer, &r.Value, &r.Ts)
		},
		S3Endpoint: minioHostPort,
		ExtraInitSQL: []string{
			"SET s3_use_ssl=false",
			fmt.Sprintf("SET s3_access_key_id='%s'", minioUser),
			fmt.Sprintf("SET s3_secret_access_key='%s'", minioPass),
			"SET s3_region='us-east-1'",
		},
	})
	if err != nil {
		t.Fatalf("New old: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	if _, err := writerOld.Write(ctx, []recOld{
		{Period: "2026-03-17", Customer: "alpha", Value: 10, Ts: now},
	}); err != nil {
		t.Fatalf("Write old: %v", err)
	}
	_ = writerOld.Close()

	// Reader projects both generations into the new name.
	reader, err := New[recNew](Config[recNew]{
		Bucket:     bucketName,
		Prefix:     prefix,
		S3Client:   s3Client,
		KeyParts:   []string{"period", "customer"},
		TableAlias: "t",
		ColumnAliases: map[string][]string{
			"amount": {"value"},
		},
		KeyFunc: func(r recNew) string {
			return fmt.Sprintf("period=%s/customer=%s",
				r.Period, r.Customer)
		},
		ScanFunc: func(rows *sql.Rows) (recNew, error) {
			var r recNew
			return r, rows.Scan(&r.Period, &r.Customer, &r.Ts, &r.Amount)
		},
		S3Endpoint: minioHostPort,
		ExtraInitSQL: []string{
			"SET s3_use_ssl=false",
			fmt.Sprintf("SET s3_access_key_id='%s'", minioUser),
			fmt.Sprintf("SET s3_secret_access_key='%s'", minioPass),
			"SET s3_region='us-east-1'",
		},
	})
	if err != nil {
		t.Fatalf("New new: %v", err)
	}
	defer reader.Close()

	got, err := reader.Read(ctx, "period=2026-03-17/customer=alpha")
	if err != nil {
		t.Fatalf("Read new: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d records, want 1", len(got))
	}
	if got[0].Amount != 10 {
		t.Errorf("amount: got %v, want 10 (from old.value via alias)", got[0].Amount)
	}
}

func TestIntegration_TruncatedPatternErrors(t *testing.T) {
	// Regression for #9: truncated key patterns must error
	// explicitly instead of silently returning zero rows.
	ctx := context.Background()
	store := newStore(t, "ts")

	now := time.Now().UTC().Truncate(time.Millisecond)
	if _, err := store.Write(ctx, []IntRecord{
		{Period: "2026-03-17", Customer: "alpha", SKU: "x", Amount: 10, Currency: "EUR", Ts: now},
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	_, err := store.Read(ctx, "period=2026-03-17")
	if err == nil {
		t.Error("expected error for truncated pattern")
	}
}

func TestIntegration_RefKeyParseSpecialChars(t *testing.T) {
	// Regression for #3: partition values with `--`, `;`,
	// `/`, and percent round-trip through encode/parse/Poll.
	ctx := context.Background()
	store := newStore(t, "ts")

	now := time.Now().UTC().Truncate(time.Millisecond)
	// Customer values containing tricky characters
	for _, customer := range []string{"foo--bar", "semi;colon", "50%off"} {
		if _, err := store.Write(ctx, []IntRecord{
			{Period: "2026-03-17", Customer: customer, SKU: "x", Amount: 10, Currency: "EUR", Ts: now},
		}); err != nil {
			t.Fatalf("Write customer=%q: %v", customer, err)
		}
	}
	time.Sleep(50 * time.Millisecond)

	entries, _, err := store.Poll(ctx, "", 100)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("Poll: got %d entries, want 3", len(entries))
	}

	// Every returned Key must round-trip back to the exact
	// customer string we wrote.
	found := map[string]bool{
		"foo--bar":   false,
		"semi;colon": false,
		"50%off":     false,
	}
	for _, e := range entries {
		// Extract the customer value from the key
		for wanted := range found {
			if e.Key == "period=2026-03-17/customer="+wanted {
				found[wanted] = true
			}
		}
	}
	for k, ok := range found {
		if !ok {
			t.Errorf("customer %q not round-tripped: entries=%+v", k, entries)
		}
	}
}

func TestIntegration_QueryAggregation(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, "ts", "period", "customer", "sku")

	now := time.Now().UTC().Truncate(time.Millisecond)
	if _, err := store.Write(ctx, []IntRecord{
		{Period: "2026-03-17", Customer: "alpha", SKU: "x", Amount: 10, Currency: "EUR", Ts: now},
		{Period: "2026-03-17", Customer: "alpha", SKU: "y", Amount: 20, Currency: "EUR", Ts: now},
		{Period: "2026-03-17", Customer: "beta", SKU: "x", Amount: 5, Currency: "EUR", Ts: now},
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	rows, err := store.Query(ctx, "period=2026-03-17/*",
		"SELECT customer, SUM(amount) AS total FROM records "+
			"GROUP BY customer ORDER BY customer")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()

	type agg struct {
		customer string
		total    float64
	}
	var got []agg
	for rows.Next() {
		var a agg
		if err := rows.Scan(&a.customer, &a.total); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		got = append(got, a)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	want := []agg{
		{"alpha", 30},
		{"beta", 5},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d: %+v", len(got), len(want), got)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("row %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

// Below are helpers not used by any test yet; kept to silence
// potential unused-import lints when tests are trimmed.
var (
	_ = url.QueryEscape
	_ = strings.Contains
	_ = errors.New
)
