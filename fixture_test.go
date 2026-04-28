//go:build integration

package s3store

import (
	"context"
	"fmt"
	"strings"
	"sync"
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

// Integration-test fixture: one MinIO container per `go test`
// invocation, each test gets its own freshly-created bucket.
//
// Uses the pgsty/minio community fork (AGPLv3) since upstream
// minio/minio was archived in Feb 2026. The fork tracks the
// original code and ships the same image format, so the
// testcontainers MinIO module Just Works when pointed at a
// pgsty/minio:RELEASE... tag.
//
// Gated on the `integration` build tag so the testcontainers
// dependency never ends up in non-test builds.

const (
	// Pinned to a specific pgsty/minio release so integration-
	// test behavior doesn't drift. The fork publishes dated
	// RELEASE.YYYY-MM-DD tags mirroring upstream's scheme; bump
	// when a newer release lands or you need a relevant fix.
	minioImage    = "pgsty/minio:RELEASE.2026-04-17T00-00-00Z"
	minioUsername = "minioadmin"
	minioPassword = "minioadmin"
)

type sharedMinio struct {
	client   *s3.Client
	hostPort string
}

//nolint:gochecknoglobals // test-only singleton
var (
	sharedOnce    sync.Once
	shared        *sharedMinio
	sharedErr     error
	bucketCounter atomic.Int64
)

// shareMinio lazily starts (or joins) the MinIO container.
// Reuse is keyed by testcontainers.SessionID so every test
// binary spawned by the same `go test` invocation — even
// across packages — shares one physical container. Ryuk reaps
// it when the invocation ends, so there is no explicit
// terminate.
//
// On Docker Desktop setups where Ryuk fails to bind its port
// (a known issue with some versions), the reused container
// persists between invocations. `docker rm -f s3store-minio-*`
// cleans them up manually. CI runners are typically ephemeral
// so this isn't a production concern.
func shareMinio(ctx context.Context) (*sharedMinio, error) {
	sharedOnce.Do(func() {
		container, err := tcminio.Run(ctx, minioImage,
			tcminio.WithUsername(minioUsername),
			tcminio.WithPassword(minioPassword),
			testcontainers.CustomizeRequestOption(
				func(req *testcontainers.GenericContainerRequest) error {
					req.Name = "s3store-minio-" + testcontainers.SessionID()
					req.Reuse = true
					return nil
				},
			),
		)
		if err != nil {
			sharedErr = fmt.Errorf("start MinIO: %w", err)
			return
		}
		connURL, err := container.ConnectionString(ctx)
		if err != nil {
			sharedErr = fmt.Errorf("get MinIO endpoint: %w", err)
			return
		}
		cfg, err := awsconfig.LoadDefaultConfig(ctx,
			awsconfig.WithRegion("us-east-1"),
			awsconfig.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider(
					minioUsername, minioPassword, "")),
		)
		if err != nil {
			sharedErr = fmt.Errorf("aws config: %w", err)
			return
		}
		client := s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.BaseEndpoint = aws.String("http://" + connURL)
			o.UsePathStyle = true
		})
		shared = &sharedMinio{client: client, hostPort: connURL}
	})
	return shared, sharedErr
}

// fixture carries an S3 client pointed at the shared MinIO
// container together with a freshly-created per-test bucket.
type fixture struct {
	S3Client *s3.Client
	HostPort string // "host:port"
	Bucket   string
}

// newFixture returns a fixture for the calling test. A fresh
// bucket is created for isolation; the underlying MinIO container
// is shared with every other test in the same `go test`
// invocation.
//
// No Close or TestMain needed: Ryuk terminates the container
// when the invocation ends.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx := t.Context()
	s, err := shareMinio(ctx)
	if err != nil {
		t.Fatalf("MinIO fixture: %v", err)
	}
	bucket := fmt.Sprintf("s3store-it-%d-%d",
		time.Now().UnixNano(), bucketCounter.Add(1))
	if _, err := s.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}); err != nil {
		t.Fatalf("create bucket %q: %v", bucket, err)
	}
	return &fixture{
		S3Client: s.client,
		HostPort: s.hostPort,
		Bucket:   bucket,
	}
}

// SeedTimingConfig PUTs the persisted timing-config objects at
// <prefix>/_config/commit-timeout and <prefix>/_config/max-clock-skew
// with the given values, so a subsequent NewS3Target / New(Config)
// call against this prefix finds them and stamps the resolved
// values (plus the derived SettleWindow) on the Target. Mirrors
// the boto3 snippet operators run once per dataset (see README's
// "Initializing a new dataset"). Tests pass values >= the floors
// (CommitTimeoutFloor = 50ms, MaxClockSkewFloor = 0); the parser
// rejects anything below.
func (f *fixture) SeedTimingConfig(
	t *testing.T, prefix string,
	commitTimeout, maxClockSkew time.Duration,
) {
	t.Helper()
	f.seedDurationConfig(t, prefix+"/_config/commit-timeout", commitTimeout)
	f.seedDurationConfig(t, prefix+"/_config/max-clock-skew", maxClockSkew)
}

// seedDurationConfig PUTs a single duration-valued config object.
// Used by SeedTimingConfig and by tests that need to seed only one
// of the two timing knobs (e.g. "missing the other" rejection
// tests).
func (f *fixture) seedDurationConfig(
	t *testing.T, key string, value time.Duration,
) {
	t.Helper()
	if _, err := f.S3Client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String(f.Bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(value.String()),
	}); err != nil {
		t.Fatalf("seed timing config %s/%s: %v", f.Bucket, key, err)
	}
}
