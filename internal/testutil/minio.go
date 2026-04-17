//go:build integration

// Package testutil provides shared integration-test helpers:
// launching a MinIO container, creating an S3 client pointed at
// it, and cleanly terminating it. Gated on the `integration`
// build tag so the testcontainers dependency never ends up in
// non-test builds.
package testutil

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go"
	tcminio "github.com/testcontainers/testcontainers-go/modules/minio"
)

// Fixture bundles a running MinIO container with a ready-to-use
// S3 client and a pre-created bucket.
type Fixture struct {
	container *tcminio.MinioContainer
	HostPort  string // "host:port", no scheme
	Username  string
	Password  string
	Bucket    string
	S3Client  *s3.Client
}

// Start launches MinIO, creates an AWS SDK client configured to
// talk to it (BaseEndpoint, path-style addressing, static
// credentials), and creates the named bucket. Returns a
// Fixture whose Close must be called to terminate the
// container.
//
// Disables testcontainers' ryuk reaper sidecar, which fails to
// bind its port on recent Docker Desktop versions. Cleanup goes
// through the Fixture's Close instead.
func Start(ctx context.Context, bucket string) (*Fixture, error) {
	os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")

	container, err := tcminio.Run(ctx, "minio/minio:latest")
	if err != nil {
		return nil, fmt.Errorf("minio run: %w", err)
	}
	cleanup := func() {
		_ = testcontainers.TerminateContainer(container)
	}

	connURL, err := container.ConnectionString(ctx)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("minio connection string: %w", err)
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				container.Username, container.Password, "")),
	)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("aws config: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://" + connURL)
		o.UsePathStyle = true
	})

	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}); err != nil {
		cleanup()
		return nil, fmt.Errorf("create bucket: %w", err)
	}

	return &Fixture{
		container: container,
		HostPort:  connURL,
		Username:  container.Username,
		Password:  container.Password,
		Bucket:    bucket,
		S3Client:  client,
	}, nil
}

// Close terminates the MinIO container.
func (f *Fixture) Close() error {
	return testcontainers.TerminateContainer(f.container)
}

// DuckDBCredentials returns the SET statements needed for
// DuckDB's httpfs extension to authenticate against this MinIO
// fixture. Endpoint, region, URL style, and use_ssl are
// auto-derived by s3sql from S3Client.Options().
func (f *Fixture) DuckDBCredentials() []string {
	return []string{
		fmt.Sprintf("SET s3_access_key_id='%s'", f.Username),
		fmt.Sprintf("SET s3_secret_access_key='%s'", f.Password),
	}
}
