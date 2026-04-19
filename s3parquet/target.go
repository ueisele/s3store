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
	"github.com/ueisele/s3store/internal/core"
)

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

	// SettleWindow is how far behind the live tip Poll, PollRecords,
	// and Index.Lookup read. Default (zero): 5s. Keeps readers
	// consistent with near-tip writers whose refs may not yet be
	// visible in S3 LIST.
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

// validate runs the full check for constructors that operate on
// partitioned data: Bucket, Prefix, S3Client, PartitionKeyParts.
// Used by NewWriter, NewReader, and BackfillIndex — anything that
// reads/writes data files keyed by partition.
func (t S3Target) validate() error {
	if err := t.validateLookup(); err != nil {
		return err
	}
	return core.ValidatePartitionKeyParts(t.PartitionKeyParts)
}

// validateLookup is the reduced check for constructors that only
// LIST / GET / PUT under a known prefix (no partition-key
// predicates): Bucket, Prefix, S3Client. Used by NewIndex —
// Lookup walks the <Prefix>/_index/<name>/ subtree, which is
// keyed by the index's own Columns, not the Target's
// PartitionKeyParts. A read-only analytics service can pass a
// minimally-populated S3Target and still build a working Index.
func (t S3Target) validateLookup() error {
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

// settleWindow returns the effective window, defaulting the zero
// value to 5s so near-tip readers don't race writers that haven't
// yet shown up in LIST.
func (t S3Target) settleWindow() time.Duration {
	if t.SettleWindow > 0 {
		return t.SettleWindow
	}
	return 5 * time.Second
}

// get downloads a single object into memory. Used by the read
// path (Read, PollRecords) and by BackfillIndex when scanning
// historical parquet data.
func (t S3Target) get(ctx context.Context, key string) ([]byte, error) {
	resp, err := t.S3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(t.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// put uploads data under key. Used by the write path (parquet +
// ref + markers) and by BackfillIndex for retroactive marker
// emission.
func (t S3Target) put(
	ctx context.Context, key string, data []byte, contentType string,
) error {
	_, err := t.S3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(t.Bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String(contentType),
	})
	return err
}

// exists reports whether an object exists, mapping S3's NotFound
// to (false, nil) so callers can distinguish "missing" from real
// failures without pattern-matching the error at every site.
func (t S3Target) exists(ctx context.Context, key string) (bool, error) {
	_, err := t.S3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(t.Bucket),
		Key:    aws.String(key),
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
	_, err := t.S3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(t.Bucket),
		Key:    aws.String(key),
	})
	return err
}

// list returns a paginator over objects under a prefix. Callers
// iterate via HasMorePages + NextPage; no in-memory accumulation
// here — large prefixes must stream.
func (t S3Target) list(prefix string) *s3.ListObjectsV2Paginator {
	return s3.NewListObjectsV2Paginator(
		t.S3Client, &s3.ListObjectsV2Input{
			Bucket: aws.String(t.Bucket),
			Prefix: aws.String(prefix),
		})
}
