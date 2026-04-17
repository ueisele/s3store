package s3parquet

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ueisele/s3store/internal/core"
)

// Config defines how a Store is set up. T is the record type,
// which must be encodable and decodable by parquet-go directly
// (struct fields tagged with `parquet:"..."`, primitive-friendly
// types). Types with fields parquet-go can't encode (e.g.
// decimal.Decimal, custom wrappers) need a companion
// parquet-layout struct and a translation step in the caller's
// package.
type Config[T any] struct {
	// Bucket is the S3 bucket name.
	Bucket string

	// Prefix under which data files are stored.
	Prefix string

	// KeyParts defines the Hive-partition key segments in order.
	KeyParts []string

	// S3Client is the AWS S3 client to use. Its endpoint, region,
	// credentials, and path-style setting are used as-is.
	S3Client *s3.Client

	// PartitionKeyOf extracts the Hive-partition key from a
	// record. Required for Write(). The returned string must
	// conform to the KeyParts layout ("part=value/part=value").
	PartitionKeyOf func(T) string

	// SettleWindow is how far behind the stream tip Poll and
	// PollRecords read. Default: 5s.
	SettleWindow time.Duration

	// EntityKeyOf returns the logical entity identifier for a
	// record. When non-nil, Read and PollRecords deduplicate to
	// the record with the maximum VersionOf per entity. When
	// nil, every record is returned (pure stream semantics).
	EntityKeyOf func(T) string

	// VersionOf returns the monotonic version of a record for
	// dedup ordering. The library passes the source file's
	// write time in insertedAt — useful as a fallback when the
	// record has no domain-level version, or combine it with a
	// business timestamp for hybrid strategies.
	//
	// Nil defaults to DefaultVersionOf (wrote-last-wins). The
	// default is assigned inside New() when EntityKeyOf is
	// also set, so dedupEnabled only checks EntityKeyOf.
	VersionOf func(record T, insertedAt time.Time) int64
}

// DefaultVersionOf returns insertedAt in microseconds. Assigned
// to Config.VersionOf inside New() when that field is nil and
// EntityKeyOf is set; also exported so users can reference the
// wrote-last-wins default explicitly in their config.
func DefaultVersionOf[T any](_ T, insertedAt time.Time) int64 {
	return insertedAt.UnixMicro()
}

func (c Config[T]) settleWindow() time.Duration {
	if c.SettleWindow > 0 {
		return c.SettleWindow
	}
	return 5 * time.Second
}

// dedupEnabled reports whether latest-per-entity dedup applies.
// Gated solely on EntityKeyOf: New() populates VersionOf with
// DefaultVersionOf when the user leaves it nil, so by the time
// a Store exists the VersionOf field is always callable if
// EntityKeyOf is set.
func (c Config[T]) dedupEnabled() bool {
	return c.EntityKeyOf != nil
}

// Store is the pure-Go entry point to an s3store.
type Store[T any] struct {
	cfg      Config[T]
	s3       *s3.Client
	dataPath string
	refPath  string
}

// New constructs a Store. Validates required config fields.
func New[T any](cfg Config[T]) (*Store[T], error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("s3parquet: Bucket is required")
	}
	if cfg.Prefix == "" {
		return nil, fmt.Errorf("s3parquet: Prefix is required")
	}
	if cfg.S3Client == nil {
		return nil, fmt.Errorf("s3parquet: S3Client is required")
	}
	if err := core.ValidateKeyParts(cfg.KeyParts); err != nil {
		return nil, err
	}
	// Default VersionOf when the user asked for dedup
	// (EntityKeyOf set) but didn't tell us how to compare
	// versions. Wrote-last-wins is the natural zero-config
	// behaviour for append-only storage.
	if cfg.EntityKeyOf != nil && cfg.VersionOf == nil {
		cfg.VersionOf = DefaultVersionOf[T]
	}
	return &Store[T]{
		cfg:      cfg,
		s3:       cfg.S3Client,
		dataPath: core.DataPath(cfg.Prefix),
		refPath:  core.RefPath(cfg.Prefix),
	}, nil
}

// Close releases resources. Pure-Go Store holds no persistent
// connections — Close is a no-op but present for API symmetry
// with s3sql.Store and for future-proofing.
func (s *Store[T]) Close() error { return nil }
