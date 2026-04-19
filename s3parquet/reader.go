package s3parquet

import (
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// reader is the internal read-side half of a Store, holding the
// state that Read / Poll / PollRecords / PollRecordsAll /
// OffsetAt need. Stage 1 keeps it unexported; Stage 2 promotes
// it to a public Reader[T] with its own narrower Config and a
// ReaderExtras[T] for Writer-derived construction.
type reader[T any] struct {
	cfg      Config[T]
	s3       *s3.Client
	dataPath string
	refPath  string

	// insertedAtFieldIndex is the reflect struct-field path for
	// Config.InsertedAtField, resolved once at New() so the hot
	// path doesn't reparse the type. nil when unset.
	insertedAtFieldIndex []int
}

// getObjectBytes downloads a single object into memory. Used by
// Read and PollRecords; avoids the per-request overhead of
// setting up a ranged io.ReaderAt in exchange for buffering the
// whole file.
func (r *reader[T]) getObjectBytes(
	ctx context.Context, key string,
) ([]byte, error) {
	resp, err := r.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(r.cfg.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}
