// Package refstream implements the S3-backed ref-stream
// operations shared between s3parquet and s3sql. Neither Poll
// nor OffsetAt needs the per-package record type T, so pulling
// them out of each typed Store into one place removes ~75 lines
// of duplication while keeping the per-package Store's read
// path (PollRecords) — which genuinely differs — untouched.
//
// Errors returned here are unadorned; callers wrap once with
// their own "s3xxx:" package prefix.
package refstream

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"
	"github.com/ueisele/s3store/internal/core"
)

// s3ListMaxKeys is the per-request page-size cap enforced by S3.
const s3ListMaxKeys int32 = 1000

// ErrDisabled is the shared sentinel returned by the stream
// methods (Poll / PollRecords / PollRecordsAll) when the
// enclosing Store was configured with DisableRefStream. Lives
// here so both s3parquet and s3sql re-export the same value,
// letting callers write errors.Is(err, …) without caring which
// sub-package produced the error.
var ErrDisabled = errors.New(
	"ref stream disabled on this Store; " +
		"Poll/PollRecords/PollRecordsAll unavailable")

// PollAllBatch is the inner batch size used by PollAll. Tuned
// for S3 LIST page size so the inner paginator does one LIST
// per iteration at steady state.
const PollAllBatch int32 = 1000

// PollOpts bundles Poll's parameters. Avoids a 7-arg positional
// function signature; each consumer unpacks its config + query
// options into this struct.
type PollOpts struct {
	Bucket       string
	RefPath      string
	DataPath     string
	Since        core.Offset
	MaxEntries   int32
	Until        core.Offset   // empty = no upper bound (reads up to the settle cutoff)
	SettleWindow time.Duration // 0 means read to live tip (use with care)

	// ConsistencyControl, when non-empty, is sent as the
	// Consistency-Control HTTP header on the ref-LIST. Matches the
	// writer's ConsistencyControl so the LIST linearizes with the
	// ref PUT under StorageGRID strong-global / strong-site. Empty
	// sends no header (correct on AWS S3 / MinIO; relies on bucket-
	// default consistency on StorageGRID). Ignored by backends that
	// don't honour the header.
	ConsistencyControl string
}

// Poll returns up to MaxEntries stream entries after Since, up
// to now - SettleWindow and (if non-empty) strictly before Until.
// One or more S3 LIST calls; no GETs.
//
// The returned Offset is either the last entry's key (so the
// caller advances) or Since unchanged (no new entries). Caller
// wraps any error with their package prefix — this function
// returns unadorned errors.
func Poll(
	ctx context.Context,
	s3Client *s3.Client,
	opts PollOpts,
) ([]core.StreamEntry, core.Offset, error) {
	if opts.MaxEntries <= 0 {
		return nil, opts.Since, errors.New("maxEntries must be > 0")
	}

	cutoffPrefix := core.RefCutoff(
		opts.RefPath, time.Now(), opts.SettleWindow)

	pageSize := min(opts.MaxEntries, s3ListMaxKeys)

	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(opts.Bucket),
		Prefix:  aws.String(opts.RefPath + "/"),
		MaxKeys: aws.Int32(pageSize),
	}
	if opts.Since != "" {
		input.StartAfter = aws.String(string(opts.Since))
	}

	// Lazy allocation: grow via append rather than pre-sizing to
	// MaxEntries. Avoids wasted capacity for callers that pass a
	// large cap but typically get far fewer entries.
	var entries []core.StreamEntry
	var lastKey string

	paginator := s3.NewListObjectsV2Paginator(s3Client, input)

	// Per-call APIOptions install the Consistency-Control middleware
	// when the caller asked for it. Empty → nil → no middleware →
	// unchanged behaviour on AWS S3 / MinIO.
	var apiOpts []func(*middleware.Stack) error
	if opts.ConsistencyControl != "" {
		apiOpts = []func(*middleware.Stack) error{
			core.AddHeaderMiddleware(
				"Consistency-Control", opts.ConsistencyControl),
		}
	}

outer:
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx, func(o *s3.Options) {
			o.APIOptions = append(o.APIOptions, apiOpts...)
		})
		if err != nil {
			return nil, opts.Since,
				fmt.Errorf("list refs: %w", err)
		}
		for _, obj := range page.Contents {
			if int32(len(entries)) >= opts.MaxEntries {
				break outer
			}
			objKey := aws.ToString(obj.Key)
			if objKey > cutoffPrefix {
				break outer
			}
			if opts.Until != "" && objKey >= string(opts.Until) {
				break outer
			}
			key, _, id, dataTsMicros, err := core.ParseRefKey(objKey)
			if err != nil {
				return nil, opts.Since,
					fmt.Errorf("parse ref: %w", err)
			}
			entries = append(entries, core.StreamEntry{
				Offset:     core.Offset(objKey),
				Key:        key,
				DataPath:   core.BuildDataFilePath(opts.DataPath, key, id),
				RefPath:    objKey,
				InsertedAt: time.UnixMicro(dataTsMicros),
			})
			lastKey = objKey
		}
	}

	if lastKey != "" {
		return entries, core.Offset(lastKey), nil
	}
	return nil, opts.Since, nil
}

// OffsetAt returns the stream offset corresponding to wall-clock
// time t against refPath. Pure computation — no S3 call.
//
// Internally encodes via core.RefCutoff with a zero settle
// window, so any ref written at or after t sorts >= the returned
// offset and any ref written before t sorts <.
func OffsetAt(refPath string, t time.Time) core.Offset {
	return core.Offset(core.RefCutoff(refPath, t, 0))
}

// PollBatch is the per-iteration callback for PollAll. Each
// consumer provides its own implementation — s3parquet decodes
// parquet files in parallel, s3sql runs a DuckDB query — so the
// callback surface keeps PollAll agnostic to T's decode path.
type PollBatch[T any] func(
	ctx context.Context,
	since core.Offset,
	maxEntries int32,
) ([]T, core.Offset, error)

// PollAll reads every T produced by repeated poll calls until
// one returns an empty batch, concatenating the results. The
// caller is responsible for encoding any upper bound (Until) in
// the closure's captured options — PollAll trusts "empty batch"
// as the termination signal.
func PollAll[T any](
	ctx context.Context,
	since core.Offset,
	poll PollBatch[T],
) ([]T, error) {
	var all []T
	for {
		batch, next, err := poll(ctx, since, PollAllBatch)
		if err != nil {
			return nil, err
		}
		if len(batch) == 0 {
			return all, nil
		}
		all = append(all, batch...)
		since = next
	}
}
