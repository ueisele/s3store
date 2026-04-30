package s3store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// s3ListMaxKeys is the per-request page-size cap enforced by S3.
const s3ListMaxKeys int32 = 1000

// Offset represents a position in the stream. Use the empty
// string Offset("") as the unbounded sentinel: as `since` it
// means stream head; as the upper bound (via WithUntilOffset)
// it means walk to the live tip (now - SettleWindow) as of
// the call. To keep up with new writes, call again from the
// last offset. ReadRangeIter takes time.Time bounds and uses
// time.Time{} as its own unbounded sentinel.
type Offset string

// StreamEntry is a lightweight ref returned by Poll.
//
//   - Offset and RefPath carry the same underlying S3 key string.
//     Offset is typed for cursor-advancing in Poll-style APIs;
//     RefPath exposes the same value as an explicit S3 object
//     path for callers that want to GET the ref directly. Mirrors
//     the Offset / RefPath split on WriteResult.
//   - Key is the Hive-style partition key ("period=X/customer=Y")
//     that the writer originally passed to WriteWithKey — useful
//     for consumers to route records by partition without parsing
//     DataPath.
//   - DataPath is the S3 object key of the data file this ref
//     points at; GET it to fetch the parquet payload.
//   - InsertedAt is the writer's wall-clock capture immediately
//     before the ref PUT (microsecond precision; same value
//     embedded as `refMicroTs` in the ref filename). It approximates
//     ref-LIST-visibility time. Slightly later than the
//     InsertedAtField parquet column (stamped at pre-encode
//     write-start so the column reflects logical record time, not
//     commit-finalize time); the two values can drift by the
//     encode + data-PUT duration.
//   - RowCount is the number of records in the data file this ref
//     points at, recovered from the token-commit's `rowcount`
//     user-metadata. Free for Poll to surface — the gate already
//     HEADs the marker per ref/token (cached per poll cycle), so
//     no extra round trip.
type StreamEntry struct {
	Offset     Offset
	Key        string
	DataPath   string
	RefPath    string
	InsertedAt time.Time
	RowCount   int64
}

// PollOption / pollOpts / WithUntilOffset live in reader_options.go.

// Poll returns up to maxEntries stream entries (refs only) after
// the given offset, capped at now - SettleWindow to avoid races
// with in-flight writes. One LIST call against the ref stream
// plus one HEAD per ref against `<token>.commit` (collapsed by a
// per-poll cache when refs share a token); no parquet GETs.
// Returns (entries, nextOffset, error); checkpoint nextOffset and
// pass it as `since` on the next call.
//
// Refs whose `<token>.commit` is missing (404) are skipped: by
// the time the ref clears the SettleWindow cutoff, the writer
// has either committed (200) or returned an error to its caller
// (no commit will land). Refs whose commit's `attemptid` doesn't
// match the ref's attempt-id are also skipped — they're orphans
// from a failed-mid-write retry under the same token.
//
// Pass WithUntilOffset to bound the walk from above so long
// streams aren't scanned past the window of interest.
//
// nextOffset advances over every ref the LIST visits, including
// ones the gate skips. Once a ref's refMicroTs is past the
// SettleWindow cutoff, its commit outcome is final — re-walking
// it on the next poll wouldn't surface anything new.
func (s *Reader[T]) Poll(
	ctx context.Context,
	since Offset,
	maxEntries int32,
	opts ...PollOption,
) (out []StreamEntry, nextOffset Offset, err error) {
	scope := s.cfg.Target.metrics.methodScope(ctx, methodPoll)
	defer func() {
		scope.addRecords(int64(len(out)))
		scope.end(&err)
	}()
	if maxEntries <= 0 {
		return nil, since, errors.New("maxEntries must be > 0")
	}

	var o pollOpts
	for _, opt := range opts {
		opt(&o)
	}

	cutoffPrefix := refCutoff(s.refPath, time.Now(),
		s.cfg.Target.SettleWindow())

	cache := newCommitCache()
	var lastKey string

	// listErr is the LIST/iteration error; gateErr is the first
	// commit-gate failure. We separate them so a gate failure
	// surfaces a wrapped error rather than a "list refs" prefix.
	var gateErr error

	listErr := s.cfg.Target.listEach(ctx,
		s.refPath+"/", string(since),
		min(maxEntries, s3ListMaxKeys),
		func(obj s3types.Object) (bool, error) {
			if int32(len(out)) >= maxEntries {
				return false, nil
			}
			objKey := aws.ToString(obj.Key)
			if objKey > cutoffPrefix {
				return false, nil
			}
			if o.until != "" && objKey >= string(o.until) {
				return false, nil
			}
			hiveKey, refMicroTs, token, attemptID, err := parseRefKey(objKey)
			if err != nil {
				// Malformed refs (externally written, or a future
				// schema this binary doesn't understand) shouldn't
				// break the consumer pipeline. Log via slog.Default
				// — applications inherit their configured handler —
				// and bump the s3store.read.malformed_refs counter
				// so silent drift stays observable, then skip.
				// Advance lastKey so the consumer's nextOffset moves
				// past the malformed entry rather than re-walking it.
				slog.Warn("s3store: skipping malformed ref",
					"key", objKey, "err", err)
				scope.recordMalformedRefs()
				lastKey = objKey
				return true, nil
			}

			entry, err := cache.lookupOrFetch(ctx, s.cfg.Target,
				s.dataPath, hiveKey, token, methodPoll)
			if err != nil {
				gateErr = err
				return false, err
			}
			// Always advance lastKey: refs past the cutoff have a
			// final commit outcome, so re-walking them on the next
			// poll would just re-issue the same HEADs.
			lastKey = objKey
			if !entry.exists {
				return true, nil
			}
			if entry.attemptID != attemptID {
				// Orphan from a failed-mid-write retry; canonical
				// attempt won the token-commit race.
				return true, nil
			}
			id := makeID(token, attemptID)
			out = append(out, StreamEntry{
				Offset:     Offset(objKey),
				Key:        hiveKey,
				DataPath:   buildDataFilePath(s.dataPath, hiveKey, id),
				RefPath:    objKey,
				InsertedAt: time.UnixMicro(refMicroTs),
				RowCount:   entry.rowCount,
			})
			return true, nil
		})
	if gateErr != nil {
		return nil, since, fmt.Errorf("gate ref by commit: %w", gateErr)
	}
	if listErr != nil {
		return nil, since, fmt.Errorf("list refs: %w", listErr)
	}

	if lastKey != "" {
		return out, Offset(lastKey), nil
	}
	return nil, since, nil
}

// PollRecords returns typed records from the files referenced by
// up to maxEntries refs after the offset, plus the next offset
// for checkpointing. Cursor-based, CDC-style: caller resumes from
// the returned offset on the next call.
//
// Replica-dedup only: records sharing (entity, version) collapse
// to one (rare retries / zombies); distinct versions of the same
// entity all flow through. Latest-per-entity dedup is NOT offered
// — meaningless on a cursor since the next batch may carry a newer
// version of the same entity. For latest-per-entity, use Read or
// ReadRangeIter (snapshot-style).
//
// Records follow ref order (= timestamp order), then parquet-file
// row order within each ref.
func (s *Reader[T]) PollRecords(
	ctx context.Context,
	since Offset,
	maxEntries int32,
	opts ...PollOption,
) (out []T, nextOffset Offset, err error) {
	scope := s.cfg.Target.metrics.methodScope(ctx, methodPollRecords)
	defer func() {
		scope.addRecords(int64(len(out)))
		scope.end(&err)
	}()
	entries, newOffset, err := s.Poll(ctx, since, maxEntries, opts...)
	if err != nil {
		return nil, since, err
	}
	if len(entries) == 0 {
		return nil, since, nil
	}

	// Carry each entry's InsertedAt (= dataTsMicros from the ref
	// filename) on the KeyMeta. Used as the fallback when the
	// reader has no InsertedAtField configured — same value the
	// writer captured, so dedup / sort matches the column path.
	keys := make([]KeyMeta, len(entries))
	for i, e := range entries {
		keys[i] = KeyMeta{
			Key:        e.DataPath,
			InsertedAt: e.InsertedAt,
		}
	}

	records, bytesTotal, err := s.downloadAndDecodeAll(ctx, keys, scope)
	if err != nil {
		return nil, since, err
	}
	scope.addFiles(int64(len(keys)))
	scope.addBytes(bytesTotal)
	// includeHistory=true: replica-dedup only, no version collapse.
	// See method docstring for why latest-per-entity isn't offered.
	return s.sortAndCollect(records, true), newOffset, nil
}

// OffsetAt returns the stream offset corresponding to wall-clock
// time t: any ref written at or after t sorts >= the returned
// offset, any ref written before t sorts <. Pure computation —
// no S3 call. Pair with WithUntilOffset on Poll / PollRecords to
// read records within a time window — or use ReadRangeIter, which
// takes time.Time bounds directly.
func (s *Reader[T]) OffsetAt(t time.Time) Offset {
	return Offset(refCutoff(s.refPath, t, 0))
}
