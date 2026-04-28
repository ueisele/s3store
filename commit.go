package s3store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Token-commit primitives.
//
// The token-commit is a zero-byte object at
// `<dataPath>/<partition>/<token>.commit` with three pieces of
// user-metadata encoding the canonical attempt:
//
//   - attemptIDMetaKey ("attemptid"): the UUIDv7 hex string
//     (32 lowercase hex chars) identifying the canonical
//     data-file basename `<token>-<attemptID>` for this commit.
//   - refMicroTsMetaKey ("refmicrots"): the decimal microseconds
//     refMicroTs the canonical ref filename is anchored on.
//   - insertedAtMetaKey ("insertedat"): the writer's pre-encode
//     wall-clock at write-start, decimal microseconds. Identical
//     to the value stamped into the parquet's InsertedAtField
//     column. Stored on the marker so a same-token retry's
//     reconstructed WriteResult.InsertedAt matches the original
//     attempt's column value (without it, retries would surface
//     `refMicroTs` — a slightly later wall-clock).
//
// Together with the partition Hive key (known from the lookup
// path) and the dataset's data / ref prefixes (known from the
// target), the metadata fully reconstructs the WriteResult of
// the original write — no second round trip, no LIST needed.
//
// The token-commit is the single atomic event that flips
// visibility for both read paths: snapshot reads pair it with
// `<token>-*.parquet` siblings via partition LIST; stream reads
// HEAD it per ref. Crash before the token-commit PUT → invisible
// to both; crash after → visible to both.

// attemptIDMetaKey is the user-metadata header carrying the
// canonical attempt's UUIDv7 hex (32 lowercase hex chars).
//
// Lowercase, no separator: AWS SDK v2 surfaces user-metadata keys
// lowercased on the response side; matching on the produce side
// keeps the round-trip consistent across SDK versions.
const attemptIDMetaKey = "attemptid"

// refMicroTsMetaKey is the user-metadata header carrying the
// canonical ref's refMicroTs (decimal microseconds).
const refMicroTsMetaKey = "refmicrots"

// insertedAtMetaKey is the user-metadata header carrying the
// writer's pre-encode wall-clock (decimal microseconds) — the same
// value stamped into the parquet's InsertedAtField column, so
// WriteResult.InsertedAt agrees with the in-file column on every
// path (fresh write, retry-found-commit, LookupCommit).
const insertedAtMetaKey = "insertedat"

// tokenCommitKey returns the S3 object key of the token-level
// commit marker. Format:
// `{dataPath}/{partition}/{token}.commit`.
func tokenCommitKey(dataPath, partition, token string) string {
	return fmt.Sprintf("%s/%s/%s.commit", dataPath, partition, token)
}

// tokenCommitMeta is the parsed user-metadata of a token-commit:
// the canonical attempt-id (UUIDv7 hex), the canonical ref's
// refMicroTs, and the writer's pre-encode wall-clock at
// write-start (= InsertedAtField column value).
type tokenCommitMeta struct {
	attemptID    string
	refMicroTs   int64
	insertedAtUs int64
}

// readTokenCommitMeta extracts the structured metadata from a
// token-commit's response headers. Validates that all three
// fields are present and well-formed; an existing commit with
// malformed metadata is a hard error, not a "missing" — silently
// ignoring it would let a corrupted commit surface as "no commit
// yet" and cause a redundant retry.
func readTokenCommitMeta(meta map[string]string) (tokenCommitMeta, error) {
	attemptID, ok := meta[attemptIDMetaKey]
	if !ok {
		return tokenCommitMeta{}, fmt.Errorf(
			"s3store: token-commit missing %s metadata",
			attemptIDMetaKey)
	}
	if len(attemptID) != attemptIDHexLen || !isLowerHex(attemptID) {
		return tokenCommitMeta{}, fmt.Errorf(
			"s3store: token-commit %s = %q (want %d lowercase hex chars)",
			attemptIDMetaKey, attemptID, attemptIDHexLen)
	}
	refMicroTs, err := readMicrosMeta(meta, refMicroTsMetaKey)
	if err != nil {
		return tokenCommitMeta{}, err
	}
	insertedAtUs, err := readMicrosMeta(meta, insertedAtMetaKey)
	if err != nil {
		return tokenCommitMeta{}, err
	}
	return tokenCommitMeta{
		attemptID:    attemptID,
		refMicroTs:   refMicroTs,
		insertedAtUs: insertedAtUs,
	}, nil
}

// readMicrosMeta extracts a decimal-microsecond field from a
// token-commit's user-metadata. Centralised so the missing /
// unparseable error messages are uniform across fields.
func readMicrosMeta(meta map[string]string, key string) (int64, error) {
	raw, ok := meta[key]
	if !ok {
		return 0, fmt.Errorf(
			"s3store: token-commit missing %s metadata", key)
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf(
			"s3store: token-commit %s = %q: %w", key, raw, err)
	}
	return v, nil
}

// headTokenCommit HEADs `<dataPath>/<partition>/<token>.commit`
// and returns its parsed metadata when present. ok=false signals
// 404 (no prior commit for this token) and is the dedup gate's
// "proceed with a fresh attempt" branch. Any other failure
// surfaces as a non-nil err.
//
// One round trip; powers the writer's upfront-dedup HEAD, the
// public LookupCommit API (Phase 2/5), and the stream-read
// commit-existence gate (Phase 2).
func headTokenCommit(
	ctx context.Context, target S3Target,
	dataPath, partition, token string,
) (meta tokenCommitMeta, ok bool, err error) {
	key := tokenCommitKey(dataPath, partition, token)
	_, raw, err := target.head(ctx, key)
	if err != nil {
		if _, notFound := errors.AsType[*s3types.NotFound](err); notFound {
			return tokenCommitMeta{}, false, nil
		}
		return tokenCommitMeta{}, false, err
	}
	meta, err = readTokenCommitMeta(raw)
	if err != nil {
		return tokenCommitMeta{}, true, fmt.Errorf("%w (key %q)", err, key)
	}
	return meta, true, nil
}

// reconstructWriteResult builds a WriteResult from a token-commit's
// metadata plus the dataset's known prefixes and the caller's
// partition. Used by the writer's upfront-dedup return path and
// (in Phase 2) by LookupCommit.
//
// InsertedAt comes from the token-commit's `insertedat` metadata
// (= the original write's pre-encode wall-clock, = the parquet
// InsertedAtField column value), so a same-token retry returns
// the original column value unchanged.
func reconstructWriteResult(
	dataPath, refPath, partition, token string, meta tokenCommitMeta,
) WriteResult {
	id := makeID(token, meta.attemptID)
	refKey := encodeRefKey(refPath, meta.refMicroTs, token,
		meta.attemptID, partition)
	return WriteResult{
		Offset:     Offset(refKey),
		DataPath:   buildDataFilePath(dataPath, partition, id),
		RefPath:    refKey,
		InsertedAt: time.UnixMicro(meta.insertedAtUs),
	}
}

// putTokenCommit writes the zero-byte token-commit marker with
// the canonical attempt's metadata (attempt-id, ref's
// refMicroTs, and the writer's pre-encode wall-clock for
// InsertedAt round-tripping). Single point of metadata shape so
// writer and any future test fixture build the same headers.
func putTokenCommit(
	ctx context.Context, target S3Target,
	dataPath, partition, token, attemptID string,
	refMicroTs, insertedAtUs int64,
) error {
	key := tokenCommitKey(dataPath, partition, token)
	return target.putWithMeta(ctx, key, []byte{},
		"application/octet-stream",
		map[string]string{
			attemptIDMetaKey:  attemptID,
			refMicroTsMetaKey: strconv.FormatInt(refMicroTs, 10),
			insertedAtMetaKey: strconv.FormatInt(insertedAtUs, 10),
		})
}
