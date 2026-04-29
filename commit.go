package s3store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
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
	return fmt.Sprintf("%s/%s/%s%s",
		dataPath, partition, token, commitMarkerSuffix)
}

// commitTokenFromBasename extracts the token from a `<token>.commit`
// basename. Returns ok=false on any other shape; callers feed every
// LIST entry that ends in `.commit` through here without further
// pre-filtering.
func commitTokenFromBasename(basename string) (token string, ok bool) {
	t, found := strings.CutSuffix(basename, commitMarkerSuffix)
	if !found || t == "" {
		return "", false
	}
	return t, true
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
	raw, err := target.head(ctx, key)
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

// gateByCommit applies the snapshot-read commit gate to a flat
// LIST result of data-file KeyMetas, dropping uncommitted parquets
// and resolving multi-attempt tokens to their canonical attempt:
//
//   - parquet whose token has no `<token>.commit` marker visible
//     in the same LIST → drop. The write either crashed before
//     the token-commit PUT or never finished; reads must not see
//     it (read stability invariant: two consecutive snapshot reads
//     return the same records, so we cannot depend on a future
//     commit landing).
//   - token has 1 parquet + commit → keep parquet. The parquet is
//     canonical by uniqueness — no HEAD needed, since any other
//     attempt-id would also be in the LIST.
//   - token has ≥2 parquets + commit → HEAD `<token>.commit` once
//     per token, read the canonical `attemptid` from metadata, and
//     keep only the parquet whose attempt-id matches. The other
//     parquets are orphans from failed-mid-write retries (the
//     library never deletes them; cleanup is operator-driven).
//
// Per-token HEADs run in parallel (bounded by
// MaxInflightRequests) so a partition with many multi-attempt
// tokens doesn't serialise. observabilityMethod is the caller's
// methodKind so the s3store.read.commit_head metric carries the
// right entry-point label.
//
// commits maps `<partition>:<token>` → struct{} for every
// `<token>.commit` observed in the same LIST as dataFiles.
// Callers populate it during the LIST iteration so the gate
// doesn't re-LIST.
func gateByCommit(
	ctx context.Context, target S3Target, dataPath string,
	dataFiles []KeyMeta, commits map[string]struct{},
	observabilityMethod methodKind,
) ([]KeyMeta, error) {
	if len(dataFiles) == 0 {
		return dataFiles, nil
	}

	// Bucket data files by (partition, token). Files whose path
	// doesn't parse as a data file flow through unchanged — higher
	// layers (hiveKeyOfDataFile callers) already validate, so a
	// non-parsing path here is genuinely outside our concern.
	type bucketKey struct{ partition, token string }
	type bucket struct {
		token     string
		partition string
		files     []KeyMeta
	}
	buckets := make(map[bucketKey]*bucket, len(dataFiles))
	var passthrough []KeyMeta
	for _, k := range dataFiles {
		partition, ok := hiveKeyOfDataFile(k.Key, dataPath)
		if !ok {
			passthrough = append(passthrough, k)
			continue
		}
		base := k.Key[strings.LastIndex(k.Key, "/")+1:]
		token, _, ok := dataFileTokenAndID(base)
		if !ok {
			passthrough = append(passthrough, k)
			continue
		}
		bk := bucketKey{partition: partition, token: token}
		b, ok := buckets[bk]
		if !ok {
			b = &bucket{token: token, partition: partition}
			buckets[bk] = b
		}
		b.files = append(b.files, k)
	}

	// Resolve buckets in two passes: trivial ones synchronously
	// (drop / keep) and ambiguous ones (≥ 2 attempts under a single
	// commit) via a fan-out HEAD. Single-pass over the bucket map
	// would force us to spawn a goroutine for the trivial branches
	// too, which is most buckets in steady state.
	type ambiguous struct {
		bucket *bucket
	}
	out := make([]KeyMeta, 0, len(dataFiles))
	out = append(out, passthrough...)
	var pending []ambiguous
	for bk, b := range buckets {
		if _, committed := commits[bk.partition+":"+bk.token]; !committed {
			continue
		}
		if len(b.files) == 1 {
			out = append(out, b.files[0])
			continue
		}
		pending = append(pending, ambiguous{bucket: b})
	}
	if len(pending) == 0 {
		return out, nil
	}

	resolved := make([][]KeyMeta, len(pending))
	err := fanOut(ctx, pending,
		target.EffectiveMaxInflightRequests(),
		target.metrics,
		func(ctx context.Context, i int, item ambiguous) error {
			b := item.bucket
			target.metrics.recordReadCommitHead(ctx, observabilityMethod)
			meta, ok, err := headTokenCommit(ctx, target,
				dataPath, b.partition, b.token)
			if err != nil {
				return fmt.Errorf(
					"s3store: head token-commit %s/%s: %w",
					b.partition, b.token, err)
			}
			if !ok {
				// Vanished between LIST and HEAD — nothing committed.
				// Read stability invariant: never include uncommitted
				// data, and operator-driven cleanup is the only
				// deletion path.
				return nil
			}
			canonicalBase := makeID(b.token, meta.attemptID) + ".parquet"
			for _, k := range b.files {
				base := k.Key[strings.LastIndex(k.Key, "/")+1:]
				if base == canonicalBase {
					resolved[i] = []KeyMeta{k}
					return nil
				}
			}
			// Canonical attempt is named in metadata but its
			// parquet wasn't in the LIST — unusual, but possible
			// if the LIST happened mid-write (data PUT not yet
			// visible) or if a sweep raced. Drop everything for
			// this token rather than emit a non-canonical attempt.
			return nil
		})
	if err != nil {
		return nil, err
	}
	for _, r := range resolved {
		out = append(out, r...)
	}
	return out, nil
}

// commitCache memoises per-poll `<token>.commit` HEAD results
// keyed by (partition, token). The stream-read gate consults it
// per ref so refs that share a token within one Poll cycle issue
// at most one HEAD against the marker. Reset between polls — a
// stale cache across cycles would let a failed-write zombie ref
// escape the gate after its commit was confirmed missing once.
//
// Concurrent safety: future poll fan-outs may parallelise this
// gate; the mutex keeps the contract correct. Today's Poll is
// serial, so contention is zero in steady state.
type commitCache struct {
	mu      sync.Mutex
	entries map[string]commitCacheEntry
}

// commitCacheEntry is a single cached HEAD outcome plus the
// metadata needed to compare against a ref's attempt-id.
type commitCacheEntry struct {
	exists    bool
	attemptID string
}

// newCommitCache returns an empty cache ready for one poll cycle.
func newCommitCache() *commitCache {
	return &commitCache{entries: map[string]commitCacheEntry{}}
}

// lookupOrFetch returns the cached commit-presence + attempt-id
// for (partition, token), HEADing the token-commit marker on a
// miss. Records s3store.read.commit_head_cache_hit on a cache hit
// and s3store.read.commit_head on a miss (via headTokenCommit's
// caller side, here). 404 is cached as ok=false so a partition
// hammered with refs from a failed write doesn't HEAD per ref.
func (c *commitCache) lookupOrFetch(
	ctx context.Context, target S3Target,
	dataPath, partition, token string,
	method methodKind,
) (commitCacheEntry, error) {
	cacheKey := partition + ":" + token

	c.mu.Lock()
	if e, ok := c.entries[cacheKey]; ok {
		c.mu.Unlock()
		target.metrics.recordReadCommitHeadCacheHit(ctx, method)
		return e, nil
	}
	c.mu.Unlock()

	target.metrics.recordReadCommitHead(ctx, method)
	meta, exists, err := headTokenCommit(ctx, target,
		dataPath, partition, token)
	if err != nil {
		return commitCacheEntry{}, err
	}
	entry := commitCacheEntry{exists: exists}
	if exists {
		entry.attemptID = meta.attemptID
	}

	c.mu.Lock()
	c.entries[cacheKey] = entry
	c.mu.Unlock()
	return entry, nil
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
