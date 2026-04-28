package s3store

import (
	"encoding/hex"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Data-file paths.
//
// Layout:
//
//	<Prefix>/data/<hiveKey>/<id>.parquet         — data files
//	<Prefix>/data/<hiveKey>/<token>.commit       — token-level commit marker
//	<Prefix>/_ref/<refMicroTs>-<id>;<hiveEsc>.ref — refs
//
// id has the uniform shape `<token>-<attemptID>` regardless of
// whether the caller supplied WithIdempotencyToken: under the
// idempotency-token path, token is the caller's value; under the
// no-token path, the writer generates a UUIDv7 and uses it as
// both token and attemptID, yielding `<UUIDv7>-<UUIDv7>`. The
// duplication keeps parsing one-case and the commit-marker key
// (`<dataPath>/<partition>/<token>.commit`) consistent across
// both paths.

// attemptIDHexLen is the length of a UUIDv7 rendered as 32
// lowercase hex digits (the canonical 36-char form with internal
// dashes stripped). Anchor for parsing the trailing attempt-id
// portion of id and ref-key strings.
const attemptIDHexLen = 32

// refMicroTsLen is the fixed width of a microsecond Unix-epoch
// timestamp rendered as a decimal integer in the realistic
// operating range. UnixMicro emits 16 decimal digits from
// 2002-01-09 (1e15 µs) through 2286-11-20 (1e16 µs); outside that
// window, lex compare diverges from numeric compare. encodeRefKey
// and refMicroTsKey both render with %016d so values below the
// range are zero-padded into the same width.
const refMicroTsLen = 16

// commitMarkerSuffix is the trailing ".commit" suffix shared by
// every token-commit marker key. Pulled out of the LIST callbacks
// so the suffix lives next to the .parquet suffix and the two are
// easy to scan side-by-side.
const commitMarkerSuffix = ".commit"

// dataPath returns the prefix under which data parquet files and
// commit markers are stored, relative to the store's top-level
// Prefix.
func dataPath(prefix string) string {
	return prefix + "/data"
}

// refPath returns the prefix under which ref files are stored,
// relative to the store's top-level Prefix.
func refPath(prefix string) string {
	return prefix + "/_ref"
}

// buildDataFilePath returns the S3 object key for a data file.
// Format: `{dataPath}/{hiveKey}/{id}.parquet`.
func buildDataFilePath(dataPath, hiveKey, id string) string {
	return fmt.Sprintf("%s/%s/%s.parquet", dataPath, hiveKey, id)
}

// makeID composes the data-file id from a token and a per-attempt
// UUIDv7 (32 lowercase hex chars, dashes stripped). Both
// sub-fields are non-empty: the writer either uses the caller's
// idempotency token verbatim or generates a fresh UUIDv7 and uses
// it as both token and attemptID. The result is the .parquet
// basename (without extension) and the attempt-id portion of the
// ref filename.
func makeID(token, attemptID string) string {
	return token + "-" + attemptID
}

// newAttemptID generates a fresh per-attempt id (UUIDv7 rendered
// as 32 lowercase hex chars, internal dashes stripped). UUIDv7's
// 48-bit Unix-millisecond prefix gives us natural lex-ordering
// within a partition; the remaining 74 random bits cover
// uniqueness against concurrent writes within the same
// millisecond. Stripped to hex so the ref filename's
// position-parse anchors on a single fixed-width field instead of
// counting four internal dashes.
//
// uuid.NewV7 returns an error only when the underlying random
// source fails — the same condition that would break crypto
// elsewhere in the program. Surface it so callers don't silently
// produce duplicate ids.
func newAttemptID() (string, error) {
	u, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("s3store: generate UUIDv7: %w", err)
	}
	return hex.EncodeToString(u[:]), nil
}

// dataFileTokenAndID extracts the token and full id
// (`<token>-<attemptID>`) from a data-file basename of shape
// `<token>-<attemptID>.parquet`. Returns ok=false on any other
// shape (caller-side filter for LIST entries that share a
// partition prefix but aren't data files). Used by the read-side
// commit gate to bucket parquets by token before pairing with
// `<token>.commit` markers.
func dataFileTokenAndID(basename string) (token, id string, ok bool) {
	id, found := strings.CutSuffix(basename, ".parquet")
	if !found {
		return "", "", false
	}
	t, _, err := parseAttemptID(id)
	if err != nil {
		return "", "", false
	}
	return t, id, true
}

// parseAttemptID is the inverse of makeID: given an id of shape
// `<token>-<attemptID:32hex>`, return the parts. Anchors on the
// trailing 32 lowercase-hex-char attemptID; everything before
// (after stripping the separating dash) is the token verbatim.
// validateIdempotencyToken forbids ';' but permits '-' inside the
// token, so the lex-anchor approach is required: a substring
// search for '-' would split a multi-dash token at the wrong
// position.
func parseAttemptID(id string) (token, attemptID string, err error) {
	const tail = 1 + attemptIDHexLen // "-{attemptID}"
	if len(id) < tail+1 {
		return "", "", fmt.Errorf(
			"s3store: id %q too short for <token>-<attemptID:%d>",
			id, attemptIDHexLen)
	}
	if id[len(id)-tail] != '-' {
		return "", "", fmt.Errorf(
			"s3store: id %q: missing '-' before attemptID", id)
	}
	attemptID = id[len(id)-attemptIDHexLen:]
	if !isLowerHex(attemptID) {
		return "", "", fmt.Errorf(
			"s3store: id %q: attemptID %q is not %d lowercase-hex chars",
			id, attemptID, attemptIDHexLen)
	}
	token = id[:len(id)-tail]
	return token, attemptID, nil
}

// isLowerHex reports whether s consists entirely of
// lowercase-hexadecimal characters (0-9, a-f).
func isLowerHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// Ref-file encoding.
//
// refSeparator splits the fixed-width "{refMicroTs}-{id}" header
// from the PathEscape'd Hive key in a ref filename. PathEscape
// always escapes ';' (as %3B), so this character cannot appear in
// the encoded key — the split on it is unambiguous even when the
// original key contains arbitrary bytes.
const refSeparator = ";"

// refMicroTsKey returns the "{refPath}/{refMicroTs}" prefix shared
// by every ref-key string this package produces — both the full
// encodeRefKey filename and the lex-bound comparator (refCutoff).
// One place for the format so the "lex compare matches numeric
// compare" assumption every consumer relies on lives next to its
// only producer.
//
// Lex == numeric ordering holds because the rendered refMicroTs
// is fixed-width refMicroTsLen across the realistic operating
// range. %016d zero-pads values below 1e15 into the same width;
// values >= 1e16 produce 17 digits and break the assumption (out
// of scope for the library's correctness guarantees through
// 2286-11-20).
func refMicroTsKey(refPath string, refMicroTs int64) string {
	return fmt.Sprintf("%s/%016d", refPath, refMicroTs)
}

// encodeRefKey builds the full ref-object key from its components.
//
// Format:
//
//	`<refPath>/<refMicroTs:16>-<token>-<attemptID:32>;<hiveEsc>.ref`
//
// refMicroTs is the writer's wall-clock at ref-PUT-time
// (microseconds, 16 decimal digits — the realistic operating
// range; see refMicroTsKey). token is the caller's idempotency
// token or a writer-generated UUIDv7 for non-idempotent writes;
// attemptID is the UUIDv7 (32 lowercase hex chars, dashes
// stripped). For non-idempotent writes the writer passes the same
// UUIDv7 as both token and attemptID so the encoded shape is
// always `<refMicroTs>-<token>-<attemptID>` with three segments.
//
// Position-parsed in parseRefKey. validateIdempotencyToken
// forbids ';' so the refSeparator split is unambiguous; the
// trailing 32-hex-char anchor is independent of token content.
//
// The returned key sorts lexicographically by refMicroTs first
// (Poll's refCutoff relies on this — any ref younger than
// `now - SettleWindow` sorts strictly above the cutoff). Within
// the same refMicroTs, ordering between concurrent writers'
// attempts is implementation-defined (depends on the
// alphanumeric collation of token + attemptID); the change-stream
// contract already tolerates this via SettleWindow.
func encodeRefKey(
	refPath string, refMicroTs int64,
	token, attemptID, hiveKey string,
) string {
	return fmt.Sprintf("%s-%s-%s%s%s.ref",
		refMicroTsKey(refPath, refMicroTs),
		token, attemptID,
		refSeparator, url.PathEscape(hiveKey))
}

// parseRefKey is the inverse of encodeRefKey. Returns the decoded
// Hive key plus the embedded fields (refMicroTs, token,
// attemptID). Accepts any S3-style key (with or without a path
// prefix) ending in the encoded ref filename.
func parseRefKey(refKey string) (
	hiveKey string, refMicroTs int64,
	token, attemptID string, err error,
) {
	name := refKey
	if idx := strings.LastIndex(name, "/"); idx >= 0 {
		name = name[idx+1:]
	}
	name = strings.TrimSuffix(name, ".ref")

	parts := strings.SplitN(name, refSeparator, 2)
	if len(parts) != 2 {
		return "", 0, "", "", fmt.Errorf(
			"s3store: invalid ref key: %s", refKey)
	}
	pre, hiveEsc := parts[0], parts[1]

	// pre = "{refMicroTs:16}-{token}-{attemptID:32}".
	// Position-parse the front (refMicroTs) and the back
	// (attemptID) anchored on fixed width; the middle (after
	// stripping the separator dashes) is the token. Token
	// must be non-empty, so the minimum pre length is
	// 16 + 1 + 1 + 1 + 32 = 51.
	const minPreLen = refMicroTsLen + 1 + 1 + 1 + attemptIDHexLen
	if len(pre) < minPreLen {
		return "", 0, "", "", fmt.Errorf(
			"s3store: invalid ref key %q: pre-separator too short",
			refKey)
	}
	if pre[refMicroTsLen] != '-' {
		return "", 0, "", "", fmt.Errorf(
			"s3store: invalid ref key %q: missing '-' after refMicroTs",
			refKey)
	}
	if pre[len(pre)-attemptIDHexLen-1] != '-' {
		return "", 0, "", "", fmt.Errorf(
			"s3store: invalid ref key %q: missing '-' before attemptID",
			refKey)
	}
	tsStr := pre[:refMicroTsLen]
	for i := 0; i < refMicroTsLen; i++ {
		if tsStr[i] < '0' || tsStr[i] > '9' {
			return "", 0, "", "", fmt.Errorf(
				"s3store: invalid ref key %q: refMicroTs has non-digit",
				refKey)
		}
	}
	refMicroTs, err = strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return "", 0, "", "", fmt.Errorf(
			"s3store: invalid refMicroTs in ref key %q: %w",
			refKey, err)
	}

	attemptID = pre[len(pre)-attemptIDHexLen:]
	if !isLowerHex(attemptID) {
		return "", 0, "", "", fmt.Errorf(
			"s3store: invalid ref key %q: attemptID %q not lowercase hex",
			refKey, attemptID)
	}

	token = pre[refMicroTsLen+1 : len(pre)-attemptIDHexLen-1]
	if token == "" {
		return "", 0, "", "", fmt.Errorf(
			"s3store: invalid ref key %q: token segment is empty",
			refKey)
	}

	hiveKey, err = url.PathUnescape(hiveEsc)
	if err != nil {
		return "", 0, "", "", fmt.Errorf(
			"s3store: invalid ref key %q: %w", refKey, err)
	}
	return hiveKey, refMicroTs, token, attemptID, nil
}

// refCutoff returns the upper-bound refs-prefix for a given
// settle window. Any ref key whose string-comparison is strictly
// greater than this cutoff falls within the settle window and
// should not yet be emitted. Lex compatibility with encodeRefKey
// is guaranteed by the shared refMicroTsKey helper.
//
// The writer captures refMicroTs at wall-clock just before the
// ref PUT, so the reader's `refCutoff = now - SettleWindow`
// comparison is reader↔writer (with MaxClockSkew bounding the
// skew) rather than reader↔server. SettleWindow must therefore
// also bound the writer's full ref-PUT + token-commit-PUT
// sequence so the cutoff cannot overtake refs whose token-commit
// is still in flight.
func refCutoff(refPath string, now time.Time, settleWindow time.Duration) string {
	return refMicroTsKey(refPath, now.Add(-settleWindow).UnixMicro())
}
