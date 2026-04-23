package core

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// DataPath returns the prefix under which data parquet files are
// stored, relative to the store's top-level Prefix.
func DataPath(prefix string) string {
	return prefix + "/data"
}

// RefPath returns the prefix under which stream-ref files are
// stored, relative to the store's top-level Prefix.
func RefPath(prefix string) string {
	return prefix + "/_stream/refs"
}

// EncodeRefKey builds the full ref-object key from its
// components. The refPath is the per-store refs prefix as
// returned by RefPath. The returned key:
//
//   - sorts lexicographically by refTsMicros (Poll relies on
//     this) — the publication timestamp captured just before
//     the ref PUT, so SettleWindow only needs to cover ref-PUT
//     latency + LIST propagation,
//   - carries the shortID that identifies the data file,
//   - carries dataTsMicros — the writer's wall-clock at write-
//     start, i.e. the timestamp embedded in the data filename —
//     so consumers can reconstruct the data path from the ref
//     without a separate LIST,
//   - carries the PathEscape'd Hive key via a RefSeparator-split
//     tail.
//
// Format: "{refTsMicros}-{shortID}-{dataTsMicros}<RefSep>{hive}.ref"
// The three pre-sep fields are dash-separated; refTsMicros is
// leftmost so it dominates lex ordering.
func EncodeRefKey(
	refPath string, refTsMicros int64, shortID string,
	dataTsMicros int64, hiveKey string,
) string {
	return fmt.Sprintf("%s/%d-%s-%d%s%s.ref",
		refPath, refTsMicros, shortID, dataTsMicros,
		RefSeparator, url.PathEscape(hiveKey))
}

// ParseRefKey is the inverse of EncodeRefKey. It accepts any
// S3-style key (with or without a path prefix) ending in the
// encoded ref filename and returns the decoded Hive key, the
// ref-publication timestamp (µs since epoch), the shortID, and
// the data-file timestamp (µs since epoch) used to reconstruct
// the data path.
func ParseRefKey(refKey string) (
	hiveKey string, refTsMicros int64, shortID string,
	dataTsMicros int64, err error,
) {
	name := refKey
	if idx := strings.LastIndex(name, "/"); idx >= 0 {
		name = name[idx+1:]
	}
	name = strings.TrimSuffix(name, ".ref")

	parts := strings.SplitN(name, RefSeparator, 2)
	if len(parts) != 2 {
		return "", 0, "", 0, fmt.Errorf(
			"s3store: invalid ref key: %s", refKey)
	}

	// pre-sep is "{refTsMicros}-{id}-{dataTsMicros}". The id may
	// itself contain '-' (e.g. an idempotency token formatted as
	// "{ISO-timestamp}-{suffix}"), so anchor on the numeric
	// timestamps at both ends: first '-' splits refTs from the
	// rest; last '-' splits the id from dataTs.
	firstDash := strings.IndexByte(parts[0], '-')
	lastDash := strings.LastIndexByte(parts[0], '-')
	if firstDash <= 0 || lastDash <= firstDash {
		return "", 0, "", 0, fmt.Errorf(
			"s3store: invalid ref key: %s", refKey)
	}
	refTsStr := parts[0][:firstDash]
	shortID = parts[0][firstDash+1 : lastDash]
	dataTsStr := parts[0][lastDash+1:]
	refTsMicros, err = strconv.ParseInt(refTsStr, 10, 64)
	if err != nil {
		return "", 0, "", 0, fmt.Errorf(
			"s3store: invalid ref ts in ref key %q: %w", refKey, err)
	}
	dataTsMicros, err = strconv.ParseInt(dataTsStr, 10, 64)
	if err != nil {
		return "", 0, "", 0, fmt.Errorf(
			"s3store: invalid data ts in ref key %q: %w", refKey, err)
	}

	hiveKey, err = url.PathUnescape(parts[1])
	if err != nil {
		return "", 0, "", 0, fmt.Errorf(
			"s3store: invalid ref key %q: %w", refKey, err)
	}
	return hiveKey, refTsMicros, shortID, dataTsMicros, nil
}

// BuildDataFilePath returns the S3 object key for a data file.
// The filename is `{id}.parquet`. id is opaque to this helper —
// the writer generates it as either `{tsMicros}-{shortID}` (the
// library's default, lex-sortable by time within a partition)
// or the caller's idempotency token verbatim.
//
// Format: `{dataPath}/{hiveKey}/{id}.parquet`.
func BuildDataFilePath(dataPath, hiveKey, id string) string {
	return fmt.Sprintf("%s/%s/%s.parquet", dataPath, hiveKey, id)
}

// MakeAutoID returns the library's default {tsMicros}-{shortID}
// id used for non-idempotent writes. tsMicros is the writer's
// wall-clock at write-start (so the filename remains lex-
// sortable by time within a partition); shortID is an 8-char
// random fragment so concurrent writes within the same
// microsecond don't collide.
func MakeAutoID(tsMicros int64, shortID string) string {
	return fmt.Sprintf("%d-%s", tsMicros, shortID)
}

// ParseDataFileName extracts the opaque id from a data-file
// filename. Callers typically pass filepath.Base(s3Key) or
// everything after the last '/'. Returns the id as the writer
// stored it (no further parsing into {tsMicros, shortID} —
// idempotent writes use the caller's token, which has its own
// shape).
func ParseDataFileName(name string) (id string, err error) {
	if !strings.HasSuffix(name, ".parquet") {
		return "", fmt.Errorf(
			"s3store: invalid data filename: %s", name)
	}
	id = strings.TrimSuffix(name, ".parquet")
	if id == "" {
		return "", fmt.Errorf(
			"s3store: invalid data filename: %s", name)
	}
	return id, nil
}

// IndexPath returns the prefix under which markers for the named
// secondary index are stored, relative to the store's top-level
// Prefix. Each index lives in its own subtree so multiple indexes
// on one store don't collide.
func IndexPath(prefix, name string) string {
	return prefix + "/_index/" + name
}

// IndexMarkerFilename is the fixed terminal filename appended to
// every marker S3 key. The last real path segment is always
// "col=value"; this constant sits after it so parse code can
// strip it uniformly and LIST paginators recognise markers.
const IndexMarkerFilename = "m.idx"

// BuildIndexMarkerPath assembles an S3 object key for an index
// marker. columns and values are paired by position. Values must
// pass ValidateHivePartitionValue before calling; this helper
// does not revalidate.
func BuildIndexMarkerPath(
	indexPath string, columns, values []string,
) string {
	segs := make([]string, len(columns))
	for i := range columns {
		segs[i] = columns[i] + "=" + values[i]
	}
	return indexPath + "/" + strings.Join(segs, "/") +
		"/" + IndexMarkerFilename
}

// ParseIndexMarkerKey is the inverse of BuildIndexMarkerPath. It
// extracts the column values from a marker key in the order they
// appear in columns. Fails if the key doesn't match the shape
// (wrong prefix, wrong suffix, wrong segment count, wrong
// column name in a segment).
func ParseIndexMarkerKey(
	markerKey, indexPath string, columns []string,
) ([]string, error) {
	prefix := indexPath + "/"
	if !strings.HasPrefix(markerKey, prefix) {
		return nil, fmt.Errorf(
			"s3store: marker key %q outside index path %q",
			markerKey, indexPath)
	}
	body := markerKey[len(prefix):]
	tail := "/" + IndexMarkerFilename
	if !strings.HasSuffix(body, tail) {
		return nil, fmt.Errorf(
			"s3store: marker key %q missing %q suffix",
			markerKey, IndexMarkerFilename)
	}
	body = body[:len(body)-len(tail)]
	segs := strings.Split(body, "/")
	if len(segs) != len(columns) {
		return nil, fmt.Errorf(
			"s3store: marker key %q has %d segments, want %d",
			markerKey, len(segs), len(columns))
	}
	out := make([]string, len(columns))
	for i, seg := range segs {
		colPrefix := columns[i] + "="
		if !strings.HasPrefix(seg, colPrefix) {
			return nil, fmt.Errorf(
				"s3store: marker key %q segment %d is %q, "+
					"expected prefix %q",
				markerKey, i, seg, colPrefix)
		}
		out[i] = seg[len(colPrefix):]
	}
	return out, nil
}

// RefCutoff returns the upper-bound refs-prefix for a given
// settle window. Any ref key whose string-comparison is strictly
// greater than this cutoff falls within the settle window and
// should not yet be emitted.
//
// The value is "{refPath}/{tsMicros}" — lexical comparison works
// because EncodeRefKey uses a fixed-width-ish decimal timestamp
// followed by '-'; any ref at time T sorts into the half-open
// range ["{refPath}/{T}", "{refPath}/{T+1}").
func RefCutoff(refPath string, now time.Time, settleWindow time.Duration) string {
	cutoff := now.Add(-settleWindow)
	return fmt.Sprintf("%s/%d", refPath, cutoff.UnixMicro())
}

// RefRangeForRetry returns the [lo, hi] ref-key string bounds for
// a scoped LIST on the idempotent-retry path. When the writer
// detects a retry (overwrite-prevention fired on the data PUT),
// it LISTs refs whose publication timestamp falls in
// [now - maxRetryAge, now] and scans for an entry matching the
// token's id.
//
// Bounds are lexical prefixes of ref keys — EncodeRefKey prepends
// the publication tsMicros to the filename, so a tsMicros-based
// string compare gives an exact-age scope.
func RefRangeForRetry(
	refPath string, now time.Time, maxRetryAge time.Duration,
) (lo, hi string) {
	loTs := now.Add(-maxRetryAge).UnixMicro()
	hiTs := now.UnixMicro()
	lo = fmt.Sprintf("%s/%d", refPath, loTs)
	hi = fmt.Sprintf("%s/%d", refPath, hiTs)
	return lo, hi
}

// ExtractRefID pulls the id field out of a ref key so the retry-
// path scoped LIST can compare it against the caller's
// IdempotencyToken. The id is the shortID portion of the ref
// filename under the default scheme, or the caller's token
// verbatim under WithIdempotencyToken — ParseRefKey already
// deserializes it into the shortID return slot, so this is a
// thin convenience wrapper. Errors on an unparseable ref key.
func ExtractRefID(refKey string) (string, error) {
	_, _, shortID, _, err := ParseRefKey(refKey)
	if err != nil {
		return "", err
	}
	return shortID, nil
}

// ValidateIdempotencyToken rejects token values that can't be
// safely embedded in a data-file path or a ref filename. Run at
// WithIdempotencyToken-application time so typos surface
// immediately at the call site, not buried inside the write
// path's PUT error.
//
// Rules:
//   - non-empty
//   - no "/" (would split the S3 key into unintended segments)
//   - no ".." (collides with the key-pattern grammar's range
//     separator; tokens with ".." would be unaddressable on read)
//   - no whitespace, no control characters — printable ASCII
//     subset 0x21..0x7E
//   - <= 200 characters so the resulting data path stays well
//     under S3's 1024-byte key limit even with long Hive keys
func ValidateIdempotencyToken(token string) error {
	if token == "" {
		return fmt.Errorf(
			"s3store: IdempotencyToken must not be empty")
	}
	if len(token) > 200 {
		return fmt.Errorf(
			"s3store: IdempotencyToken must be <= 200 characters "+
				"(got %d)", len(token))
	}
	if strings.Contains(token, "/") {
		return fmt.Errorf(
			"s3store: IdempotencyToken %q must not contain '/'",
			token)
	}
	if strings.Contains(token, "..") {
		return fmt.Errorf(
			"s3store: IdempotencyToken %q must not contain "+
				"'..' (reserved by the key-pattern grammar)",
			token)
	}
	for i := 0; i < len(token); i++ {
		c := token[i]
		if c < 0x21 || c > 0x7E {
			return fmt.Errorf(
				"s3store: IdempotencyToken %q contains a "+
					"non-printable-ASCII byte at index %d "+
					"(want 0x21..0x7E)", token, i)
		}
	}
	return nil
}
