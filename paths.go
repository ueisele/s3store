package s3store

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Data-file paths.
//
// Layout:
//   <Prefix>/data/<hiveKey>/<id>.parquet         — data files
//   <Prefix>/_ref/<refTsMicros>-<id>...          — ref files
//
// id is opaque to these helpers — the writer generates it as
// either {tsMicros}-{shortID} (the library's default, lex-sortable
// by time within a partition) or the caller's idempotency token
// verbatim.

// dataPath returns the prefix under which data parquet files are
// stored, relative to the store's top-level Prefix.
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

// makeAutoID returns the library's default {tsMicros}-{shortID}
// id used for non-idempotent writes. tsMicros is the writer's
// wall-clock at write-start (so the filename remains lex-
// sortable by time within a partition); shortID is an 8-char
// random fragment so concurrent writes within the same
// microsecond don't collide.
func makeAutoID(tsMicros int64, shortID string) string {
	return fmt.Sprintf("%d-%s", tsMicros, shortID)
}

// Ref-file encoding.
//
// refSeparator splits the fixed-width "{ts}-{id}" header from
// the PathEscape'd Hive key in a ref filename. PathEscape always
// escapes ';' (as %3B), so this character cannot appear in the
// encoded key — the split on it is unambiguous even when the
// original key contains arbitrary bytes.
const refSeparator = ";"

// refTsKey returns the "{refPath}/{tsMicros}" prefix shared by
// every ref-key string this package produces — both the full
// encodeRefKey filename and the lex-bound comparators (refCutoff,
// findExistingRef). One place for the format so the "lex compare
// matches numeric compare" assumption every consumer relies on
// lives next to its only producer.
//
// Lex == numeric ordering holds because the rendered tsMicros is
// fixed-width across the realistic operating range: time.UnixMicro
// emits 16 decimal digits from 2002-01-09 (1e15 µs) through
// 2286-11-20 (1e16 µs), which covers any timestamp this library
// will ever encode in production. Outside that window — pre-2002
// (15 digits) or post-2286 (17+ digits) — string compare diverges
// from numeric compare and Poll/findExistingRef break silently.
// If the format ever needs to change (e.g. zero-padding to %019d),
// it changes here, and the migration story has to handle existing
// refs whose tsMicros is rendered with the old width.
func refTsKey(refPath string, tsMicros int64) string {
	return fmt.Sprintf("%s/%d", refPath, tsMicros)
}

// encodeRefKey builds the full ref-object key from its components.
// The returned key:
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
//   - carries the PathEscape'd Hive key via a refSeparator-split
//     tail.
//
// Format: "{refTsMicros}-{shortID}-{dataTsMicros}<RefSep>{hive}.ref"
// The three pre-sep fields are dash-separated; refTsMicros is
// leftmost so it dominates lex ordering. See refTsKey for the
// lex-compatibility constraint.
func encodeRefKey(
	refPath string, refTsMicros int64, shortID string,
	dataTsMicros int64, hiveKey string,
) string {
	return fmt.Sprintf("%s-%s-%d%s%s.ref",
		refTsKey(refPath, refTsMicros), shortID, dataTsMicros,
		refSeparator, url.PathEscape(hiveKey))
}

// parseRefKey is the inverse of encodeRefKey. It accepts any
// S3-style key (with or without a path prefix) ending in the
// encoded ref filename and returns the decoded Hive key, the
// ref-publication timestamp (µs since epoch), the shortID, and
// the data-file timestamp (µs since epoch) used to reconstruct
// the data path.
func parseRefKey(refKey string) (
	hiveKey string, refTsMicros int64, shortID string,
	dataTsMicros int64, err error,
) {
	name := refKey
	if idx := strings.LastIndex(name, "/"); idx >= 0 {
		name = name[idx+1:]
	}
	name = strings.TrimSuffix(name, ".ref")

	parts := strings.SplitN(name, refSeparator, 2)
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

// refCutoff returns the upper-bound refs-prefix for a given
// settle window. Any ref key whose string-comparison is strictly
// greater than this cutoff falls within the settle window and
// should not yet be emitted. Lex compatibility with encodeRefKey
// is guaranteed by the shared refTsKey helper.
func refCutoff(refPath string, now time.Time, settleWindow time.Duration) string {
	return refTsKey(refPath, now.Add(-settleWindow).UnixMicro())
}
