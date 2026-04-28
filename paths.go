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

// makeID composes a per-attempt id from its parts. token may be
// empty; the result is "{tsMicros}-{shortID}" in that case
// (auto-id form, today's makeAutoID shape) or
// "{token}-{tsMicros}-{shortID}" when a token is provided. The
// id is used as the data file's basename (with ".parquet" / ".commit"
// suffixes) and as the embedded id field in ref filenames.
func makeID(token string, tsMicros int64, shortID string) string {
	auto := makeAutoID(tsMicros, shortID)
	if token == "" {
		return auto
	}
	return token + "-" + auto
}

// parseID is the inverse of makeID: given an id of either
// {tsMicros}-{shortID} or {token}-{tsMicros}-{shortID}, return
// the components. Anchors on the trailing fixed-width fields
// (16-digit tsMicros + dash + 8-hex shortID = 25 chars), so a
// token containing arbitrary printable ASCII (including dashes)
// parses unambiguously. tsMicros is fixed-width 16 digits across
// the realistic operating range — same assumption refTsKey relies
// on; shortID is fixed-width 8 hex chars (the makeAutoID shape).
//
// Used by the upfront-LIST dedup gate's reconstruction of
// WriteResult: the LIST entry's basename is parsed back into the
// (token, tsMicros, shortID) triple needed to rebuild the ref key.
func parseID(id string) (token string, tsMicros int64, shortID string, _ error) {
	const tail = 16 + 1 + 8 // "{tsMicros}-{shortID}"
	if len(id) < tail {
		return "", 0, "", fmt.Errorf(
			"s3store: id %q: too short for {tsMicros}-{shortID} suffix",
			id)
	}
	suffix := id[len(id)-tail:]
	if suffix[16] != '-' {
		return "", 0, "", fmt.Errorf(
			"s3store: id %q: missing - between tsMicros and shortID",
			id)
	}
	tsStr := suffix[:16]
	for i := 0; i < 16; i++ {
		if tsStr[i] < '0' || tsStr[i] > '9' {
			return "", 0, "", fmt.Errorf(
				"s3store: id %q: tsMicros field has non-digit", id)
		}
	}
	shortID = suffix[17:]
	for i := 0; i < 8; i++ {
		c := shortID[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return "", 0, "", fmt.Errorf(
				"s3store: id %q: shortID has non-hex char", id)
		}
	}
	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return "", 0, "", fmt.Errorf(
			"s3store: id %q: parse tsMicros: %w", id, err)
	}
	if len(id) == tail {
		return "", ts, shortID, nil
	}
	// More chars before the auto-id portion → token + "-" prefix.
	if id[len(id)-tail-1] != '-' {
		return "", 0, "", fmt.Errorf(
			"s3store: id %q: missing - between token and tsMicros",
			id)
	}
	token = id[:len(id)-tail-1]
	return token, ts, shortID, nil
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

// encodeRefKey builds the full ref-object key from its
// components. The returned key:
//
//   - sorts lexicographically by dataLM first (Poll relies on
//     this) — the data file's server-stamped LastModified at
//     second precision, so the reader's refCutoff = now -
//     SettleWindow comparison is reader↔server only (the
//     writer's clock is not in the protocol),
//   - carries tsMicros (writer's wall-clock at write-start)
//     as the within-second tiebreaker so refs from same-second
//     writes have stable sub-second order,
//   - carries shortID for further uniqueness,
//   - carries the optional idempotency token (omitted when
//     empty — no trailing dash) so consumers can reconstruct
//     the data file's id without a separate LIST,
//   - carries the PathEscape'd Hive key via a refSeparator-split
//     tail.
//
// Format with token:
//
//	"{dataLM}-{tsMicros}-{shortID}-{token}<RefSep>{hive}.ref"
//
// Format without token:
//
//	"{dataLM}-{tsMicros}-{shortID}<RefSep>{hive}.ref"
//
// dataLM and tsMicros are 16-digit fixed-width decimals (the
// realistic operating range — see refTsKey); shortID is 8
// fixed-width hex chars (the makeAutoID format). Position-parsed
// in parseRefKey; the token (which may itself contain dashes)
// is everything after the third '-' in the pre-sep portion.
// validateIdempotencyToken forbids ';' so a token can never
// confuse the refSeparator split.
func encodeRefKey(
	refPath string, dataLM int64, tsMicros int64,
	shortID, token, hiveKey string,
) string {
	if token == "" {
		return fmt.Sprintf("%s-%d-%s%s%s.ref",
			refTsKey(refPath, dataLM), tsMicros, shortID,
			refSeparator, url.PathEscape(hiveKey))
	}
	return fmt.Sprintf("%s-%d-%s-%s%s%s.ref",
		refTsKey(refPath, dataLM), tsMicros, shortID, token,
		refSeparator, url.PathEscape(hiveKey))
}

// parseRefKey is the inverse of encodeRefKey. It accepts any
// S3-style key (with or without a path prefix) ending in the
// encoded ref filename and returns the decoded Hive key plus
// the embedded fields. token is "" when the ref was written
// without WithIdempotencyToken (auto-id form).
func parseRefKey(refKey string) (
	hiveKey string, dataLM int64, tsMicros int64,
	shortID, token string, err error,
) {
	name := refKey
	if idx := strings.LastIndex(name, "/"); idx >= 0 {
		name = name[idx+1:]
	}
	name = strings.TrimSuffix(name, ".ref")

	parts := strings.SplitN(name, refSeparator, 2)
	if len(parts) != 2 {
		return "", 0, 0, "", "", fmt.Errorf(
			"s3store: invalid ref key: %s", refKey)
	}
	pre, hiveEsc := parts[0], parts[1]

	// pre = "{dataLM:16}-{tsMicros:16}-{shortID:8}" or
	//       "{dataLM:16}-{tsMicros:16}-{shortID:8}-{token}".
	// Position-parse the three fixed-width fields anchored at the
	// front; everything past them (if any) is the token.
	const headLen = 16 + 1 + 16 + 1 + 8 // "{dataLM}-{tsMicros}-{shortID}"
	if len(pre) < headLen {
		return "", 0, 0, "", "", fmt.Errorf(
			"s3store: invalid ref key %q: pre-separator too short",
			refKey)
	}
	if pre[16] != '-' || pre[33] != '-' {
		return "", 0, 0, "", "", fmt.Errorf(
			"s3store: invalid ref key %q: malformed fixed-width header",
			refKey)
	}
	dataLM, err = strconv.ParseInt(pre[:16], 10, 64)
	if err != nil {
		return "", 0, 0, "", "", fmt.Errorf(
			"s3store: invalid dataLM in ref key %q: %w", refKey, err)
	}
	tsMicros, err = strconv.ParseInt(pre[17:33], 10, 64)
	if err != nil {
		return "", 0, 0, "", "", fmt.Errorf(
			"s3store: invalid tsMicros in ref key %q: %w", refKey, err)
	}
	shortID = pre[34:headLen]

	if len(pre) > headLen {
		// More chars → must start with '-' followed by token.
		if pre[headLen] != '-' {
			return "", 0, 0, "", "", fmt.Errorf(
				"s3store: invalid ref key %q: missing - before token",
				refKey)
		}
		token = pre[headLen+1:]
	}

	hiveKey, err = url.PathUnescape(hiveEsc)
	if err != nil {
		return "", 0, 0, "", "", fmt.Errorf(
			"s3store: invalid ref key %q: %w", refKey, err)
	}
	return hiveKey, dataLM, tsMicros, shortID, token, nil
}

// refCutoff returns the upper-bound refs-prefix for a given
// settle window. Any ref key whose string-comparison is strictly
// greater than this cutoff falls within the settle window and
// should not yet be emitted. Lex compatibility with encodeRefKey
// is guaranteed by the shared refTsKey helper.
func refCutoff(refPath string, now time.Time, settleWindow time.Duration) string {
	return refTsKey(refPath, now.Add(-settleWindow).UnixMicro())
}
