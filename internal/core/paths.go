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

	// pre-sep is "{refTsMicros}-{shortID}-{dataTsMicros}".
	seg := strings.SplitN(parts[0], "-", 3)
	if len(seg) != 3 {
		return "", 0, "", 0, fmt.Errorf(
			"s3store: invalid ref key: %s", refKey)
	}
	refTsMicros, err = strconv.ParseInt(seg[0], 10, 64)
	if err != nil {
		return "", 0, "", 0, fmt.Errorf(
			"s3store: invalid ref ts in ref key %q: %w", refKey, err)
	}
	shortID = seg[1]
	dataTsMicros, err = strconv.ParseInt(seg[2], 10, 64)
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
// The filename includes the write timestamp (µs since epoch)
// followed by the shortID so S3 LIST of a partition prefix
// returns files in chronological write order, and so the
// timestamp is recoverable without consulting the ref stream.
func BuildDataFilePath(
	dataPath string, hiveKey string, tsMicros int64, shortID string,
) string {
	return fmt.Sprintf("%s/%s/%d-%s.parquet",
		dataPath, hiveKey, tsMicros, shortID)
}

// ParseDataFileName is the inverse of BuildDataFilePath for the
// filename portion: it extracts the tsMicros and shortID from
// the last path segment of a data-file key. Callers typically
// pass filepath.Base(s3Key) or everything after the last '/'.
func ParseDataFileName(name string) (tsMicros int64, shortID string, err error) {
	name = strings.TrimSuffix(name, ".parquet")
	tsStr, short, ok := strings.Cut(name, "-")
	if !ok {
		return 0, "", fmt.Errorf(
			"s3store: invalid data filename: %s", name)
	}
	tsMicros, err = strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return 0, "", fmt.Errorf(
			"s3store: invalid ts in data filename %q: %w", name, err)
	}
	return tsMicros, short, nil
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
