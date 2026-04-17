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
//   - sorts lexicographically by tsMicros (Poll relies on this),
//   - carries the shortID that identifies the data file,
//   - carries the PathEscape'd Hive key via a RefSeparator-split
//     tail.
func EncodeRefKey(
	refPath string, tsMicros int64, shortID string, hiveKey string,
) string {
	return fmt.Sprintf("%s/%d-%s%s%s.ref",
		refPath, tsMicros, shortID,
		RefSeparator, url.PathEscape(hiveKey))
}

// ParseRefKey is the inverse of EncodeRefKey. It accepts any
// S3-style key (with or without a path prefix) ending in the
// encoded ref filename and returns the decoded Hive key, the
// write timestamp (µs since epoch), and the shortID.
func ParseRefKey(refKey string) (
	hiveKey string, tsMicros int64, shortID string, err error,
) {
	name := refKey
	if idx := strings.LastIndex(name, "/"); idx >= 0 {
		name = name[idx+1:]
	}
	name = strings.TrimSuffix(name, ".ref")

	parts := strings.SplitN(name, RefSeparator, 2)
	if len(parts) != 2 {
		return "", 0, "", fmt.Errorf(
			"s3store: invalid ref key: %s", refKey)
	}

	tsStr, short, ok := strings.Cut(parts[0], "-")
	if !ok {
		return "", 0, "", fmt.Errorf(
			"s3store: invalid ref key: %s", refKey)
	}
	tsMicros, err = strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return "", 0, "", fmt.Errorf(
			"s3store: invalid ts in ref key %q: %w", refKey, err)
	}

	hiveKey, err = url.PathUnescape(parts[1])
	if err != nil {
		return "", 0, "", fmt.Errorf(
			"s3store: invalid ref key %q: %w", refKey, err)
	}
	return hiveKey, tsMicros, short, nil
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
