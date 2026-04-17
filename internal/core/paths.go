package core

import (
	"fmt"
	"net/url"
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
// encoded ref filename and returns the decoded Hive key and the
// shortID.
func ParseRefKey(refKey string) (hiveKey string, shortID string, err error) {
	name := refKey
	if idx := strings.LastIndex(name, "/"); idx >= 0 {
		name = name[idx+1:]
	}
	name = strings.TrimSuffix(name, ".ref")

	parts := strings.SplitN(name, RefSeparator, 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf(
			"s3store: invalid ref key: %s", refKey)
	}

	_, shortID, ok := strings.Cut(parts[0], "-")
	if !ok {
		return "", "", fmt.Errorf(
			"s3store: invalid ref key: %s", refKey)
	}

	hiveKey, err = url.PathUnescape(parts[1])
	if err != nil {
		return "", "", fmt.Errorf(
			"s3store: invalid ref key %q: %w", refKey, err)
	}
	return hiveKey, shortID, nil
}

// BuildDataFilePath returns the S3 object key for a data file,
// given the store's data prefix (from DataPath), the Hive-style
// partition key, and the shortID.
func BuildDataFilePath(dataPath string, hiveKey string, shortID string) string {
	return fmt.Sprintf("%s/%s/%s.parquet", dataPath, hiveKey, shortID)
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
