package core

import (
	"path"
	"time"
)

// ApplyIdempotentRead filters a flat LIST of data-file KeyMetas
// so the result reflects state as of the first write of the given
// idempotency token (see WithIdempotentRead for the motivating
// use-case).
//
// The token is a raw idempotency token as passed to
// WithIdempotencyToken on the write side — the data file's id
// equals the token verbatim, so its filename is "{token}.parquet".
//
// Two filters apply per partition:
//
//   - Self-exclusion: files whose basename equals "{token}.parquet"
//     are dropped.
//   - Later-write exclusion: among files matching the token, the
//     minimum LastModified becomes barrier[partition]. For every
//     other file in the same partition, files whose LastModified
//     is >= barrier[partition] are dropped.
//
// Partitions where the token does not appear are unfiltered — on
// the first attempt the full current state is returned.
//
// Caller validates the token via ValidateIdempotencyToken before
// reaching here (the reader code does this explicitly). Keys that
// don't parse as data files under dataPath pass through unfiltered
// so the helper doesn't silently drop garbage that higher layers
// are meant to notice.
func ApplyIdempotentRead(
	keys []KeyMeta, dataPath, token string,
) []KeyMeta {
	if token == "" || len(keys) == 0 {
		return keys
	}
	tokenFilename := token + ".parquet"

	// Pass 1: compute min(LastModified) per partition across token
	// matches.
	barrier := make(map[string]time.Time)
	for _, k := range keys {
		if path.Base(k.Key) != tokenFilename {
			continue
		}
		hk, ok := HiveKeyOfDataFile(k.Key, dataPath)
		if !ok {
			continue
		}
		cur, seen := barrier[hk]
		if !seen || k.InsertedAt.Before(cur) {
			barrier[hk] = k.InsertedAt
		}
	}

	if len(barrier) == 0 {
		return keys
	}

	// Pass 2: apply self-exclusion + later-write exclusion per
	// partition. Non-data-file keys and keys in unbarriered
	// partitions flow through untouched.
	out := make([]KeyMeta, 0, len(keys))
	for _, k := range keys {
		hk, ok := HiveKeyOfDataFile(k.Key, dataPath)
		if !ok {
			out = append(out, k)
			continue
		}
		b, has := barrier[hk]
		if !has {
			out = append(out, k)
			continue
		}
		if path.Base(k.Key) == tokenFilename {
			continue
		}
		if !k.InsertedAt.Before(b) {
			continue
		}
		out = append(out, k)
	}
	return out
}
