package s3store

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Commit-marker primitives shared by the write path's upfront-LIST
// dedup gate, the post-marker timeliness check, the snapshot
// reader's pairing, and (Phase 7) LookupCommit.
//
// A commit marker is a zero-byte sibling of the data file at
// {dataPath}/{partition}/{id}.commit. The marker existing alone
// is not enough — its server-stamped LastModified must satisfy
// `marker.LM - data.LM < CommitTimeout` for the commit to be
// considered valid. Both timestamps come from S3's response
// headers, so the comparison is free of client/server clock skew.
//
// Per-attempt-paths under WithIdempotencyToken: every retry of
// the same logical write lands at a fresh per-attempt id
// ({token}-{tsMicros}-{shortID}), so the write path never
// overwrites — sidesteps multi-site StorageGRID's eventual-
// consistency exposure on overwrites. The upfront LIST under
// {partition}/{token}- is the dedup gate.

// commitMarkerKey returns the S3 object key of the commit marker
// for a data file. Format: {dataPath}/{partition}/{id}.commit
// (sibling of the .parquet under the same partition).
func commitMarkerKey(dataPath, partition, id string) string {
	return fmt.Sprintf("%s/%s/%s.commit", dataPath, partition, id)
}

// dataLMMetaKey is the user-metadata header that the writer
// stamps on the commit marker carrying the data file's server-
// stamped LastModified, in microseconds since the Unix epoch
// encoded as decimal. The change-stream read path consumes it
// to apply isCommitValid with a single per-ref HEAD on the
// marker — no second HEAD on the data file. Snapshot reads,
// the upfront-LIST dedup gate, and LookupCommit get
// data.LastModified directly from their LIST response and
// don't need the marker's metadata.
//
// Lowercase: AWS SDK v2 surfaces user-metadata keys lowercased.
const dataLMMetaKey = "datalm"

// isCommitValid reports whether a commit marker is timely enough
// to count as committed. Both timestamps are S3-server-stamped, so
// the comparison is free of client/server clock skew and produces
// the same answer for every reader regardless of clock state.
//
// The contract is server-time-only: the writer's wall clock is
// not in the protocol. dataLM is captured by the writer's
// post-data HEAD (so refTsMicros is server-stamped), and markerLM
// is read from the marker HEAD or LIST response.
//
// A zero dataLM or markerLM trips the false branch — caller's
// responsibility to feed populated values.
func isCommitValid(dataLM, markerLM time.Time, commitTimeout time.Duration) bool {
	if dataLM.IsZero() || markerLM.IsZero() {
		return false
	}
	return markerLM.Sub(dataLM) < commitTimeout
}

// truncLMToSecond normalizes an S3-server-stamped LastModified to
// second precision. Required because the same object's LM comes
// back at *different* precisions depending on how we read it:
//
//   - HEAD's Last-Modified header is HTTP-date (RFC 1123) format,
//     which only carries second precision — anything sub-second
//     is silently truncated by the protocol.
//   - LIST's LastModified is an ISO 8601 timestamp, and MinIO
//     emits it at millisecond precision; AWS S3 emits it at
//     second precision (matching what the bucket persists).
//
// Without normalization, a HEAD and a LIST against the same
// object return values differing by up to 999 ms — the upfront-
// LIST dedup gate's reconstructed refTsMicros (from LIST) would
// not match the original write's refTsMicros (from HEAD), and
// retries would surface "drifted RefPath" even when the
// reconstruction is logically correct. Truncation to seconds
// produces the same value via either path on every backend.
//
// The cost is granularity: refs from two writes within the same
// wall-clock second can collide on refTsMicros. The id portion
// of the ref filename keeps them distinct (every per-attempt id
// embeds tsMicros + an 8-hex shortID, so collisions are
// vanishingly improbable), and lex ordering by (refTsMicros, id)
// remains stable.
func truncLMToSecond(t time.Time) time.Time {
	return t.Truncate(time.Second)
}

// commitInfo bundles a data file and its commit-marker sibling —
// the unit the upfront-LIST dedup gate, snapshot reads, and
// LookupCommit work in. Both LMs come from a LIST page so the
// helper does no HEADs.
//
// The .commit may be missing on a freshly-LISTed pair: a
// concurrent attempt that PUT its data but hasn't reached the
// marker PUT yet, or a failed attempt that died after data PUT.
// markerLM is zero in that case; isCommitValid returns false; the
// pair is treated as uncommitted (and is invisible to every read
// path with marker gating).
type commitInfo struct {
	id        string
	dataKey   string
	dataLM    time.Time
	markerKey string
	markerLM  time.Time // zero if the .commit sibling is absent
}

// valid reports whether ci passes the timeliness check.
func (ci commitInfo) valid(commitTimeout time.Duration) bool {
	return isCommitValid(ci.dataLM, ci.markerLM, commitTimeout)
}

// listCommitsForToken LISTs sibling .parquet/.commit pairs under
// {dataPath}/{partition}/{token}- and pairs them by id. Both LMs
// come from the LIST response — no HEADs.
//
// The trailing "-" anchors the LIST to per-attempt entries of
// this exact token (the per-attempt id format is
// {token}-{tsMicros}-{shortID}). Without the trailing "-", a
// LIST under "{partition}/{token}" would also pick up
// "{token}foo-..." paths if such existed.
func listCommitsForToken(
	ctx context.Context, target S3Target,
	dataPath, partition, token string,
) ([]commitInfo, error) {
	prefix := fmt.Sprintf("%s/%s/%s-", dataPath, partition, token)
	return listCommitsAtPrefix(ctx, target, prefix)
}

// listCommitsAtPrefix is the LIST-pair primitive: groups .parquet
// and .commit entries under prefix into commitInfo records keyed
// by id. Returns the pairs in arbitrary order — callers that need
// a specific ordering (e.g., return-the-first-valid) must impose
// it themselves.
//
// LIST-page LastModified values are truncated to second precision
// via truncLMToSecond so they match what target.head returns on
// the HEAD path (HEAD's Last-Modified is HTTP-date format, second
// precision only). Without normalization, the upfront-LIST dedup
// gate would reconstruct a refTsMicros that disagrees with the
// original write's HEAD-derived refTsMicros — same logical commit,
// different ref-key bytes — and retries would surface as
// "drifted RefPath" even when correct. See truncLMToSecond.
func listCommitsAtPrefix(
	ctx context.Context, target S3Target, prefix string,
) ([]commitInfo, error) {
	pairs := make(map[string]*commitInfo)
	err := target.listEach(ctx, prefix, "", 0,
		func(obj s3types.Object) (bool, error) {
			key := aws.ToString(obj.Key)
			id, isParquet, isCommit := parseDataOrCommitKey(key)
			if !isParquet && !isCommit {
				return true, nil
			}
			ci := pairs[id]
			if ci == nil {
				ci = &commitInfo{id: id}
				pairs[id] = ci
			}
			lm := truncLMToSecond(aws.ToTime(obj.LastModified))
			if isParquet {
				ci.dataKey = key
				ci.dataLM = lm
			} else {
				ci.markerKey = key
				ci.markerLM = lm
			}
			return true, nil
		})
	if err != nil {
		return nil, err
	}
	out := make([]commitInfo, 0, len(pairs))
	for _, ci := range pairs {
		out = append(out, *ci)
	}
	return out, nil
}

// parseDataOrCommitKey returns the id (basename without
// extension) of a data file or commit marker, plus which type.
// Returns (_, false, false) for keys that match neither shape —
// callers skip such entries silently.
func parseDataOrCommitKey(s3Key string) (id string, isParquet, isCommit bool) {
	base := path.Base(s3Key)
	switch {
	case strings.HasSuffix(base, ".parquet"):
		return strings.TrimSuffix(base, ".parquet"), true, false
	case strings.HasSuffix(base, ".commit"):
		return strings.TrimSuffix(base, ".commit"), false, true
	}
	return "", false, false
}

// findValidCommitForToken runs the upfront-LIST under
// {partition}/{token}- and returns the first valid commit
// (a pair whose timeliness check passes). Both LMs come from the
// LIST response — no HEADs. Returns ok=false when no valid commit
// exists, including when the token has no entries at all (first
// attempt of this logical write).
//
// Used by the write path's upfront-dedup gate; Phase 7's
// LookupCommit shares the same primitive.
func findValidCommitForToken(
	ctx context.Context, target S3Target,
	dataPath, partition, token string,
	commitTimeout time.Duration,
) (commitInfo, bool, error) {
	pairs, err := listCommitsForToken(ctx, target, dataPath, partition, token)
	if err != nil {
		return commitInfo{}, false, err
	}
	for _, ci := range pairs {
		if ci.valid(commitTimeout) {
			return ci, true, nil
		}
	}
	return commitInfo{}, false, nil
}

// reconstructWriteResult builds a WriteResult from a valid
// commitInfo using the existing encodeRefKey formula. The writer
// never recorded the WriteResult locally for prior attempts, so
// the upfront-LIST dedup gate (and Phase 7's LookupCommit) have
// to reconstruct it from LIST data + filename parsing:
//
//	DataPath   = ci.dataKey (full .parquet key from LIST)
//	InsertedAt = time.UnixMicro(tsMicros) parsed from the id
//	RefPath    = encodeRefKey(refPath, ci.dataLM, ci.id, tsMicros, hiveKey)
//	Offset     = Offset(RefPath)
//
// All inputs are derivable without a single HEAD or GET.
func reconstructWriteResult(
	refPath string, ci commitInfo, hiveKey string,
) (WriteResult, error) {
	tsMicros, err := tsMicrosFromID(ci.id)
	if err != nil {
		return WriteResult{}, err
	}
	refKey := encodeRefKey(refPath, ci.dataLM.UnixMicro(),
		ci.id, tsMicros, hiveKey)
	return WriteResult{
		Offset:     Offset(refKey),
		DataPath:   ci.dataKey,
		RefPath:    refKey,
		InsertedAt: time.UnixMicro(tsMicros),
	}, nil
}

// tsMicrosFromID extracts the writer's wall-clock-at-write-start
// (in microseconds since the Unix epoch) from an id of shape
// {token}-{tsMicros}-{shortID} or {tsMicros}-{shortID}. The id
// is unambiguously parseable because the trailing
// {tsMicros}-{shortID} has fixed widths in the realistic
// operating range (16-digit tsMicros from 2002-01-09 through
// 2286-11-20; 8-hex-char shortID).
//
// The token itself may contain dashes (an idempotency token is
// "printable ASCII without /" per validateIdempotencyToken), so
// we anchor on the last two dashes: the last splits shortID off,
// the second-to-last splits tsMicros off.
func tsMicrosFromID(id string) (int64, error) {
	lastDash := strings.LastIndexByte(id, '-')
	if lastDash <= 0 {
		return 0, fmt.Errorf(
			"s3store: id %q: missing - separator", id)
	}
	prefix := id[:lastDash]
	secondLastDash := strings.LastIndexByte(prefix, '-')
	var tsStr string
	if secondLastDash < 0 {
		// {tsMicros}-{shortID} (token-less auto-id).
		tsStr = prefix
	} else {
		// {token}-{tsMicros}-{shortID}; extract tsMicros.
		tsStr = prefix[secondLastDash+1:]
	}
	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf(
			"s3store: id %q: parse tsMicros %q: %w",
			id, tsStr, err)
	}
	return ts, nil
}
