package s3parquet

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/ueisele/s3store/internal/core"
)

// IndexLookupDef is the read-side subset of an index definition:
// Name + Columns, independent of the record type T. Build an
// Index directly from this when the caller is a read-only
// service (dashboard, query API) that never writes markers and
// doesn't need def.Of.
type IndexLookupDef[K comparable] struct {
	// Name identifies the index under the target's
	// <Prefix>/_index/ subtree. Required. Must be non-empty and
	// free of '/'.
	Name string

	// Columns lists K's parquet column names in the order they
	// appear in the S3 path. Earlier columns form a narrower
	// LIST prefix when Lookup specifies them literally. Pick the
	// order based on how queries filter: most-selective first.
	// K must carry a `parquet:"..."` tag for every entry in
	// Columns, and no additional tagged fields.
	Columns []string

	// ConsistencyControl sets the Consistency-Control HTTP header
	// on marker PUTs (write path, BackfillIndex) and marker LISTs
	// (Lookup). Together they give read-after-write visibility for
	// Lookup on backends where the default consistency level is
	// weaker than strong (e.g. NetApp StorageGRID with its default
	// read-after-new-write). Zero (ConsistencyDefault) sends no
	// header — correct on AWS S3 and MinIO, where LIST has been
	// strongly consistent since 2020 regardless of any header.
	//
	// NewIndexWithRegister / NewIndexFromStoreWithRegister copy the
	// enclosing Writer's ConsistencyControl onto the def when this
	// field is unset, so register-style callers don't need to
	// duplicate the setting. Explicit values on the def override.
	ConsistencyControl ConsistencyLevel
}

// IndexDef declares a secondary index attached to a dataset. On
// every Write through a RegisterIndex-wired Writer, the store
// calls Of per record, collects a deduplicated set of K values
// across the batch, and writes one empty "marker" object per
// distinct K into the S3 subtree at <Prefix>/_index/<Name>/.
// Each K's fields become hive-style path segments, so a LIST
// under a prefix of those segments returns every tuple matching
// that prefix.
//
// This is a cgo-free covering index: the marker filename encodes
// every lookup column, so Lookup answers its query with LIST
// only — no parquet reads, no DuckDB. Intended for high-cardinality
// equality lookups ("which customers had usage of SKU X in
// period P?") where partition pruning can't help.
//
// IndexDef embeds IndexLookupDef[K], so read-only callers (who
// only need Name + Columns) can accept the broader IndexDef and
// read just the read-side fields, but a writing caller still
// supplies Of.
type IndexDef[T any, K comparable] struct {
	IndexLookupDef[K]

	// Of returns the K tuples extracted from a single record. The
	// writer dedups marker paths across the batch via a
	// map[string]struct{}, so returning the same K for many
	// records is cheap — only one PUT happens per distinct path.
	// Returning an empty slice is fine (no marker for that record).
	Of func(T) []K
}

// Index is the typed read-handle for a secondary index.
// Pure read-side — Lookup issues LIST only. Built from an
// S3Target + IndexLookupDef so it carries no T; a single Index
// can be shared by services that read the index but never touch
// the underlying record schema.
//
// To build: NewIndex(target, lookupDef) for pure-read callers,
// NewIndexWithRegister(writer, def) / NewIndexFromStoreWithRegister(store, def)
// for callers that also want Write to emit markers for this
// index.
type Index[K comparable] struct {
	target       S3Target
	name         string
	columns      []string
	indexPath    string
	fieldIndices []int
	consistency  ConsistencyLevel
}

// NewIndex builds a query handle for an index whose markers a
// writer elsewhere produced. No Writer argument, no marker-
// emission registration — the live writer is expected to be
// wired separately (or not needed at all, e.g. for a read-only
// analytics service).
//
// Validation matches the write-side register path: Name
// non-empty and no '/', Columns passes ValidatePartitionKeyParts,
// every entry in Columns has a matching parquet-tagged string
// field on K, and no extra tagged fields on K.
func NewIndex[K comparable](
	target S3Target, def IndexLookupDef[K],
) (*Index[K], error) {
	// Lookup never consults target.PartitionKeyParts() — the
	// index's own Columns drive the LIST path — so we use the
	// reduced-validation helper instead of the full Target check.
	if err := target.ValidateLookup(); err != nil {
		return nil, err
	}
	return buildIndex(target, def)
}

// NewIndexWithRegister registers def on w so future Write calls
// emit markers, AND returns an Index[K] read-handle built from
// w.Target(). Use when a service writes and reads through a
// Writer but has no Reader/Store.
//
// Inherits w.cfg.ConsistencyControl onto the returned Index when
// def.ConsistencyControl is unset, so a register-style caller
// that already configured strong consistency on the Writer gets
// the same level on marker LISTs automatically.
func NewIndexWithRegister[T any, K comparable](
	w *Writer[T], def IndexDef[T, K],
) (*Index[K], error) {
	if w == nil {
		return nil, fmt.Errorf(
			"s3parquet: NewIndexWithRegister: writer is nil")
	}
	if def.ConsistencyControl == "" {
		def.ConsistencyControl = w.cfg.ConsistencyControl
	}
	if err := RegisterIndex(w, def); err != nil {
		return nil, err
	}
	return NewIndex(w.Target(), def.IndexLookupDef)
}

// NewIndexFromStoreWithRegister registers def on store.Writer
// AND returns an Index[K] read-handle built from store.Target().
// Single-call convenience for the common shape of a process that
// writes and queries through one Store.
func NewIndexFromStoreWithRegister[T any, K comparable](
	s *Store[T], def IndexDef[T, K],
) (*Index[K], error) {
	if s == nil {
		return nil, fmt.Errorf(
			"s3parquet: NewIndexFromStoreWithRegister: store is nil")
	}
	return NewIndexWithRegister(s.Writer, def)
}

// RegisterIndex wires def onto w so that every subsequent Write
// emits the markers def.Of produces. Call before the first
// Write; records written before registration produce no markers
// for this index (use BackfillIndex to cover those).
//
// Not concurrency-safe against Write: registration mutates the
// writer's index list and should complete at setup time. Safe
// to call multiple times with different defs to register
// multiple indexes on one writer.
func RegisterIndex[T any, K comparable](
	w *Writer[T], def IndexDef[T, K],
) error {
	if w == nil {
		return fmt.Errorf(
			"s3parquet: RegisterIndex: writer is nil")
	}
	if def.Of == nil {
		return fmt.Errorf(
			"s3parquet: RegisterIndex %q: Of is required", def.Name)
	}
	idx, err := buildIndex(w.Target(), def.IndexLookupDef)
	if err != nil {
		return err
	}
	w.registerIndex(indexWriter[T]{
		name: def.Name,
		pathsOf: func(rec T) ([]string, error) {
			entries := def.Of(rec)
			if len(entries) == 0 {
				return nil, nil
			}
			paths := make([]string, 0, len(entries))
			for _, e := range entries {
				p, err := idx.markerPath(e)
				if err != nil {
					return nil, err
				}
				paths = append(paths, p)
			}
			return paths, nil
		},
	})
	return nil
}

// buildIndex is the shared constructor behind NewIndex,
// NewIndexWithRegister, RegisterIndex, and BackfillIndex — they
// all need identical validation and the same {columns,
// fieldIndices, indexPath, name} state so marker paths stay
// byte-identical across call sites.
func buildIndex[K comparable](
	target S3Target, def IndexLookupDef[K],
) (*Index[K], error) {
	if def.Name == "" {
		return nil, fmt.Errorf(
			"s3parquet: index: Name is required")
	}
	if strings.Contains(def.Name, "/") {
		return nil, fmt.Errorf(
			"s3parquet: index: Name %q must not contain '/'",
			def.Name)
	}
	if err := core.ValidatePartitionKeyParts(def.Columns); err != nil {
		return nil, fmt.Errorf(
			"s3parquet: index %q: %w", def.Name, err)
	}
	fieldIndices, err := buildIndexBinder[K](def.Columns)
	if err != nil {
		return nil, fmt.Errorf(
			"s3parquet: index %q: %w", def.Name, err)
	}
	return &Index[K]{
		target:       target,
		name:         def.Name,
		columns:      def.Columns,
		indexPath:    core.IndexPath(target.Prefix(), def.Name),
		fieldIndices: fieldIndices,
		consistency:  def.ConsistencyControl,
	}, nil
}

// maxMarkerKeyLen caps the length of any marker S3 object key.
// S3's hard limit is 1024 bytes; we leave 24 bytes of buffer for
// any future additions (e.g. a shortID variant). Rejecting at
// build time surfaces the problem as a config-or-data error
// rather than an opaque S3 InvalidKey mid-batch.
const maxMarkerKeyLen = 1000

// entryToValues extracts the column values from a K in the order
// declared by columns, and validates each so the caller can
// safely embed them in an S3 key.
func (i *Index[K]) entryToValues(entry K) ([]string, error) {
	v := reflect.ValueOf(entry)
	out := make([]string, len(i.columns))
	for j, col := range i.columns {
		value := v.Field(i.fieldIndices[j]).String()
		if err := core.ValidateHivePartitionValue(value); err != nil {
			return nil, fmt.Errorf(
				"s3parquet: index %q column %q: %w",
				i.name, col, err)
		}
		out[j] = value
	}
	return out, nil
}

// markerPath builds the S3 key for the marker representing K
// under this index, enforcing the 1000-byte cap so a
// pathologically long K surfaces a clear error.
func (i *Index[K]) markerPath(entry K) (string, error) {
	values, err := i.entryToValues(entry)
	if err != nil {
		return "", err
	}
	p := core.BuildIndexMarkerPath(i.indexPath, i.columns, values)
	if len(p) > maxMarkerKeyLen {
		return "", fmt.Errorf(
			"s3parquet: index %q marker key is "+
				"%d bytes, exceeds %d (S3 limit is "+
				"1024; narrow Columns or shorten values)",
			i.name, len(p), maxMarkerKeyLen)
	}
	return p, nil
}

// Lookup returns every K whose marker matches the key pattern.
// pattern uses the same grammar as Reader.Read (see
// core.ValidateKeyPattern), evaluated against Columns.
//
// Results are unbounded: narrow the pattern if an index has
// millions of matching markers. No deduplication is needed —
// S3 PUT is idempotent, so distinct markers imply distinct K.
//
// Read-after-write: Lookup carries the Index's ConsistencyControl
// on the LIST, so it sees every marker the writer has already
// published under the matching level — strong on AWS S3, strong
// on MinIO, strong-global / strong-site on StorageGRID when the
// header is honoured. Unlike Poll there is no SettleWindow filter
// here; marker visibility is delegated to the storage layer.
//
// Single-pattern sugar over LookupMany — use LookupMany when
// the caller has an arbitrary set of column tuples (e.g. a
// non-Cartesian "(sku=A, customer=X), (sku=B, customer=Y)"
// selection).
func (i *Index[K]) Lookup(
	ctx context.Context, pattern string,
) ([]K, error) {
	return i.LookupMany(ctx, []string{pattern})
}

// LookupMany runs Lookup across every pattern and returns the
// unioned set of K values. Overlapping patterns are safe: a
// marker listed by two plans is counted once, so the returned
// slice has no duplicate K entries.
//
// Each pattern uses the grammar described on Lookup. Pass more
// than one when the target set isn't a Cartesian product of
// per-column values. LISTs fan out with the same concurrency
// cap as Reader's GETs (pollDownloadConcurrency), literal-
// duplicate patterns are dropped up front. Empty slice → (nil,
// nil). First malformed pattern fails the whole call with its
// index.
func (i *Index[K]) LookupMany(
	ctx context.Context, patterns []string,
) ([]K, error) {
	patterns = core.DedupePatterns(patterns)
	if len(patterns) == 0 {
		return nil, nil
	}

	plans, err := core.BuildReadPlans(patterns, i.indexPath, i.columns)
	if err != nil {
		return nil, fmt.Errorf(
			"s3parquet: index %q LookupMany %w", i.name, err)
	}

	keys, err := i.listAllMatchingMarkers(ctx, plans)
	if err != nil {
		return nil, err
	}

	out := make([]K, 0, len(keys))
	for _, key := range keys {
		values, err := core.ParseIndexMarkerKey(
			key, i.indexPath, i.columns)
		if err != nil {
			return nil, err
		}
		out = append(out, i.valuesToEntry(values))
	}
	return out, nil
}

// valuesToEntry builds a K from the []string the LIST paginator
// produces, using the same fieldIndices that write.go uses. All
// K fields are required to be string type (enforced at
// buildIndexBinder), so the assignment is a direct SetString and
// cannot fail.
func (i *Index[K]) valuesToEntry(values []string) K {
	var zero K
	v := reflect.ValueOf(&zero).Elem()
	for j := range i.columns {
		v.Field(i.fieldIndices[j]).SetString(values[j])
	}
	return zero
}

// listMatchingMarkers LISTs every marker under plan.ListPrefix and
// returns the keys that match plan.Match. S3 handles pagination;
// filtering runs per-page in memory.
//
// Carries the Index's ConsistencyControl on the LIST so that
// Lookup sees every marker the writer has already published. With
// a strong level (or on AWS S3, which is strong-LIST by default),
// Lookup is read-after-write — no settle window needed.
func (i *Index[K]) listMatchingMarkers(
	ctx context.Context, plan *core.ReadPlan,
) ([]string, error) {
	paginator := i.target.list(plan.ListPrefix)

	var keys []string
	suffix := "/" + core.IndexMarkerFilename
	for paginator.HasMorePages() {
		page, err := i.target.listPage(
			ctx, paginator, i.consistency)
		if err != nil {
			return nil, fmt.Errorf(
				"s3parquet: index %q list: %w", i.name, err)
		}
		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			if !strings.HasSuffix(key, suffix) {
				continue
			}
			hiveKey, ok := hiveKeyOfMarker(key, i.indexPath)
			if !ok {
				continue
			}
			if plan.Match(hiveKey) {
				keys = append(keys, key)
			}
		}
	}
	return keys, nil
}

// listAllMatchingMarkers runs listMatchingMarkers across every
// plan with bounded concurrency and returns the unioned set of
// marker keys, deduplicated (overlapping plans can list the
// same marker, e.g. "sku=*" and "sku=s1" both cover the s1
// markers).
func (i *Index[K]) listAllMatchingMarkers(
	ctx context.Context, plans []*core.ReadPlan,
) ([]string, error) {
	return core.RunPlansConcurrent(ctx, plans,
		i.target.EffectiveMaxInflightRequests(),
		i.listMatchingMarkers, identityKey)
}

// hiveKeyOfMarker returns the "col=val/col=val/..." body of a
// marker S3 key, stripping the base index path and the terminal
// "/m.idx" segment. Returns (rest, true) on well-shaped keys and
// ("", false) otherwise.
func hiveKeyOfMarker(s3Key, indexPath string) (string, bool) {
	prefix := indexPath + "/"
	if !strings.HasPrefix(s3Key, prefix) {
		return "", false
	}
	rest := s3Key[len(prefix):]
	tail := "/" + core.IndexMarkerFilename
	if !strings.HasSuffix(rest, tail) {
		return "", false
	}
	return rest[:len(rest)-len(tail)], true
}

// BackfillStats reports the work BackfillIndex did: how many
// parquet objects it scanned, how many records it decoded, and
// how many marker PUTs it issued. Markers is per-object, not
// globally deduplicated — a marker path produced by N parquet
// files is counted N times (reflects S3 request cost, not
// unique marker count). Useful for progress logging in a
// migration job.
type BackfillStats struct {
	DataObjects int
	Records     int
	Markers     int
}

// BackfillIndex scans existing parquet data under pattern and
// writes index markers for every record already present. The
// normal path is to wire RegisterIndex onto the live writer
// before the first Write; BackfillIndex is the relief valve for
// records written before that registration.
//
// Standalone by design: no Writer / Reader argument, no in-
// writer registration. The migration job constructs an S3Target
// pointing at the same dataset the live writer uses; BackfillIndex
// issues both GETs (parquet data) and PUTs (markers) through
// target.S3Client().
//
// pattern uses the same grammar as Reader.Read and is evaluated
// against target.PartitionKeyParts() (NOT the index's Columns) —
// backfill LISTs parquet data files, which are keyed by
// partition. "*" covers everything; shard across partitions to
// parallelize a migration.
//
// until is an exclusive upper bound on data-file LastModified.
// Typical use: until = OffsetAt(deployTime_of_live_writer), so
// backfill covers historical gaps (< deploy) while the live
// writer covers everything from deploy onward. Passing an empty
// Offset("") disables the bound — backfill covers every file
// currently present, at the cost of redundant PUTs for data the
// live writer has already marked (harmless because PUT is
// idempotent).
//
// onMissingData is invoked when a data-file GET returns S3
// NoSuchKey (dangling ref or LIST-to-GET race); the file is
// skipped rather than failing the whole backfill. Pass nil to
// disable the hook — skip-on-NoSuchKey is applied either way.
//
// Safe to run concurrently with a live writer that also emits
// markers on Write (S3 PUT is idempotent; duplicates are
// harmless). Safe to retry after a crash or cancel.
//
// Concurrency model matches Reader.Read: pollDownloadConcurrency
// across objects, serial PUTs within each object so net in-flight
// S3 requests stay at ≈ concurrency rather than concurrency².
// Peak memory is bounded by (concurrency × largest-object size).
//
// Single-pattern sugar over BackfillIndexMany — use that when a
// migration needs to cover an arbitrary set of partition tuples
// that can't be expressed as one Cartesian pattern.
func BackfillIndex[T any, K comparable](
	ctx context.Context,
	target S3Target,
	def IndexDef[T, K],
	pattern string,
	until Offset,
	onMissingData func(dataPath string),
) (BackfillStats, error) {
	return BackfillIndexMany(
		ctx, target, def, []string{pattern}, until, onMissingData)
}

// BackfillIndexMany runs BackfillIndex across every pattern in
// patterns, LISTing with bounded concurrency and feeding the
// deduplicated union of data-file keys through a single
// backfill pipeline. Stats aggregate across patterns: each
// parquet file is scanned once even if two patterns match it.
//
// Empty slice is a no-op: (BackfillStats{}, nil). First
// malformed pattern fails with its index.
func BackfillIndexMany[T any, K comparable](
	ctx context.Context,
	target S3Target,
	def IndexDef[T, K],
	patterns []string,
	until Offset,
	onMissingData func(dataPath string),
) (BackfillStats, error) {
	var stats BackfillStats

	patterns = core.DedupePatterns(patterns)
	if len(patterns) == 0 {
		return stats, nil
	}

	// Full Target check — BackfillIndex LISTs partitioned data
	// files (plan.Match consults PartitionKeyParts), so
	// validateLookup's reduced subset isn't enough.
	if err := target.Validate(); err != nil {
		return stats, err
	}
	if def.Of == nil {
		return stats, fmt.Errorf(
			"s3parquet: BackfillIndexMany %q: Of is required", def.Name)
	}

	idx, err := buildIndex(target, def.IndexLookupDef)
	if err != nil {
		return stats, err
	}

	dataPath := core.DataPath(target.Prefix())
	plans, err := core.BuildReadPlans(patterns, dataPath, target.PartitionKeyParts())
	if err != nil {
		return stats, fmt.Errorf(
			"s3parquet: BackfillIndexMany %w", err)
	}

	keys, err := listDataFilesBelowUntil(
		ctx, target, plans, dataPath, until, def.ConsistencyControl)
	if err != nil {
		return stats, err
	}
	if len(keys) == 0 {
		return stats, nil
	}
	stats.DataObjects = len(keys)

	var recordsTotal, markersTotal atomic.Int64
	err = core.FanOut(ctx, keys, target.EffectiveMaxInflightRequests(),
		func(ctx context.Context, _ int, key string) error {
			paths, nRecs, err := backfillMarkersForObject(
				ctx, target, idx, def.Of, key, def.ConsistencyControl)
			if err != nil {
				// LIST-to-GET race: a data file listed a moment
				// ago is gone now. Skip-and-notify matches the
				// read path's at-least-once posture — one missing
				// file shouldn't fail the whole backfill. Other
				// GET errors remain fatal.
				if _, ok := errors.AsType[*s3types.NoSuchKey](err); ok {
					if onMissingData != nil {
						onMissingData(key)
					}
					return nil
				}
				return err
			}
			recordsTotal.Add(int64(nRecs))

			// Serial marker PUTs within the object — the per-target
			// MaxInflightRequests semaphore on target.put already
			// caps net in-flight, so per-object fan-out would just
			// queue at the semaphore.
			for _, p := range paths {
				if err := target.put(
					ctx, p, nil, "application/octet-stream",
					def.ConsistencyControl,
				); err != nil {
					return fmt.Errorf(
						"s3parquet: backfill index %q: put marker: %w",
						idx.name, err)
				}
			}
			markersTotal.Add(int64(len(paths)))
			return nil
		})

	stats.Records = int(recordsTotal.Load())
	stats.Markers = int(markersTotal.Load())
	return stats, err
}

// listDataFilesBelowUntil LISTs parquet data files matching plan
// and returns those whose S3 LastModified is strictly before the
// time encoded in until. An empty until disables the filter.
// A non-empty but unparseable until is an error — callers should
// pass Offset("") to mean "no bound" rather than relying on
// silent fallthrough.
func listDataFilesBelowUntil(
	ctx context.Context,
	target S3Target,
	plans []*core.ReadPlan,
	dataPath string,
	until Offset,
	consistency ConsistencyLevel,
) ([]string, error) {
	var cutoff time.Time
	filter := false
	if until != "" {
		cutoff = parseUntilToTime(until)
		if cutoff.IsZero() {
			return nil, fmt.Errorf(
				"s3parquet: BackfillIndex: until %q is not a "+
					"valid Offset (use OffsetAt to construct one)",
				string(until))
		}
		filter = true
	}

	return core.RunPlansConcurrent(ctx, plans,
		target.EffectiveMaxInflightRequests(),
		func(ctx context.Context, plan *core.ReadPlan) ([]string, error) {
			return listDataFilesForPlan(
				ctx, target, plan, dataPath, filter, cutoff, consistency)
		}, identityKey)
}

// listDataFilesForPlan is the per-plan body extracted so the
// single-plan and multi-plan paths share one code path for the
// LIST + filter logic.
func listDataFilesForPlan(
	ctx context.Context,
	target S3Target,
	plan *core.ReadPlan,
	dataPath string,
	filter bool,
	cutoff time.Time,
	consistency ConsistencyLevel,
) ([]string, error) {
	paginator := target.list(plan.ListPrefix)

	var keys []string
	for paginator.HasMorePages() {
		page, err := target.listPage(
			ctx, paginator, consistency)
		if err != nil {
			return nil, fmt.Errorf(
				"s3parquet: backfill list data files: %w", err)
		}
		for _, obj := range page.Contents {
			objKey := aws.ToString(obj.Key)
			if !strings.HasSuffix(objKey, ".parquet") {
				continue
			}
			hiveKey, ok := core.HiveKeyOfDataFile(objKey, dataPath)
			if !ok {
				continue
			}
			if !plan.Match(hiveKey) {
				continue
			}
			if filter && obj.LastModified != nil &&
				!obj.LastModified.Before(cutoff) {
				continue
			}
			keys = append(keys, objKey)
		}
	}
	return keys, nil
}

// backfillMarkersForObject decodes one parquet data object and
// returns the deduplicated marker paths its records produce under
// this index, plus the record count (for stats). Pulled out of
// BackfillIndex's main loop so the dedup map doesn't leak across
// objects — each file stands on its own, keeping memory bounded
// by the largest file rather than the full backfill set.
func backfillMarkersForObject[T any, K comparable](
	ctx context.Context,
	target S3Target,
	idx *Index[K],
	of func(T) []K,
	key string,
	consistency ConsistencyLevel,
) ([]string, int, error) {
	data, err := target.get(ctx, key, consistency)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"s3parquet: backfill get %s: %w", key, err)
	}
	recs, err := decodeParquet[T](data)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"s3parquet: backfill decode %s: %w", key, err)
	}

	seen := make(map[string]struct{})
	for _, rec := range recs {
		for _, entry := range of(rec) {
			p, err := idx.markerPath(entry)
			if err != nil {
				return nil, 0, fmt.Errorf(
					"s3parquet: backfill index %q on %s: %w",
					idx.name, key, err)
			}
			seen[p] = struct{}{}
		}
	}
	if len(seen) == 0 {
		return nil, len(recs), nil
	}
	paths := make([]string, 0, len(seen))
	for p := range seen {
		paths = append(paths, p)
	}
	return paths, len(recs), nil
}

// parseUntilToTime recovers the time.Time encoded in an Offset.
// Two shapes are accepted:
//
//   - RefCutoff prefix "{refPath}/{tsMicros}" (what OffsetAt
//     returns) — the intended input.
//   - Full ref key "{refPath}/{tsMicros}-{shortID}_<hiveKey>.ref"
//     — a WriteResult.Offset, accepted as a convenience so a
//     caller can pass the last-written offset directly.
//
// Returns a zero time.Time on shapes that don't carry a parseable
// decimal timestamp so the caller can reject the input.
func parseUntilToTime(off Offset) time.Time {
	s := string(off)
	// Full ref key path first — ParseRefKey accepts the shape.
	// The ref-publication timestamp is what RefCutoff encodes, so
	// use refTsMicros (not dataTsMicros) as the "what time does
	// this offset correspond to" answer.
	if _, refTsMicros, _, _, err := core.ParseRefKey(s); err == nil {
		return time.UnixMicro(refTsMicros)
	}
	// RefCutoff prefix: "{refPath}/{tsMicros}". The last '/'
	// separator starts the decimal tail.
	i := strings.LastIndex(s, "/")
	if i < 0 || i == len(s)-1 {
		return time.Time{}
	}
	tsMicros, err := strconv.ParseInt(s[i+1:], 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.UnixMicro(tsMicros)
}

// buildIndexBinder reflects on K to produce the field-index map
// the write and read paths use to project between K and the
// []string form. Every column must have a matching
// parquet-tagged string field; no extra tagged fields are
// allowed (we want tight coupling between K and Columns); and
// the same tag must not appear on two fields (the parquet-go
// "last-wins" behaviour would silently drop one of them).
func buildIndexBinder[K any](columns []string) ([]int, error) {
	t := reflect.TypeFor[K]()
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf(
			"index entry type %s must be a struct", t)
	}

	tagged := make(map[string]int, t.NumField())
	for i := range t.NumField() {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		tag := f.Tag.Get("parquet")
		if tag == "" {
			continue
		}
		name, _, _ := strings.Cut(tag, ",")
		if name == "" || name == "-" {
			continue
		}
		if f.Type.Kind() != reflect.String {
			return nil, fmt.Errorf(
				"index entry field %q (%s): only string "+
					"fields are supported, got %s",
				name, f.Name, f.Type.Kind())
		}
		if prev, dup := tagged[name]; dup {
			return nil, fmt.Errorf(
				"index entry has duplicate parquet tag %q on "+
					"fields %q and %q",
				name, t.Field(prev).Name, f.Name)
		}
		tagged[name] = i
	}

	fieldIndices := make([]int, len(columns))
	for i, col := range columns {
		fi, ok := tagged[col]
		if !ok {
			return nil, fmt.Errorf(
				"index column %q has no matching "+
					"parquet-tagged field on the entry type",
				col)
		}
		fieldIndices[i] = fi
	}

	if len(tagged) != len(columns) {
		for name := range tagged {
			if !slices.Contains(columns, name) {
				return nil, fmt.Errorf(
					"index entry has extra parquet-tagged "+
						"field %q not in Columns", name)
			}
		}
	}

	return fieldIndices, nil
}
