package s3parquet

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/ueisele/s3store/internal/core"
)

// IndexDef declares a secondary index attached to a Store[T]. On
// every Write, the store calls Of per record, collects a
// deduplicated set of K values across the batch, and writes one
// empty "marker" object per distinct K into the S3 subtree at
// <Prefix>/_index/<Name>/. Each K's fields become hive-style
// path segments, so a LIST under a prefix of those segments
// returns every tuple matching that prefix.
//
// This is a cgo-free covering index: the marker filename encodes
// every lookup column, so Lookup answers its query with LIST
// only — no parquet reads, no DuckDB. Intended for high-cardinality
// equality lookups ("which customers had usage of SKU X in
// period P?") where partition pruning can't help.
type IndexDef[T any, K comparable] struct {
	// Name identifies the index under the store's <Prefix>/_index/
	// subtree. Required. Must be non-empty and free of '/'.
	Name string

	// Columns lists K's parquet column names in the order they
	// appear in the S3 path. Earlier columns form a narrower
	// LIST prefix when Lookup specifies them literally. Pick the
	// order based on how queries filter: most-selective first.
	// K must carry a `parquet:"..."` tag for every entry in
	// Columns, and no additional tagged fields.
	Columns []string

	// Of returns the K tuples extracted from a single record. The
	// store dedups marker paths across the batch via a
	// map[string]struct{}, so returning the same K for many
	// records is cheap — only one PUT happens per distinct path
	// (which, since the path is a deterministic function of K, is
	// equivalent to deduping by K). Returning an empty slice is
	// fine (no marker for that record).
	Of func(T) []K
}

// Index is the typed query handle returned by NewIndex. It holds
// the state needed to serialize K values to marker paths on write
// and parse marker paths back into K values on read.
//
// Indexes straddle the write/read split: they emit markers on
// Write (via the writer half) and resolve Lookup via S3 LIST plus
// Backfill via parquet reads (via the reader half). Keep both
// pointers so the Index is independent of the composing Store.
type Index[T any, K comparable] struct {
	writer    *writer[T]
	reader    *reader[T]
	name      string
	columns   []string
	indexPath string

	// fieldIndices[i] is the struct-field index on K that carries
	// the value for columns[i]. Populated once by buildIndexBinder.
	fieldIndices []int

	of func(T) []K
}

// NewIndex registers a secondary index on store and returns a
// typed handle for querying it. Call before the first Write so
// records aren't missed — writes that precede registration produce
// no markers (see repopulate-from-data in the backlog).
//
// Validation (at registration, not on every write):
//   - Name non-empty and contains no '/'.
//   - Columns list passes ValidatePartitionKeyParts.
//   - Every entry in Columns corresponds to a parquet-tagged
//     string field on K; no extra tagged fields on K.
//   - Of is non-nil.
func NewIndex[T any, K comparable](
	store *Store[T],
	def IndexDef[T, K],
) (*Index[T, K], error) {
	if store == nil {
		return nil, fmt.Errorf(
			"s3parquet: NewIndex: store is nil")
	}
	if def.Name == "" {
		return nil, fmt.Errorf(
			"s3parquet: NewIndex: Name is required")
	}
	if strings.Contains(def.Name, "/") {
		return nil, fmt.Errorf(
			"s3parquet: NewIndex: Name %q must not contain '/'",
			def.Name)
	}
	if err := core.ValidatePartitionKeyParts(def.Columns); err != nil {
		return nil, fmt.Errorf(
			"s3parquet: NewIndex %q: %w", def.Name, err)
	}
	if def.Of == nil {
		return nil, fmt.Errorf(
			"s3parquet: NewIndex %q: Of is required", def.Name)
	}

	fieldIndices, err := buildIndexBinder[K](def.Columns)
	if err != nil {
		return nil, fmt.Errorf(
			"s3parquet: NewIndex %q: %w", def.Name, err)
	}

	indexPath := core.IndexPath(store.writer.cfg.Prefix, def.Name)

	idx := &Index[T, K]{
		writer:       store.writer,
		reader:       store.reader,
		name:         def.Name,
		columns:      def.Columns,
		indexPath:    indexPath,
		fieldIndices: fieldIndices,
		of:           def.Of,
	}

	store.registerIndex(indexWriter[T]{
		name: def.Name,
		pathsOf: func(rec T) ([]string, error) {
			entries := idx.of(rec)
			if len(entries) == 0 {
				return nil, nil
			}
			paths := make([]string, 0, len(entries))
			for _, e := range entries {
				values, err := idx.entryToValues(e)
				if err != nil {
					return nil, err
				}
				p := core.BuildIndexMarkerPath(
					idx.indexPath, idx.columns, values)
				if len(p) > maxMarkerKeyLen {
					return nil, fmt.Errorf(
						"s3parquet: index %q marker key is "+
							"%d bytes, exceeds %d (S3 limit is "+
							"1024; narrow Columns or shorten values)",
						idx.name, len(p), maxMarkerKeyLen)
				}
				paths = append(paths, p)
			}
			return paths, nil
		},
	})

	return idx, nil
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
func (i *Index[T, K]) entryToValues(entry K) ([]string, error) {
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

// BackfillStats reports the work Backfill did: how many parquet
// objects it scanned, how many records it decoded, and how many
// marker PUTs it issued. Markers is per-object, not globally
// deduplicated — a marker path produced by N parquet files is
// counted N times (reflects S3 request cost, not unique marker
// count). Useful for progress logging in a migration job.
type BackfillStats struct {
	DataObjects int
	Records     int
	Markers     int
}

// Backfill scans existing parquet data under pattern and writes
// index markers for every record already in the store. The normal
// path is to call NewIndex before the first Write; Backfill is
// the relief valve for adding an index to a live store or
// recovering after an index was missed.
//
// pattern is evaluated against the Store's PartitionKeyParts
// (same grammar as Store.Read), NOT against the index's Columns
// — Backfill LISTs parquet data files, which are keyed by
// partition. "*" backfills everything. Pattern-scoping lets a
// migration job parallelize itself across partitions — run one
// Backfill per shard rather than a single multi-hour call.
//
// Safe to run concurrently with Write: S3 PUTs are idempotent, so
// a marker produced by both paths is just written twice. Safe to
// retry after a cancel or crash for the same reason: work already
// done is work already persisted.
//
// Processes parquet objects with bounded parallelism
// (pollDownloadConcurrency), matching the Read path. Within an
// object, marker PUTs run serially so the net in-flight S3
// request count stays at roughly pollDownloadConcurrency rather
// than compounding to concurrency squared. Peak memory is bounded
// by (concurrency × largest-object size).
func (i *Index[T, K]) Backfill(
	ctx context.Context, pattern string,
) (BackfillStats, error) {
	var stats BackfillStats

	plan, err := buildReadPlan(
		pattern, i.reader.dataPath, i.reader.cfg.PartitionKeyParts)
	if err != nil {
		return stats, err
	}

	keys, err := i.reader.listMatchingParquet(ctx, plan)
	if err != nil {
		return stats, err
	}
	if len(keys) == 0 {
		return stats, nil
	}
	stats.DataObjects = len(keys)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var recordsTotal, markersTotal atomic.Int64
	errs := make([]error, len(keys))
	sem := make(chan struct{}, pollDownloadConcurrency)
	var wg sync.WaitGroup

	for n, key := range keys {
		wg.Add(1)
		go func(n int, key string) {
			defer wg.Done()
			// Acquire inside the goroutine so a parent-ctx
			// cancel or sibling failure unblocks waiters
			// promptly instead of backlogging the queue.
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				errs[n] = ctx.Err()
				return
			}
			defer func() { <-sem }()

			paths, nRecs, err := i.markerPathsForObject(ctx, key)
			if err != nil {
				// LIST-to-GET race: a data file listed a moment
				// ago is gone now. Skip-and-notify matches
				// downloadAndDecodeAll's at-least-once posture —
				// one missing file shouldn't fail the whole
				// backfill. Other GET errors remain fatal.
				var nsk *s3types.NoSuchKey
				if errors.As(err, &nsk) {
					if i.reader.cfg.OnMissingData != nil {
						i.reader.cfg.OnMissingData(key)
					}
					return
				}
				errs[n] = err
				cancel()
				return
			}
			recordsTotal.Add(int64(nRecs))

			// Serial PUTs within the object. Across objects we
			// already run pollDownloadConcurrency-wide, so
			// per-object fan-out would compound to
			// concurrency² and overwhelm the SDK connection
			// pool. Net in-flight ≈ concurrency.
			for _, p := range paths {
				if err := i.writer.putObject(
					ctx, p, nil, "application/octet-stream",
				); err != nil {
					errs[n] = fmt.Errorf(
						"s3parquet: backfill index %q: put marker: %w",
						i.name, err)
					cancel()
					return
				}
			}
			markersTotal.Add(int64(len(paths)))
		}(n, key)
	}
	wg.Wait()

	stats.Records = int(recordsTotal.Load())
	stats.Markers = int(markersTotal.Load())

	// First real error wins; skip cancellations so we report
	// the root-cause failure rather than the cancel it
	// triggered in sibling goroutines.
	for _, e := range errs {
		if e == nil || errors.Is(e, context.Canceled) {
			continue
		}
		return stats, e
	}
	return stats, nil
}

// markerPathsForObject decodes one parquet data object and
// returns the deduplicated marker paths its records produce under
// this index, plus the record count (for stats). Pulled out of
// Backfill so the dedup map doesn't leak across objects — each
// file stands on its own, keeping memory bounded by the largest
// file rather than the full backfill set.
func (i *Index[T, K]) markerPathsForObject(
	ctx context.Context, key string,
) ([]string, int, error) {
	data, err := i.reader.getObjectBytes(ctx, key)
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
		for _, entry := range i.of(rec) {
			values, err := i.entryToValues(entry)
			if err != nil {
				return nil, 0, fmt.Errorf(
					"s3parquet: backfill index %q on %s: %w",
					i.name, key, err)
			}
			p := core.BuildIndexMarkerPath(
				i.indexPath, i.columns, values)
			if len(p) > maxMarkerKeyLen {
				return nil, 0, fmt.Errorf(
					"s3parquet: backfill index %q: marker key "+
						"is %d bytes, exceeds %d",
					i.name, len(p), maxMarkerKeyLen)
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

// Lookup returns every K whose marker matches the key pattern.
// pattern uses the same grammar as Store.Read (see
// core.ValidateKeyPattern), evaluated against Columns.
//
// Results are unbounded: narrow the pattern if an index has
// millions of matching markers. No deduplication is needed —
// S3 PUT is idempotent, so distinct markers imply distinct K.
//
// SettleWindow applies the same way it does to Poll: markers
// whose S3 LastModified timestamp is within now - SettleWindow
// are hidden, so a caller that writes and immediately Looks Up
// sees consistent state with PollRecords.
func (i *Index[T, K]) Lookup(
	ctx context.Context, pattern string,
) ([]K, error) {
	plan, err := buildReadPlan(pattern, i.indexPath, i.columns)
	if err != nil {
		return nil, err
	}

	keys, err := i.listMatchingMarkers(ctx, plan)
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
func (i *Index[T, K]) valuesToEntry(values []string) K {
	var zero K
	v := reflect.ValueOf(&zero).Elem()
	for j := range i.columns {
		v.Field(i.fieldIndices[j]).SetString(values[j])
	}
	return zero
}

// listMatchingMarkers LISTs every marker under plan.ListPrefix and
// returns the keys that match plan.Match AND were modified before
// the settle-window cutoff. S3 handles pagination; filtering runs
// per-page in memory.
func (i *Index[T, K]) listMatchingMarkers(
	ctx context.Context, plan *readPlan,
) ([]string, error) {
	cutoff := time.Now().Add(-i.reader.cfg.settleWindow())

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(i.reader.cfg.Bucket),
		Prefix: aws.String(plan.ListPrefix),
	}
	paginator := s3.NewListObjectsV2Paginator(i.reader.s3, input)

	var keys []string
	suffix := "/" + core.IndexMarkerFilename
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf(
				"s3parquet: index %q list: %w", i.name, err)
		}
		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			if !strings.HasSuffix(key, suffix) {
				continue
			}
			if obj.LastModified != nil &&
				obj.LastModified.After(cutoff) {
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
