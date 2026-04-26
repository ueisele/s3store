package s3parquet

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/ueisele/s3store/internal/core"
)

// IndexLookupDef is the read-side definition of a secondary
// index. Build an IndexReader[K] from it via NewIndexReader
// when a service queries an existing index (dashboard, query
// API).
//
// Name + Columns identify the index in S3; From projects the
// per-marker column values back into a typed K. From is optional
// — when nil, the library reflects K's parquet-tagged string
// fields against Columns and assembles K from the values slice.
// Provide a custom From for K's that have no parquet tags or
// when extra validation/transformation is needed.
type IndexLookupDef[K comparable] struct {
	// Name identifies the index under the target's
	// <Prefix>/_index/ subtree. Required. Must be non-empty and
	// free of '/'.
	Name string

	// Columns lists the index's column names in the order they
	// appear in the S3 path. Earlier columns form a narrower LIST
	// prefix when Lookup specifies them literally. Pick the order
	// based on how queries filter: most-selective first.
	Columns []string

	// From projects a Lookup-result values slice (one entry per
	// Columns position, in declared order) onto a typed K. Nil →
	// reflection: every parquet-tagged field on K must match
	// exactly one Columns entry. String fields are set directly;
	// time.Time fields are parsed via time.Parse(Layout.Time, ...).
	// Use a custom From when K has no parquet tags or when extra
	// validation is needed.
	//
	// values length is guaranteed to equal len(Columns); custom
	// From implementations may rely on positional access (e.g.
	// values[0] for Columns[0]).
	//
	// Errors are propagated by Lookup with the index name as
	// wrapping context.
	From func(values []string) (K, error)

	// Layout configures how non-string parquet-tagged fields on K
	// are parsed from path segments when From is nil. Honored
	// only by the auto-binding path; an explicit From bypasses
	// Layout entirely.
	//
	// For correctness, Layout.Time on the read side MUST match
	// IndexDef.Layout.Time on the write side — drift produces
	// silently wrong values or parse errors. Define one constant
	// and reuse it on both sides.
	Layout Layout
}

// IndexDef is the write-side definition of a secondary index.
// Pass a slice of these on WriterConfig.Indexes / Config.Indexes
// at construction; the writer iterates each registered index per
// record, builds the marker S3 key from Columns + Of's return,
// and PUTs one empty marker per distinct key.
//
// The previous late-binding RegisterIndex API is gone: indexes
// are part of the writer's construction, so registration cannot
// race with Write and "registered after first Write" is not a
// reachable state.
type IndexDef[T any] struct {
	// Name identifies the index under the target's
	// <Prefix>/_index/ subtree. Required. Must be non-empty,
	// free of '/', and unique across IndexDef[T] entries on the
	// same writer.
	Name string

	// Columns lists the index's column names in the order they
	// appear in the S3 path. Same ordering rules as
	// IndexLookupDef.Columns (most-selective first for LIST
	// pruning).
	Columns []string

	// Of extracts the per-record column values, in Columns order,
	// as a slice. Returning (nil, nil) skips the record (no marker
	// emitted). A non-nil values slice must have length equal to
	// len(Columns); each entry must satisfy
	// core.ValidateHivePartitionValue.
	//
	// Of is optional: nil → reflection. The library walks T's
	// parquet-tagged fields and, for every Columns entry, picks
	// the matching field. String fields project directly;
	// time.Time fields require Layout.Time to be set. T may
	// carry parquet tags not in Columns — those are ignored.
	//
	// Provide a custom Of when the auto-projection isn't enough
	// (per-column time formats, derived values, computed strings):
	//
	//	Of: func(r Record) ([]string, error) {
	//	    return []string{r.SKU, r.At.Format(time.RFC3339)}, nil
	//	}
	Of func(T) ([]string, error)

	// Layout configures how non-string parquet-tagged fields on T
	// are formatted into path segments when Of is nil. Honored
	// only by the auto-projection path; an explicit Of bypasses
	// Layout entirely.
	Layout Layout
}

// Layout configures auto-projection formatting for non-string
// fields on T. Each field is the layout for one type family;
// empty means "auto-projection refuses this type — set the
// field, or write Of explicitly."
//
// Layout choices are part of the marker S3 key — once published,
// changing the layout orphans every prior marker. Pick a stable
// format up front (use BackfillIndex if you ever need to migrate).
type Layout struct {
	// Time is the layout string passed to time.Time.Format for
	// any column matching a time.Time field on T. Examples:
	// time.RFC3339, "2006-01-02" (date-only), "2006-01" (month).
	// Empty + a time column on T → error at NewWriter.
	//
	// time.Time.Format uses the value's own zone; call .UTC()
	// upstream if you want zone-stable keys.
	Time string
}

// IndexReader is the typed read-handle for a secondary index.
// Build via NewIndexReader from a S3Target + IndexLookupDef[K].
// Lookup issues LIST only — no parquet reads, no DuckDB.
type IndexReader[K comparable] struct {
	target    S3Target
	name      string
	columns   []string
	indexPath string
	bind      func(values []string) (K, error)
}

// NewIndexReader builds a query handle for an index. Validates
// Name + Columns and, when def.From is nil, K's parquet tags
// against Columns. With a custom From, only the def-level fields
// are validated — the caller is responsible for producing valid
// K's.
func NewIndexReader[K comparable](
	target S3Target, def IndexLookupDef[K],
) (*IndexReader[K], error) {
	// Lookup never consults target.PartitionKeyParts() — the
	// index's own Columns drive the LIST path — so we use the
	// reduced-validation helper instead of the full Target check.
	if err := target.ValidateLookup(); err != nil {
		return nil, err
	}
	if err := validateIndexDefShape(def.Name, def.Columns); err != nil {
		return nil, err
	}
	bind := def.From
	if bind == nil {
		b, err := defaultBinder[K](def.Columns, def.Layout)
		if err != nil {
			return nil, fmt.Errorf(
				"s3parquet: index %q: %w", def.Name, err)
		}
		bind = b
	}
	return &IndexReader[K]{
		target:    target,
		name:      def.Name,
		columns:   def.Columns,
		indexPath: core.IndexPath(target.Prefix(), def.Name),
		bind:      bind,
	}, nil
}

// validateIndexDefShape validates Name + Columns. Shared between
// NewIndexReader and the write-side validation in NewWriter.
func validateIndexDefShape(name string, columns []string) error {
	if name == "" {
		return fmt.Errorf("s3parquet: index: Name is required")
	}
	if strings.Contains(name, "/") {
		return fmt.Errorf(
			"s3parquet: index: Name %q must not contain '/'", name)
	}
	if err := core.ValidatePartitionKeyParts(columns); err != nil {
		return fmt.Errorf("s3parquet: index %q: %w", name, err)
	}
	return nil
}

// defaultBinder builds a reflection-based bind closure: every
// parquet-tagged field on K must match one of the columns, no
// extras allowed, no duplicate tags. String fields project
// directly; time.Time fields are parsed via
// time.Parse(layout.Time, ...). The returned closure populates K
// from a values slice (positional, aligned to columns).
func defaultBinder[K any](columns []string, layout Layout) (
	func(values []string) (K, error), error,
) {
	binders, err := buildReadBinders[K](columns, layout)
	if err != nil {
		return nil, err
	}
	return func(values []string) (K, error) {
		var k K
		if len(values) != len(columns) {
			return k, fmt.Errorf(
				"index bind: got %d values, want %d (one per Column)",
				len(values), len(columns))
		}
		v := reflect.ValueOf(&k).Elem()
		for j, b := range binders {
			if b.isTime {
				ts, err := time.Parse(b.timeLayout, values[j])
				if err != nil {
					return k, fmt.Errorf(
						"index bind: column %q: parse time %q with "+
							"layout %q: %w",
						columns[j], values[j], b.timeLayout, err)
				}
				v.Field(b.fieldIdx).Set(reflect.ValueOf(ts))
			} else {
				v.Field(b.fieldIdx).SetString(values[j])
			}
		}
		return k, nil
	}, nil
}

// readBinder is the per-column bind plan for K, parallel to
// writeProjector on the write side. fieldIdx is the (top-level)
// struct-field index on K; isTime + timeLayout pick the
// time.Parse branch.
type readBinder struct {
	fieldIdx   int
	isTime     bool
	timeLayout string
}

// resolveOf returns def.Of when set, otherwise the auto-projection
// closure built from T's parquet tags + def.Columns + def.Layout.
// Validation (every column has a matching parquet-tagged field
// of a supported type) runs once at resolve time so per-record
// work is just a slice alloc + N field-projection calls.
//
// Used by NewWriter (to wire the write path) and BackfillIndex
// (to walk historical data files), so both code paths share one
// resolution rule.
func resolveOf[T any](def IndexDef[T]) (func(T) ([]string, error), error) {
	if def.Of != nil {
		return def.Of, nil
	}
	projs, err := buildWriteProjectors(reflect.TypeFor[T](), def.Columns, def.Layout)
	if err != nil {
		return nil, fmt.Errorf("s3parquet: index %q: %w", def.Name, err)
	}
	return func(rec T) ([]string, error) {
		v := reflect.ValueOf(rec)
		out := make([]string, len(projs))
		for j, p := range projs {
			f := v.Field(p.fieldIdx)
			if p.isTime {
				out[j] = f.Interface().(time.Time).Format(p.timeLayout)
			} else {
				out[j] = f.String()
			}
		}
		return out, nil
	}, nil
}

// writeProjector is the per-column projection plan baked once at
// NewWriter and replayed on every record. fieldIdx is the
// (top-level) struct-field index on T; isTime + timeLayout pick
// the time.Time formatting branch.
type writeProjector struct {
	fieldIdx   int
	isTime     bool
	timeLayout string
}

// buildWriteProjectors walks t's parquet tags, picks the field
// matching each column, and decides per-column projection
// strategy (string field → SetString; time.Time field →
// Format(layout)). Looser than buildReadBinders: t may carry
// parquet tags not in columns (record types are typically
// wider) — those are ignored.
func buildWriteProjectors(
	t reflect.Type, columns []string, layout Layout,
) ([]writeProjector, error) {
	fields, err := core.ParquetFields(t)
	if err != nil {
		return nil, err
	}
	type fieldInfo struct {
		idx  int
		ftyp reflect.Type
	}
	tagged := make(map[string]fieldInfo, len(fields))
	for _, pf := range fields {
		tagged[pf.Name] = fieldInfo{
			idx:  pf.Field.Index[0],
			ftyp: pf.Field.Type,
		}
	}

	timeType := reflect.TypeFor[time.Time]()
	out := make([]writeProjector, len(columns))
	for j, col := range columns {
		info, ok := tagged[col]
		if !ok {
			return nil, fmt.Errorf(
				"index column %q has no matching parquet-tagged "+
					"field on %s (provide a custom Of, or add the tag)",
				col, t)
		}
		switch {
		case info.ftyp.Kind() == reflect.String:
			out[j] = writeProjector{fieldIdx: info.idx}
		case info.ftyp == timeType:
			if layout.Time == "" {
				return nil, fmt.Errorf(
					"index column %q matches a time.Time field on "+
						"%s but Layout.Time is empty (set Layout.Time, "+
						"or provide a custom Of)", col, t)
			}
			out[j] = writeProjector{
				fieldIdx:   info.idx,
				isTime:     true,
				timeLayout: layout.Time,
			}
		default:
			return nil, fmt.Errorf(
				"index column %q matches a parquet-tagged field on "+
					"%s but it is %s, not string or time.Time "+
					"(provide a custom Of to format the value)",
				col, t, info.ftyp.Kind())
		}
	}
	return out, nil
}

// maxMarkerKeyLen caps the length of any marker S3 object key.
// S3's hard limit is 1024 bytes; we leave 24 bytes of buffer for
// any future additions (e.g. a shortID variant). Rejecting at
// build time surfaces the problem as a config-or-data error
// rather than an opaque S3 InvalidKey mid-batch.
const maxMarkerKeyLen = 1000

// markerPathFromValues builds the marker S3 key from columns + a
// values slice (one entry per column, in declared order).
// Validates length match and that values are safe for hive-
// partition path segments. Enforces the 1000-byte cap.
func markerPathFromValues(
	name, indexPath string, columns []string, values []string,
) (string, error) {
	if len(values) != len(columns) {
		return "", fmt.Errorf(
			"s3parquet: index %q: Of returned %d values, want %d "+
				"(one per Column)", name, len(values), len(columns))
	}
	for j, col := range columns {
		if err := core.ValidateHivePartitionValue(values[j]); err != nil {
			return "", fmt.Errorf(
				"s3parquet: index %q column %q: %w", name, col, err)
		}
	}
	p := core.BuildIndexMarkerPath(indexPath, columns, values)
	if len(p) > maxMarkerKeyLen {
		return "", fmt.Errorf(
			"s3parquet: index %q marker key is %d bytes, exceeds "+
				"%d (S3 limit is 1024; narrow Columns or shorten "+
				"values)", name, len(p), maxMarkerKeyLen)
	}
	return p, nil
}

// Lookup returns every K whose marker matches any of the given
// key patterns. Patterns use the grammar from
// core.ValidateKeyPattern, evaluated against Columns. Pass
// multiple patterns when the target set isn't a Cartesian product
// (e.g. (sku=A, customer=X) and (sku=B, customer=Y) but not the
// off-diagonal pairs); overlapping patterns are deduplicated, so
// the result has no duplicate K entries.
//
// Read-after-write: the marker LIST inherits the target's
// ConsistencyControl, so every marker the writer has already
// published is visible. Unlike Poll there is no SettleWindow
// filter.
//
// Results are unbounded — narrow the patterns if an index has
// millions of matching markers. Empty patterns slice returns
// (nil, nil); a malformed pattern fails with the offending index.
func (i *IndexReader[K]) Lookup(
	ctx context.Context, patterns []string,
) ([]K, error) {
	patterns = core.DedupePatterns(patterns)
	if len(patterns) == 0 {
		return nil, nil
	}

	plans, err := core.BuildReadPlans(patterns, i.indexPath, i.columns)
	if err != nil {
		return nil, fmt.Errorf(
			"s3parquet: index %q Lookup %w", i.name, err)
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
		k, err := i.bind(values)
		if err != nil {
			return nil, fmt.Errorf(
				"s3parquet: index %q: %w", i.name, err)
		}
		out = append(out, k)
	}
	return out, nil
}

// listAllMatchingMarkers fans LIST calls across plans, filters
// each page to marker keys whose hive body satisfies plan.Match,
// and returns the unioned, deduplicated set of marker S3 keys.
// Overlapping plans can list the same marker (e.g. "sku=*" and
// "sku=s1" both cover the s1 markers); UnionKeys collapses them.
//
// Inherits ConsistencyControl from i.target on every LIST. With
// a strong level (or AWS S3, which is strong-LIST by default),
// Lookup is read-after-write — no settle window needed.
func (i *IndexReader[K]) listAllMatchingMarkers(
	ctx context.Context, plans []*core.ReadPlan,
) ([]string, error) {
	suffix := "/" + core.IndexMarkerFilename
	return core.FanOutMapReduce(ctx, plans,
		i.target.EffectiveMaxInflightRequests(),
		func(ctx context.Context, plan *core.ReadPlan) ([]string, error) {
			var keys []string
			err := i.target.listEach(ctx, plan.ListPrefix, "", 0,
				func(obj s3types.Object) (bool, error) {
					key := aws.ToString(obj.Key)
					if !strings.HasSuffix(key, suffix) {
						return true, nil
					}
					hiveKey, ok := hiveKeyOfMarker(key, i.indexPath)
					if !ok {
						return true, nil
					}
					if plan.Match(hiveKey) {
						keys = append(keys, key)
					}
					return true, nil
				})
			if err != nil {
				return nil, fmt.Errorf(
					"s3parquet: index %q list: %w", i.name, err)
			}
			return keys, nil
		},
		func(per [][]string) []string { return core.UnionKeys(per, identityKey) })
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

// BackfillIndex scans existing parquet data and writes index
// markers for every record already present. The normal path is
// to wire indexes via WriterConfig.Indexes / Config.Indexes
// before the first Write; BackfillIndex is the relief valve for
// records written before the index existed.
//
// keyPatterns use the grammar from core.ValidateKeyPattern,
// evaluated against target.PartitionKeyParts() (NOT the index's
// Columns) — backfill walks parquet data files, which are keyed
// by partition. "*" covers everything; shard across partitions
// to parallelize a migration. Overlapping patterns are
// deduplicated, so each parquet file is scanned at most once.
//
// until is an exclusive upper bound on data-file LastModified.
// Typical use: until = deployTime_of_live_writer, so backfill
// covers historical gaps while the live writer covers everything
// from deploy onward. Pass time.Time{} (the zero value) to cover
// every file currently present (redundant with the live writer
// but harmless — PUT is idempotent).
//
// onMissingData is invoked when a data-file GET returns S3
// NoSuchKey (dangling ref or LIST-to-GET race); the file is
// skipped either way. Pass nil to disable the hook.
//
// Safe to run concurrently with a live writer (S3 PUT is
// idempotent) and safe to retry after a crash. Empty patterns
// slice is a no-op: (BackfillStats{}, nil). First malformed
// pattern fails with its index.
func BackfillIndex[T any](
	ctx context.Context,
	target S3Target,
	def IndexDef[T],
	keyPatterns []string,
	until time.Time,
	onMissingData func(dataPath string),
) (BackfillStats, error) {
	var stats BackfillStats

	keyPatterns = core.DedupePatterns(keyPatterns)
	if len(keyPatterns) == 0 {
		return stats, nil
	}

	// Full Target check — BackfillIndex LISTs partitioned data
	// files (plan.Match consults PartitionKeyParts), so
	// validateLookup's reduced subset isn't enough.
	if err := target.Validate(); err != nil {
		return stats, err
	}
	if err := validateIndexDefShape(def.Name, def.Columns); err != nil {
		return stats, err
	}
	of, err := resolveOf(def)
	if err != nil {
		return stats, err
	}

	indexPath := core.IndexPath(target.Prefix(), def.Name)

	dataPath := core.DataPath(target.Prefix())
	plans, err := core.BuildReadPlans(keyPatterns, dataPath, target.PartitionKeyParts())
	if err != nil {
		return stats, fmt.Errorf(
			"s3parquet: BackfillIndex %w", err)
	}

	keys, err := listDataFilesBelowUntil(
		ctx, target, plans, dataPath, until)
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
				ctx, target, def.Name, def.Columns, of, indexPath, key)
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
				); err != nil {
					return fmt.Errorf(
						"s3parquet: backfill index %q: put marker: %w",
						def.Name, err)
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
// and returns those with S3 LastModified strictly before until.
// A zero until (time.Time{}) disables the filter and lists every
// matching file.
func listDataFilesBelowUntil(
	ctx context.Context,
	target S3Target,
	plans []*core.ReadPlan,
	dataPath string,
	until time.Time,
) ([]string, error) {
	filter := !until.IsZero()
	return core.FanOutMapReduce(ctx, plans,
		target.EffectiveMaxInflightRequests(),
		func(ctx context.Context, plan *core.ReadPlan) ([]string, error) {
			return listDataFilesForPlan(
				ctx, target, plan, dataPath, filter, until)
		},
		func(per [][]string) []string { return core.UnionKeys(per, identityKey) })
}

// listDataFilesForPlan is the per-plan body extracted so the
// single-plan and multi-plan paths share one code path for the
// LIST + filter logic. ConsistencyControl on the LIST is
// inherited from target.
func listDataFilesForPlan(
	ctx context.Context,
	target S3Target,
	plan *core.ReadPlan,
	dataPath string,
	filter bool,
	cutoff time.Time,
) ([]string, error) {
	var keys []string
	err := target.listEach(ctx, plan.ListPrefix, "", 0,
		func(obj s3types.Object) (bool, error) {
			objKey := aws.ToString(obj.Key)
			if !strings.HasSuffix(objKey, ".parquet") {
				return true, nil
			}
			hiveKey, ok := core.HiveKeyOfDataFile(objKey, dataPath)
			if !ok {
				return true, nil
			}
			if !plan.Match(hiveKey) {
				return true, nil
			}
			if filter && obj.LastModified != nil &&
				!obj.LastModified.Before(cutoff) {
				return true, nil
			}
			keys = append(keys, objKey)
			return true, nil
		})
	if err != nil {
		return nil, fmt.Errorf(
			"s3parquet: backfill list data files: %w", err)
	}
	return keys, nil
}

// backfillMarkersForObject decodes one parquet data object and
// returns the deduplicated marker paths its records produce under
// the resolved Of, plus the record count (for stats). Pulled out
// of BackfillIndex's main loop so the dedup map doesn't leak
// across objects — each file stands on its own, keeping memory
// bounded by the largest file rather than the full backfill set.
func backfillMarkersForObject[T any](
	ctx context.Context,
	target S3Target,
	name string,
	columns []string,
	of func(T) ([]string, error),
	indexPath string,
	key string,
) ([]string, int, error) {
	data, err := target.get(ctx, key)
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
		values, err := of(rec)
		if err != nil {
			return nil, 0, fmt.Errorf(
				"s3parquet: backfill index %q on %s: %w",
				name, key, err)
		}
		if values == nil {
			continue
		}
		p, err := markerPathFromValues(name, indexPath, columns, values)
		if err != nil {
			return nil, 0, fmt.Errorf(
				"s3parquet: backfill index %q on %s: %w",
				name, key, err)
		}
		seen[p] = struct{}{}
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

// buildReadBinders reflects on K to produce per-column bind
// plans the read path's default From closure replays on every
// Lookup result. Every Columns entry must match a parquet-tagged
// field on K (string or time.Time); no extra tagged fields are
// allowed (tight coupling between K and Columns); duplicate tags
// fail (parquet-go "last-wins" would silently drop one).
//
// time.Time fields require layout.Time to be set — empty + a
// time field on K errors, mirroring the write side's
// buildWriteProjectors rule.
func buildReadBinders[K any](
	columns []string, layout Layout,
) ([]readBinder, error) {
	fields, err := core.ParquetFields(reflect.TypeFor[K]())
	if err != nil {
		return nil, err
	}
	type fieldInfo struct {
		idx  int
		ftyp reflect.Type
	}
	tagged := make(map[string]fieldInfo, len(fields))
	for _, pf := range fields {
		tagged[pf.Name] = fieldInfo{
			idx:  pf.Field.Index[0],
			ftyp: pf.Field.Type,
		}
	}

	timeType := reflect.TypeFor[time.Time]()
	out := make([]readBinder, len(columns))
	for j, col := range columns {
		info, ok := tagged[col]
		if !ok {
			return nil, fmt.Errorf(
				"index column %q has no matching parquet-tagged "+
					"field on the entry type", col)
		}
		switch {
		case info.ftyp.Kind() == reflect.String:
			out[j] = readBinder{fieldIdx: info.idx}
		case info.ftyp == timeType:
			if layout.Time == "" {
				return nil, fmt.Errorf(
					"index column %q matches a time.Time field on "+
						"the entry type but Layout.Time is empty "+
						"(set Layout.Time, or provide a custom From)",
					col)
			}
			out[j] = readBinder{
				fieldIdx:   info.idx,
				isTime:     true,
				timeLayout: layout.Time,
			}
		default:
			return nil, fmt.Errorf(
				"index entry field tagged %q is %s, only string "+
					"and time.Time fields are supported by the "+
					"default binder (provide a custom From)",
				col, info.ftyp.Kind())
		}
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

	return out, nil
}
