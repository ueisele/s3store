package s3store

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"
)

// ProjectionLookupDef is the read-side definition of a secondary
// projection. Build a ProjectionReader[K] from it via
// NewProjectionReader when a service queries an existing projection
// (dashboard, query API).
//
// Name + Columns identify the projection in S3; From projects the
// per-marker column values back into a typed K. From is optional
// — when nil, the library reflects K's parquet-tagged string
// fields against Columns and assembles K from the values slice.
// Provide a custom From for K's that have no parquet tags or
// when extra validation/transformation is needed.
type ProjectionLookupDef[K comparable] struct {
	// Name identifies the projection under the target's
	// <Prefix>/_projection/ subtree. Required. Must be non-empty
	// and free of '/'.
	Name string

	// Columns lists the projection's column names in the order
	// they appear in the S3 path. Earlier columns form a narrower
	// LIST prefix when Lookup specifies them literally. Pick the
	// order based on how queries filter: most-selective first.
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
	// Errors are propagated by Lookup with the projection name as
	// wrapping context.
	From func(values []string) (K, error)

	// Layout configures how non-string parquet-tagged fields on K
	// are parsed from path segments when From is nil. Honored
	// only by the auto-binding path; an explicit From bypasses
	// Layout entirely.
	//
	// For correctness, Layout.Time on the read side MUST match
	// ProjectionDef.Layout.Time on the write side — drift produces
	// silently wrong values or parse errors. Define one constant
	// and reuse it on both sides.
	Layout Layout
}

// ProjectionDef is the write-side definition of a secondary
// projection. Pass a slice of these on WriterConfig.Projections /
// Config.Projections at construction; the writer iterates each
// registered projection per record, builds the marker S3 key from
// Columns + Of's return, and PUTs one empty marker per distinct
// key.
//
// The previous late-binding RegisterProjection API is gone:
// projections are part of the writer's construction, so
// registration cannot race with Write and "registered after first
// Write" is not a reachable state.
type ProjectionDef[T any] struct {
	// Name identifies the projection under the target's
	// <Prefix>/_projection/ subtree. Required. Must be non-empty,
	// free of '/', and unique across ProjectionDef[T] entries on
	// the same writer.
	Name string

	// Columns lists the projection's column names in the order
	// they appear in the S3 path. Same ordering rules as
	// ProjectionLookupDef.Columns (most-selective first for LIST
	// pruning).
	Columns []string

	// Of extracts the per-record column values, in Columns order,
	// as a slice. Returning (nil, nil) skips the record (no marker
	// emitted). A non-nil values slice must have length equal to
	// len(Columns); each entry must satisfy
	// validateHivePartitionValue.
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
// format up front (use BackfillProjection if you ever need to
// migrate).
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

// validateProjectionDefShape validates Name + Columns. Shared
// between NewProjectionReader and the write-side validation in
// NewWriter.
func validateProjectionDefShape(name string, columns []string) error {
	if name == "" {
		return fmt.Errorf("s3store: projection: Name is required")
	}
	if strings.Contains(name, "/") {
		return fmt.Errorf(
			"s3store: projection: Name %q must not contain '/'", name)
	}
	if err := validatePartitionKeyParts(columns); err != nil {
		return fmt.Errorf("s3store: projection %q: %w", name, err)
	}
	return nil
}

// fieldPlan is the per-column reflection plan reused by the
// write-side projector closure (resolveOf) and the read-side
// binder closure (defaultBinder). fieldIdx is the (top-level)
// struct-field index on the target type; isTime + timeLayout
// pick the time.Time formatting / parsing branch.
type fieldPlan struct {
	fieldIdx   int
	isTime     bool
	timeLayout string
}

// buildFieldPlans walks t's parquet tags and returns one
// fieldPlan per column. Used by both the write projector and
// the read binder so the "string field projects directly,
// time.Time field requires Layout.Time" rule lives in one
// place. errCtx wraps every error message ("Of" or "From") so
// the surfaced hint points at the right user knob.
//
// requireExact rejects t carrying parquet tags not listed in
// columns. The write side passes false (record types are
// typically wider than the projection — extra tags ignored). The
// read side passes true (the entry type must match Columns 1:1
// — an extra tagged field would silently round-trip nothing).
func buildFieldPlans(
	t reflect.Type, columns []string, layout Layout,
	requireExact bool, errCtx string,
) ([]fieldPlan, error) {
	fields, err := ParquetFields(t)
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
	out := make([]fieldPlan, len(columns))
	for j, col := range columns {
		info, ok := tagged[col]
		if !ok {
			return nil, fmt.Errorf(
				"projection column %q has no matching parquet-tagged "+
					"field on %s (provide a custom %s, or add the tag)",
				col, t, errCtx)
		}
		switch {
		case info.ftyp.Kind() == reflect.String:
			out[j] = fieldPlan{fieldIdx: info.idx}
		case info.ftyp == timeType:
			if layout.Time == "" {
				return nil, fmt.Errorf(
					"projection column %q matches a time.Time field on "+
						"%s but Layout.Time is empty (set Layout.Time, "+
						"or provide a custom %s)", col, t, errCtx)
			}
			out[j] = fieldPlan{
				fieldIdx:   info.idx,
				isTime:     true,
				timeLayout: layout.Time,
			}
		default:
			return nil, fmt.Errorf(
				"projection column %q matches a parquet-tagged field on "+
					"%s but it is %s, not string or time.Time "+
					"(provide a custom %s)",
				col, t, info.ftyp.Kind(), errCtx)
		}
	}

	if requireExact {
		for name := range tagged {
			if !slices.Contains(columns, name) {
				return nil, fmt.Errorf(
					"%s has parquet-tagged field %q not in Columns "+
						"(remove the tag or provide a custom %s)",
					t, name, errCtx)
			}
		}
	}

	return out, nil
}

// resolveOf returns def.Of when set, otherwise the auto-projection
// closure built from T's parquet tags + def.Columns + def.Layout.
// Validation (every column has a matching parquet-tagged field
// of a supported type) runs once at resolve time so per-record
// work is just a slice alloc + N field-projection calls.
//
// Used by NewWriter (to wire the write path) and BackfillProjection
// (to walk historical data files), so both code paths share one
// resolution rule. T may carry parquet tags not in columns —
// those are ignored, since record types are typically wider
// than the projection.
func resolveOf[T any](def ProjectionDef[T]) (func(T) ([]string, error), error) {
	if def.Of != nil {
		return def.Of, nil
	}
	plans, err := buildFieldPlans(
		reflect.TypeFor[T](), def.Columns, def.Layout, false, "Of")
	if err != nil {
		return nil, fmt.Errorf("s3store: projection %q: %w", def.Name, err)
	}
	return func(rec T) ([]string, error) {
		v := reflect.ValueOf(rec)
		out := make([]string, len(plans))
		for j, p := range plans {
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

// projectionBasePath returns the prefix under which markers for
// the named secondary projection are stored, relative to the
// store's top-level Prefix. Each projection lives in its own
// subtree so multiple projections on one store don't collide.
func projectionBasePath(prefix, name string) string {
	return prefix + "/_projection/" + name
}

// projectionMarkerFilename is the fixed terminal filename appended
// to every marker S3 key. The last real path segment is always
// "col=value"; this constant sits after it so parse code can
// strip it uniformly and LIST paginators recognise markers.
const projectionMarkerFilename = "m.proj"

// buildProjectionMarkerPath assembles an S3 object key for a
// projection marker. columns and values are paired by position.
// Values must pass validateHivePartitionValue before calling;
// this helper does not revalidate.
func buildProjectionMarkerPath(
	projectionPath string, columns, values []string,
) string {
	segs := make([]string, len(columns))
	for i := range columns {
		segs[i] = columns[i] + "=" + values[i]
	}
	return projectionPath + "/" + strings.Join(segs, "/") +
		"/" + projectionMarkerFilename
}

// parseProjectionMarkerKey is the inverse of
// buildProjectionMarkerPath. It extracts the column values from a
// marker key in the order they appear in columns. Fails if the
// key doesn't match the shape (wrong prefix, wrong suffix, wrong
// segment count, wrong column name in a segment).
func parseProjectionMarkerKey(
	markerKey, projectionPath string, columns []string,
) ([]string, error) {
	prefix := projectionPath + "/"
	if !strings.HasPrefix(markerKey, prefix) {
		return nil, fmt.Errorf(
			"s3store: marker key %q outside projection path %q",
			markerKey, projectionPath)
	}
	body := markerKey[len(prefix):]
	tail := "/" + projectionMarkerFilename
	if !strings.HasSuffix(body, tail) {
		return nil, fmt.Errorf(
			"s3store: marker key %q missing %q suffix",
			markerKey, projectionMarkerFilename)
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
	name, projectionPath string, columns []string, values []string,
) (string, error) {
	if len(values) != len(columns) {
		return "", fmt.Errorf(
			"s3store: projection %q: Of returned %d values, want %d "+
				"(one per Column)", name, len(values), len(columns))
	}
	for j, col := range columns {
		if err := validateHivePartitionValue(values[j]); err != nil {
			return "", fmt.Errorf(
				"s3store: projection %q column %q: %w", name, col, err)
		}
	}
	p := buildProjectionMarkerPath(projectionPath, columns, values)
	if len(p) > maxMarkerKeyLen {
		return "", fmt.Errorf(
			"s3store: projection %q marker key is %d bytes, exceeds "+
				"%d (S3 limit is 1024; narrow Columns or shorten "+
				"values)", name, len(p), maxMarkerKeyLen)
	}
	return p, nil
}
