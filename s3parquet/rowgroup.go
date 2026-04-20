package s3parquet

import (
	"fmt"
	"time"

	"github.com/parquet-go/parquet-go"
)

// RowGroup is the pruning-surface wrapper passed to a
// RowGroupPredicate. Wraps parquet-go's RowGroup so the public
// API isn't pinned to parquet-go versions. Holds a reference to
// the parent file's Schema so Column can resolve column names
// without re-walking the schema per call.
type RowGroup struct {
	inner  parquet.RowGroup
	schema *parquet.Schema
}

// NumRows returns the number of rows in this row group.
func (rg RowGroup) NumRows() int64 {
	return rg.inner.NumRows()
}

// Column returns the chunk-level stats for the named column, or
// (ColumnStats{}, false) if the column is absent from the file
// or its column index is missing (the writer didn't emit page
// stats).
//
// name is the top-level parquet field name as tagged on the
// record struct — e.g. "customer" for `parquet:"customer"`.
func (rg RowGroup) Column(name string) (ColumnStats, bool) {
	leaf, ok := rg.schema.Lookup(name)
	if !ok {
		return ColumnStats{}, false
	}
	chunks := rg.inner.ColumnChunks()
	if leaf.ColumnIndex >= len(chunks) {
		return ColumnStats{}, false
	}
	return ColumnStats{chunk: chunks[leaf.ColumnIndex]}, true
}

// ColumnStats exposes the chunk-level min/max bounds across the
// pages of one column in one row group. Typed accessors return
// the collapsed (row-group-wide) min/max derived from the per-
// page stats in the parquet column index.
type ColumnStats struct {
	chunk parquet.ColumnChunk
}

// kind returns the chunk's physical parquet kind, or false when
// the chunk is unset. Used by typed accessors to short-circuit
// on physical-type mismatches instead of silently coercing via
// parquet.Value's zero-value fallbacks (Value.Int64() on a
// ByteArray returns 0, which would make Int64Bounds produce
// (0, 0, true) — a silent wrong answer).
func (c ColumnStats) kind() (parquet.Kind, bool) {
	if c.chunk == nil {
		return 0, false
	}
	return c.chunk.Type().Kind(), true
}

// StringBounds returns the row-group-wide min and max string
// values for the column, or ok=false when the column isn't of a
// byte-array physical type, when the column index is missing, or
// when every page is null.
func (c ColumnStats) StringBounds() (min, max string, ok bool) {
	k, ok := c.kind()
	if !ok {
		return "", "", false
	}
	if k != parquet.ByteArray && k != parquet.FixedLenByteArray {
		return "", "", false
	}
	lo, hi, present := c.reduce(func(v parquet.Value) (any, bool) {
		if v.IsNull() {
			return nil, false
		}
		return string(v.ByteArray()), true
	})
	if !present {
		return "", "", false
	}
	return lo.(string), hi.(string), true
}

// Int64Bounds mirrors StringBounds for INT64-physical columns
// (including timestamp logical types, which are INT64 at the
// physical layer). Returns ok=false on non-INT64 columns rather
// than coercing — parquet.Value's Int64() accessor returns the
// zero value for mismatched physical types, which would quietly
// produce (0, 0, true) without the guard.
func (c ColumnStats) Int64Bounds() (min, max int64, ok bool) {
	k, ok := c.kind()
	if !ok {
		return 0, 0, false
	}
	if k != parquet.Int64 {
		return 0, 0, false
	}
	lo, hi, present := c.reduce(func(v parquet.Value) (any, bool) {
		if v.IsNull() {
			return nil, false
		}
		return v.Int64(), true
	})
	if !present {
		return 0, 0, false
	}
	return lo.(int64), hi.(int64), true
}

// TimestampBounds reads the column as INT64 microseconds since
// Unix epoch and returns the row-group-wide min/max as time.Time
// in UTC. Intended for columns tagged `parquet:"ts,timestamp(microsecond)"`
// and similar — the caller owns the logical-type choice, this
// helper assumes microsecond resolution.
func (c ColumnStats) TimestampBounds() (min, max time.Time, ok bool) {
	lo, hi, present := c.Int64Bounds()
	if !present {
		return time.Time{}, time.Time{}, false
	}
	return time.UnixMicro(lo).UTC(), time.UnixMicro(hi).UTC(), true
}

// reduce collapses per-page min/max from the column index into
// row-group-wide bounds. extract converts a parquet.Value into
// whichever concrete type the caller cares about; returning
// (_, false) skips the page (e.g. null page, or wrong type).
// Comparison uses each typed path's natural ordering; this is
// done inside StringBounds / Int64Bounds rather than here to
// keep the helper type-agnostic.
func (c ColumnStats) reduce(
	extract func(parquet.Value) (any, bool),
) (min, max any, ok bool) {
	if c.chunk == nil {
		return nil, nil, false
	}
	idx, err := c.chunk.ColumnIndex()
	if err != nil || idx == nil {
		return nil, nil, false
	}
	n := idx.NumPages()
	if n == 0 {
		return nil, nil, false
	}
	for p := 0; p < n; p++ {
		if idx.NullPage(p) {
			continue
		}
		pmin, okMin := extract(idx.MinValue(p))
		pmax, okMax := extract(idx.MaxValue(p))
		if !okMin || !okMax {
			return nil, nil, false
		}
		if !ok {
			min, max, ok = pmin, pmax, true
			continue
		}
		if less(pmin, min) {
			min = pmin
		}
		if less(max, pmax) {
			max = pmax
		}
	}
	return min, max, ok
}

// less compares two interface{} values that came out of the same
// extractor (so they're guaranteed to share a concrete type).
// Panics on an unhandled type rather than silently returning
// false — a silent false here would let a new typed bounds
// accessor compute incorrect row-group-wide min/max without any
// obvious symptom. Panic surfaces the missing case at the first
// call; add the missing type here whenever a new *Bounds helper
// lands on ColumnStats.
func less(a, b any) bool {
	switch av := a.(type) {
	case string:
		return av < b.(string)
	case int64:
		return av < b.(int64)
	}
	panic(fmt.Sprintf(
		"s3parquet: rowgroup less: unsupported type %T — "+
			"extend the switch when a new ColumnStats accessor "+
			"is added", a))
}

// RowGroupPredicate decides whether a given row group should be
// fetched and decoded. Returning false skips the row group —
// its column chunks are never read from S3.
//
// When stats are missing (no column index in the writer, or the
// predicate's target column isn't in the file) a predicate
// should default to returning true. Over-fetching degrades to a
// full scan; under-fetching silently drops records.
type RowGroupPredicate func(RowGroup) bool

// EqualsString returns a predicate that accepts a row group
// whose chunk-level min/max bounds for the named string column
// bracket value. When stats are unavailable for the column,
// accepts the group (safe fallback — full scan of that file).
func EqualsString(column, value string) RowGroupPredicate {
	return func(rg RowGroup) bool {
		c, ok := rg.Column(column)
		if !ok {
			return true
		}
		lo, hi, ok := c.StringBounds()
		if !ok {
			return true
		}
		return lo <= value && value <= hi
	}
}

// InStrings is a multi-value EqualsString: the row group is
// accepted when any of the supplied values could be present
// given the chunk's min/max bounds.
//
// An empty values slice returns a predicate that rejects every
// row group — "in the empty set" matches nothing. Callers
// building the slice dynamically should short-circuit with an
// empty-result path rather than calling Read at all, to avoid
// paying for a LIST that'll drop every file.
func InStrings(column string, values []string) RowGroupPredicate {
	if len(values) == 0 {
		return func(RowGroup) bool { return false }
	}
	return func(rg RowGroup) bool {
		c, ok := rg.Column(column)
		if !ok {
			return true
		}
		lo, hi, ok := c.StringBounds()
		if !ok {
			return true
		}
		for _, v := range values {
			if lo <= v && v <= hi {
				return true
			}
		}
		return false
	}
}

// TimestampRange accepts row groups whose chunk-level min/max
// for the column overlap [from, to) (half-open). Safe fallback
// to accept when stats are unavailable. Assumes the column was
// written with microsecond resolution — matches this library's
// standard timestamp tagging.
func TimestampRange(column string, from, to time.Time) RowGroupPredicate {
	fromMicro := from.UnixMicro()
	toMicro := to.UnixMicro()
	return func(rg RowGroup) bool {
		c, ok := rg.Column(column)
		if !ok {
			return true
		}
		lo, hi, ok := c.Int64Bounds()
		if !ok {
			return true
		}
		// half-open overlap: [lo, hi] vs [fromMicro, toMicro)
		return lo < toMicro && hi >= fromMicro
	}
}

// And returns a predicate that accepts a row group only when
// every ps predicate accepts it. Zero-arg returns a predicate
// that accepts all groups.
func And(ps ...RowGroupPredicate) RowGroupPredicate {
	return func(rg RowGroup) bool {
		for _, p := range ps {
			if !p(rg) {
				return false
			}
		}
		return true
	}
}

// Or returns a predicate that accepts a row group when any ps
// predicate accepts it. Zero-arg returns a predicate that
// rejects all groups.
func Or(ps ...RowGroupPredicate) RowGroupPredicate {
	return func(rg RowGroup) bool {
		for _, p := range ps {
			if p(rg) {
				return true
			}
		}
		return false
	}
}
