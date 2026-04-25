package s3sql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/go-viper/mapstructure/v2"
)

// ScanAll materializes every row from rows into a []T, binding
// columns to fields on T by parquet tag name. Columns absent
// from T are discarded; T-fields whose tag is absent from the
// result set stay at the Go zero value. NULL always maps to the
// field's zero value.
//
// T must be a struct (or pointer to struct) whose exported fields
// carry `parquet:"<name>"` tags. Tag options after the first
// comma (e.g. `parquet:"ts,timestamp(microsecond)"`) are
// stripped — only the column name is consulted. Untagged,
// dash-tagged ("-"), and unexported fields are ignored.
//
// Supported field types: every primitive (int*, uint*, float*,
// bool, string), time.Time, []byte, any type implementing
// sql.Scanner (e.g. shopspring/decimal.Decimal, uuid.UUID), plus
// composite shapes — struct, map, and slice — decoded from
// DuckDB's STRUCT / MAP / LIST representations via mapstructure.
// Nested structs use the same `parquet` tag for their fields.
//
// Closes rows automatically before returning. Use when arbitrary
// SQL via Query / QueryMany should be materialized into typed
// records; use the returned *sql.Rows directly when you want to
// stream / aggregate without buffering.
//
//	rows, err := r.Query(ctx, "*", "SELECT * FROM records WHERE ...")
//	if err != nil { return nil, err }
//	out, err := s3sql.ScanAll[MyRec](rows)
func ScanAll[T any](rows *sql.Rows) ([]T, error) {
	defer func() { _ = rows.Close() }()

	b, err := buildScanBinder(reflect.TypeFor[T]())
	if err != nil {
		return nil, fmt.Errorf("s3sql: ScanAll: %w", err)
	}

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("s3sql: ScanAll: get columns: %w", err)
	}
	fbs := make([]*fieldScanBinder, len(cols))
	for i, c := range cols {
		fbs[i] = b.byName[c]
	}

	var records []T
	for rows.Next() {
		dests := make([]any, len(cols))
		for i, fb := range fbs {
			if fb == nil {
				dests[i] = new(any)
				continue
			}
			dests[i] = fb.makeDest()
		}
		if err := rows.Scan(dests...); err != nil {
			return nil, fmt.Errorf("s3sql: ScanAll: scan row: %w", err)
		}
		var rec T
		rv := reflect.ValueOf(&rec).Elem()
		for i, fb := range fbs {
			if fb == nil {
				continue
			}
			fb.assign(rv.FieldByIndex(fb.fieldIndex), dests[i])
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("s3sql: ScanAll: iterate rows: %w", err)
	}
	return records, nil
}

// fieldScanBinder holds the per-field plan for scanning one
// result column into one field of T. makeDest allocates a fresh
// NULL-aware Scan destination each row; assign copies from that
// destination into the struct field if the value wasn't NULL.
type fieldScanBinder struct {
	fieldIndex []int
	makeDest   func() any
	assign     func(dstField reflect.Value, src any)
}

// scanBinder is the per-T plan built from the target type's
// parquet tags. byName maps column name → binder.
type scanBinder struct {
	byName map[string]*fieldScanBinder
}

// buildScanBinder walks T's exported fields once, picks a NULL-
// safe Scan destination per field based on the field's Go type,
// and returns a plan ScanAll can drive with zero extra reflection
// per field per row. T may carry zero parquet-tagged fields —
// that's a valid (no-op) binder, useful when ScanAll is called
// with a T that intentionally discards every column.
func buildScanBinder(t reflect.Type) (*scanBinder, error) {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf(
			"target type must be a struct, got %s", t.Kind())
	}

	b := &scanBinder{byName: make(map[string]*fieldScanBinder)}
	for i := range t.NumField() {
		sf := t.Field(i)
		if !sf.IsExported() {
			continue
		}
		tag, ok := sf.Tag.Lookup("parquet")
		if !ok {
			continue
		}
		name := strings.SplitN(tag, ",", 2)[0]
		if name == "" || name == "-" {
			continue
		}

		makeDest, assign, err := makeFieldScanner(sf.Type)
		if err != nil {
			return nil, fmt.Errorf(
				"field %s (%s): %w", sf.Name, sf.Type, err)
		}
		b.byName[name] = &fieldScanBinder{
			fieldIndex: sf.Index,
			makeDest:   makeDest,
			assign:     assign,
		}
	}
	return b, nil
}

// Pre-computed reflect.Types used by makeFieldScanner and the
// composite decode hook.
var (
	scannerType    = reflect.TypeFor[sql.Scanner]()
	timeType       = reflect.TypeFor[time.Time]()
	orderedMapType = reflect.TypeFor[duckdb.OrderedMap]()
)

// makeFieldScanner returns (destination factory, assignment
// function) for a single Go field type. NULL always maps to the
// field's Go zero value; the assign function is a no-op in that
// case.
func makeFieldScanner(
	t reflect.Type,
) (func() any, func(reflect.Value, any), error) {
	// Honour sql.Scanner first so user-defined types take
	// precedence over any kind-based fallback. time.Time handled
	// before that check because it doesn't implement sql.Scanner.
	if t == timeType {
		return func() any { return &sql.NullTime{} },
			func(dst reflect.Value, src any) {
				n := src.(*sql.NullTime)
				if n.Valid {
					dst.Set(reflect.ValueOf(n.Time))
				}
			}, nil
	}
	if reflect.PointerTo(t).Implements(scannerType) {
		return func() any {
				return &nullScanner{
					inner: reflect.New(t).Interface().(sql.Scanner),
				}
			},
			func(dst reflect.Value, src any) {
				ns := src.(*nullScanner)
				if ns.valid {
					dst.Set(reflect.ValueOf(ns.inner).Elem())
				}
			}, nil
	}

	// []byte is natively nullable in database/sql (nil slice
	// means SQL NULL); no wrapper needed.
	if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
		return func() any {
				var b []byte
				return &b
			},
			func(dst reflect.Value, src any) {
				b := *(src.(*[]byte))
				if b != nil {
					dst.SetBytes(b)
				}
			}, nil
	}

	// Slice/Map/Struct fallthrough: the duckdb-go driver returns
	// LIST/ARRAY as []any, STRUCT as map[string]any, and MAP as
	// duckdb.OrderedMap (handled via a decode hook). Routed
	// through mapstructure.Decode into the user's target Go type.
	// []byte is handled earlier — that path is more efficient.
	if t.Kind() == reflect.Slice ||
		t.Kind() == reflect.Map ||
		t.Kind() == reflect.Struct {
		return func() any {
				return &compositeScanner{
					target: reflect.New(t),
				}
			},
			func(dst reflect.Value, src any) {
				cs := src.(*compositeScanner)
				if cs.valid {
					dst.Set(cs.target.Elem())
				}
			}, nil
	}

	switch t.Kind() {
	case reflect.String:
		return func() any { return &sql.NullString{} },
			func(dst reflect.Value, src any) {
				n := src.(*sql.NullString)
				if n.Valid {
					dst.SetString(n.String)
				}
			}, nil
	case reflect.Int, reflect.Int8, reflect.Int16,
		reflect.Int32, reflect.Int64:
		return func() any { return &sql.NullInt64{} },
			func(dst reflect.Value, src any) {
				n := src.(*sql.NullInt64)
				if n.Valid {
					dst.SetInt(n.Int64)
				}
			}, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16,
		reflect.Uint32, reflect.Uint64:
		return func() any { return &sql.NullInt64{} },
			func(dst reflect.Value, src any) {
				n := src.(*sql.NullInt64)
				if n.Valid {
					dst.SetUint(uint64(n.Int64))
				}
			}, nil
	case reflect.Float32, reflect.Float64:
		return func() any { return &sql.NullFloat64{} },
			func(dst reflect.Value, src any) {
				n := src.(*sql.NullFloat64)
				if n.Valid {
					dst.SetFloat(n.Float64)
				}
			}, nil
	case reflect.Bool:
		return func() any { return &sql.NullBool{} },
			func(dst reflect.Value, src any) {
				n := src.(*sql.NullBool)
				if n.Valid {
					dst.SetBool(n.Bool)
				}
			}, nil
	}
	return nil, nil, fmt.Errorf(
		"unsupported type %s; implement sql.Scanner to use "+
			"a custom type", t)
}

// nullScanner adapts a user sql.Scanner into a NULL-aware Scan
// destination. On NULL we leave the inner value at its Go zero
// so the parent assign step skips the copy and the struct field
// stays zero. The scanned value lives behind inner (a *T); the
// assign step recovers it via reflect.ValueOf(inner).Elem().
type nullScanner struct {
	inner sql.Scanner
	valid bool
}

// Scan implements sql.Scanner.
func (n *nullScanner) Scan(src any) error {
	if src == nil {
		n.valid = false
		return nil
	}
	n.valid = true
	return n.inner.Scan(src)
}

// compositeScanner bridges the driver's raw composite values
// (LIST → []any, STRUCT → map[string]any, MAP → OrderedMap)
// into an arbitrary Go target type via mapstructure. target is a
// *T allocated at makeDest time; on a non-NULL Scan the decoder
// populates *T, which assign then copies into the struct field.
type compositeScanner struct {
	target reflect.Value
	valid  bool
}

// Scan implements sql.Scanner. NULL leaves target at its Go zero
// and valid=false so assign can skip the copy. Non-NULL decodes
// via mapstructure with a hook that unwraps OrderedMap.
func (c *compositeScanner) Scan(src any) error {
	if src == nil {
		c.valid = false
		return nil
	}
	c.valid = true

	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		Result:     c.target.Interface(),
		DecodeHook: orderedMapDecodeHook,
		// Use "parquet" so the same struct tag that drives the
		// top-level binder also names fields of nested structs.
		TagName: "parquet",
	})
	if err != nil {
		return fmt.Errorf("build decoder: %w", err)
	}
	if err := decoder.Decode(src); err != nil {
		return fmt.Errorf("decode composite: %w", err)
	}
	return nil
}

// orderedMapDecodeHook converts duckdb.OrderedMap (returned by
// the driver for MAP columns) into map[any]any so mapstructure
// can decode it into the user's target map type.
func orderedMapDecodeHook(
	from reflect.Type, _ reflect.Type, data any,
) (any, error) {
	if from != orderedMapType {
		return data, nil
	}
	om := data.(duckdb.OrderedMap)
	result := make(map[any]any, om.Len())
	for _, k := range om.Keys() {
		v, _ := om.Get(k)
		result[k] = v
	}
	return result, nil
}
