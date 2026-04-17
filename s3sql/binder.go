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

// fieldBinder holds the per-field plan for scanning one result
// column into one field of T. makeDest allocates a fresh NULL-
// aware Scan destination each row; assign copies from that
// destination into the struct field if the value wasn't NULL.
type fieldBinder struct {
	columnName string
	fieldIndex []int
	makeDest   func() any
	assign     func(dstField reflect.Value, src any)
}

// binder is the per-Store plan built once at New() from the
// target type's parquet tags. byName maps column name → binder;
// columns preserves tag-declaration order for diagnostics.
type binder struct {
	byName  map[string]*fieldBinder
	columns []string
}

// buildBinder walks T's exported fields once, picks a NULL-safe
// Scan destination per field based on the field's Go type, and
// returns a plan the row scanner can drive with zero extra
// reflection per field per row.
func buildBinder(t reflect.Type) (*binder, error) {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf(
			"target type must be a struct, got %s", t.Kind())
	}

	b := &binder{byName: make(map[string]*fieldBinder)}
	for i := 0; i < t.NumField(); i++ {
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
		b.byName[name] = &fieldBinder{
			columnName: name,
			fieldIndex: sf.Index,
			makeDest:   makeDest,
			assign:     assign,
		}
		b.columns = append(b.columns, name)
	}

	if len(b.columns) == 0 {
		return nil, fmt.Errorf(
			"type %s has no parquet-tagged exported fields",
			t.Name())
	}
	return b, nil
}

// scannerType is the reflect.Type for sql.Scanner; comparing
// against a precomputed value avoids recomputing on every
// makeFieldScanner call.
var scannerType = reflect.TypeOf((*sql.Scanner)(nil)).Elem()

// timeType is reflect.TypeOf(time.Time{}) — pre-computed because
// time.Time is a struct and would otherwise fall through the
// Kind switch below.
var timeType = reflect.TypeOf(time.Time{})

// makeFieldScanner returns (destination factory, assignment
// function) for a single Go field type. NULL always maps to
// the field's Go zero value; the assign function is a no-op in
// that case.
func makeFieldScanner(
	t reflect.Type,
) (func() any, func(reflect.Value, any), error) {
	// Honor sql.Scanner first so user-defined types (e.g.
	// decimal.Decimal, uuid.UUID) take precedence over any
	// kind-based fallback. Time handled before this check
	// because time.Time itself doesn't implement sql.Scanner.
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
				// reflect.New(t) returns reflect.Value of *T;
				// Interface() gives us the concrete pointer,
				// which implements sql.Scanner.
				ptr := reflect.New(t)
				return &nullScanner{
					inner: ptr.Interface().(sql.Scanner),
					value: ptr.Elem(),
				}
			},
			func(dst reflect.Value, src any) {
				ns := src.(*nullScanner)
				if ns.valid {
					dst.Set(ns.value)
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
	// DuckDB LIST/ARRAY as []any, STRUCT as map[string]any, and
	// MAP as duckdb.OrderedMap (handled via a decode hook). We
	// route those through mapstructure.Decode into the user's
	// target Go type. Covers nested fields and slices of
	// primitives or structs.
	//
	// []byte is intentionally handled earlier — the byte-slice
	// code path above is more efficient than the composite path.
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
// destination. On NULL we leave the inner value at its Go zero,
// so the parent assign step skips the copy and the struct field
// stays zero.
type nullScanner struct {
	inner sql.Scanner
	value reflect.Value // reflect.Value of the T sitting behind inner
	valid bool
}

// Scan implements sql.Scanner. NULL (src == nil) sets valid=false
// and returns nil; any non-NULL value is delegated to the inner
// Scanner.
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
// into an arbitrary Go target type via mapstructure. target is
// a *T allocated at makeDest time; on a non-NULL Scan the
// decoder populates *T, which the assign step then copies into
// the struct field.
type compositeScanner struct {
	target reflect.Value
	valid  bool
}

// Scan implements sql.Scanner. NULL leaves target at its Go
// zero and valid=false so assign can skip the copy. Non-NULL
// decodes via mapstructure with a hook that unwraps OrderedMap.
func (c *compositeScanner) Scan(src any) error {
	if src == nil {
		c.valid = false
		return nil
	}
	c.valid = true

	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		Result:     c.target.Interface(),
		DecodeHook: orderedMapDecodeHook,
		// TagName defaults to "mapstructure"; use "parquet" so
		// the same struct tag that drives the top-level binder
		// also names fields of nested structs.
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
// can decode it into the user's target map type. Without the
// hook, mapstructure can't see OrderedMap's unexported keys
// and values.
func orderedMapDecodeHook(
	from reflect.Type, _ reflect.Type, data any,
) (any, error) {
	if from != reflect.TypeOf(duckdb.OrderedMap{}) {
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
