package s3store

import (
	"fmt"
	"reflect"
	"strings"
)

// ParquetField pairs a parquet tag name with the struct field
// it labels. Returned by ParquetFields in declaration order so
// callers can build their own per-field projection / scan / bind
// plans on top of one shared tag-walking rule.
type ParquetField struct {
	// Name is the tag value with options stripped (everything
	// before the first ',').
	Name string

	// Field is the underlying struct field. Index/Type/Tag are
	// the fields callers most commonly read.
	Field reflect.StructField
}

// ParquetFields returns one entry per exported parquet-tagged
// field on t, in field-declaration order. Used by the index
// projector/binder builders so the parquet tag convention is
// interpreted in exactly one place.
//
// Tag handling matches the project's convention everywhere:
//
//   - Tag name is everything before the first ',' — options
//     (e.g. `parquet:"ts,timestamp(microsecond)"`) are stripped.
//   - Empty tag names and "-" are treated as "skip".
//   - Untagged exported fields are skipped silently.
//   - Unexported fields are skipped silently.
//
// Errors:
//
//   - t is not a struct.
//   - Two fields share the same parquet tag name. parquet-go's
//     encoder would silently last-wins one of them; surfacing it
//     as a config error here protects every downstream caller.
func ParquetFields(t reflect.Type) ([]ParquetField, error) {
	if t == nil {
		return nil, fmt.Errorf("parquet fields: nil type")
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf(
			"parquet fields: type %s must be a struct, got %s",
			t, t.Kind())
	}
	var out []ParquetField
	seen := map[string]string{}
	for i := range t.NumField() {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		tag, ok := f.Tag.Lookup("parquet")
		if !ok {
			continue
		}
		name, _, _ := strings.Cut(tag, ",")
		if name == "" || name == "-" {
			continue
		}
		if prev, dup := seen[name]; dup {
			return nil, fmt.Errorf(
				"duplicate parquet tag %q on fields %q and %q",
				name, prev, f.Name)
		}
		seen[name] = f.Name
		out = append(out, ParquetField{Name: name, Field: f})
	}
	return out, nil
}
