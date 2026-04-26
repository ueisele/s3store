package s3store

import (
	"reflect"
	"strings"
	"testing"
)

func TestParquetFields_Valid(t *testing.T) {
	type Rec struct {
		SKU      string `parquet:"sku"`
		Ignored  string // no tag → skipped
		Customer string `parquet:"customer,timestamp(microsecond)"` // option stripped
		Empty    string `parquet:""`                                // empty name → skipped
		Dash     string `parquet:"-"`                               // "-" → skipped
	}
	type RecWithUnexported struct {
		Public  string `parquet:"public"`
		private string `parquet:"private"` //nolint:unused // intentional: tests skip-unexported rule
	}
	if got, err := ParquetFields(reflect.TypeFor[RecWithUnexported]()); err != nil {
		t.Fatalf("ParquetFields(unexported): %v", err)
	} else if len(got) != 1 || got[0].Name != "public" {
		t.Errorf("unexported skipped: got %+v, want one field 'public'", got)
	}

	got, err := ParquetFields(reflect.TypeFor[Rec]())
	if err != nil {
		t.Fatalf("ParquetFields: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d fields, want 2: %+v", len(got), got)
	}
	if got[0].Name != "sku" {
		t.Errorf("got[0].Name = %q, want %q", got[0].Name, "sku")
	}
	if got[1].Name != "customer" {
		t.Errorf("got[1].Name = %q, want %q", got[1].Name, "customer")
	}
}

func TestParquetFields_Order(t *testing.T) {
	type Rec struct {
		C string `parquet:"c"`
		A string `parquet:"a"`
		B string `parquet:"b"`
	}
	got, err := ParquetFields(reflect.TypeFor[Rec]())
	if err != nil {
		t.Fatalf("ParquetFields: %v", err)
	}
	want := []string{"c", "a", "b"}
	if len(got) != len(want) {
		t.Fatalf("got %d fields, want %d", len(got), len(want))
	}
	for i, w := range want {
		if got[i].Name != w {
			t.Errorf("got[%d] = %q, want %q (declaration order)",
				i, got[i].Name, w)
		}
	}
}

func TestParquetFields_RejectsDuplicate(t *testing.T) {
	type Rec struct {
		First  string `parquet:"shared"`
		Second string `parquet:"shared"`
	}
	_, err := ParquetFields(reflect.TypeFor[Rec]())
	if err == nil {
		t.Fatal("expected error for duplicate tag, got nil")
	}
	if !strings.Contains(err.Error(), "duplicate") {
		t.Errorf("error %q should mention 'duplicate'", err)
	}
}

func TestParquetFields_RejectsNonStruct(t *testing.T) {
	if _, err := ParquetFields(reflect.TypeFor[string]()); err == nil {
		t.Error("expected error for non-struct, got nil")
	}
	if _, err := ParquetFields(nil); err == nil {
		t.Error("expected error for nil type, got nil")
	}
}

func TestParquetFields_PreservesField(t *testing.T) {
	type Rec struct {
		SKU string `parquet:"sku,extra"`
	}
	got, err := ParquetFields(reflect.TypeFor[Rec]())
	if err != nil {
		t.Fatalf("ParquetFields: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d, want 1", len(got))
	}
	if got[0].Field.Tag.Get("parquet") != "sku,extra" {
		t.Errorf("Field.Tag did not round-trip the original tag")
	}
	if len(got[0].Field.Index) != 1 || got[0].Field.Index[0] != 0 {
		t.Errorf("Field.Index = %v, want [0]", got[0].Field.Index)
	}
}
