package s3parquet

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// testIndexRec is the record shape the index tests use. Two
// partitionable fields + one "lookup-ish" one (sku).
type testIndexRec struct {
	Period   string `parquet:"period"`
	Customer string `parquet:"customer"`
	SKU      string `parquet:"sku"`
}

// SkuIndexEntry is a typical index-entry struct: all fields
// string, one parquet tag each, no extras.
type SkuIndexEntry struct {
	SKU      string `parquet:"sku"`
	Period   string `parquet:"period"`
	Customer string `parquet:"customer"`
}

func newIndexTestStore(t *testing.T) *Store[testIndexRec] {
	t.Helper()
	s, err := New(Config[testIndexRec]{
		Bucket:            "b",
		Prefix:            "p",
		S3Client:          &s3.Client{},
		PartitionKeyParts: []string{"period", "customer"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

// TestBuildIndexBinder_Valid guards the happy path: columns
// match parquet tags on a string-only struct and the returned
// field indices are correctly aligned with the columns slice.
func TestBuildIndexBinder_Valid(t *testing.T) {
	cols := []string{"sku", "period", "customer"}
	fi, err := buildIndexBinder[SkuIndexEntry](cols)
	if err != nil {
		t.Fatalf("buildIndexBinder: %v", err)
	}
	if len(fi) != 3 {
		t.Fatalf("got %d indices, want 3", len(fi))
	}
	// Sanity: field indices point to fields with matching tags.
	entry := SkuIndexEntry{SKU: "s", Period: "p", Customer: "c"}
	values, err := (&Index[testIndexRec, SkuIndexEntry]{
		columns:      cols,
		fieldIndices: fi,
		name:         "test",
	}).entryToValues(entry)
	if err != nil {
		t.Fatalf("entryToValues: %v", err)
	}
	want := []string{"s", "p", "c"}
	for i := range want {
		if values[i] != want[i] {
			t.Errorf("values[%d] = %q, want %q", i, values[i], want[i])
		}
	}
}

// TestBuildIndexBinder_Rejects covers every validation branch: a
// typo in Columns, a non-string field, an extra tagged field on
// the entry struct, and a non-struct type.
func TestBuildIndexBinder_Rejects(t *testing.T) {
	t.Run("missing tag on entry", func(t *testing.T) {
		if _, err := buildIndexBinder[SkuIndexEntry](
			[]string{"sku", "period", "not_on_struct"},
		); err == nil {
			t.Error("expected error, got nil")
		}
	})

	t.Run("non-string field", func(t *testing.T) {
		type BadEntry struct {
			SKU    string `parquet:"sku"`
			Amount int    `parquet:"amount"`
		}
		if _, err := buildIndexBinder[BadEntry](
			[]string{"sku", "amount"},
		); err == nil {
			t.Error("expected error, got nil")
		}
	})

	t.Run("extra tag on entry not in columns", func(t *testing.T) {
		if _, err := buildIndexBinder[SkuIndexEntry](
			[]string{"sku", "period"}, // customer missing
		); err == nil {
			t.Error("expected error, got nil")
		}
	})

	t.Run("non-struct", func(t *testing.T) {
		if _, err := buildIndexBinder[string](
			[]string{"a"},
		); err == nil {
			t.Error("expected error, got nil")
		}
	})

	t.Run("duplicate parquet tag", func(t *testing.T) {
		type DupEntry struct {
			First  string `parquet:"shared"`
			Second string `parquet:"shared"`
		}
		if _, err := buildIndexBinder[DupEntry](
			[]string{"shared"},
		); err == nil {
			t.Error("expected error for duplicate tag, got nil")
		}
	})
}

// TestPathsOf_MultipleEntriesPerRecord guards that a single
// record can produce multiple markers when Of returns more than
// one K, and that dedup still collapses duplicates across the
// batch.
func TestPathsOf_MultipleEntriesPerRecord(t *testing.T) {
	s := newIndexTestStore(t)
	_, err := NewIndex(s, IndexDef[testIndexRec, SkuIndexEntry]{
		Name:    "multi_idx",
		Columns: []string{"sku", "period", "customer"},
		// Each record emits two entries (e.g., a record that
		// touches two SKUs). Record-to-record duplicates collapse
		// at the batch level.
		Of: func(r testIndexRec) []SkuIndexEntry {
			return []SkuIndexEntry{
				{SKU: "s1", Period: r.Period, Customer: r.Customer},
				{SKU: "s2", Period: r.Period, Customer: r.Customer},
			}
		},
	})
	if err != nil {
		t.Fatalf("NewIndex: %v", err)
	}

	// Two records, same (period, customer). Each emits 2 entries,
	// total 4 — but the two records share the same (period,
	// customer), so dedup collapses the batch to 2 distinct paths
	// (one per SKU).
	batch := []testIndexRec{
		{Period: "P", Customer: "C", SKU: "ignored"},
		{Period: "P", Customer: "C", SKU: "ignored"},
	}
	got, err := s.collectIndexMarkerPaths(batch)
	if err != nil {
		t.Fatalf("collectIndexMarkerPaths: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("got %d marker paths, want 2: %v", len(got), got)
	}
}

// TestPathsOf_RejectsOversizedKey guards the 1000-byte cap we
// enforce at path build time so a pathologically long entry
// surfaces a clear error instead of an opaque S3 InvalidKey.
func TestPathsOf_RejectsOversizedKey(t *testing.T) {
	s := newIndexTestStore(t)
	_, err := NewIndex(s, IndexDef[testIndexRec, SkuIndexEntry]{
		Name:    "big_idx",
		Columns: []string{"sku", "period", "customer"},
		Of: func(r testIndexRec) []SkuIndexEntry {
			return []SkuIndexEntry{{
				SKU:      strings.Repeat("X", 500),
				Period:   strings.Repeat("Y", 500),
				Customer: "c",
			}}
		},
	})
	if err != nil {
		t.Fatalf("NewIndex: %v", err)
	}

	_, err = s.collectIndexMarkerPaths(
		[]testIndexRec{{Period: "P", Customer: "C", SKU: "S"}})
	if err == nil {
		t.Error("expected error for oversized marker key, got nil")
	}
	if err != nil && !strings.Contains(err.Error(), "exceeds") {
		t.Errorf("error %q should mention the length limit", err)
	}
}

// TestEntryToValues_ValidatesValues guards that values carrying
// '/' or '..' are rejected before they can corrupt a marker path.
func TestEntryToValues_ValidatesValues(t *testing.T) {
	cols := []string{"sku", "period", "customer"}
	fi, _ := buildIndexBinder[SkuIndexEntry](cols)
	idx := &Index[testIndexRec, SkuIndexEntry]{
		columns:      cols,
		fieldIndices: fi,
		name:         "test",
	}

	if _, err := idx.entryToValues(
		SkuIndexEntry{SKU: "s", Period: "2026-03-01",
			Customer: "a/b"},
	); err == nil {
		t.Error("expected error for '/' in customer, got nil")
	}
	if _, err := idx.entryToValues(
		SkuIndexEntry{SKU: "s..bad", Period: "p",
			Customer: "c"},
	); err == nil {
		t.Error("expected error for '..' in sku, got nil")
	}
	if _, err := idx.entryToValues(
		SkuIndexEntry{SKU: "s", Period: "", Customer: "c"},
	); err == nil {
		t.Error("expected error for empty period, got nil")
	}
}

// TestNewIndex_Validation covers NewIndex's registration-time
// checks: nil store, empty name, name with '/', bad Columns,
// missing Of.
func TestNewIndex_Validation(t *testing.T) {
	s := newIndexTestStore(t)
	ofStub := func(testIndexRec) []SkuIndexEntry { return nil }

	cases := []struct {
		name string
		def  IndexDef[testIndexRec, SkuIndexEntry]
	}{
		{"empty name", IndexDef[testIndexRec, SkuIndexEntry]{
			Columns: []string{"sku", "period", "customer"},
			Of:      ofStub,
		}},
		{"name with slash", IndexDef[testIndexRec, SkuIndexEntry]{
			Name:    "bad/name",
			Columns: []string{"sku", "period", "customer"},
			Of:      ofStub,
		}},
		{"empty columns", IndexDef[testIndexRec, SkuIndexEntry]{
			Name: "idx",
			Of:   ofStub,
		}},
		{"duplicate column", IndexDef[testIndexRec, SkuIndexEntry]{
			Name:    "idx",
			Columns: []string{"sku", "sku", "customer"},
			Of:      ofStub,
		}},
		{"missing Of", IndexDef[testIndexRec, SkuIndexEntry]{
			Name:    "idx",
			Columns: []string{"sku", "period", "customer"},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewIndex(s, tc.def)
			if err == nil {
				t.Error("expected error, got nil")
			}
		})
	}

	t.Run("nil store", func(t *testing.T) {
		_, err := NewIndex[testIndexRec, SkuIndexEntry](nil,
			IndexDef[testIndexRec, SkuIndexEntry]{
				Name:    "idx",
				Columns: []string{"sku", "period", "customer"},
				Of:      ofStub,
			})
		if err == nil {
			t.Error("expected error for nil store, got nil")
		}
	})
}

// TestNewIndex_RegistersWriter proves NewIndex hooks a writer
// into the store — collectIndexMarkerPaths produces the expected
// marker paths for a batch once the index is registered.
func TestNewIndex_RegistersWriter(t *testing.T) {
	s := newIndexTestStore(t)
	_, err := NewIndex(s, IndexDef[testIndexRec, SkuIndexEntry]{
		Name:    "sku_idx",
		Columns: []string{"sku", "period", "customer"},
		Of: func(r testIndexRec) []SkuIndexEntry {
			return []SkuIndexEntry{{
				SKU: r.SKU, Period: r.Period, Customer: r.Customer,
			}}
		},
	})
	if err != nil {
		t.Fatalf("NewIndex: %v", err)
	}

	batch := []testIndexRec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1"},
		{Period: "2026-03-17", Customer: "abc", SKU: "s1"}, // dup
		{Period: "2026-03-17", Customer: "abc", SKU: "s2"},
	}
	got, err := s.collectIndexMarkerPaths(batch)
	if err != nil {
		t.Fatalf("collectIndexMarkerPaths: %v", err)
	}
	// Dedup collapses the duplicate s1 to one marker.
	if len(got) != 2 {
		t.Fatalf("got %d marker paths, want 2: %v", len(got), got)
	}
	for _, p := range got {
		if !strings.HasPrefix(p, "p/_index/sku_idx/") {
			t.Errorf("marker path %q missing expected prefix", p)
		}
		if !strings.HasSuffix(p, "/m.idx") {
			t.Errorf("marker path %q missing /m.idx suffix", p)
		}
	}
}
