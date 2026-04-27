package s3store

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// testProjectionRec is the record shape the projection tests use. Two
// partitionable fields + one "lookup-ish" one (sku).
type testProjectionRec struct {
	Period   string `parquet:"period"`
	Customer string `parquet:"customer"`
	SKU      string `parquet:"sku"`
}

// SkuProjectionEntry is a typical projection-entry struct: all fields
// string, one parquet tag each, no extras.
type SkuProjectionEntry struct {
	SKU      string `parquet:"sku"`
	Period   string `parquet:"period"`
	Customer string `parquet:"customer"`
}

func newProjectionTestStore(t *testing.T, projections ...ProjectionDef[testProjectionRec]) *Store[testProjectionRec] {
	t.Helper()
	s, err := New(Config[testProjectionRec]{
		Bucket:            "b",
		Prefix:            "p",
		S3Client:          &s3.Client{},
		PartitionKeyParts: []string{"period", "customer"},
		Projections:       projections,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

// TestBuildProjectionBinder_Valid guards the happy path: columns
// match parquet tags on a string-only struct and the resulting
// reflection-based binder round-trips a values slice back into
// the typed entry.
func TestBuildProjectionBinder_Valid(t *testing.T) {
	cols := []string{"sku", "period", "customer"}
	bind, err := defaultBinder[SkuProjectionEntry](cols, Layout{})
	if err != nil {
		t.Fatalf("defaultBinder: %v", err)
	}
	got, err := bind([]string{"s", "p", "c"})
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	want := SkuProjectionEntry{SKU: "s", Period: "p", Customer: "c"}
	if got != want {
		t.Errorf("bind: got %+v, want %+v", got, want)
	}
}

// TestBuildProjectionBinder_Rejects covers every validation branch: a
// typo in Columns, a non-string field, an extra tagged field on
// the entry struct, and a non-struct type.
func TestBuildProjectionBinder_Rejects(t *testing.T) {
	t.Run("missing tag on entry", func(t *testing.T) {
		if _, err := defaultBinder[SkuProjectionEntry](
			[]string{"sku", "period", "not_on_struct"},
			Layout{},
		); err == nil {
			t.Error("expected error, got nil")
		}
	})

	t.Run("non-string field", func(t *testing.T) {
		type BadEntry struct {
			SKU    string `parquet:"sku"`
			Amount int    `parquet:"amount"`
		}
		if _, err := defaultBinder[BadEntry](
			[]string{"sku", "amount"},
			Layout{},
		); err == nil {
			t.Error("expected error, got nil")
		}
	})

	t.Run("extra tag on entry not in columns", func(t *testing.T) {
		if _, err := defaultBinder[SkuProjectionEntry](
			[]string{"sku", "period"}, // customer missing
			Layout{},
		); err == nil {
			t.Error("expected error, got nil")
		}
	})

	t.Run("non-struct", func(t *testing.T) {
		if _, err := defaultBinder[string](
			[]string{"a"},
			Layout{},
		); err == nil {
			t.Error("expected error, got nil")
		}
	})

	t.Run("duplicate parquet tag", func(t *testing.T) {
		type DupEntry struct {
			First  string `parquet:"shared"`
			Second string `parquet:"shared"`
		}
		if _, err := defaultBinder[DupEntry](
			[]string{"shared"},
			Layout{},
		); err == nil {
			t.Error("expected error for duplicate tag, got nil")
		}
	})
}

// TestCollectProjectionMarkerPaths_BatchDedup proves the writer
// collects one marker per distinct (projection, column-values) tuple
// across a batch — duplicate records collapse to a single PUT.
func TestCollectProjectionMarkerPaths_BatchDedup(t *testing.T) {
	s := newProjectionTestStore(t, ProjectionDef[testProjectionRec]{
		Name:    "sku_idx",
		Columns: []string{"sku", "period", "customer"},
		Of: func(r testProjectionRec) ([]string, error) {
			return []string{r.SKU, r.Period, r.Customer}, nil
		},
	})

	batch := []testProjectionRec{
		{Period: "2026-03-17", Customer: "abc", SKU: "s1"},
		{Period: "2026-03-17", Customer: "abc", SKU: "s1"}, // dup
		{Period: "2026-03-17", Customer: "abc", SKU: "s2"},
	}
	got, err := s.collectProjectionMarkerPaths(batch)
	if err != nil {
		t.Fatalf("collectProjectionMarkerPaths: %v", err)
	}
	// Dedup collapses the duplicate s1 to one marker.
	if len(got) != 2 {
		t.Fatalf("got %d marker paths, want 2: %v", len(got), got)
	}
	for _, p := range got {
		if !strings.HasPrefix(p, "p/_projection/sku_idx/") {
			t.Errorf("marker path %q missing expected prefix", p)
		}
		if !strings.HasSuffix(p, "/m.proj") {
			t.Errorf("marker path %q missing /m.proj suffix", p)
		}
	}
}

// TestOf_NilSliceSkipsRecord guards that returning (nil, nil)
// from Of skips the record entirely — no marker is emitted.
func TestOf_NilSliceSkipsRecord(t *testing.T) {
	s := newProjectionTestStore(t, ProjectionDef[testProjectionRec]{
		Name:    "skip_idx",
		Columns: []string{"sku", "period", "customer"},
		Of: func(r testProjectionRec) ([]string, error) {
			if r.SKU == "skip" {
				return nil, nil
			}
			return []string{r.SKU, r.Period, r.Customer}, nil
		},
	})

	got, err := s.collectProjectionMarkerPaths([]testProjectionRec{
		{Period: "P", Customer: "C", SKU: "s1"},
		{Period: "P", Customer: "C", SKU: "skip"},
	})
	if err != nil {
		t.Fatalf("collectProjectionMarkerPaths: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("got %d paths, want 1 (the non-skipped record)",
			len(got))
	}
}

// TestOf_PropagatesError guards that an error returned by Of
// fails the whole batch.
func TestOf_PropagatesError(t *testing.T) {
	s := newProjectionTestStore(t, ProjectionDef[testProjectionRec]{
		Name:    "err_idx",
		Columns: []string{"sku", "period", "customer"},
		Of: func(r testProjectionRec) ([]string, error) {
			return nil, errors.New("of failed")
		},
	})

	_, err := s.collectProjectionMarkerPaths([]testProjectionRec{
		{Period: "P", Customer: "C", SKU: "s1"},
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "of failed") {
		t.Errorf("error %q should wrap Of's error", err)
	}
}

// TestOf_RejectsLengthMismatch guards that an Of returning a
// slice whose length doesn't match Columns fails the write at
// marker-path time.
func TestOf_RejectsLengthMismatch(t *testing.T) {
	s := newProjectionTestStore(t, ProjectionDef[testProjectionRec]{
		Name:    "missing_col_idx",
		Columns: []string{"sku", "period", "customer"},
		Of: func(r testProjectionRec) ([]string, error) {
			// Only 2 values for 3 columns.
			return []string{r.SKU, r.Period}, nil
		},
	})

	_, err := s.collectProjectionMarkerPaths([]testProjectionRec{
		{Period: "P", Customer: "C", SKU: "s1"},
	})
	if err == nil {
		t.Fatal("expected error for length mismatch, got nil")
	}
	if !strings.Contains(err.Error(), "values") {
		t.Errorf("error %q should mention the values count", err)
	}
}

// TestOf_AutoProjectsT guards the Of==nil path: when Of is left
// unset, the library reflects T's parquet tags + Columns and
// emits the matching marker without any caller code.
func TestOf_AutoProjectsT(t *testing.T) {
	s := newProjectionTestStore(t, ProjectionDef[testProjectionRec]{
		Name:    "auto_idx",
		Columns: []string{"sku", "customer"},
		// Of intentionally nil.
	})

	got, err := s.collectProjectionMarkerPaths([]testProjectionRec{
		{Period: "P", Customer: "abc", SKU: "s1"},
	})
	if err != nil {
		t.Fatalf("collectProjectionMarkerPaths: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d paths, want 1", len(got))
	}
	want := "p/_projection/auto_idx/sku=s1/customer=abc/m.proj"
	if got[0] != want {
		t.Errorf("marker path: got %q, want %q", got[0], want)
	}
}

// TestOf_AutoRejectsMissingTag guards that Of==nil fails at
// NewWriter when a Columns entry doesn't match any parquet tag
// on T.
func TestOf_AutoRejectsMissingTag(t *testing.T) {
	_, err := New(Config[testProjectionRec]{
		Bucket:            "b",
		Prefix:            "p",
		S3Client:          &s3.Client{},
		PartitionKeyParts: []string{"period", "customer"},
		Projections: []ProjectionDef[testProjectionRec]{{
			Name:    "bad_idx",
			Columns: []string{"sku", "not_on_t"},
			// Of nil → auto-project, but "not_on_t" has no match.
		}},
	})
	if err == nil {
		t.Fatal("expected error for unknown column, got nil")
	}
	if !strings.Contains(err.Error(), "not_on_t") {
		t.Errorf("error %q should name the missing column", err)
	}
}

// TestOf_AutoRejectsNonStringTag guards that Of==nil fails when
// a Columns entry matches a parquet-tagged field on T but the
// field is not a string.
func TestOf_AutoRejectsNonStringTag(t *testing.T) {
	type RecWithInt struct {
		SKU    string `parquet:"sku"`
		Amount int    `parquet:"amount"`
	}
	_, err := New(Config[RecWithInt]{
		Bucket:            "b",
		Prefix:            "p",
		S3Client:          &s3.Client{},
		PartitionKeyParts: []string{"sku"},
		Projections: []ProjectionDef[RecWithInt]{{
			Name:    "amount_idx",
			Columns: []string{"sku", "amount"},
		}},
	})
	if err == nil {
		t.Fatal("expected error for non-string field, got nil")
	}
	if !strings.Contains(err.Error(), "amount") {
		t.Errorf("error %q should name the non-string column", err)
	}
}

// TestOf_AutoProjectsTimeWithLayout guards that Layout.Time
// formats time.Time fields on T into the marker path when Of is
// nil. RFC3339 layout is a typical choice; the rule applies to
// any layout string time.Time.Format accepts.
func TestOf_AutoProjectsTimeWithLayout(t *testing.T) {
	type Rec struct {
		SKU string    `parquet:"sku"`
		At  time.Time `parquet:"at"`
	}
	s, err := New(Config[Rec]{
		Bucket:            "b",
		Prefix:            "p",
		S3Client:          &s3.Client{},
		PartitionKeyParts: []string{"sku"},
		Projections: []ProjectionDef[Rec]{{
			Name:    "at_idx",
			Columns: []string{"sku", "at"},
			Layout:  Layout{Time: time.RFC3339},
		}},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got, err := s.collectProjectionMarkerPaths([]Rec{
		{SKU: "s1", At: time.Date(2026, 3, 17, 12, 0, 0, 0, time.UTC)},
	})
	if err != nil {
		t.Fatalf("collectProjectionMarkerPaths: %v", err)
	}
	want := "p/_projection/at_idx/sku=s1/at=2026-03-17T12:00:00Z/m.proj"
	if len(got) != 1 || got[0] != want {
		t.Errorf("marker path: got %v, want %q", got, want)
	}
}

// TestOf_AutoTimeRequiresLayout guards that auto-projection
// fails at NewWriter when a column matches a time.Time field
// but Layout.Time is empty — no silent default.
func TestOf_AutoTimeRequiresLayout(t *testing.T) {
	type Rec struct {
		SKU string    `parquet:"sku"`
		At  time.Time `parquet:"at"`
	}
	_, err := New(Config[Rec]{
		Bucket:            "b",
		Prefix:            "p",
		S3Client:          &s3.Client{},
		PartitionKeyParts: []string{"sku"},
		Projections: []ProjectionDef[Rec]{{
			Name:    "at_idx",
			Columns: []string{"sku", "at"},
			// Layout intentionally empty.
		}},
	})
	if err == nil {
		t.Fatal("expected error for empty Layout.Time, got nil")
	}
	if !strings.Contains(err.Error(), "Layout.Time") {
		t.Errorf("error %q should mention Layout.Time", err)
	}
}

// TestPathsOf_RejectsOversizedKey guards the 1000-byte cap we
// enforce at path build time so a pathologically long entry
// surfaces a clear error instead of an opaque S3 InvalidKey.
func TestPathsOf_RejectsOversizedKey(t *testing.T) {
	s := newProjectionTestStore(t, ProjectionDef[testProjectionRec]{
		Name:    "big_idx",
		Columns: []string{"sku", "period", "customer"},
		Of: func(r testProjectionRec) ([]string, error) {
			return []string{
				strings.Repeat("X", 500),
				strings.Repeat("Y", 500),
				"c",
			}, nil
		},
	})

	_, err := s.collectProjectionMarkerPaths(
		[]testProjectionRec{{Period: "P", Customer: "C", SKU: "S"}})
	if err == nil {
		t.Error("expected error for oversized marker key, got nil")
	}
	if err != nil && !strings.Contains(err.Error(), "exceeds") {
		t.Errorf("error %q should mention the length limit", err)
	}
}

// TestMarkerPathFromValues_ValidatesValues guards that values
// carrying '/' or '..' or empty strings are rejected before they
// can corrupt a marker path.
func TestMarkerPathFromValues_ValidatesValues(t *testing.T) {
	cols := []string{"sku", "period", "customer"}
	projectionPath := "p/_projection/test"

	if _, err := markerPathFromValues("test", projectionPath, cols,
		[]string{"s", "2026-03-01", "a/b"},
	); err == nil {
		t.Error("expected error for '/' in customer, got nil")
	}
	if _, err := markerPathFromValues("test", projectionPath, cols,
		[]string{"s..bad", "p", "c"},
	); err == nil {
		t.Error("expected error for '..' in sku, got nil")
	}
	if _, err := markerPathFromValues("test", projectionPath, cols,
		[]string{"s", "", "c"},
	); err == nil {
		t.Error("expected error for empty period, got nil")
	}
}

// TestWriterConfig_ProjectionValidation covers the construction-time
// checks: empty name, name with '/', bad Columns, duplicate
// projection names. Of is now optional (nil → auto-project).
func TestWriterConfig_ProjectionValidation(t *testing.T) {
	ofStub := func(testProjectionRec) ([]string, error) {
		return nil, nil
	}

	cases := []struct {
		name string
		idx  ProjectionDef[testProjectionRec]
	}{
		{"empty name", ProjectionDef[testProjectionRec]{
			Columns: []string{"sku", "period", "customer"},
			Of:      ofStub,
		}},
		{"name with slash", ProjectionDef[testProjectionRec]{
			Name:    "bad/name",
			Columns: []string{"sku", "period", "customer"},
			Of:      ofStub,
		}},
		{"empty columns", ProjectionDef[testProjectionRec]{
			Name: "idx",
			Of:   ofStub,
		}},
		{"duplicate column", ProjectionDef[testProjectionRec]{
			Name:    "idx",
			Columns: []string{"sku", "sku", "customer"},
			Of:      ofStub,
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(Config[testProjectionRec]{
				Bucket:            "b",
				Prefix:            "p",
				S3Client:          &s3.Client{},
				PartitionKeyParts: []string{"period", "customer"},
				Projections:       []ProjectionDef[testProjectionRec]{tc.idx},
			})
			if err == nil {
				t.Error("expected error, got nil")
			}
		})
	}

	t.Run("duplicate names", func(t *testing.T) {
		def := ProjectionDef[testProjectionRec]{
			Name:    "same",
			Columns: []string{"sku", "period", "customer"},
			Of:      ofStub,
		}
		_, err := New(Config[testProjectionRec]{
			Bucket:            "b",
			Prefix:            "p",
			S3Client:          &s3.Client{},
			PartitionKeyParts: []string{"period", "customer"},
			Projections:       []ProjectionDef[testProjectionRec]{def, def},
		})
		if err == nil {
			t.Error("expected error for duplicate names, got nil")
		}
	})
}

// TestNewProjectionReader_ReadOnly proves NewProjectionReader builds a query handle
// from a bare S3Target + ProjectionLookupDef without any Writer.
func TestNewProjectionReader_ReadOnly(t *testing.T) {
	target := NewS3Target(S3TargetConfig{
		Bucket:            "b",
		Prefix:            "p",
		S3Client:          &s3.Client{},
		PartitionKeyParts: []string{"period", "customer"},
	})
	idx, err := NewProjectionReader(target, ProjectionLookupDef[SkuProjectionEntry]{
		Name:    "sku_idx",
		Columns: []string{"sku", "period", "customer"},
	})
	if err != nil {
		t.Fatalf("NewProjectionReader: %v", err)
	}
	if idx.name != "sku_idx" {
		t.Errorf("name: got %q, want sku_idx", idx.name)
	}
	if idx.projectionPath != "p/_projection/sku_idx" {
		t.Errorf("projectionPath: got %q, want p/_projection/sku_idx",
			idx.projectionPath)
	}
	if idx.bind == nil {
		t.Error("bind: got nil, want default reflection binder")
	}
}

// TestNewProjectionReader_CustomFrom proves a non-nil From overrides the
// default reflection binder, even for K's that have no parquet
// tags at all.
func TestNewProjectionReader_CustomFrom(t *testing.T) {
	type Untagged struct {
		SKU      string
		Customer string
	}
	target := NewS3Target(S3TargetConfig{
		Bucket: "b", Prefix: "p", S3Client: &s3.Client{},
		PartitionKeyParts: []string{"period", "customer"},
	})
	idx, err := NewProjectionReader(target, ProjectionLookupDef[Untagged]{
		Name:    "untagged_idx",
		Columns: []string{"sku", "customer"},
		// values aligned to Columns: [sku, customer].
		From: func(values []string) (Untagged, error) {
			return Untagged{SKU: values[0], Customer: values[1]}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewProjectionReader with custom From: %v", err)
	}
	got, err := idx.bind([]string{"s1", "abc"})
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	want := Untagged{SKU: "s1", Customer: "abc"}
	if got != want {
		t.Errorf("bind: got %+v, want %+v", got, want)
	}
}

// TestNewProjectionReader_LayoutTime guards that Layout.Time on the
// read side parses time.Time fields back into K via
// time.Parse(Layout.Time, ...). Mirror of the write-side
// auto-projection rule.
func TestNewProjectionReader_LayoutTime(t *testing.T) {
	type SkuAtKey struct {
		SKU string    `parquet:"sku"`
		At  time.Time `parquet:"at"`
	}
	target := NewS3Target(S3TargetConfig{
		Bucket: "b", Prefix: "p", S3Client: &s3.Client{},
		PartitionKeyParts: []string{"period", "customer"},
	})
	idx, err := NewProjectionReader(target, ProjectionLookupDef[SkuAtKey]{
		Name:    "at_idx",
		Columns: []string{"sku", "at"},
		Layout:  Layout{Time: time.RFC3339},
	})
	if err != nil {
		t.Fatalf("NewProjectionReader: %v", err)
	}
	got, err := idx.bind([]string{"s1", "2026-03-17T12:00:00Z"})
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	want := SkuAtKey{
		SKU: "s1",
		At:  time.Date(2026, 3, 17, 12, 0, 0, 0, time.UTC),
	}
	if got.SKU != want.SKU || !got.At.Equal(want.At) {
		t.Errorf("bind: got %+v, want %+v", got, want)
	}
}

// TestNewProjectionReader_LayoutTimeRequired guards that a time.Time
// field on K + empty Layout.Time errors at NewProjectionReader,
// mirroring the write-side requirement.
func TestNewProjectionReader_LayoutTimeRequired(t *testing.T) {
	type SkuAtKey struct {
		SKU string    `parquet:"sku"`
		At  time.Time `parquet:"at"`
	}
	target := NewS3Target(S3TargetConfig{
		Bucket: "b", Prefix: "p", S3Client: &s3.Client{},
		PartitionKeyParts: []string{"period", "customer"},
	})
	_, err := NewProjectionReader(target, ProjectionLookupDef[SkuAtKey]{
		Name:    "at_idx",
		Columns: []string{"sku", "at"},
		// Layout intentionally empty.
	})
	if err == nil {
		t.Fatal("expected error for empty Layout.Time, got nil")
	}
	if !strings.Contains(err.Error(), "Layout.Time") {
		t.Errorf("error %q should mention Layout.Time", err)
	}
}

// TestNewProjectionReader_LayoutTimeParseError guards that a
// malformed time string in a marker key surfaces as a Lookup
// error (with the projection name and column wrapped in).
func TestNewProjectionReader_LayoutTimeParseError(t *testing.T) {
	type SkuAtKey struct {
		SKU string    `parquet:"sku"`
		At  time.Time `parquet:"at"`
	}
	target := NewS3Target(S3TargetConfig{
		Bucket: "b", Prefix: "p", S3Client: &s3.Client{},
		PartitionKeyParts: []string{"period", "customer"},
	})
	idx, err := NewProjectionReader(target, ProjectionLookupDef[SkuAtKey]{
		Name:    "at_idx",
		Columns: []string{"sku", "at"},
		Layout:  Layout{Time: time.RFC3339},
	})
	if err != nil {
		t.Fatalf("NewProjectionReader: %v", err)
	}
	if _, err := idx.bind([]string{"s1", "not-a-time"}); err == nil {
		t.Error("expected parse error for malformed time, got nil")
	}
}

func TestProjectionBasePath(t *testing.T) {
	got := projectionBasePath("store", "sku_period_idx")
	want := "store/_projection/sku_period_idx"
	if got != want {
		t.Errorf("projectionBasePath = %q, want %q", got, want)
	}
}

func TestBuildAndParseProjectionMarkerKey(t *testing.T) {
	const projectionPath = "store/_projection/sku_period_idx"
	columns := []string{
		"sku_id", "charge_period_start",
		"causing_customer", "charge_period_end",
	}
	values := []string{
		"SKU-123", "2026-03-01T00",
		"abc", "2026-04-01T00",
	}

	key := buildProjectionMarkerPath(projectionPath, columns, values)

	if !strings.HasSuffix(key, "/m.proj") {
		t.Errorf("buildProjectionMarkerPath %q missing /m.proj suffix", key)
	}
	if !strings.HasPrefix(key, projectionPath+"/") {
		t.Errorf("buildProjectionMarkerPath %q missing %q prefix",
			key, projectionPath)
	}

	got, err := parseProjectionMarkerKey(key, projectionPath, columns)
	if err != nil {
		t.Fatalf("parseProjectionMarkerKey: %v", err)
	}
	if !reflect.DeepEqual(got, values) {
		t.Errorf("round-trip: got %v, want %v", got, values)
	}
}

func TestParseProjectionMarkerKey_Rejects(t *testing.T) {
	const projectionPath = "store/_projection/idx"
	columns := []string{"a", "b"}

	cases := []struct {
		name, key string
	}{
		{"wrong prefix", "other/_projection/idx/a=1/b=2/m.proj"},
		{"wrong suffix", "store/_projection/idx/a=1/b=2/other.txt"},
		{"wrong segment count", "store/_projection/idx/a=1/m.proj"},
		{"wrong column name", "store/_projection/idx/x=1/b=2/m.proj"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseProjectionMarkerKey(
				tc.key, projectionPath, columns,
			); err == nil {
				t.Errorf("expected error, got nil for %q", tc.key)
			}
		})
	}
}
