package s3parquet

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

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
		indexPath: indexBasePath(target.Prefix(), def.Name),
		bind:      bind,
	}, nil
}

// defaultBinder builds a reflection-based bind closure for the
// read side: parses values back into K. String fields are
// SetString'd; time.Time fields are time.Parse'd via
// Layout.Time. Every parquet-tagged field on K must map to a
// column — no extras allowed.
func defaultBinder[K any](columns []string, layout Layout) (
	func(values []string) (K, error), error,
) {
	plans, err := buildFieldPlans(
		reflect.TypeFor[K](), columns, layout, true, "From")
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
		for j, p := range plans {
			if p.isTime {
				ts, err := time.Parse(p.timeLayout, values[j])
				if err != nil {
					return k, fmt.Errorf(
						"index bind: column %q: parse time %q with "+
							"layout %q: %w",
						columns[j], values[j], p.timeLayout, err)
				}
				v.Field(p.fieldIdx).Set(reflect.ValueOf(ts))
			} else {
				v.Field(p.fieldIdx).SetString(values[j])
			}
		}
		return k, nil
	}, nil
}

// Lookup returns every K whose marker matches any of the given
// key patterns. Patterns use the grammar from
// validateKeyPattern, evaluated against Columns. Pass
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
	patterns = dedupePatterns(patterns)
	if len(patterns) == 0 {
		return nil, nil
	}

	plans, err := buildReadPlans(patterns, i.indexPath, i.columns)
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
		values, err := parseIndexMarkerKey(
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
// each page to marker keys whose hive body satisfies plan.match,
// and returns the unioned, deduplicated set of marker S3 keys.
// Overlapping plans can list the same marker (e.g. "sku=*" and
// "sku=s1" both cover the s1 markers); UnionKeys collapses them.
//
// Inherits ConsistencyControl from i.target on every LIST. With
// a strong level (or AWS S3, which is strong-LIST by default),
// Lookup is read-after-write — no settle window needed.
func (i *IndexReader[K]) listAllMatchingMarkers(
	ctx context.Context, plans []*readPlan,
) ([]string, error) {
	suffix := "/" + indexMarkerFilename
	return fanOutMapReduce(ctx, plans,
		i.target.EffectiveMaxInflightRequests(),
		func(ctx context.Context, plan *readPlan) ([]string, error) {
			var keys []string
			err := i.target.listEach(ctx, plan.listPrefix, "", 0,
				func(obj s3types.Object) (bool, error) {
					key := aws.ToString(obj.Key)
					if !strings.HasSuffix(key, suffix) {
						return true, nil
					}
					hiveKey, ok := hiveKeyOfMarker(key, i.indexPath)
					if !ok {
						return true, nil
					}
					if plan.match(hiveKey) {
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
		func(per [][]string) []string { return unionKeys(per, identityKey) })
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
	tail := "/" + indexMarkerFilename
	if !strings.HasSuffix(rest, tail) {
		return "", false
	}
	return rest[:len(rest)-len(tail)], true
}
