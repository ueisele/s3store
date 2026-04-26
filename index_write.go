package s3store

import (
	"context"
	"fmt"
)

// Index write side: per-index marker emission during Write.
//
// indexWriter is the internal, K-erased per-index marker emitter
// the write path consumes. Given a record, pathOf returns the S3
// marker key the record produces under this index, or "" when
// IndexDef.Of returned (nil, nil) signalling no marker.
type indexWriter[T any] struct {
	name   string
	pathOf func(T) (string, error)
}

// buildIndexWriters validates each IndexDef and resolves it into
// an indexWriter[T] closure ready for the write path. Rejects
// duplicate Names so two indexes can't silently share the same
// _index/<Name>/ subtree.
func buildIndexWriters[T any](
	target S3Target, defs []IndexDef[T],
) ([]indexWriter[T], error) {
	if len(defs) == 0 {
		return nil, nil
	}
	seenNames := make(map[string]struct{}, len(defs))
	out := make([]indexWriter[T], 0, len(defs))
	for _, def := range defs {
		if err := validateIndexDefShape(def.Name, def.Columns); err != nil {
			return nil, err
		}
		if _, dup := seenNames[def.Name]; dup {
			return nil, fmt.Errorf(
				"s3parquet: duplicate index name %q in WriterConfig.Indexes",
				def.Name)
		}
		seenNames[def.Name] = struct{}{}

		of, err := resolveOf(def)
		if err != nil {
			return nil, err
		}

		indexPath := indexBasePath(target.Prefix(), def.Name)
		name := def.Name
		columns := def.Columns
		out = append(out, indexWriter[T]{
			name: name,
			pathOf: func(rec T) (string, error) {
				values, err := of(rec)
				if err != nil {
					return "", err
				}
				if values == nil {
					return "", nil
				}
				return markerPathFromValues(name, indexPath, columns, values)
			},
		})
	}
	return out, nil
}

// collectIndexMarkerPaths iterates every registered index over
// every record in the batch and returns the deduplicated set of
// marker S3 keys. Dedup is via map[string]struct{} on the full
// path, which is correct because different indexes live under
// different _index/<name>/ prefixes — no cross-index collisions.
func (s *Writer[T]) collectIndexMarkerPaths(records []T) ([]string, error) {
	if len(s.indexes) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{})
	for _, idx := range s.indexes {
		for _, rec := range records {
			p, err := idx.pathOf(rec)
			if err != nil {
				return nil, fmt.Errorf(
					"s3parquet: index %q: %w", idx.name, err)
			}
			if p == "" {
				continue
			}
			seen[p] = struct{}{}
		}
	}
	if len(seen) == 0 {
		return nil, nil
	}
	out := make([]string, 0, len(seen))
	for p := range seen {
		out = append(out, p)
	}
	return out, nil
}

// putMarkers fans marker PUTs out through fanOut. The
// per-target MaxInflightRequests semaphore inside target.put caps
// net in-flight S3 requests, so the fan-out can't overshoot
// http.Transport.MaxConnsPerHost — extra goroutines just queue at
// the semaphore. Returns the first PUT error; partial success on
// failure is accepted — orphan markers are tolerated at Lookup
// time.
func (s *Writer[T]) putMarkers(
	ctx context.Context, paths []string,
) error {
	return fanOut(ctx, paths,
		s.cfg.Target.EffectiveMaxInflightRequests(),
		func(ctx context.Context, _ int, p string) error {
			return s.cfg.Target.put(
				ctx, p, nil, "application/octet-stream",
			)
		})
}
