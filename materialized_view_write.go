package s3store

import (
	"context"
	"fmt"
)

// Materialized view write side: per-view marker emission during
// Write.
//
// matviewWriter is the internal, K-erased per-view marker emitter
// the write path consumes. Given a record, pathOf returns the S3
// marker key the record produces under this view, or "" when
// MaterializedViewDef.Of returned (nil, nil) signalling no marker.
type matviewWriter[T any] struct {
	name   string
	pathOf func(T) (string, error)
}

// buildMatviewWriters validates each MaterializedViewDef and
// resolves it into a matviewWriter[T] closure ready for the write
// path. Rejects duplicate Names so two views can't silently share
// the same _matview/<Name>/ subtree.
func buildMatviewWriters[T any](
	target S3Target, defs []MaterializedViewDef[T],
) ([]matviewWriter[T], error) {
	if len(defs) == 0 {
		return nil, nil
	}
	seenNames := make(map[string]struct{}, len(defs))
	out := make([]matviewWriter[T], 0, len(defs))
	for _, def := range defs {
		if err := validateMatviewDefShape(def.Name, def.Columns); err != nil {
			return nil, err
		}
		if _, dup := seenNames[def.Name]; dup {
			return nil, fmt.Errorf(
				"duplicate matview name %q in WriterConfig.MaterializedViews",
				def.Name)
		}
		seenNames[def.Name] = struct{}{}

		of, err := resolveOf(def)
		if err != nil {
			return nil, err
		}

		matviewPath := matviewBasePath(target.Prefix(), def.Name)
		name := def.Name
		columns := def.Columns
		out = append(out, matviewWriter[T]{
			name: name,
			pathOf: func(rec T) (string, error) {
				values, err := of(rec)
				if err != nil {
					return "", err
				}
				if values == nil {
					return "", nil
				}
				return markerPathFromValues(name, matviewPath, columns, values)
			},
		})
	}
	return out, nil
}

// collectMatviewMarkerPaths iterates every registered materialized
// view over every record in the batch and returns the deduplicated
// set of marker S3 keys. Dedup is via map[string]struct{} on the
// full path, which is correct because different views live under
// different _matview/<name>/ prefixes — no cross-view collisions.
func (s *Writer[T]) collectMatviewMarkerPaths(records []T) ([]string, error) {
	if len(s.matviews) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{})
	for _, mv := range s.matviews {
		for _, rec := range records {
			p, err := mv.pathOf(rec)
			if err != nil {
				return nil, fmt.Errorf(
					"matview %q: %w", mv.name, err)
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
		s.cfg.Target.metrics,
		func(ctx context.Context, _ int, p string) error {
			return s.cfg.Target.put(
				ctx, p, nil, "application/octet-stream",
			)
		})
}
