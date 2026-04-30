package s3store

import (
	"context"
	"fmt"
)

// Projection write side: per-projection marker emission during
// Write.
//
// projectionWriter is the internal, K-erased per-projection
// marker emitter the write path consumes. Given a record, pathOf
// returns the S3 marker key the record produces under this
// projection, or "" when ProjectionDef.Of returned (nil, nil)
// signalling no marker.
type projectionWriter[T any] struct {
	name   string
	pathOf func(T) (string, error)
}

// buildProjectionWriters validates each ProjectionDef and resolves
// it into a projectionWriter[T] closure ready for the write path.
// Rejects duplicate Names so two projections can't silently share
// the same _projection/<Name>/ subtree.
func buildProjectionWriters[T any](
	target S3Target, defs []ProjectionDef[T],
) ([]projectionWriter[T], error) {
	if len(defs) == 0 {
		return nil, nil
	}
	seenNames := make(map[string]struct{}, len(defs))
	out := make([]projectionWriter[T], 0, len(defs))
	for _, def := range defs {
		if err := validateProjectionDefShape(def.Name, def.Columns); err != nil {
			return nil, err
		}
		if _, dup := seenNames[def.Name]; dup {
			return nil, fmt.Errorf(
				"duplicate projection name %q in WriterConfig.Projections",
				def.Name)
		}
		seenNames[def.Name] = struct{}{}

		of, err := resolveOf(def)
		if err != nil {
			return nil, err
		}

		projectionPath := projectionBasePath(target.Prefix(), def.Name)
		name := def.Name
		columns := def.Columns
		out = append(out, projectionWriter[T]{
			name: name,
			pathOf: func(rec T) (string, error) {
				values, err := of(rec)
				if err != nil {
					return "", err
				}
				if values == nil {
					return "", nil
				}
				return markerPathFromValues(name, projectionPath, columns, values)
			},
		})
	}
	return out, nil
}

// collectProjectionMarkerPaths iterates every registered
// projection over every record in the batch and returns the
// deduplicated set of marker S3 keys. Dedup is via
// map[string]struct{} on the full path, which is correct because
// different projections live under different _projection/<name>/
// prefixes — no cross-projection collisions.
func (s *Writer[T]) collectProjectionMarkerPaths(records []T) ([]string, error) {
	if len(s.projections) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{})
	for _, proj := range s.projections {
		for _, rec := range records {
			p, err := proj.pathOf(rec)
			if err != nil {
				return nil, fmt.Errorf(
					"projection %q: %w", proj.name, err)
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
