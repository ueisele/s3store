package s3sql

import (
	"context"

	"github.com/ueisele/s3store/internal/core"
)

// Read returns the latest version of all records matching the
// key pattern. Uses DuckDB with union_by_name for schema
// evolution and QUALIFY for deduplication.
//
// Accepts the shared glob grammar (see core.ValidateKeyPattern).
// When VersionColumn is empty, dedup is a no-op — every record
// in every matching file is returned in DuckDB order.
func (s *Store[T]) Read(
	ctx context.Context,
	keyPattern string,
	opts ...core.QueryOption,
) ([]T, error) {
	rows, err := s.Query(ctx, keyPattern,
		"SELECT * FROM "+s.cfg.TableAlias, opts...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return s.scanAll(rows)
}
