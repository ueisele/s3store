package s3store

import (
	"context"
)

// Read returns the latest version of all records matching the
// key pattern. Uses DuckDB with union_by_name for schema
// evolution and QUALIFY for deduplication.
//
// Supports arbitrary glob patterns:
//   - "charge_period=2026-03-17/customer=abc"  → exact key
//   - "charge_period=2026-03-17/*"             → all customers
//   - "charge_period=2026-03-*/customer=abc"   → March, one customer
//   - "*"                                       → all data
func (s *Store[T]) Read(
	ctx context.Context, keyPattern string,
) ([]T, error) {
	rows, err := s.Query(ctx, keyPattern,
		"SELECT * FROM "+s.cfg.TableAlias)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return s.scanAll(rows)
}
