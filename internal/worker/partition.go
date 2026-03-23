package worker

import (
	"context"
	"database/sql"
	"fmt"
)

// EnsurePartition creates a tenant partition if it doesn't exist.
// Events table is partitioned by tenant_id (list partitioning).
func EnsurePartition(ctx context.Context, db *sql.DB, tenantID string) error {
	partName := fmt.Sprintf("events_%s", tenantID[:8])

	query := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s PARTITION OF events FOR VALUES IN ('%s')`,
		partName, tenantID,
	)

	_, err := db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("create partition %s: %w", partName, err)
	}
	return nil
}
