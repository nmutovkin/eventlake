package rollup

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"
)

type Job struct {
	db *sql.DB
}

func NewJob(db *sql.DB) *Job {
	return &Job{db: db}
}

// RunHourly computes hourly rollups for the previous hour.
func (j *Job) RunHourly(ctx context.Context) error {
	now := time.Now().UTC().Truncate(time.Hour)
	start := now.Add(-1 * time.Hour)
	return j.computeRollups(ctx, "hour", start, now)
}

// RunDaily computes daily rollups for the previous day.
func (j *Job) RunDaily(ctx context.Context) error {
	now := time.Now().UTC().Truncate(24 * time.Hour)
	start := now.Add(-24 * time.Hour)
	return j.computeRollups(ctx, "day", start, now)
}

// Backfill recomputes rollups for a specific period range.
func (j *Job) Backfill(ctx context.Context, period string, from, to time.Time) error {
	var step time.Duration
	switch period {
	case "hour":
		step = time.Hour
	case "day":
		step = 24 * time.Hour
	default:
		return fmt.Errorf("unsupported period: %s", period)
	}

	for t := from; t.Before(to); t = t.Add(step) {
		if err := j.computeRollups(ctx, period, t, t.Add(step)); err != nil {
			return fmt.Errorf("backfill %s at %s: %w", period, t, err)
		}
	}
	return nil
}

func (j *Job) computeRollups(ctx context.Context, period string, start, end time.Time) error {
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	// Delete existing rollups for this window (idempotent recompute)
	_, err = tx.ExecContext(ctx,
		`DELETE FROM rollups WHERE period = $1 AND period_start = $2`,
		period, start,
	)
	if err != nil {
		return fmt.Errorf("delete old rollups: %w", err)
	}

	// Insert total counts per tenant + event_type (no group_key)
	_, err = tx.ExecContext(ctx,
		`INSERT INTO rollups (tenant_id, event_type, period, period_start, count)
		 SELECT tenant_id, event_type, $1, $2, count(*)
		 FROM events
		 WHERE timestamp >= $2 AND timestamp < $3
		 GROUP BY tenant_id, event_type`,
		period, start, end,
	)
	if err != nil {
		return fmt.Errorf("insert total rollups: %w", err)
	}

	// Insert grouped counts for common property keys
	// We extract the top property keys from the events themselves
	_, err = tx.ExecContext(ctx,
		`INSERT INTO rollups (tenant_id, event_type, period, period_start, group_key, group_value, count)
		 SELECT tenant_id, event_type, $1, $2, key, value, count(*)
		 FROM events, jsonb_each_text(properties) AS p(key, value)
		 WHERE timestamp >= $2 AND timestamp < $3
		 GROUP BY tenant_id, event_type, key, value`,
		period, start, end,
	)
	if err != nil {
		return fmt.Errorf("insert grouped rollups: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	log.Printf("rollup %s [%s, %s) computed", period, start.Format(time.RFC3339), end.Format(time.RFC3339))
	return nil
}
