package query

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type Engine struct {
	db *sql.DB
}

func NewEngine(db *sql.DB) *Engine {
	return &Engine{db: db}
}

func (e *Engine) Execute(ctx context.Context, tenantID string, req *Request) (*Response, error) {
	start := time.Now()

	// Try rollup path first
	if e.canUseRollup(req) {
		query, args := e.buildRollupSQL(tenantID, req)
		rows, err := e.db.QueryContext(ctx, query, args...)
		if err == nil {
			defer rows.Close()
			results, err := scanResults(rows)
			if err == nil && len(results) > 0 {
				return &Response{
					Results: results,
					Meta: Meta{
						QueryMS: time.Since(start).Milliseconds(),
						Source:  "rollup",
					},
				}, nil
			}
		}
		// Fall through to raw query if rollup fails or returns nothing
	}

	query, args := e.buildRawSQL(tenantID, req)

	rows, err := e.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	results, err := scanResults(rows)
	if err != nil {
		return nil, fmt.Errorf("scan: %w", err)
	}

	return &Response{
		Results: results,
		Meta: Meta{
			QueryMS: time.Since(start).Milliseconds(),
			Source:  "raw",
		},
	}, nil
}

// canUseRollup checks if the query can be served from rollup tables.
// Rollups work when: granularity is hour/day, at most one group_by on a property, no filters.
func (e *Engine) canUseRollup(req *Request) bool {
	if req.Granularity == "none" && len(req.GroupBy) == 0 {
		return false // simple count — rollup can handle but needs granularity or grouping to be useful
	}
	if len(req.Filters) > 0 {
		return false // rollups don't store filtered subsets
	}
	if len(req.GroupBy) > 1 {
		return false // rollups store single group_key
	}
	return true
}

func (e *Engine) buildRollupSQL(tenantID string, req *Request) (string, []any) {
	var b strings.Builder
	args := []any{tenantID, req.EventType, req.TimeRange.From, req.TimeRange.To}
	paramIdx := 5

	b.WriteString("SELECT ")
	var selectCols []string
	var groupCols []string

	if req.Granularity != "none" {
		selectCols = append(selectCols, "period_start AS period")
		groupCols = append(groupCols, "period_start")
	}

	if len(req.GroupBy) == 1 {
		selectCols = append(selectCols, fmt.Sprintf("group_value AS %s", sanitizeIdent(req.GroupBy[0])))
		groupCols = append(groupCols, "group_value")
	}

	selectCols = append(selectCols, "sum(count)::bigint AS count")
	b.WriteString(strings.Join(selectCols, ", "))

	// Determine which rollup period to use
	period := "day"
	if req.Granularity == "hour" {
		period = "hour"
	}

	b.WriteString(" FROM rollups WHERE tenant_id = $1 AND event_type = $2")
	b.WriteString(fmt.Sprintf(" AND period = $%d", paramIdx))
	args = append(args, period)
	paramIdx++

	b.WriteString(" AND period_start >= $3 AND period_start < $4")

	// Filter by group_key if grouping
	if len(req.GroupBy) == 1 {
		b.WriteString(fmt.Sprintf(" AND group_key = $%d", paramIdx))
		args = append(args, req.GroupBy[0])
		paramIdx++
	} else {
		b.WriteString(" AND group_key = ''")
	}

	if len(groupCols) > 0 {
		b.WriteString(" GROUP BY ")
		b.WriteString(strings.Join(groupCols, ", "))
	}

	if req.Granularity != "none" {
		b.WriteString(" ORDER BY period_start")
	}

	return b.String(), args
}

func (e *Engine) buildRawSQL(tenantID string, req *Request) (string, []any) {
	var b strings.Builder
	args := []any{tenantID, req.EventType, req.TimeRange.From, req.TimeRange.To}
	paramIdx := 5

	// SELECT clause
	b.WriteString("SELECT ")

	var selectCols []string
	var groupCols []string

	if req.Granularity != "none" {
		trunc := granularityToTrunc(req.Granularity)
		col := fmt.Sprintf("date_trunc('%s', timestamp) AS period", trunc)
		selectCols = append(selectCols, col)
		groupCols = append(groupCols, "period")
	}

	for _, g := range req.GroupBy {
		col := fmt.Sprintf("properties->>$%d AS %s", paramIdx, sanitizeIdent(g))
		selectCols = append(selectCols, col)
		groupCols = append(groupCols, sanitizeIdent(g))
		args = append(args, g)
		paramIdx++
	}

	selectCols = append(selectCols, "count(*) AS count")
	b.WriteString(strings.Join(selectCols, ", "))

	// FROM + WHERE
	b.WriteString(" FROM events WHERE tenant_id = $1 AND event_type = $2")
	b.WriteString(" AND timestamp >= $3 AND timestamp < $4")

	// Filters on properties
	for _, f := range req.Filters {
		sqlOp := validOps[f.Op]
		if f.Op == "contains" {
			b.WriteString(fmt.Sprintf(" AND properties->>$%d %s $%d", paramIdx, sqlOp, paramIdx+1))
			args = append(args, f.Property, fmt.Sprintf("%%%v%%", f.Value))
		} else {
			b.WriteString(fmt.Sprintf(" AND properties->>$%d %s $%d", paramIdx, sqlOp, paramIdx+1))
			args = append(args, f.Property, fmt.Sprint(f.Value))
		}
		paramIdx += 2
	}

	// GROUP BY
	if len(groupCols) > 0 {
		b.WriteString(" GROUP BY ")
		b.WriteString(strings.Join(groupCols, ", "))
	}

	// ORDER BY
	if req.Granularity != "none" {
		b.WriteString(" ORDER BY period")
	}

	return b.String(), args
}

func granularityToTrunc(g string) string {
	switch g {
	case "hour":
		return "hour"
	case "day":
		return "day"
	default:
		return "day"
	}
}

func sanitizeIdent(s string) string {
	// Only allow alphanumeric and underscore for column aliases
	var b strings.Builder
	for _, c := range s {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			b.WriteRune(c)
		}
	}
	result := b.String()
	if result == "" {
		return "col"
	}
	return result
}

func scanResults(rows *sql.Rows) ([]map[string]any, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results []map[string]any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}

		row := make(map[string]any, len(cols))
		for i, col := range cols {
			v := vals[i]
			// Convert time.Time to string for JSON
			if t, ok := v.(time.Time); ok {
				v = t.Format(time.RFC3339)
			}
			// Convert []byte to string
			if b, ok := v.([]byte); ok {
				v = string(b)
			}
			row[col] = v
		}
		results = append(results, row)
	}

	if results == nil {
		results = []map[string]any{}
	}

	return results, rows.Err()
}
