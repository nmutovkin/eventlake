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

	query, args := e.buildSQL(tenantID, req)

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

func (e *Engine) buildSQL(tenantID string, req *Request) (string, []any) {
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
