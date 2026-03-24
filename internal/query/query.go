package query

import (
	"fmt"
	"time"
)

type Request struct {
	EventType   string   `json:"event_type"`
	TimeRange   Range    `json:"time_range"`
	Filters     []Filter `json:"filters,omitempty"`
	GroupBy     []string `json:"group_by,omitempty"`
	Metric      string   `json:"metric"` // "count"
	Granularity string   `json:"granularity,omitempty"` // "none", "hour", "day"
}

type Range struct {
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}

type Filter struct {
	Property string `json:"property"`
	Op       string `json:"op"`    // "eq", "neq", "gt", "lt", "contains"
	Value    any    `json:"value"`
}

type Response struct {
	Results []map[string]any `json:"results"`
	Meta    Meta             `json:"meta"`
}

type Meta struct {
	QueryMS int64  `json:"query_ms"`
	Source  string `json:"source"` // "raw", "rollup"
}

var validOps = map[string]string{
	"eq":       "=",
	"neq":      "!=",
	"gt":       ">",
	"lt":       "<",
	"gte":      ">=",
	"lte":      "<=",
	"contains": "LIKE",
}

func (r *Request) Validate() error {
	if r.EventType == "" {
		return fmt.Errorf("event_type is required")
	}
	if r.TimeRange.From.IsZero() || r.TimeRange.To.IsZero() {
		return fmt.Errorf("time_range.from and time_range.to are required")
	}
	if r.TimeRange.To.Before(r.TimeRange.From) {
		return fmt.Errorf("time_range.to must be after time_range.from")
	}
	if r.TimeRange.To.Sub(r.TimeRange.From) > 90*24*time.Hour {
		return fmt.Errorf("time range cannot exceed 90 days")
	}
	if r.Metric == "" {
		r.Metric = "count"
	}
	if r.Metric != "count" {
		return fmt.Errorf("unsupported metric: %s (only 'count' is supported)", r.Metric)
	}
	if r.Granularity == "" {
		r.Granularity = "none"
	}
	switch r.Granularity {
	case "none", "hour", "day":
	default:
		return fmt.Errorf("unsupported granularity: %s", r.Granularity)
	}
	for _, f := range r.Filters {
		if f.Property == "" {
			return fmt.Errorf("filter property is required")
		}
		if _, ok := validOps[f.Op]; !ok {
			return fmt.Errorf("unsupported filter op: %s", f.Op)
		}
	}
	return nil
}
