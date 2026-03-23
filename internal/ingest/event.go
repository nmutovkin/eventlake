package ingest

import (
	"fmt"
	"time"
)

type Event struct {
	EventType      string         `json:"event_type"`
	Timestamp      time.Time      `json:"timestamp"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
	UserID         string         `json:"user_id,omitempty"`
	SessionID      string         `json:"session_id,omitempty"`
	Properties     map[string]any `json:"properties,omitempty"`
}

type BatchRequest struct {
	Events []Event `json:"events"`
}

type BatchResponse struct {
	Accepted int          `json:"accepted"`
	Rejected int          `json:"rejected"`
	Errors   []EventError `json:"errors,omitempty"`
}

type EventError struct {
	Index   int    `json:"index"`
	Message string `json:"message"`
}

func (e *Event) Validate(index int) *EventError {
	if e.EventType == "" {
		return &EventError{Index: index, Message: "event_type is required"}
	}
	if e.Timestamp.IsZero() {
		return &EventError{Index: index, Message: "timestamp is required"}
	}
	if time.Since(e.Timestamp) > 7*24*time.Hour {
		return &EventError{Index: index, Message: "timestamp is older than 7 days"}
	}
	if e.Timestamp.After(time.Now().Add(1 * time.Minute)) {
		return &EventError{Index: index, Message: "timestamp is in the future"}
	}
	if len(e.EventType) > 256 {
		return &EventError{Index: index, Message: fmt.Sprintf("event_type too long (%d chars, max 256)", len(e.EventType))}
	}
	return nil
}
