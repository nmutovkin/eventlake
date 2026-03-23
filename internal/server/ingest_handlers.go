package server

import (
	"encoding/json"
	"net/http"

	"github.com/nmutovkin/eventlake/internal/ingest"
)

const maxBatchSize = 100

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	tenantID := TenantIDFromContext(r.Context())

	var req ingest.BatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}

	if len(req.Events) == 0 {
		writeError(w, http.StatusBadRequest, "events array is empty")
		return
	}
	if len(req.Events) > maxBatchSize {
		writeError(w, http.StatusBadRequest, "batch too large, max 100 events")
		return
	}

	var valid []ingest.Event
	var errors []ingest.EventError

	for i, e := range req.Events {
		if err := e.Validate(i); err != nil {
			errors = append(errors, *err)
		} else {
			valid = append(valid, e)
		}
	}

	if len(valid) > 0 {
		if err := s.publisher.Publish(r.Context(), tenantID, valid); err != nil {
			writeError(w, http.StatusInternalServerError, "failed to accept events")
			return
		}
	}

	writeJSON(w, http.StatusAccepted, ingest.BatchResponse{
		Accepted: len(valid),
		Rejected: len(errors),
		Errors:   errors,
	})
}
