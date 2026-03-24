package server

import (
	"encoding/json"
	"net/http"

	"github.com/nmutovkin/eventlake/internal/query"
)

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	tenantID := TenantIDFromContext(r.Context())

	var req query.Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}

	if err := req.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	resp, err := s.queryEngine.Execute(r.Context(), tenantID, &req)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "query failed")
		return
	}

	writeJSON(w, http.StatusOK, resp)
}
