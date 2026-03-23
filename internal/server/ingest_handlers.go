package server

import "net/http"

func (s *Server) handleIngestPlaceholder(w http.ResponseWriter, r *http.Request) {
	tenantID := TenantIDFromContext(r.Context())
	writeJSON(w, http.StatusAccepted, map[string]string{
		"status":    "accepted",
		"tenant_id": tenantID,
	})
}
