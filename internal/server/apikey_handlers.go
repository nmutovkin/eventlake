package server

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
)

func (s *Server) handleCreateAPIKey(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("id")

	var req struct {
		Label string `json:"label"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}

	key, err := s.apiKeys.Create(r.Context(), tenantID, req.Label)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create api key")
		return
	}
	writeJSON(w, http.StatusCreated, key)
}

func (s *Server) handleListAPIKeys(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("id")

	keys, err := s.apiKeys.ListByTenant(r.Context(), tenantID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list api keys")
		return
	}
	writeJSON(w, http.StatusOK, keys)
}

func (s *Server) handleRevokeAPIKey(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("id")
	keyID := r.PathValue("keyID")

	err := s.apiKeys.Revoke(r.Context(), tenantID, keyID)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "api key not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to revoke api key")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "revoked"})
}
