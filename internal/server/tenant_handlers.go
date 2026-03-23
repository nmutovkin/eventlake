package server

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
)

func (s *Server) handleCreateTenant(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}

	t, err := s.tenants.Create(r.Context(), req.Name)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create tenant")
		return
	}
	writeJSON(w, http.StatusCreated, t)
}

func (s *Server) handleGetTenant(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	t, err := s.tenants.GetByID(r.Context(), id)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "tenant not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get tenant")
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (s *Server) handleListTenants(w http.ResponseWriter, r *http.Request) {
	tenants, err := s.tenants.List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list tenants")
		return
	}
	writeJSON(w, http.StatusOK, tenants)
}
