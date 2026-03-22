package server

import (
	"database/sql"
	"encoding/json"
	"net/http"

	"github.com/nmutovkin/eventlake/internal/config"
)

type Server struct {
	cfg    *config.Config
	db     *sql.DB
	router *http.ServeMux
}

func New(cfg *config.Config, db *sql.DB) *Server {
	s := &Server{
		cfg:    cfg,
		db:     db,
		router: http.NewServeMux(),
	}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler {
	return s.router
}

func (s *Server) routes() {
	s.router.HandleFunc("GET /healthz", s.handleHealth)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}
