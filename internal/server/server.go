package server

import (
	"database/sql"
	"net/http"

	"github.com/redis/go-redis/v9"

	"github.com/nmutovkin/eventlake/internal/apikey"
	"github.com/nmutovkin/eventlake/internal/config"
	"github.com/nmutovkin/eventlake/internal/ingest"
	"github.com/nmutovkin/eventlake/internal/tenant"
)

type Server struct {
	cfg       *config.Config
	db        *sql.DB
	tenants   *tenant.Store
	apiKeys   *apikey.Store
	publisher *ingest.Publisher
	router    *http.ServeMux
}

func New(cfg *config.Config, db *sql.DB, rdb *redis.Client) *Server {
	s := &Server{
		cfg:       cfg,
		db:        db,
		tenants:   tenant.NewStore(db),
		apiKeys:   apikey.NewStore(db),
		publisher: ingest.NewPublisher(rdb),
		router:    http.NewServeMux(),
	}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler {
	return s.router
}

func (s *Server) routes() {
	s.router.HandleFunc("GET /healthz", s.handleHealth)

	// Tenants
	s.router.HandleFunc("POST /v1/tenants", s.handleCreateTenant)
	s.router.HandleFunc("GET /v1/tenants", s.handleListTenants)
	s.router.HandleFunc("GET /v1/tenants/{id}", s.handleGetTenant)

	// API Keys
	s.router.HandleFunc("POST /v1/tenants/{id}/api-keys", s.handleCreateAPIKey)
	s.router.HandleFunc("GET /v1/tenants/{id}/api-keys", s.handleListAPIKeys)
	s.router.HandleFunc("DELETE /v1/tenants/{id}/api-keys/{keyID}", s.handleRevokeAPIKey)

	// Authenticated endpoints
	authed := http.NewServeMux()
	authed.HandleFunc("POST /v1/events", s.handleIngest)
	s.router.Handle("/", s.requireAuth(authed))
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
