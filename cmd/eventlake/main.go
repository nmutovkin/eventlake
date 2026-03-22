package main

import (
	"log"
	"net/http"

	"github.com/nmutovkin/eventlake/internal/config"
	"github.com/nmutovkin/eventlake/internal/database"
	"github.com/nmutovkin/eventlake/internal/server"
)

func main() {
	cfg := config.Load()
	if err := cfg.Validate(); err != nil {
		log.Fatalf("invalid config: %v", err)
	}

	db, err := database.Connect(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("database connection: %v", err)
	}
	defer db.Close()

	if err := database.Migrate(db); err != nil {
		log.Fatalf("database migration: %v", err)
	}
	log.Println("migrations complete")

	srv := server.New(cfg, db)

	log.Printf("eventlake listening on :%s", cfg.Port)
	if err := http.ListenAndServe(":"+cfg.Port, srv.Handler()); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
