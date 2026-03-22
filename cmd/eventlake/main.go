package main

import (
	"log"
	"net/http"

	"github.com/nmutovkin/eventlake/internal/config"
	"github.com/nmutovkin/eventlake/internal/server"
)

func main() {
	cfg := config.Load()
	if err := cfg.Validate(); err != nil {
		log.Fatalf("invalid config: %v", err)
	}

	srv := server.New(cfg)

	log.Printf("eventlake listening on :%s", cfg.Port)
	if err := http.ListenAndServe(":"+cfg.Port, srv.Handler()); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
