package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"syscall"

	"github.com/nmutovkin/eventlake/internal/config"
	"github.com/nmutovkin/eventlake/internal/database"
	rdclient "github.com/nmutovkin/eventlake/internal/redis"
	"github.com/nmutovkin/eventlake/internal/server"
	"github.com/nmutovkin/eventlake/internal/worker"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

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

	rdb, err := rdclient.Connect(cfg.RedisURL)
	if err != nil {
		log.Fatalf("redis connection: %v", err)
	}
	defer rdb.Close()

	// Start write worker
	w := worker.NewWriter(db, rdb)
	go func() {
		if err := w.Run(ctx); err != nil && ctx.Err() == nil {
			log.Fatalf("write worker error: %v", err)
		}
	}()

	srv := server.New(cfg, db, rdb)

	log.Printf("eventlake listening on :%s", cfg.Port)
	if err := http.ListenAndServe(":"+cfg.Port, srv.Handler()); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
