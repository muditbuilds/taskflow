package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/google/uuid"
	"github.com/muditbuilds/taskflow/internal/api"
	"github.com/muditbuilds/taskflow/internal/config"
	"github.com/muditbuilds/taskflow/internal/models"
	"github.com/muditbuilds/taskflow/internal/queue"
	"github.com/muditbuilds/taskflow/internal/scheduler"
	"github.com/muditbuilds/taskflow/internal/store"
	"github.com/redis/go-redis/v9"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg, err := config.Load()
	if err != nil {
		log.Error("load config", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pgStore, err := store.NewPGStore(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Error("postgres", "error", err)
		os.Exit(1)
	}
	defer pgStore.Close()
	if sql, err := os.ReadFile("migrations/001_schema.up.sql"); err == nil {
		if err := pgStore.Migrate(ctx, sql); err != nil {
			log.Warn("migrate", "error", err)
		}
	}

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, DB: cfg.RedisDB})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Error("redis", "error", err)
		os.Exit(1)
	}
	defer rdb.Close()

	q := queue.NewRedisQueue(rdb, cfg.LockTTL)
	sched := scheduler.NewWithRedis(pgStore, pgStore, q, rdb, log)
	sched.Start(ctx)
	defer sched.Stop()

	handlers := &api.Handlers{
		TaskStore: pgStore,
		DagStore:  pgStore,
		CreateDagTasksAndEnqueue: func(ctx context.Context, dagRunID uuid.UUID, def models.DagDefinition) error {
			return scheduler.CreateDagTasksAndEnqueueRoots(ctx, dagRunID, def, pgStore, pgStore, q)
		},
	}
	router := api.NewRouter(handlers)

	server := &http.Server{Addr: ":" + strconv.Itoa(cfg.HTTPPort), Handler: router}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("http", "error", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	_ = server.Shutdown(context.Background())
	log.Info("shutdown complete")
}
