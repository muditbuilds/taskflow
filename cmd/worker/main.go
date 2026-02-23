package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/google/uuid"
	"github.com/muditbuilds/taskflow/internal/config"
	"github.com/muditbuilds/taskflow/internal/models"
	"github.com/muditbuilds/taskflow/internal/queue"
	"github.com/muditbuilds/taskflow/internal/store"
	"github.com/muditbuilds/taskflow/internal/worker"
	"github.com/redis/go-redis/v9"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg, err := config.Load()
	if err != nil {
		log.Error("load config", "error", err)
		os.Exit(1)
	}
	if cfg.WorkerID == "" {
		cfg.WorkerID = "worker-1"
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pgStore, err := store.NewPGStore(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Error("postgres", "error", err)
		os.Exit(1)
	}
	defer pgStore.Close()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, DB: cfg.RedisDB})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Error("redis", "error", err)
		os.Exit(1)
	}
	defer rdb.Close()

	q := queue.NewRedisQueue(rdb, cfg.LockTTL)

	registry := worker.NewRegistry()
	registry.Register("echo", func(ctx context.Context, t *models.Task) error {
		_, _ = json.Marshal(t.Payload)
		return nil
	})
	registry.Register("noop", func(ctx context.Context, t *models.Task) error { return nil })

	w := worker.New(cfg.WorkerID, pgStore, q, registry,
		worker.WithLockTTL(cfg.LockTTL),
		worker.WithPollInterval(cfg.PollInterval),
		worker.WithLogger(log),
		worker.WithOnCompleted(func(ctx context.Context, taskID uuid.UUID) error {
			return q.PublishTaskCompleted(ctx, taskID)
		}),
	)

	go w.Start(ctx)
	defer w.Stop()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	cancel()
	log.Info("worker shutdown complete")
}
