package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPPort     int
	PostgresDSN  string
	RedisAddr    string
	RedisDB      int
	WorkerID     string
	LockTTL      time.Duration
	PollInterval time.Duration
}

func Load() (*Config, error) {
	cfg := &Config{
		HTTPPort:     getIntEnv("TASKFLOW_HTTP_PORT", 8080),
		PostgresDSN:  getEnv("TASKFLOW_POSTGRES_DSN", "postgres://taskflow:taskflow@localhost:5432/taskflow?sslmode=disable"),
		RedisAddr:    getEnv("TASKFLOW_REDIS_ADDR", "localhost:6379"),
		RedisDB:      getIntEnv("TASKFLOW_REDIS_DB", 0),
		WorkerID:     getEnv("TASKFLOW_WORKER_ID", ""),
		LockTTL:      getDurationEnv("TASKFLOW_LOCK_TTL", 60*time.Second),
		PollInterval: getDurationEnv("TASKFLOW_POLL_INTERVAL", 100*time.Millisecond),
	}
	if cfg.WorkerID == "" && os.Getenv("TASKFLOW_RUN_WORKER") == "1" {
		cfg.WorkerID = fmt.Sprintf("worker-%d", os.Getpid())
	}
	return cfg, nil
}

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func getIntEnv(key string, defaultVal int) int {
	if v := os.Getenv(key); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil {
			return n
		}
	}
	return defaultVal
}

func getDurationEnv(key string, defaultVal time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		d, err := time.ParseDuration(v)
		if err == nil {
			return d
		}
	}
	return defaultVal
}
