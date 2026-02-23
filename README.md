# Taskflow

Distributed task scheduler with exactly-once execution guarantees. Uses Redis for distributed locking and priority queues, PostgreSQL for durable task state. Supports priority queues, delayed execution, and static DAG-based workflows.

## Features

- **Distributed architecture**: Redis (ElastiCache) for distributed locking and queue coordination; PostgreSQL (RDS) for durable task and DAG state.
- **Priority queues & delayed execution**: Tasks are scored by priority and scheduled time; workers claim work via atomic dequeue + lock.
- **Static DAG workflows**: Submit a DAG as JSON (steps + edges). Root steps are enqueued immediately; dependent steps run when parents complete.
- **Work-stealing**: Workers pull tasks from a shared Redis queue with lock acquisition; no central dispatcher per task.
- **Failure handling**: Configurable retries with exponential backoff; tasks that exceed retries move to DLQ (dead-letter); DAG run marked failed when a step goes to DLQ.
- **At-least-once delivery**: Task state in PostgreSQL; queue in Redis; locks prevent double execution.

## Tech stack

- **Golang** 1.21+
- **Redis** (ElastiCache-compatible) – queues, locks, task-completion pub/sub
- **PostgreSQL** (RDS-compatible) – tasks, DAG runs
- **Docker** – local run

## Quick start

### Docker Compose (scheduler + worker + Postgres + Redis)

```bash
docker compose up -d postgres redis
# Run migrations and start scheduler (optional: run migrations manually once)
docker compose up -d scheduler
docker compose up -d worker
```

API: `http://localhost:8080`

### Local (no Docker)

1. Start Postgres and Redis locally (e.g. default ports 5432, 6379).
2. Create DB: `createdb taskflow` and user/password if needed.
3. Run migrations: `psql -f migrations/001_schema.up.sql` (or run scheduler once with migrations in CWD).
4. Start scheduler: `go run ./cmd/scheduler`
5. Start worker(s): `TASKFLOW_WORKER_ID=worker-1 go run ./cmd/worker`

Env (defaults):

- `TASKFLOW_HTTP_PORT` – scheduler HTTP port (default 8080)
- `TASKFLOW_POSTGRES_DSN` – Postgres connection string
- `TASKFLOW_REDIS_ADDR` – Redis address
- `TASKFLOW_WORKER_ID` – worker identifier
- `TASKFLOW_LOCK_TTL` – claim lock TTL (default 60s)
- `TASKFLOW_POLL_INTERVAL` – worker poll interval (default 100ms)

## API

- `POST /api/v1/tasks` – enqueue a single task (JSON: `type`, `payload`, optional `priority`, `scheduled_at`, `max_retries`).
- `GET /api/v1/tasks/{id}` – get task status and details.
- `POST /api/v1/dags` – submit a DAG (JSON: `steps` array, `edges` array). Each step: `id`, `type`, `payload`, optional `priority`. Each edge: `from`, `to` (step IDs).
- `GET /health` – health check.

## Go client

```go
import "github.com/muditbuilds/taskflow/pkg/client"

c := client.New("http://localhost:8080")
id, err := c.EnqueueTask(ctx, client.EnqueueTaskRequest{
    Type:    "echo",
    Payload: json.RawMessage(`{"key":"value"}`),
    Priority: 1,
})
dagRunID, err := c.SubmitDag(ctx, client.SubmitDagRequest{
    Steps: []client.DagStep{{ID: "a", Type: "noop", Payload: []byte("{}")}, {ID: "b", Type: "noop", Payload: []byte("{}")}},
    Edges: []client.DagEdge{{From: "a", To: "b"}},
})
```

## Project layout

- `cmd/scheduler` – HTTP API + enqueue loop + DAG completion subscriber
- `cmd/worker` – work-stealing loop + task execution (register handlers by type)
- `internal/api` – REST handlers and router
- `internal/config` – env-based config
- `internal/models` – task and DAG types
- `internal/queue` – Redis queue and lock interface + impl
- `internal/scheduler` – enqueue loop, DAG progression, completion subscriber
- `internal/store` – PostgreSQL task and DAG store
- `internal/worker` – executor registry and worker loop
- `pkg/client` – Go SDK for the API
- `migrations/` – SQL schema

## License

MIT
