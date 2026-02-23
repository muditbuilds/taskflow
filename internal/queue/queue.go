package queue

import (
	"context"
	"time"

	"github.com/google/uuid"
)

const (
	QueueName     = "taskflow:queue"
	ProcessingSet = "taskflow:processing"
	LockPrefix    = "taskflow:lock:"
)

const ChannelCompleted = "taskflow:completed"

// Queue uses Redis for priority queue and distributed locking.
// Score = -priority (higher priority first) then scheduled_at unix for ordering.
type Queue interface {
	Enqueue(ctx context.Context, taskID uuid.UUID, priority int, scheduledAt time.Time) error
	Dequeue(ctx context.Context, workerID string, block time.Duration) (*Claim, error)
	Release(ctx context.Context, taskID uuid.UUID, workerID string) error
	Complete(ctx context.Context, taskID uuid.UUID, workerID string) error
	ExtendLock(ctx context.Context, taskID uuid.UUID, workerID string, ttl time.Duration) error
	// PublishTaskCompleted notifies listeners (e.g. scheduler) that a task completed, for DAG progression.
	PublishTaskCompleted(ctx context.Context, taskID uuid.UUID) error
}

type Claim struct {
	TaskID   uuid.UUID
	WorkerID string
	LockKey  string
}
