package store

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/muditbuilds/taskflow/internal/models"
)

var ErrNotFound = errors.New("not found")

type TaskStore interface {
	CreateTask(ctx context.Context, t *models.Task) error
	GetTask(ctx context.Context, id uuid.UUID) (*models.Task, error)
	UpdateTaskStatus(ctx context.Context, id uuid.UUID, status models.TaskStatus, runAt, completedAt *time.Time, errorMsg string) error
	IncrementRetry(ctx context.Context, id uuid.UUID, nextRunAt time.Time) error
	MoveToDLQ(ctx context.Context, id uuid.UUID, errorMsg string) error
	ListPendingScheduled(ctx context.Context, limit int, maxScheduled time.Time) ([]*models.Task, error)
	ListByDagRun(ctx context.Context, dagRunID uuid.UUID) ([]*models.Task, error)
	MarkTaskQueued(ctx context.Context, id uuid.UUID) error
}

type DagStore interface {
	CreateDagRun(ctx context.Context, definition []byte) (uuid.UUID, error)
	GetDagRun(ctx context.Context, id uuid.UUID) (*models.DagRun, error)
	UpdateDagRunStatus(ctx context.Context, id uuid.UUID, status models.DagRunStatus) error
}
