package scheduler

import (
	"context"

	"github.com/google/uuid"
	"github.com/muditbuilds/taskflow/internal/models"
	"github.com/muditbuilds/taskflow/internal/queue"
	"github.com/muditbuilds/taskflow/internal/store"
)

// CreateDagTasksAndEnqueueRoots creates task rows for every step of the DAG, marks run as running, and enqueues root steps.
func CreateDagTasksAndEnqueueRoots(ctx context.Context, dagRunID uuid.UUID, def models.DagDefinition, taskStore store.TaskStore, dagStore store.DagStore, q queue.Queue) error {
	parsed, err := models.ParseDag(def)
	if err != nil {
		return err
	}
	dagRun, err := dagStore.GetDagRun(ctx, dagRunID)
	if err != nil {
		return err
	}
	scheduledAt := dagRun.CreatedAt

	for _, step := range parsed.Steps {
		t := &models.Task{
			ID:          uuid.New(),
			Type:        step.Type,
			Payload:     step.Payload,
			Status:      models.TaskStatusPending,
			Priority:    step.Priority,
			ScheduledAt: scheduledAt,
			MaxRetries:  3,
			DagRunID:    &dagRunID,
			DagStepID:   step.ID,
		}
		if err := taskStore.CreateTask(ctx, t); err != nil {
			return err
		}
		if _, isRoot := parsed.Ready[step.ID]; isRoot {
			_ = taskStore.MarkTaskQueued(ctx, t.ID)
			_ = q.Enqueue(ctx, t.ID, t.Priority, t.ScheduledAt)
		}
	}
	return dagStore.UpdateDagRunStatus(ctx, dagRunID, models.DagRunStatusRunning)
}
