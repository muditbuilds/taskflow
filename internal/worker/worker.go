package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/muditbuilds/taskflow/internal/models"
	"github.com/muditbuilds/taskflow/internal/queue"
	"github.com/muditbuilds/taskflow/internal/store"
)

type NotifyCompletedFunc func(ctx context.Context, taskID uuid.UUID) error
type NotifyFailedFunc func(ctx context.Context, taskID uuid.UUID, toDLQ bool) error

type Worker struct {
	workerID   string
	taskStore  store.TaskStore
	q          queue.Queue
	registry   *Registry
	lockTTL    time.Duration
	pollInterval time.Duration
	log        *slog.Logger
	onCompleted NotifyCompletedFunc
	onFailed   NotifyFailedFunc
	stop       chan struct{}
}

type WorkerOption func(*Worker)

func WithLockTTL(d time.Duration) WorkerOption {
	return func(w *Worker) { w.lockTTL = d }
}

func WithPollInterval(d time.Duration) WorkerOption {
	return func(w *Worker) { w.pollInterval = d }
}

func WithLogger(log *slog.Logger) WorkerOption {
	return func(w *Worker) { w.log = log }
}

func WithOnCompleted(f NotifyCompletedFunc) WorkerOption {
	return func(w *Worker) { w.onCompleted = f }
}

func WithOnFailed(f NotifyFailedFunc) WorkerOption {
	return func(w *Worker) { w.onFailed = f }
}

func New(workerID string, taskStore store.TaskStore, q queue.Queue, registry *Registry, opts ...WorkerOption) *Worker {
	w := &Worker{
		workerID:     workerID,
		taskStore:    taskStore,
		q:            q,
		registry:     registry,
		lockTTL:      60 * time.Second,
		pollInterval: 100 * time.Millisecond,
		log:          slog.Default(),
		stop:         make(chan struct{}),
	}
	for _, o := range opts {
		o(w)
	}
	return w
}

func (w *Worker) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stop:
			return
		default:
			claim, err := w.q.Dequeue(ctx, w.workerID, 0)
			if err != nil {
				w.log.Error("dequeue", "error", err)
				time.Sleep(w.pollInterval)
				continue
			}
			if claim == nil {
				time.Sleep(w.pollInterval)
				continue
			}
			w.runTask(ctx, claim)
		}
	}
}

func (w *Worker) Stop() {
	close(w.stop)
}

func (w *Worker) runTask(ctx context.Context, claim *queue.Claim) {
	task, err := w.taskStore.GetTask(ctx, claim.TaskID)
	if err != nil {
		w.log.Warn("get task", "task_id", claim.TaskID, "error", err)
		_ = w.q.Release(ctx, claim.TaskID, claim.WorkerID)
		return
	}
	if task.Status != models.TaskStatusQueued && task.Status != models.TaskStatusPending {
		_ = w.q.Complete(ctx, claim.TaskID, claim.WorkerID)
		return
	}

	runAt := time.Now()
	_ = w.taskStore.UpdateTaskStatus(ctx, claim.TaskID, models.TaskStatusRunning, &runAt, nil, "")

	handler, ok := w.registry.Get(task.Type)
	if !ok {
		w.failTask(ctx, claim, task, "unknown task type: "+task.Type, true)
		return
	}

	err = handler(ctx, task)
	if err == nil {
		completedAt := time.Now()
		_ = w.taskStore.UpdateTaskStatus(ctx, claim.TaskID, models.TaskStatusCompleted, nil, &completedAt, "")
		_ = w.q.Complete(ctx, claim.TaskID, claim.WorkerID)
		if w.onCompleted != nil {
			_ = w.onCompleted(ctx, claim.TaskID)
		}
		w.log.Info("task completed", "task_id", claim.TaskID, "type", task.Type)
		return
	}

	_ = w.q.Complete(ctx, claim.TaskID, claim.WorkerID)
	if task.CanRetry() {
		nextRun := time.Now().Add(task.NextRetryDelay())
		_ = w.taskStore.IncrementRetry(ctx, claim.TaskID, nextRun)
		w.log.Info("task retry scheduled", "task_id", claim.TaskID, "retry_count", task.RetryCount+1)
		if w.onFailed != nil {
			_ = w.onFailed(ctx, claim.TaskID, false)
		}
	} else {
		w.failTask(ctx, claim, task, err.Error(), true)
	}
}

func (w *Worker) failTask(ctx context.Context, claim *queue.Claim, task *models.Task, errMsg string, toDLQ bool) {
	_ = w.taskStore.MoveToDLQ(ctx, claim.TaskID, errMsg)
	if w.onFailed != nil {
		_ = w.onFailed(ctx, claim.TaskID, toDLQ)
	}
	w.log.Warn("task failed", "task_id", claim.TaskID, "error", errMsg, "dlq", toDLQ)
}
