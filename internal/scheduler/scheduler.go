package scheduler

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/muditbuilds/taskflow/internal/models"
	"github.com/muditbuilds/taskflow/internal/queue"
	"github.com/muditbuilds/taskflow/internal/store"
	"github.com/redis/go-redis/v9"
)

const (
	enqueueBatchSize = 100
	enqueueInterval  = 50 * time.Millisecond
	dagPollInterval  = 200 * time.Millisecond
)

type Scheduler struct {
	taskStore store.TaskStore
	dagStore  store.DagStore
	q         queue.Queue
	redis     *redis.Client
	log       *slog.Logger
	stop      chan struct{}
	wg        sync.WaitGroup
}

func New(taskStore store.TaskStore, dagStore store.DagStore, q queue.Queue, log *slog.Logger) *Scheduler {
	return NewWithRedis(taskStore, dagStore, q, nil, log)
}

func NewWithRedis(taskStore store.TaskStore, dagStore store.DagStore, q queue.Queue, rdb *redis.Client, log *slog.Logger) *Scheduler {
	if log == nil {
		log = slog.Default()
	}
	return &Scheduler{
		taskStore: taskStore,
		dagStore:  dagStore,
		q:         q,
		redis:     rdb,
		log:       log,
		stop:      make(chan struct{}),
	}
}

func (s *Scheduler) Start(ctx context.Context) {
	s.wg.Add(2)
	go s.enqueueLoop(ctx)
	go s.dagProgressLoop(ctx)
	if s.redis != nil {
		s.wg.Add(1)
		go s.completionSubLoop(ctx)
	}
	s.log.Info("scheduler started")
}

func (s *Scheduler) completionSubLoop(ctx context.Context) {
	defer s.wg.Done()
	pubsub := s.redis.Subscribe(ctx, queue.ChannelCompleted)
	defer pubsub.Close()
	ch := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stop:
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			taskID, err := uuid.Parse(msg.Payload)
			if err != nil {
				s.log.Warn("invalid task id in completion channel", "payload", msg.Payload)
				continue
			}
			if err := s.NotifyTaskCompleted(ctx, taskID); err != nil {
				s.log.Error("notify task completed", "task_id", taskID, "error", err)
			}
		}
	}
}

func (s *Scheduler) Stop() {
	close(s.stop)
	s.wg.Wait()
	s.log.Info("scheduler stopped")
}

func (s *Scheduler) enqueueLoop(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(enqueueInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stop:
			return
		case <-ticker.C:
			if err := s.enqueuePending(ctx); err != nil {
				s.log.Error("enqueue pending", "error", err)
			}
		}
	}
}

func (s *Scheduler) enqueuePending(ctx context.Context) error {
	now := time.Now()
	tasks, err := s.taskStore.ListPendingScheduled(ctx, enqueueBatchSize, now)
	if err != nil {
		return err
	}
	for _, t := range tasks {
		if err := s.taskStore.MarkTaskQueued(ctx, t.ID); err != nil {
			s.log.Warn("mark queued failed", "task_id", t.ID, "error", err)
			continue
		}
		if err := s.q.Enqueue(ctx, t.ID, t.Priority, t.ScheduledAt); err != nil {
			s.log.Warn("enqueue failed", "task_id", t.ID, "error", err)
			_ = s.taskStore.UpdateTaskStatus(ctx, t.ID, models.TaskStatusPending, nil, nil, "")
			continue
		}
		s.log.Debug("enqueued", "task_id", t.ID, "priority", t.Priority)
	}
	return nil
}

func (s *Scheduler) dagProgressLoop(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(dagPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stop:
			return
		case <-ticker.C:
			if err := s.progressDagRuns(ctx); err != nil {
				s.log.Error("dag progress", "error", err)
			}
		}
	}
}

func (s *Scheduler) progressDagRuns(ctx context.Context) error {
	// Get running DAG runs and check for completed steps; enqueue newly ready steps.
	// We don't have a direct "list running dag runs" in store; we could add it.
	// For now we drive DAG progression from task completion in the worker (worker notifies or we poll tasks by dag_run_id).
	// Simpler: when a task completes, worker calls NotifyTaskCompleted(taskID). Scheduler then loads dag run, checks if step completed, and enqueues next steps.
	// So we need NotifyTaskCompleted on scheduler. That way we don't need to poll all dag runs.
	return nil
}

// NotifyTaskCompleted is called when a worker completes a task. If the task is part of a DAG, enqueues next ready steps.
func (s *Scheduler) NotifyTaskCompleted(ctx context.Context, taskID uuid.UUID) error {
	t, err := s.taskStore.GetTask(ctx, taskID)
	if err != nil {
		return err
	}
	if t.DagRunID == nil {
		return nil
	}
	dagRun, err := s.dagStore.GetDagRun(ctx, *t.DagRunID)
	if err != nil {
		return err
	}
	if dagRun.Status != models.DagRunStatusRunning {
		return nil
	}
	var def models.DagDefinition
	if err := json.Unmarshal(dagRun.Definition, &def); err != nil {
		return err
	}
	parsed, err := models.ParseDag(def)
	if err != nil {
		return err
	}
	tasks, err := s.taskStore.ListByDagRun(ctx, *t.DagRunID)
	if err != nil {
		return err
	}
	var completed []string
	for _, tt := range tasks {
		if tt.Status == models.TaskStatusCompleted && tt.DagStepID != "" {
			completed = append(completed, tt.DagStepID)
		}
	}
	ready := parsed.ReadyToRun(completed)
	for _, stepID := range ready {
		// Find task for this step that is still pending
		for _, tt := range tasks {
			if tt.DagStepID == stepID && tt.Status == models.TaskStatusPending {
				if err := s.taskStore.MarkTaskQueued(ctx, tt.ID); err != nil {
					continue
				}
				if err := s.q.Enqueue(ctx, tt.ID, tt.Priority, tt.ScheduledAt); err != nil {
					_ = s.taskStore.UpdateTaskStatus(ctx, tt.ID, models.TaskStatusPending, nil, nil, "")
					continue
				}
				s.log.Debug("dag enqueued step", "dag_run_id", t.DagRunID, "step_id", stepID)
				break
			}
		}
	}
	// Check if DAG is fully completed
	allDone := true
	for _, tt := range tasks {
		if tt.Status != models.TaskStatusCompleted && tt.Status != models.TaskStatusDLQ && tt.Status != models.TaskStatusCancelled {
			allDone = false
			break
		}
	}
	if allDone {
		_ = s.dagStore.UpdateDagRunStatus(ctx, *t.DagRunID, models.DagRunStatusCompleted)
		s.log.Info("dag run completed", "dag_run_id", t.DagRunID)
	}
	return nil
}

// NotifyTaskFailed updates DAG run status if needed (e.g. fail whole DAG on critical step).
func (s *Scheduler) NotifyTaskFailed(ctx context.Context, taskID uuid.UUID, toDLQ bool) error {
	t, err := s.taskStore.GetTask(ctx, taskID)
	if err != nil {
		return err
	}
	if t.DagRunID == nil {
		return nil
	}
	if toDLQ {
		_ = s.dagStore.UpdateDagRunStatus(ctx, *t.DagRunID, models.DagRunStatusFailed)
	}
	return nil
}
