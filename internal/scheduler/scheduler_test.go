package scheduler

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/muditbuilds/taskflow/internal/models"
	"github.com/muditbuilds/taskflow/internal/queue"
	"github.com/muditbuilds/taskflow/internal/store"
)

type mockTaskStore struct {
	tasks    map[uuid.UUID]*models.Task
	byDagRun map[uuid.UUID][]*models.Task
	pending  []*models.Task
}

func (m *mockTaskStore) CreateTask(ctx context.Context, t *models.Task) error {
	if m.tasks == nil {
		m.tasks = make(map[uuid.UUID]*models.Task)
	}
	m.tasks[t.ID] = t
	return nil
}

func (m *mockTaskStore) GetTask(ctx context.Context, id uuid.UUID) (*models.Task, error) {
	if m.tasks != nil {
		if t, ok := m.tasks[id]; ok {
			return t, nil
		}
	}
	return nil, store.ErrNotFound
}

func (m *mockTaskStore) UpdateTaskStatus(ctx context.Context, id uuid.UUID, status models.TaskStatus, runAt, completedAt *time.Time, errorMsg string) error {
	return nil
}

func (m *mockTaskStore) IncrementRetry(ctx context.Context, id uuid.UUID, nextRunAt time.Time) error {
	return nil
}

func (m *mockTaskStore) MoveToDLQ(ctx context.Context, id uuid.UUID, errorMsg string) error {
	return nil
}

func (m *mockTaskStore) ListPendingScheduled(ctx context.Context, limit int, maxScheduled time.Time) ([]*models.Task, error) {
	return m.pending, nil
}

func (m *mockTaskStore) ListByDagRun(ctx context.Context, dagRunID uuid.UUID) ([]*models.Task, error) {
	if m.byDagRun != nil {
		return m.byDagRun[dagRunID], nil
	}
	return nil, nil
}

func (m *mockTaskStore) MarkTaskQueued(ctx context.Context, id uuid.UUID) error {
	return nil
}

type mockDagStore struct {
	runs map[uuid.UUID]*models.DagRun
}

func (m *mockDagStore) CreateDagRun(ctx context.Context, definition []byte) (uuid.UUID, error) {
	return uuid.New(), nil
}

func (m *mockDagStore) GetDagRun(ctx context.Context, id uuid.UUID) (*models.DagRun, error) {
	if m.runs != nil {
		if r, ok := m.runs[id]; ok {
			return r, nil
		}
	}
	return nil, store.ErrNotFound
}

func (m *mockDagStore) UpdateDagRunStatus(ctx context.Context, id uuid.UUID, status models.DagRunStatus) error {
	return nil
}

type mockQueue struct{}

func (m *mockQueue) Enqueue(ctx context.Context, taskID uuid.UUID, priority int, scheduledAt time.Time) error {
	return nil
}

func (m *mockQueue) Dequeue(ctx context.Context, workerID string, block time.Duration) (*queue.Claim, error) {
	return nil, nil
}

func (m *mockQueue) Release(ctx context.Context, taskID uuid.UUID, workerID string) error {
	return nil
}

func (m *mockQueue) Complete(ctx context.Context, taskID uuid.UUID, workerID string) error {
	return nil
}

func (m *mockQueue) ExtendLock(ctx context.Context, taskID uuid.UUID, workerID string, ttl time.Duration) error {
	return nil
}

func (m *mockQueue) PublishTaskCompleted(ctx context.Context, taskID uuid.UUID) error {
	return nil
}

func TestCreateDagTasksAndEnqueueRoots(t *testing.T) {
	def := models.DagDefinition{
		Steps: []models.DagStep{
			{ID: "a", Type: "t1", Payload: json.RawMessage(`{}`)},
			{ID: "b", Type: "t2", Payload: json.RawMessage(`{}`)},
		},
		Edges: []models.DagEdge{{From: "a", To: "b"}},
	}
	dagRunID := uuid.New()
	taskStore := &mockTaskStore{tasks: make(map[uuid.UUID]*models.Task)}
	dagStore := &mockDagStore{
		runs: map[uuid.UUID]*models.DagRun{
			dagRunID: {ID: dagRunID, Definition: mustMarshal(def), Status: models.DagRunStatusPending, CreatedAt: time.Now()},
		},
	}
	q := &mockQueue{}

	err := CreateDagTasksAndEnqueueRoots(context.Background(), dagRunID, def, taskStore, dagStore, q)
	if err != nil {
		t.Fatal(err)
	}
	if len(taskStore.tasks) != 2 {
		t.Errorf("expected 2 tasks created, got %d", len(taskStore.tasks))
	}
}

func mustMarshal(v interface{}) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}
