package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/muditbuilds/taskflow/internal/models"
)

type PGStore struct {
	pool *pgxpool.Pool
}

func NewPGStore(ctx context.Context, dsn string) (*PGStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, err
	}
	return &PGStore{pool: pool}, nil
}

func (s *PGStore) CreateTask(ctx context.Context, t *models.Task) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO tasks (id, type, payload, status, priority, scheduled_at, retry_count, max_retries, dag_run_id, dag_step_id, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW(), NOW())`,
		t.ID, t.Type, t.Payload, t.Status, t.Priority, t.ScheduledAt, t.RetryCount, t.MaxRetries, t.DagRunID, t.DagStepID,
	)
	return err
}

func (s *PGStore) GetTask(ctx context.Context, id uuid.UUID) (*models.Task, error) {
	var t models.Task
	var payload []byte
	err := s.pool.QueryRow(ctx, `
		SELECT id, type, payload, status, priority, scheduled_at, retry_count, max_retries, run_at, completed_at, error_message, dag_run_id, dag_step_id, created_at, updated_at
		FROM tasks WHERE id = $1`,
		id,
	).Scan(
		&t.ID, &t.Type, &payload, &t.Status, &t.Priority, &t.ScheduledAt, &t.RetryCount, &t.MaxRetries,
		&t.RunAt, &t.CompletedAt, &t.ErrorMessage, &t.DagRunID, &t.DagStepID, &t.CreatedAt, &t.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	t.Payload = payload
	return &t, nil
}

func (s *PGStore) UpdateTaskStatus(ctx context.Context, id uuid.UUID, status models.TaskStatus, runAt, completedAt *time.Time, errorMsg string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE tasks SET status = $1, run_at = COALESCE($2, run_at), completed_at = COALESCE($3, completed_at), error_message = $4, updated_at = NOW() WHERE id = $5`,
		status, runAt, completedAt, errorMsg, id,
	)
	return err
}

func (s *PGStore) IncrementRetry(ctx context.Context, id uuid.UUID, nextRunAt time.Time) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE tasks SET retry_count = retry_count + 1, scheduled_at = $1, status = 'pending', updated_at = NOW() WHERE id = $2`,
		nextRunAt, id,
	)
	return err
}

func (s *PGStore) MoveToDLQ(ctx context.Context, id uuid.UUID, errorMsg string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE tasks SET status = 'dlq', error_message = $1, updated_at = NOW() WHERE id = $2`,
		errorMsg, id,
	)
	return err
}

func (s *PGStore) ListPendingScheduled(ctx context.Context, limit int, maxScheduled time.Time) ([]*models.Task, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, type, payload, status, priority, scheduled_at, retry_count, max_retries, run_at, completed_at, error_message, dag_run_id, dag_step_id, created_at, updated_at
		FROM tasks WHERE status = 'pending' AND scheduled_at <= $1 ORDER BY priority DESC, scheduled_at ASC LIMIT $2`,
		maxScheduled, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*models.Task
	for rows.Next() {
		var t models.Task
		var payload []byte
		if err := rows.Scan(
			&t.ID, &t.Type, &payload, &t.Status, &t.Priority, &t.ScheduledAt, &t.RetryCount, &t.MaxRetries,
			&t.RunAt, &t.CompletedAt, &t.ErrorMessage, &t.DagRunID, &t.DagStepID, &t.CreatedAt, &t.UpdatedAt,
		); err != nil {
			return nil, err
		}
		t.Payload = payload
		out = append(out, &t)
	}
	return out, rows.Err()
}

func (s *PGStore) ListByDagRun(ctx context.Context, dagRunID uuid.UUID) ([]*models.Task, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, type, payload, status, priority, scheduled_at, retry_count, max_retries, run_at, completed_at, error_message, dag_run_id, dag_step_id, created_at, updated_at
		FROM tasks WHERE dag_run_id = $1 ORDER BY dag_step_id`,
		dagRunID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*models.Task
	for rows.Next() {
		var t models.Task
		var payload []byte
		if err := rows.Scan(
			&t.ID, &t.Type, &payload, &t.Status, &t.Priority, &t.ScheduledAt, &t.RetryCount, &t.MaxRetries,
			&t.RunAt, &t.CompletedAt, &t.ErrorMessage, &t.DagRunID, &t.DagStepID, &t.CreatedAt, &t.UpdatedAt,
		); err != nil {
			return nil, err
		}
		t.Payload = payload
		out = append(out, &t)
	}
	return out, rows.Err()
}

func (s *PGStore) MarkTaskQueued(ctx context.Context, id uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `UPDATE tasks SET status = 'queued', updated_at = NOW() WHERE id = $1`, id)
	return err
}

func (s *PGStore) CreateDagRun(ctx context.Context, definition []byte) (uuid.UUID, error) {
	var id uuid.UUID
	err := s.pool.QueryRow(ctx, `
		INSERT INTO dag_runs (definition, status) VALUES ($1, 'pending') RETURNING id`,
		definition,
	).Scan(&id)
	return id, err
}

func (s *PGStore) GetDagRun(ctx context.Context, id uuid.UUID) (*models.DagRun, error) {
	var r models.DagRun
	var def []byte
	err := s.pool.QueryRow(ctx, `
		SELECT id, definition, status, created_at, updated_at FROM dag_runs WHERE id = $1`, id,
	).Scan(&r.ID, &def, &r.Status, &r.CreatedAt, &r.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	r.Definition = json.RawMessage(def)
	return &r, nil
}

func (s *PGStore) UpdateDagRunStatus(ctx context.Context, id uuid.UUID, status models.DagRunStatus) error {
	_, err := s.pool.Exec(ctx, `UPDATE dag_runs SET status = $1, updated_at = NOW() WHERE id = $2`, status, id)
	return err
}

func (s *PGStore) Close() {
	s.pool.Close()
}

// Migrate runs the given SQL migration (e.g. from migrations/001_schema.up.sql).
func (s *PGStore) Migrate(ctx context.Context, sql []byte) error {
	return Migrate(ctx, s.pool, sql)
}
