package models

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusQueued    TaskStatus = "queued"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusCompleted TaskStatus = "completed"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusDLQ       TaskStatus = "dlq"
	TaskStatusCancelled TaskStatus = "cancelled"
)

type Task struct {
	ID           uuid.UUID       `json:"id"`
	Type         string          `json:"type"`
	Payload      json.RawMessage `json:"payload"`
	Status       TaskStatus      `json:"status"`
	Priority     int             `json:"priority"`     // higher = more urgent
	ScheduledAt  time.Time       `json:"scheduled_at"`
	RetryCount   int             `json:"retry_count"`
	MaxRetries   int             `json:"max_retries"`
	RunAt        *time.Time      `json:"run_at,omitempty"`
	CompletedAt  *time.Time      `json:"completed_at,omitempty"`
	ErrorMessage string          `json:"error_message,omitempty"`
	DagRunID     *uuid.UUID      `json:"dag_run_id,omitempty"`
	DagStepID    string          `json:"dag_step_id,omitempty"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`
}

func (t *Task) CanRetry() bool {
	return t.RetryCount < t.MaxRetries
}

func (t *Task) NextRetryDelay() time.Duration {
	// Exponential backoff: 2^retry_count seconds, cap at 5 min
	sec := 1 << uint(t.RetryCount)
	if sec > 300 {
		sec = 300
	}
	return time.Duration(sec) * time.Second
}
