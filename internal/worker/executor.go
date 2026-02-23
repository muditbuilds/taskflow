package worker

import (
	"context"

	"github.com/muditbuilds/taskflow/internal/models"
)

// TaskHandler executes a task by type. Returns nil on success, error to trigger retry or DLQ.
type TaskHandler func(ctx context.Context, task *models.Task) error

// Registry maps task type name to handler.
type Registry struct {
	handlers map[string]TaskHandler
}

func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]TaskHandler)}
}

func (r *Registry) Register(taskType string, h TaskHandler) {
	r.handlers[taskType] = h
}

func (r *Registry) Get(taskType string) (TaskHandler, bool) {
	h, ok := r.handlers[taskType]
	return h, ok
}
