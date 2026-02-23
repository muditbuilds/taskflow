package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/muditbuilds/taskflow/internal/models"
	"github.com/muditbuilds/taskflow/internal/store"
)

type CreateDagTasksFunc func(ctx context.Context, dagRunID uuid.UUID, def models.DagDefinition) error

type Handlers struct {
	TaskStore           store.TaskStore
	DagStore            store.DagStore
	CreateDagTasksAndEnqueue CreateDagTasksFunc
}

type EnqueueTaskRequest struct {
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Priority    int             `json:"priority,omitempty"`
	ScheduledAt *time.Time      `json:"scheduled_at,omitempty"`
	MaxRetries  int             `json:"max_retries,omitempty"`
}

type EnqueueTaskResponse struct {
	TaskID string `json:"task_id"`
}

type SubmitDagRequest struct {
	Steps []models.DagStep `json:"steps"`
	Edges []models.DagEdge `json:"edges"`
}

type SubmitDagResponse struct {
	DagRunID string `json:"dag_run_id"`
}

type TaskResponse struct {
	ID           string          `json:"id"`
	Type         string          `json:"type"`
	Payload      json.RawMessage `json:"payload"`
	Status       string          `json:"status"`
	Priority     int             `json:"priority"`
	ScheduledAt  time.Time       `json:"scheduled_at"`
	RetryCount   int             `json:"retry_count"`
	MaxRetries   int             `json:"max_retries"`
	ErrorMessage string          `json:"error_message,omitempty"`
	CreatedAt    time.Time       `json:"created_at"`
}

func (h *Handlers) EnqueueTask(w http.ResponseWriter, r *http.Request) {
	var req EnqueueTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}
	if req.Type == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "type is required"})
		return
	}
	scheduledAt := time.Now()
	if req.ScheduledAt != nil {
		scheduledAt = *req.ScheduledAt
	}
	maxRetries := 3
	if req.MaxRetries > 0 {
		maxRetries = req.MaxRetries
	}
	t := &models.Task{
		ID:          uuid.New(),
		Type:        req.Type,
		Payload:     req.Payload,
		Status:      models.TaskStatusPending,
		Priority:    req.Priority,
		ScheduledAt: scheduledAt,
		MaxRetries:  maxRetries,
	}
	if err := h.TaskStore.CreateTask(r.Context(), t); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusAccepted, EnqueueTaskResponse{TaskID: t.ID.String()})
}

func (h *Handlers) SubmitDag(w http.ResponseWriter, r *http.Request) {
	var req SubmitDagRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}
	def := models.DagDefinition{Steps: req.Steps, Edges: req.Edges}
	if _, err := models.ParseDag(def); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid DAG: " + err.Error()})
		return
	}
	defBytes, _ := json.Marshal(def)
	dagRunID, err := h.DagStore.CreateDagRun(r.Context(), defBytes)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if h.CreateDagTasksAndEnqueue != nil {
		if err := h.CreateDagTasksAndEnqueue(r.Context(), dagRunID, def); err != nil {
			_ = h.DagStore.UpdateDagRunStatus(r.Context(), dagRunID, models.DagRunStatusFailed)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
	}
	writeJSON(w, http.StatusAccepted, SubmitDagResponse{DagRunID: dagRunID.String()})
}

func (h *Handlers) GetTask(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid task id"})
		return
	}
	t, err := h.TaskStore.GetTask(r.Context(), id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "task not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, taskToResponse(t))
}

func (h *Handlers) Health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func taskToResponse(t *models.Task) TaskResponse {
	resp := TaskResponse{
		ID:          t.ID.String(),
		Type:        t.Type,
		Payload:     t.Payload,
		Status:       string(t.Status),
		Priority:    t.Priority,
		ScheduledAt: t.ScheduledAt,
		RetryCount:  t.RetryCount,
		MaxRetries:  t.MaxRetries,
		ErrorMessage: t.ErrorMessage,
		CreatedAt:   t.CreatedAt,
	}
	return resp
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
