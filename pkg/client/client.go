// Package client provides a Go SDK for the Taskflow REST API.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
)

const defaultBaseURL = "http://localhost:8080"

// Client talks to the Taskflow scheduler API.
type Client struct {
	BaseURL    string
	HTTPClient *http.Client
}

// New returns a client with default base URL and timeout.
func New(baseURL string) *Client {
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	return &Client{
		BaseURL: baseURL,
		HTTPClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// EnqueueTaskRequest is the payload for enqueueing a single task.
type EnqueueTaskRequest struct {
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Priority    int             `json:"priority,omitempty"`
	ScheduledAt *time.Time      `json:"scheduled_at,omitempty"`
	MaxRetries  int             `json:"max_retries,omitempty"`
}

// EnqueueTask enqueues a task and returns its ID.
func (c *Client) EnqueueTask(ctx context.Context, req EnqueueTaskRequest) (uuid.UUID, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return uuid.Nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/api/v1/tasks", bytes.NewReader(body))
	if err != nil {
		return uuid.Nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTPClient.Do(httpReq)
	if err != nil {
		return uuid.Nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		var errBody struct{ Error string }
		_ = json.NewDecoder(resp.Body).Decode(&errBody)
		return uuid.Nil, fmt.Errorf("enqueue task: %s (%s)", resp.Status, errBody.Error)
	}
	var out struct {
		TaskID string `json:"task_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return uuid.Nil, err
	}
	return uuid.Parse(out.TaskID)
}

// DagStep is one node in a DAG.
type DagStep struct {
	ID       string          `json:"id"`
	Type     string          `json:"type"`
	Payload  json.RawMessage `json:"payload"`
	Priority int             `json:"priority,omitempty"`
}

// DagEdge is a dependency edge (From must complete before To runs).
type DagEdge struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// SubmitDagRequest is the payload for submitting a DAG.
type SubmitDagRequest struct {
	Steps []DagStep `json:"steps"`
	Edges []DagEdge `json:"edges"`
}

// SubmitDag submits a DAG and returns the run ID.
func (c *Client) SubmitDag(ctx context.Context, req SubmitDagRequest) (uuid.UUID, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return uuid.Nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/api/v1/dags", bytes.NewReader(body))
	if err != nil {
		return uuid.Nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTPClient.Do(httpReq)
	if err != nil {
		return uuid.Nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		var errBody struct{ Error string }
		_ = json.NewDecoder(resp.Body).Decode(&errBody)
		return uuid.Nil, fmt.Errorf("submit dag: %s (%s)", resp.Status, errBody.Error)
	}
	var out struct {
		DagRunID string `json:"dag_run_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return uuid.Nil, err
	}
	return uuid.Parse(out.DagRunID)
}

// GetTask returns task status and details.
func (c *Client) GetTask(ctx context.Context, taskID uuid.UUID) (*TaskInfo, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/api/v1/tasks/"+taskID.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.HTTPClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		var errBody struct{ Error string }
		_ = json.NewDecoder(resp.Body).Decode(&errBody)
		return nil, fmt.Errorf("get task: %s (%s)", resp.Status, errBody.Error)
	}
	var t TaskInfo
	if err := json.NewDecoder(resp.Body).Decode(&t); err != nil {
		return nil, err
	}
	return &t, nil
}

// TaskInfo is the API response for a task.
type TaskInfo struct {
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

// ErrNotFound is returned when a task does not exist.
var ErrNotFound = fmt.Errorf("task not found")

// Health checks the scheduler is up.
func (c *Client) Health(ctx context.Context) error {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/health", nil)
	if err != nil {
		return err
	}
	resp, err := c.HTTPClient.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health: %s", resp.Status)
	}
	return nil
}
