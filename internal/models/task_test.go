package models

import (
	"testing"
	"time"
)

func TestTask_CanRetry(t *testing.T) {
	task := &Task{RetryCount: 0, MaxRetries: 3}
	if !task.CanRetry() {
		t.Error("expected CanRetry true when RetryCount < MaxRetries")
	}
	task.RetryCount = 3
	if task.CanRetry() {
		t.Error("expected CanRetry false when RetryCount >= MaxRetries")
	}
}

func TestTask_NextRetryDelay(t *testing.T) {
	task := &Task{RetryCount: 0, MaxRetries: 3}
	if d := task.NextRetryDelay(); d != time.Second {
		t.Errorf("expected 1s for retry 0, got %v", d)
	}
	task.RetryCount = 1
	if d := task.NextRetryDelay(); d != 2*time.Second {
		t.Errorf("expected 2s for retry 1, got %v", d)
	}
	task.RetryCount = 2
	if d := task.NextRetryDelay(); d != 4*time.Second {
		t.Errorf("expected 4s for retry 2, got %v", d)
	}
	task.RetryCount = 10
	if d := task.NextRetryDelay(); d != 300*time.Second {
		t.Errorf("expected 5min cap, got %v", d)
	}
}
