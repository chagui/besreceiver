// Package tasklib provides the core Task model and validation for TaskForge.
package tasklib

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	PriorityLow      = 1
	PriorityMedium   = 2
	PriorityHigh     = 3
	PriorityCritical = 4
)

const (
	StatusPending   = "pending"
	StatusRunning   = "running"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
)

// Task represents a unit of work in the TaskForge platform.
type Task struct {
	ID        string            `json:"id"`
	Name      string            `json:"name"`
	Priority  int               `json:"priority"`
	Status    string            `json:"status"`
	CreatedAt time.Time         `json:"created_at"`
	UpdatedAt time.Time         `json:"updated_at"`
	Labels    map[string]string `json:"labels,omitempty"`
}

// New creates a pending Task with the given name and priority.
func New(name string, priority int) *Task {
	now := time.Now()
	return &Task{
		ID:        generateID(name, now),
		Name:      name,
		Priority:  priority,
		Status:    StatusPending,
		CreatedAt: now,
		UpdatedAt: now,
		Labels:    make(map[string]string),
	}
}

// Validate checks that the task fields are well-formed.
func (t *Task) Validate() error {
	if strings.TrimSpace(t.Name) == "" {
		return errors.New("task name must not be empty")
	}
	if t.Priority < PriorityLow || t.Priority > PriorityCritical {
		return fmt.Errorf("priority %d out of range [%d, %d]", t.Priority, PriorityLow, PriorityCritical)
	}
	switch t.Status {
	case StatusPending, StatusRunning, StatusCompleted, StatusFailed:
	default:
		return fmt.Errorf("invalid status: %q", t.Status)
	}
	return nil
}

// MarkRunning transitions the task to running status.
func (t *Task) MarkRunning() error {
	if t.Status != StatusPending {
		return fmt.Errorf("cannot start task in status %q", t.Status)
	}
	t.Status = StatusRunning
	t.UpdatedAt = time.Now()
	return nil
}

// Complete transitions the task to completed status.
func (t *Task) Complete() error {
	if t.Status != StatusRunning {
		return fmt.Errorf("cannot complete task in status %q", t.Status)
	}
	t.Status = StatusCompleted
	t.UpdatedAt = time.Now()
	return nil
}

// Fail transitions the task to failed status.
func (t *Task) Fail() error {
	if t.Status != StatusRunning {
		return fmt.Errorf("cannot fail task in status %q", t.Status)
	}
	t.Status = StatusFailed
	t.UpdatedAt = time.Now()
	return nil
}

// PriorityString returns a human-readable priority label.
func PriorityString(p int) string {
	switch p {
	case PriorityLow:
		return "LOW"
	case PriorityMedium:
		return "MEDIUM"
	case PriorityHigh:
		return "HIGH"
	case PriorityCritical:
		return "CRITICAL"
	default:
		return "UNKNOWN"
	}
}

func generateID(name string, t time.Time) string {
	r := strings.NewReplacer(" ", "-", "/", "-", "\\", "-")
	return fmt.Sprintf("task-%s-%d", strings.ToLower(r.Replace(name)), t.UnixNano()%100000)
}
