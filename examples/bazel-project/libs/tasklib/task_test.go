package tasklib

import "testing"

func TestNewTask(t *testing.T) {
	task := New("Deploy Service", PriorityHigh)
	if task.Name != "Deploy Service" {
		t.Errorf("expected name 'Deploy Service', got %q", task.Name)
	}
	if task.Priority != PriorityHigh {
		t.Errorf("expected priority %d, got %d", PriorityHigh, task.Priority)
	}
	if task.Status != StatusPending {
		t.Errorf("expected status %q, got %q", StatusPending, task.Status)
	}
	if task.ID == "" {
		t.Error("expected non-empty ID")
	}
}

func TestValidation(t *testing.T) {
	tests := []struct {
		name    string
		task    *Task
		wantErr bool
	}{
		{"valid", New("ok", PriorityLow), false},
		{"empty name", &Task{Name: "", Priority: PriorityLow, Status: StatusPending}, true},
		{"priority too low", &Task{Name: "x", Priority: 0, Status: StatusPending}, true},
		{"priority too high", &Task{Name: "x", Priority: 5, Status: StatusPending}, true},
		{"bad status", &Task{Name: "x", Priority: PriorityLow, Status: "bogus"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.task.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() err = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func TestLifecycle(t *testing.T) {
	task := New("Build Image", PriorityMedium)
	if err := task.MarkRunning(); err != nil {
		t.Fatalf("MarkRunning: %v", err)
	}
	if task.Status != StatusRunning {
		t.Errorf("expected running, got %q", task.Status)
	}
	if err := task.Complete(); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if task.Status != StatusCompleted {
		t.Errorf("expected completed, got %q", task.Status)
	}
}

func TestFailLifecycle(t *testing.T) {
	task := New("Broken Job", PriorityCritical)
	_ = task.MarkRunning()
	if err := task.Fail(); err != nil {
		t.Fatalf("Fail: %v", err)
	}
	if task.Status != StatusFailed {
		t.Errorf("expected failed, got %q", task.Status)
	}
}

func TestPriorityString(t *testing.T) {
	if s := PriorityString(PriorityCritical); s != "CRITICAL" {
		t.Errorf("expected CRITICAL, got %q", s)
	}
	if s := PriorityString(99); s != "UNKNOWN" {
		t.Errorf("expected UNKNOWN, got %q", s)
	}
}
