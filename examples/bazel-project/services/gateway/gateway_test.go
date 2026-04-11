package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"taskforge/libs/tasklib"
)

func TestHealthEndpoint(t *testing.T) {
	s := &server{tasks: make(map[string]*tasklib.Task)}
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	s.handleHealth(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestCreateAndList(t *testing.T) {
	s := &server{tasks: make(map[string]*tasklib.Task)}

	body := bytes.NewBufferString(`{"name":"test task","priority":2}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/create", body)
	w := httptest.NewRecorder()
	s.handleCreate(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("create: expected 201, got %d: %s", w.Code, w.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/tasks", nil)
	w = httptest.NewRecorder()
	s.handleTasks(w, req)

	var tasks []tasklib.Task
	if err := json.NewDecoder(w.Body).Decode(&tasks); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(tasks) != 1 {
		t.Errorf("expected 1 task, got %d", len(tasks))
	}
}

func TestCreateInvalidPriority(t *testing.T) {
	s := &server{tasks: make(map[string]*tasklib.Task)}
	body := bytes.NewBufferString(`{"name":"bad","priority":99}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/create", body)
	w := httptest.NewRecorder()
	s.handleCreate(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}
