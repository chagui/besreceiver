// Package main implements the TaskForge gateway HTTP server.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"

	"taskforge/libs/tasklib"
)

type server struct {
	mu    sync.Mutex
	tasks map[string]*tasklib.Task
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	s := &server{tasks: make(map[string]*tasklib.Task)}

	http.HandleFunc("/health", s.handleHealth)
	http.HandleFunc("/tasks", s.handleTasks)
	http.HandleFunc("/tasks/create", s.handleCreate)

	log.Printf("TaskForge Gateway listening on :%s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

func (s *server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, `{"status":"ok","service":"gateway"}`)
}

func (s *server) handleTasks(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tasks := make([]*tasklib.Task, 0, len(s.tasks))
	for _, t := range s.tasks {
		tasks = append(tasks, t)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tasks)
}

type createRequest struct {
	Name     string `json:"name"`
	Priority int    `json:"priority"`
}

func (s *server) handleCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req createRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}

	t := tasklib.New(req.Name, req.Priority)
	if err := t.Validate(); err != nil {
		http.Error(w, "validation error: "+err.Error(), http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	s.tasks[t.ID] = t
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(t)
}
