package durex

import (
	"context"
	"embed"
	"encoding/json"
	"net/http"
	"strconv"
	"time"
)

//go:embed dashboard.html
var dashboardFS embed.FS

// DashboardHandler returns an http.Handler that serves the Durex dashboard.
// Mount this at your desired path to enable the web dashboard.
//
// Example:
//
//	http.Handle("/durex/", http.StripPrefix("/durex", executor.DashboardHandler()))
//	http.ListenAndServe(":8080", nil)
func (e *Executor) DashboardHandler() http.Handler {
	mux := http.NewServeMux()

	// Serve the dashboard HTML
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" && r.URL.Path != "" {
			http.NotFound(w, r)
			return
		}

		data, err := dashboardFS.ReadFile("dashboard.html")
		if err != nil {
			http.Error(w, "Dashboard not found", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(data)
	})

	// API endpoints
	mux.HandleFunc("/api/stats", e.handleAPIStats)
	mux.HandleFunc("/api/commands", e.handleAPICommands)
	mux.HandleFunc("/api/health", e.handleAPIHealth)
	mux.HandleFunc("/api/commands/retry", e.handleAPIRetry)
	mux.HandleFunc("/api/commands/cancel", e.handleAPICancel)
	mux.HandleFunc("/api/commands/history", e.handleAPIHistory)

	return mux
}

// ServeDashboard starts an HTTP server serving the dashboard on the given address.
// This is a convenience method for simple deployments.
// For production, use DashboardHandler() and integrate with your existing server.
//
// Example:
//
//	go executor.ServeDashboard(":8080")
func (e *Executor) ServeDashboard(addr string) error {
	server := &http.Server{
		Addr:         addr,
		Handler:      e.DashboardHandler(),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	e.logger.Info("durex: starting dashboard", "addr", addr)
	return server.ListenAndServe()
}

// handleAPIStats returns executor statistics as JSON.
func (e *Executor) handleAPIStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stats, err := e.Stats(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Convert to JSON-friendly format
	response := statsResponse{
		Pending:            stats.Pending,
		Completed:          stats.Completed,
		Failed:             stats.Failed,
		DeadLetter:         stats.DeadLetter,
		Repeating:          stats.Repeating,
		QueueSize:          stats.QueueSize,
		RegisteredCommands: stats.RegisteredCommands,
		WorkerCount:        stats.WorkerCount,
	}

	if stats.RateLimit != nil {
		response.RateLimit = &rateLimitResponse{
			GlobalLimit:   stats.RateLimit.GlobalLimit,
			GlobalCurrent: stats.RateLimit.GlobalCurrent,
			Commands:      make(map[string]commandRateLimitResponse),
		}
		for name, cmd := range stats.RateLimit.Commands {
			response.RateLimit.Commands[name] = commandRateLimitResponse(cmd)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

// handleAPICommands returns recent commands as JSON.
func (e *Executor) handleAPICommands(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	limit := 20
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 && parsed <= 100 {
			limit = parsed
		}
	}

	status := r.URL.Query().Get("status")

	var commands []*Instance
	var err error

	// Try to use QueryableStorage for better queries
	if qs, ok := e.storage.(QueryableStorage); ok {
		query := Query{
			Limit:     limit,
			OrderBy:   "created_at",
			OrderDesc: true,
		}
		if status != "" {
			s := Status(status)
			query.Status = &s
		}
		commands, err = qs.Find(r.Context(), query)
	} else {
		// Fall back to FindPending
		commands, err = e.storage.FindPending(r.Context())
		if len(commands) > limit {
			commands = commands[:limit]
		}
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Convert to JSON-friendly format
	response := commandsResponse{
		Commands: make([]commandResponse, len(commands)),
	}

	for i, cmd := range commands {
		response.Commands[i] = commandResponse{
			ID:            cmd.ID,
			Name:          cmd.Name,
			Status:        string(cmd.Status),
			Attempt:       cmd.Attempt,
			Retries:       cmd.Retries,
			Priority:      cmd.Priority,
			CreatedAt:     cmd.CreatedAt,
			ReadyAt:       cmd.ReadyAt,
			StartedAt:     cmd.StartedAt,
			CompletedAt:   cmd.CompletedAt,
			DeadlineAt:    cmd.DeadlineAt,
			ParentID:      cmd.ParentID,
			TraceID:       cmd.TraceID,
			CorrelationID: cmd.CorrelationID,
			Error:         cmd.Error,
			Tags:          cmd.Tags,
			Cron:          cmd.Cron,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

// handleAPIHealth returns executor health status for load balancers.
func (e *Executor) handleAPIHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	health := healthResponse{
		Status:      "healthy",
		Started:     e.started.Load(),
		WorkerCount: e.parallelism,
		Timestamp:   time.Now(),
	}

	// Check if executor is running
	if !e.started.Load() {
		health.Status = "unhealthy"
		health.Message = "executor not started"
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(health)
		return
	}

	// Check if stopping
	if e.stopping.Load() {
		health.Status = "degraded"
		health.Message = "executor is shutting down"
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(health)
		return
	}

	// Check storage connectivity
	_, err := e.storage.Count(ctx, nil)
	if err != nil {
		health.Status = "unhealthy"
		health.StorageOK = false
		health.Message = "storage error: " + err.Error()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(health)
		return
	}
	health.StorageOK = true

	// Get queue depth
	health.QueueDepth = len(e.queue)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(health)
}

// handleAPIRetry retries a failed or dead-lettered command.
func (e *Executor) handleAPIRetry(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	id := r.URL.Query().Get("id")
	if id == "" {
		http.Error(w, "Missing 'id' parameter", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	// Get the command
	instance, err := e.storage.Get(ctx, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	// Check if it can be retried
	if instance.Status != StatusFailed && instance.Status != StatusDeadLetter {
		http.Error(w, "Command cannot be retried (status: "+string(instance.Status)+")", http.StatusBadRequest)
		return
	}

	// Reset for retry
	instance.Status = StatusPending
	instance.Error = ""
	instance.StartedAt = nil
	instance.CompletedAt = nil
	instance.Attempt = 0
	instance.ReadyAt = time.Now()

	if err := e.storage.Update(ctx, instance); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Schedule for execution
	e.schedule(instance)

	e.logger.Info("durex: command retried via dashboard",
		"id", instance.ID,
		"name", instance.Name,
	)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":  "ok",
		"message": "Command scheduled for retry",
		"id":      id,
	})
}

// handleAPICancel cancels a pending or started command.
func (e *Executor) handleAPICancel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	id := r.URL.Query().Get("id")
	if id == "" {
		http.Error(w, "Missing 'id' parameter", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	err := e.Cancel(ctx, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	e.logger.Info("durex: command cancelled via dashboard", "id", id)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":  "ok",
		"message": "Command cancelled",
		"id":      id,
	})
}

// handleAPIHistory returns the execution history for a command.
func (e *Executor) handleAPIHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	id := r.URL.Query().Get("id")
	if id == "" {
		http.Error(w, "Missing 'id' parameter", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	history, err := e.History(ctx, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id":      id,
		"history": history,
	})
}

// API response types

type healthResponse struct {
	Status      string    `json:"status"` // healthy, degraded, unhealthy
	Started     bool      `json:"started"`
	StorageOK   bool      `json:"storage_ok"`
	WorkerCount int       `json:"worker_count"`
	QueueDepth  int       `json:"queue_depth"`
	Message     string    `json:"message,omitempty"`
	Timestamp   time.Time `json:"timestamp"`
}

type statsResponse struct {
	Pending            int64              `json:"pending"`
	Completed          int64              `json:"completed"`
	Failed             int64              `json:"failed"`
	DeadLetter         int64              `json:"dead_letter"`
	Repeating          int64              `json:"repeating"`
	QueueSize          int                `json:"queue_size"`
	RegisteredCommands int                `json:"registered_commands"`
	WorkerCount        int                `json:"worker_count"`
	RateLimit          *rateLimitResponse `json:"rate_limit,omitempty"`
}

type rateLimitResponse struct {
	GlobalLimit   int                                 `json:"global_limit"`
	GlobalCurrent int                                 `json:"global_current"`
	Commands      map[string]commandRateLimitResponse `json:"commands"`
}

type commandRateLimitResponse struct {
	Limit   int `json:"limit"`
	Current int `json:"current"`
	Waiting int `json:"waiting"`
}

type commandsResponse struct {
	Commands []commandResponse `json:"commands"`
}

type commandResponse struct {
	ID            string     `json:"id"`
	Name          string     `json:"name"`
	Status        string     `json:"status"`
	Attempt       int        `json:"attempt"`
	Retries       int        `json:"retries"`
	Priority      int        `json:"priority"`
	CreatedAt     time.Time  `json:"created_at"`
	ReadyAt       time.Time  `json:"ready_at"`
	StartedAt     *time.Time `json:"started_at,omitempty"`
	CompletedAt   *time.Time `json:"completed_at,omitempty"`
	DeadlineAt    *time.Time `json:"deadline_at,omitempty"`
	ParentID      *string    `json:"parent_id,omitempty"`
	TraceID       string     `json:"trace_id,omitempty"`
	CorrelationID string     `json:"correlation_id,omitempty"`
	Error         string     `json:"error,omitempty"`
	Tags          []string   `json:"tags,omitempty"`
	Cron          string     `json:"cron,omitempty"`
}

// Ensure context is used
var _ = context.Background
