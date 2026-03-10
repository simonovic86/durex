package durex_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/simonovic86/durex"
	"github.com/simonovic86/durex/storage"
)

func newTestExecutor(t *testing.T) (*durex.Executor, context.Context) {
	t.Helper()
	store := storage.NewMemory()
	executor := durex.New(store,
		durex.WithParallelism(1),
		durex.WithDeadLetterQueue(),
	)

	executor.HandleFunc("test", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.Empty(), nil
	})
	executor.HandleFunc("fail", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.Empty(), errors.New("failure")
	})

	ctx := context.Background()
	executor.Start(ctx)
	t.Cleanup(func() { executor.Stop() })
	return executor, ctx
}

func TestDashboard_Stats(t *testing.T) {
	executor, ctx := newTestExecutor(t)

	executor.Add(ctx, durex.Spec{Name: "test"})
	time.Sleep(200 * time.Millisecond)

	handler := executor.DashboardHandler()
	req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Status = %d, want 200", w.Code)
	}

	var resp map[string]any
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if _, ok := resp["completed"]; !ok {
		t.Error("Response missing 'completed' field")
	}
	if _, ok := resp["worker_count"]; !ok {
		t.Error("Response missing 'worker_count' field")
	}
}

func TestDashboard_Stats_MethodNotAllowed(t *testing.T) {
	executor, _ := newTestExecutor(t)
	handler := executor.DashboardHandler()

	req := httptest.NewRequest(http.MethodPost, "/api/stats", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Status = %d, want 405", w.Code)
	}
}

func TestDashboard_Commands(t *testing.T) {
	executor, ctx := newTestExecutor(t)

	executor.Add(ctx, durex.Spec{Name: "test"})
	time.Sleep(200 * time.Millisecond)

	handler := executor.DashboardHandler()
	req := httptest.NewRequest(http.MethodGet, "/api/commands", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Status = %d, want 200", w.Code)
	}

	var resp struct {
		Commands []map[string]any `json:"commands"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(resp.Commands) == 0 {
		t.Error("Expected at least 1 command")
	}
}

func TestDashboard_Commands_WithLimit(t *testing.T) {
	executor, ctx := newTestExecutor(t)

	for i := 0; i < 5; i++ {
		executor.Add(ctx, durex.Spec{Name: "test"})
	}
	time.Sleep(300 * time.Millisecond)

	handler := executor.DashboardHandler()
	req := httptest.NewRequest(http.MethodGet, "/api/commands?limit=2", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	var resp struct {
		Commands []map[string]any `json:"commands"`
	}
	json.NewDecoder(w.Body).Decode(&resp)
	if len(resp.Commands) > 2 {
		t.Errorf("Got %d commands, limit was 2", len(resp.Commands))
	}
}

func TestDashboard_Commands_WithStatus(t *testing.T) {
	executor, ctx := newTestExecutor(t)

	executor.Add(ctx, durex.Spec{Name: "test"})
	executor.Add(ctx, durex.Spec{Name: "fail"})
	time.Sleep(300 * time.Millisecond)

	handler := executor.DashboardHandler()
	req := httptest.NewRequest(http.MethodGet, "/api/commands?status=COMPLETED", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	var resp struct {
		Commands []map[string]any `json:"commands"`
	}
	json.NewDecoder(w.Body).Decode(&resp)
	for _, cmd := range resp.Commands {
		if cmd["status"] != "COMPLETED" {
			t.Errorf("Got status %v, want COMPLETED", cmd["status"])
		}
	}
}

func TestDashboard_Health_Started(t *testing.T) {
	executor, _ := newTestExecutor(t)

	handler := executor.DashboardHandler()
	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Status = %d, want 200", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["status"] != "healthy" {
		t.Errorf("health status = %v, want healthy", resp["status"])
	}
	if resp["started"] != true {
		t.Error("started should be true")
	}
}

func TestDashboard_Health_NotStarted(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(1))
	// Don't start

	handler := executor.DashboardHandler()
	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("Status = %d, want 503", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["status"] != "unhealthy" {
		t.Errorf("health status = %v, want unhealthy", resp["status"])
	}
}

func TestDashboard_Retry(t *testing.T) {
	executor, ctx := newTestExecutor(t)

	inst, _ := executor.Add(ctx, durex.Spec{Name: "fail"})
	time.Sleep(200 * time.Millisecond)

	handler := executor.DashboardHandler()
	req := httptest.NewRequest(http.MethodPost, "/api/commands/retry?id="+inst.ID, nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Status = %d, want 200. Body: %s", w.Code, w.Body.String())
	}
}

func TestDashboard_Retry_MissingID(t *testing.T) {
	executor, _ := newTestExecutor(t)

	handler := executor.DashboardHandler()
	req := httptest.NewRequest(http.MethodPost, "/api/commands/retry", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Status = %d, want 400", w.Code)
	}
}

func TestDashboard_Cancel(t *testing.T) {
	executor, ctx := newTestExecutor(t)

	inst, _ := executor.Add(ctx, durex.Spec{Name: "test", Delay: time.Hour})

	handler := executor.DashboardHandler()
	req := httptest.NewRequest(http.MethodPost, "/api/commands/cancel?id="+inst.ID, nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Status = %d, want 200. Body: %s", w.Code, w.Body.String())
	}

	got, _ := executor.Get(ctx, inst.ID)
	if got.Status != durex.StatusCancelled {
		t.Errorf("Status = %s, want CANCELLED", got.Status)
	}
}

func TestDashboard_Cancel_MissingID(t *testing.T) {
	executor, _ := newTestExecutor(t)

	handler := executor.DashboardHandler()
	req := httptest.NewRequest(http.MethodPost, "/api/commands/cancel", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Status = %d, want 400", w.Code)
	}
}

func TestDashboard_History(t *testing.T) {
	executor, ctx := newTestExecutor(t)

	inst, _ := executor.Add(ctx, durex.Spec{Name: "test"})
	time.Sleep(200 * time.Millisecond)

	handler := executor.DashboardHandler()
	req := httptest.NewRequest(http.MethodGet, "/api/commands/history?id="+inst.ID, nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Status = %d, want 200", w.Code)
	}

	var resp struct {
		ID      string `json:"id"`
		History []any  `json:"history"`
	}
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.ID != inst.ID {
		t.Errorf("ID = %q, want %q", resp.ID, inst.ID)
	}
	if len(resp.History) == 0 {
		t.Error("Expected history events")
	}
}

func TestDashboard_History_MissingID(t *testing.T) {
	executor, _ := newTestExecutor(t)

	handler := executor.DashboardHandler()
	req := httptest.NewRequest(http.MethodGet, "/api/commands/history", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Status = %d, want 400", w.Code)
	}
}

func TestDashboard_NotFound(t *testing.T) {
	executor, _ := newTestExecutor(t)

	handler := executor.DashboardHandler()
	req := httptest.NewRequest(http.MethodGet, "/nonexistent", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Status = %d, want 404", w.Code)
	}
}

func TestDashboard_Index(t *testing.T) {
	executor, _ := newTestExecutor(t)

	handler := executor.DashboardHandler()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Status = %d, want 200", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "text/html; charset=utf-8" {
		t.Errorf("Content-Type = %q, want text/html", ct)
	}
}
