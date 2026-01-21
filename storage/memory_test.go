package storage_test

import (
	"context"
	"testing"
	"time"

	"github.com/simonovic86/durex"
	"github.com/simonovic86/durex/storage"
)

func TestMemory_CreateAndGet(t *testing.T) {
	store := storage.NewMemory()
	ctx := context.Background()

	cmd := &durex.Instance{
		ID:        "test-1",
		Name:      "testCommand",
		Status:    durex.StatusPending,
		CreatedAt: time.Now(),
		ReadyAt:   time.Now(),
		Data:      durex.M{"key": "value"},
	}

	// Create
	if err := store.Create(ctx, cmd); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Get
	got, err := store.Get(ctx, "test-1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if got.ID != cmd.ID {
		t.Errorf("ID mismatch: expected %s, got %s", cmd.ID, got.ID)
	}

	if got.Name != cmd.Name {
		t.Errorf("Name mismatch: expected %s, got %s", cmd.Name, got.Name)
	}

	if got.Data["key"] != "value" {
		t.Errorf("Data mismatch: expected 'value', got %v", got.Data["key"])
	}
}

func TestMemory_CreateDuplicate(t *testing.T) {
	store := storage.NewMemory()
	ctx := context.Background()

	cmd := &durex.Instance{
		ID:        "test-1",
		Name:      "testCommand",
		Status:    durex.StatusPending,
		CreatedAt: time.Now(),
		ReadyAt:   time.Now(),
	}

	if err := store.Create(ctx, cmd); err != nil {
		t.Fatalf("First create failed: %v", err)
	}

	err := store.Create(ctx, cmd)
	if err != durex.ErrAlreadyExists {
		t.Errorf("Expected ErrAlreadyExists, got %v", err)
	}
}

func TestMemory_Update(t *testing.T) {
	store := storage.NewMemory()
	ctx := context.Background()

	cmd := &durex.Instance{
		ID:        "test-1",
		Name:      "testCommand",
		Status:    durex.StatusPending,
		CreatedAt: time.Now(),
		ReadyAt:   time.Now(),
	}

	if err := store.Create(ctx, cmd); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	cmd.Status = durex.StatusCompleted
	if err := store.Update(ctx, cmd); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	got, err := store.Get(ctx, "test-1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if got.Status != durex.StatusCompleted {
		t.Errorf("Status not updated: expected COMPLETED, got %s", got.Status)
	}
}

func TestMemory_UpdateNotFound(t *testing.T) {
	store := storage.NewMemory()
	ctx := context.Background()

	cmd := &durex.Instance{
		ID:     "nonexistent",
		Name:   "test",
		Status: durex.StatusPending,
	}

	err := store.Update(ctx, cmd)
	if err != durex.ErrNotFound {
		t.Errorf("Expected ErrNotFound, got %v", err)
	}
}

func TestMemory_Delete(t *testing.T) {
	store := storage.NewMemory()
	ctx := context.Background()

	cmd := &durex.Instance{
		ID:        "test-1",
		Name:      "testCommand",
		Status:    durex.StatusPending,
		CreatedAt: time.Now(),
		ReadyAt:   time.Now(),
	}

	if err := store.Create(ctx, cmd); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	if err := store.Delete(ctx, "test-1"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err := store.Get(ctx, "test-1")
	if err != durex.ErrNotFound {
		t.Errorf("Expected ErrNotFound after delete, got %v", err)
	}
}

func TestMemory_FindPending(t *testing.T) {
	store := storage.NewMemory()
	ctx := context.Background()

	statuses := []durex.Status{
		durex.StatusPending,
		durex.StatusStarted,
		durex.StatusCompleted,
		durex.StatusFailed,
		durex.StatusRepeating,
	}

	for i, status := range statuses {
		cmd := &durex.Instance{
			ID:        durex.GenerateID(),
			Name:      "test",
			Status:    status,
			CreatedAt: time.Now(),
			ReadyAt:   time.Now(),
		}
		if err := store.Create(ctx, &durex.Instance{
			ID:        cmd.ID,
			Name:      cmd.Name,
			Status:    statuses[i],
			CreatedAt: time.Now(),
			ReadyAt:   time.Now(),
		}); err != nil {
			t.Fatalf("Create failed: %v", err)
		}
	}

	pending, err := store.FindPending(ctx)
	if err != nil {
		t.Fatalf("FindPending failed: %v", err)
	}

	// Should find PENDING, STARTED, REPEATING (3 commands)
	if len(pending) != 3 {
		t.Errorf("Expected 3 pending commands, got %d", len(pending))
	}
}

func TestMemory_FindByStatus(t *testing.T) {
	store := storage.NewMemory()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		status := durex.StatusPending
		if i%2 == 0 {
			status = durex.StatusCompleted
		}

		cmd := &durex.Instance{
			ID:        durex.GenerateID(),
			Name:      "test",
			Status:    status,
			CreatedAt: time.Now(),
			ReadyAt:   time.Now(),
		}
		if err := store.Create(ctx, cmd); err != nil {
			t.Fatalf("Create failed: %v", err)
		}
	}

	completed, err := store.FindByStatus(ctx, durex.StatusCompleted)
	if err != nil {
		t.Fatalf("FindByStatus failed: %v", err)
	}

	if len(completed) != 3 {
		t.Errorf("Expected 3 completed commands, got %d", len(completed))
	}
}

func TestMemory_FindByParent(t *testing.T) {
	store := storage.NewMemory()
	ctx := context.Background()

	parentID := "parent-1"

	// Create parent
	parent := &durex.Instance{
		ID:        parentID,
		Name:      "parent",
		Status:    durex.StatusCompleted,
		CreatedAt: time.Now(),
		ReadyAt:   time.Now(),
	}
	if err := store.Create(ctx, parent); err != nil {
		t.Fatalf("Create parent failed: %v", err)
	}

	// Create children
	for i := 0; i < 3; i++ {
		child := &durex.Instance{
			ID:        durex.GenerateID(),
			Name:      "child",
			Status:    durex.StatusPending,
			ParentID:  &parentID,
			CreatedAt: time.Now(),
			ReadyAt:   time.Now(),
		}
		if err := store.Create(ctx, child); err != nil {
			t.Fatalf("Create child failed: %v", err)
		}
	}

	children, err := store.FindByParent(ctx, parentID)
	if err != nil {
		t.Fatalf("FindByParent failed: %v", err)
	}

	if len(children) != 3 {
		t.Errorf("Expected 3 children, got %d", len(children))
	}
}

func TestMemory_Cleanup(t *testing.T) {
	store := storage.NewMemory()
	ctx := context.Background()

	now := time.Now()
	oldTime := now.Add(-2 * time.Hour)

	// Create old completed command
	old := &durex.Instance{
		ID:          "old-1",
		Name:        "test",
		Status:      durex.StatusCompleted,
		CreatedAt:   oldTime,
		ReadyAt:     oldTime,
		CompletedAt: &oldTime,
	}
	if err := store.Create(ctx, old); err != nil {
		t.Fatalf("Create old failed: %v", err)
	}

	// Create recent completed command
	recent := &durex.Instance{
		ID:          "recent-1",
		Name:        "test",
		Status:      durex.StatusCompleted,
		CreatedAt:   now,
		ReadyAt:     now,
		CompletedAt: &now,
	}
	if err := store.Create(ctx, recent); err != nil {
		t.Fatalf("Create recent failed: %v", err)
	}

	// Cleanup commands older than 1 hour
	count, err := store.Cleanup(ctx, time.Hour)
	if err != nil {
		t.Fatalf("Cleanup failed: %v", err)
	}

	if count != 1 {
		t.Errorf("Expected 1 cleanup, got %d", count)
	}

	// Old should be gone
	_, err = store.Get(ctx, "old-1")
	if err != durex.ErrNotFound {
		t.Error("Old command should be deleted")
	}

	// Recent should remain
	_, err = store.Get(ctx, "recent-1")
	if err != nil {
		t.Error("Recent command should still exist")
	}
}

func TestMemory_Count(t *testing.T) {
	store := storage.NewMemory()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		cmd := &durex.Instance{
			ID:        durex.GenerateID(),
			Name:      "test",
			Status:    durex.StatusPending,
			CreatedAt: time.Now(),
			ReadyAt:   time.Now(),
		}
		if err := store.Create(ctx, cmd); err != nil {
			t.Fatalf("Create failed: %v", err)
		}
	}

	// Total count
	total, err := store.Count(ctx, nil)
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if total != 5 {
		t.Errorf("Expected 5, got %d", total)
	}

	// Count by status
	pending := durex.StatusPending
	statusCount, err := store.Count(ctx, &pending)
	if err != nil {
		t.Fatalf("Count by status failed: %v", err)
	}
	if statusCount != 5 {
		t.Errorf("Expected 5 pending, got %d", statusCount)
	}
}

func TestMemory_Close(t *testing.T) {
	store := storage.NewMemory()
	ctx := context.Background()

	cmd := &durex.Instance{
		ID:        "test-1",
		Name:      "test",
		Status:    durex.StatusPending,
		CreatedAt: time.Now(),
		ReadyAt:   time.Now(),
	}
	if err := store.Create(ctx, cmd); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Operations should fail after close
	_, err := store.Get(ctx, "test-1")
	if err != durex.ErrStorageClosed {
		t.Errorf("Expected ErrStorageClosed, got %v", err)
	}
}

func TestMemory_IsolatedMutations(t *testing.T) {
	store := storage.NewMemory()
	ctx := context.Background()

	cmd := &durex.Instance{
		ID:        "test-1",
		Name:      "test",
		Status:    durex.StatusPending,
		Data:      durex.M{"key": "original"},
		CreatedAt: time.Now(),
		ReadyAt:   time.Now(),
	}

	if err := store.Create(ctx, cmd); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Mutate original
	cmd.Data["key"] = "mutated"

	// Get should return original value
	got, err := store.Get(ctx, "test-1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if got.Data["key"] != "original" {
		t.Errorf("Storage should be isolated from external mutations")
	}
}
