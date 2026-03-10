package durex_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/simonovic86/durex"
	"github.com/simonovic86/durex/storage"
)

func TestHookedStorage_AfterCreate(t *testing.T) {
	var called atomic.Int32
	var lastID string

	hs := &durex.HookedStorage{
		Storage: storage.NewMemory(),
		Hooks: durex.Hooks{
			AfterCreate: func(_ context.Context, cmd *durex.Instance) {
				called.Add(1)
				lastID = cmd.ID
			},
		},
	}

	ctx := context.Background()
	inst := &durex.Instance{
		ID:        "test-1",
		Name:      "cmd",
		Status:    durex.StatusPending,
		CreatedAt: time.Now(),
		ReadyAt:   time.Now(),
	}

	if err := hs.Create(ctx, inst); err != nil {
		t.Fatalf("Create: %v", err)
	}

	if called.Load() != 1 {
		t.Error("AfterCreate not called")
	}
	if lastID != "test-1" {
		t.Errorf("lastID = %q, want test-1", lastID)
	}
}

func TestHookedStorage_AfterUpdate(t *testing.T) {
	var called atomic.Int32

	mem := storage.NewMemory()
	hs := &durex.HookedStorage{
		Storage: mem,
		Hooks: durex.Hooks{
			AfterUpdate: func(_ context.Context, cmd *durex.Instance) {
				called.Add(1)
			},
		},
	}

	ctx := context.Background()
	inst := &durex.Instance{
		ID:        "test-1",
		Name:      "cmd",
		Status:    durex.StatusPending,
		CreatedAt: time.Now(),
		ReadyAt:   time.Now(),
	}
	mem.Create(ctx, inst)

	inst.Status = durex.StatusStarted
	if err := hs.Update(ctx, inst); err != nil {
		t.Fatalf("Update: %v", err)
	}

	if called.Load() != 1 {
		t.Error("AfterUpdate not called")
	}
}

func TestHookedStorage_AfterDelete(t *testing.T) {
	var deletedID string

	mem := storage.NewMemory()
	hs := &durex.HookedStorage{
		Storage: mem,
		Hooks: durex.Hooks{
			AfterDelete: func(_ context.Context, id string) {
				deletedID = id
			},
		},
	}

	ctx := context.Background()
	inst := &durex.Instance{
		ID:        "test-1",
		Name:      "cmd",
		Status:    durex.StatusPending,
		CreatedAt: time.Now(),
		ReadyAt:   time.Now(),
	}
	mem.Create(ctx, inst)

	if err := hs.Delete(ctx, "test-1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if deletedID != "test-1" {
		t.Errorf("deletedID = %q, want test-1", deletedID)
	}
}

func TestHookedStorage_OnError(t *testing.T) {
	var errorOp string
	var errorErr error

	mem := storage.NewMemory()
	hs := &durex.HookedStorage{
		Storage: mem,
		Hooks: durex.Hooks{
			OnError: func(_ context.Context, op string, err error) {
				errorOp = op
				errorErr = err
			},
		},
	}

	ctx := context.Background()

	// Update a non-existent command should trigger OnError
	inst := &durex.Instance{
		ID:        "nonexistent",
		Name:      "cmd",
		Status:    durex.StatusPending,
		CreatedAt: time.Now(),
		ReadyAt:   time.Now(),
	}
	err := hs.Update(ctx, inst)
	if err == nil {
		t.Fatal("Expected error")
	}

	if errorOp != "Update" {
		t.Errorf("errorOp = %q, want Update", errorOp)
	}
	if !errors.Is(errorErr, durex.ErrNotFound) {
		t.Errorf("errorErr = %v, want ErrNotFound", errorErr)
	}
}

func TestHookedStorage_NilHooks(t *testing.T) {
	mem := storage.NewMemory()
	hs := &durex.HookedStorage{
		Storage: mem,
		Hooks:   durex.Hooks{}, // All nil
	}

	ctx := context.Background()
	inst := &durex.Instance{
		ID:        "test-1",
		Name:      "cmd",
		Status:    durex.StatusPending,
		CreatedAt: time.Now(),
		ReadyAt:   time.Now(),
	}

	// Should not panic with nil hooks
	if err := hs.Create(ctx, inst); err != nil {
		t.Fatalf("Create: %v", err)
	}

	inst.Status = durex.StatusStarted
	if err := hs.Update(ctx, inst); err != nil {
		t.Fatalf("Update: %v", err)
	}

	if err := hs.Delete(ctx, "test-1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
}

func TestHookedStorage_DelegatesMethods(t *testing.T) {
	mem := storage.NewMemory()
	hs := &durex.HookedStorage{Storage: mem}

	ctx := context.Background()
	now := time.Now()
	inst := &durex.Instance{
		ID:        "test-1",
		Name:      "cmd",
		Status:    durex.StatusPending,
		CreatedAt: now,
		ReadyAt:   now,
	}
	mem.Create(ctx, inst)

	// Get should delegate
	got, err := hs.Get(ctx, "test-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ID != "test-1" {
		t.Errorf("Get.ID = %q, want test-1", got.ID)
	}

	// FindPending should delegate
	pending, err := hs.FindPending(ctx)
	if err != nil {
		t.Fatalf("FindPending: %v", err)
	}
	if len(pending) != 1 {
		t.Errorf("FindPending = %d, want 1", len(pending))
	}

	// Count should delegate
	count, err := hs.Count(ctx, nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 1 {
		t.Errorf("Count = %d, want 1", count)
	}
}
