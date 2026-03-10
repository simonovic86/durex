package durex_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/simonovic86/durex"
	"github.com/simonovic86/durex/storage"
)

func TestExecutor_AddMany(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(4))

	var executed atomic.Int32
	executor.HandleFunc("task", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		executed.Add(1)
		return durex.Empty(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	instances, err := executor.AddMany(ctx,
		durex.Spec{Name: "task", Data: durex.M{"n": 1}},
		durex.Spec{Name: "task", Data: durex.M{"n": 2}},
		durex.Spec{Name: "task", Data: durex.M{"n": 3}},
	)
	if err != nil {
		t.Fatalf("AddMany: %v", err)
	}
	if len(instances) != 3 {
		t.Fatalf("AddMany returned %d instances, want 3", len(instances))
	}

	time.Sleep(300 * time.Millisecond)

	if got := executed.Load(); got != 3 {
		t.Errorf("executed = %d, want 3", got)
	}
}

func TestExecutor_AddMany_PartialFailure(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(1))

	executor.HandleFunc("task", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.Empty(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	// Second spec has same unique key as first — should fail
	instances, err := executor.AddMany(ctx,
		durex.Spec{Name: "task", UniqueKey: "same"},
		durex.Spec{Name: "task", UniqueKey: "same"}, // Duplicate
		durex.Spec{Name: "task", UniqueKey: "other"},
	)
	if err == nil {
		t.Error("Expected error for duplicate unique key")
	}
	if len(instances) != 1 {
		t.Errorf("Should have created 1 instance before failure, got %d", len(instances))
	}
}

func TestExecutor_CancelByTag(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(1))

	// Use a blocking handler so commands stay in PENDING
	executor.HandleFunc("task", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		<-ctx.Done()
		return durex.Empty(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	// Add commands with different tags
	executor.Add(ctx, durex.Spec{Name: "task", Tags: []string{"batch-1"}, Delay: time.Hour})
	executor.Add(ctx, durex.Spec{Name: "task", Tags: []string{"batch-1"}, Delay: time.Hour})
	executor.Add(ctx, durex.Spec{Name: "task", Tags: []string{"batch-2"}, Delay: time.Hour})

	time.Sleep(100 * time.Millisecond)

	cancelled, err := executor.CancelByTag(ctx, "batch-1")
	if err != nil {
		t.Fatalf("CancelByTag: %v", err)
	}
	if cancelled != 2 {
		t.Errorf("cancelled = %d, want 2", cancelled)
	}

	// batch-2 should still be pending
	cmds, _ := store.FindByStatus(ctx, durex.StatusPending)
	if len(cmds) != 1 {
		t.Errorf("remaining pending = %d, want 1", len(cmds))
	}
}

func TestExecutor_CancelByTag_RecordEvent(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(1))

	executor.HandleFunc("task", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		<-ctx.Done()
		return durex.Empty(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	inst, _ := executor.Add(ctx, durex.Spec{Name: "task", Tags: []string{"batch"}, Delay: time.Hour})
	time.Sleep(50 * time.Millisecond)

	executor.CancelByTag(ctx, "batch")

	got, _ := executor.Get(ctx, inst.ID)
	if got.Status != durex.StatusCancelled {
		t.Fatalf("Status = %s, want CANCELLED", got.Status)
	}

	// Check that EventCancelled was recorded
	found := false
	for _, ev := range got.History {
		if ev.Type == durex.EventCancelled {
			found = true
			break
		}
	}
	if !found {
		t.Error("EventCancelled not recorded in history")
	}
}
