package durex_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/simonovic86/durex"
	"github.com/simonovic86/durex/storage"
)

func TestExecutorHistory(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store)

	executed := make(chan struct{})
	executor.HandleFunc("test", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		close(executed)
		return durex.Empty(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	instance, err := executor.Add(ctx, durex.Spec{Name: "test"})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// Wait for execution
	select {
	case <-executed:
	case <-time.After(2 * time.Second):
		t.Fatal("command did not execute")
	}

	// Allow time for completion
	time.Sleep(100 * time.Millisecond)

	// Get history
	history, err := executor.History(ctx, instance.ID)
	if err != nil {
		t.Fatalf("History failed: %v", err)
	}

	// Should have at least: created, started, completed
	if len(history) < 3 {
		t.Errorf("expected at least 3 events, got %d: %+v", len(history), history)
	}

	// Check event types
	eventTypes := make(map[durex.EventType]bool)
	for _, e := range history {
		eventTypes[e.Type] = true
	}

	if !eventTypes[durex.EventCreated] {
		t.Error("missing 'created' event")
	}
	if !eventTypes[durex.EventStarted] {
		t.Error("missing 'started' event")
	}
	if !eventTypes[durex.EventCompleted] {
		t.Error("missing 'completed' event")
	}
}

func TestExecutorHistoryWithRetry(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithBackoff(durex.NoBackoff()))

	attempts := 0
	done := make(chan struct{})
	executor.HandleFunc("retryTest", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		attempts++
		if attempts < 3 {
			return durex.Empty(), errors.New("temporary error")
		}
		close(done)
		return durex.Empty(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	instance, err := executor.Add(ctx, durex.Spec{
		Name:    "retryTest",
		Retries: 3,
	})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// Wait for completion
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("command did not complete")
	}

	time.Sleep(100 * time.Millisecond)

	history, err := executor.History(ctx, instance.ID)
	if err != nil {
		t.Fatalf("History failed: %v", err)
	}

	// Count retry events
	retryCount := 0
	for _, e := range history {
		if e.Type == durex.EventRetrying {
			retryCount++
			if e.Error == "" {
				t.Error("retry event should have error message")
			}
		}
	}

	if retryCount != 2 {
		t.Errorf("expected 2 retry events, got %d", retryCount)
	}
}

func TestExecutorHistoryCancel(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store)

	executor.HandleFunc("cancelTest", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.Empty(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	// Add with delay so we can cancel
	instance, err := executor.Add(ctx, durex.Spec{
		Name:  "cancelTest",
		Delay: time.Hour,
	})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// Cancel it
	if err := executor.Cancel(ctx, instance.ID); err != nil {
		t.Fatalf("Cancel failed: %v", err)
	}

	history, err := executor.History(ctx, instance.ID)
	if err != nil {
		t.Fatalf("History failed: %v", err)
	}

	// Should have: created, cancelled
	hasCancelled := false
	for _, e := range history {
		if e.Type == durex.EventCancelled {
			hasCancelled = true
		}
	}

	if !hasCancelled {
		t.Error("missing 'cancelled' event")
	}
}
