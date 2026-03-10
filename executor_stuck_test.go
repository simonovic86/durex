package durex_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/simonovic86/durex"
	"github.com/simonovic86/durex/storage"
)

func TestExecutor_StuckCommandRecovery(t *testing.T) {
	store := storage.NewMemory()

	// Create a command that appears stuck (in STARTED status with old StartedAt)
	ctx := context.Background()
	past := time.Now().Add(-10 * time.Minute)
	stuckCmd := &durex.Instance{
		ID:        "stuck-1",
		Name:      "task",
		Status:    durex.StatusStarted,
		StartedAt: &past,
		CreatedAt: past,
		ReadyAt:   past,
		Attempt:   1,
	}
	store.Create(ctx, stuckCmd)

	var executed atomic.Int32
	executor := durex.New(store,
		durex.WithParallelism(1),
		durex.WithStuckCommandRecovery(100*time.Millisecond, time.Minute),
	)

	executor.HandleFunc("task", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		executed.Add(1)
		return durex.Empty(), nil
	})

	executor.Start(ctx)
	defer executor.Stop()

	// Wait for recovery loop to detect and reschedule
	time.Sleep(500 * time.Millisecond)

	if got := executed.Load(); got < 1 {
		t.Errorf("executed = %d, want >= 1 (stuck command should be recovered)", got)
	}

	got, _ := store.Get(ctx, "stuck-1")
	if got.Status != durex.StatusCompleted {
		t.Errorf("Status = %s, want COMPLETED after recovery", got.Status)
	}
}

func TestExecutor_StuckCommandRecovery_NotStuck(t *testing.T) {
	store := storage.NewMemory()
	ctx := context.Background()

	executor := durex.New(store,
		durex.WithParallelism(1),
		durex.WithStuckCommandRecovery(100*time.Millisecond, 5*time.Minute),
	)

	executor.HandleFunc("task", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.Empty(), nil
	})

	executor.Start(ctx)
	defer executor.Stop()

	// Insert a STARTED command directly into storage after start
	// (simulates a command that is actively being executed by another worker)
	now := time.Now()
	recentCmd := &durex.Instance{
		ID:        "recent-1",
		Name:      "task",
		Status:    durex.StatusStarted,
		StartedAt: &now,
		CreatedAt: now,
		ReadyAt:   now,
		Attempt:   1,
	}
	store.Create(ctx, recentCmd)

	time.Sleep(300 * time.Millisecond)

	// The recent command should NOT be recovered (not stuck yet — started < threshold)
	got, _ := store.Get(ctx, "recent-1")
	if got.Status != durex.StatusStarted {
		t.Errorf("Status = %s, want STARTED (not yet stuck)", got.Status)
	}
}

func TestExecutor_PermanentCommands(t *testing.T) {
	store := storage.NewMemory()
	var executed atomic.Int32

	executor := durex.New(store,
		durex.WithParallelism(1),
		durex.WithPermanentCommands("heartbeat"),
	)

	executor.HandleFunc("heartbeat", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		executed.Add(1)
		return durex.Empty(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	time.Sleep(300 * time.Millisecond)

	if got := executed.Load(); got < 1 {
		t.Errorf("executed = %d, want >= 1 (permanent command should auto-start)", got)
	}
}

func TestExecutor_PermanentCommands_WithDefaults(t *testing.T) {
	store := storage.NewMemory()
	var executed atomic.Int32

	executor := durex.New(store,
		durex.WithParallelism(1),
		durex.WithPermanentCommands("monitor"),
	)

	executor.HandleFunc("monitor", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		executed.Add(1)
		return durex.Repeat(), nil
	}, durex.Period(100*time.Millisecond))

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	time.Sleep(350 * time.Millisecond)

	if got := executed.Load(); got < 2 {
		t.Errorf("executed = %d, want >= 2 (repeating permanent command)", got)
	}
}
