package durex_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/simonovic86/durex"
	"github.com/simonovic86/durex/storage"
)

func TestExecutor_GracefulShutdown(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store,
		durex.WithParallelism(2),
		durex.WithGracefulShutdown(5*time.Second),
	)

	started := make(chan struct{})
	var completed atomic.Int32
	executor.HandleFunc("slow", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		close(started)
		// Do work that takes a bit, ignoring context cancellation
		time.Sleep(100 * time.Millisecond)
		completed.Add(1)
		return durex.Empty(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)

	executor.Add(ctx, durex.Spec{Name: "slow"})
	<-started // Wait until handler actually starts

	err := executor.Stop()
	if err != nil {
		t.Errorf("Stop should not error with generous timeout: %v", err)
	}

	if got := completed.Load(); got != 1 {
		t.Errorf("completed = %d, want 1 (should finish before timeout)", got)
	}
}

func TestExecutor_ShutdownTimeout(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store,
		durex.WithParallelism(1),
		durex.WithGracefulShutdown(100*time.Millisecond),
	)

	executor.HandleFunc("stuck", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		// Block forever, ignore context
		select {}
	})

	ctx := context.Background()
	executor.Start(ctx)

	executor.Add(ctx, durex.Spec{Name: "stuck"})
	time.Sleep(50 * time.Millisecond) // Let it start

	start := time.Now()
	err := executor.Stop()
	elapsed := time.Since(start)

	if err == nil {
		t.Error("Stop should return error on timeout")
	}

	if elapsed > time.Second {
		t.Errorf("Stop took %v, should have timed out around 100ms", elapsed)
	}
}

func TestExecutor_DoubleStop(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(1))

	executor.HandleFunc("noop", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.Empty(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)

	// First stop
	if err := executor.Stop(); err != nil {
		t.Fatalf("First Stop: %v", err)
	}

	// Second stop should be a no-op
	if err := executor.Stop(); err != nil {
		t.Fatalf("Second Stop: %v", err)
	}
}

func TestExecutor_StopBeforeStart(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(1))

	// Stop without starting should be fine
	if err := executor.Stop(); err != nil {
		t.Fatalf("Stop before Start: %v", err)
	}
}

func TestExecutor_AddAfterStop(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(1))

	executor.HandleFunc("test", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.Empty(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	executor.Stop()

	_, err := executor.Add(ctx, durex.Spec{Name: "test"})
	if err == nil {
		t.Error("Add after Stop should return error")
	}
}

func TestExecutor_RestartAfterStop(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(1))

	var executed atomic.Int32
	executor.HandleFunc("restart-test", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		executed.Add(1)
		return durex.Empty(), nil
	})

	ctx := context.Background()

	// First run
	if err := executor.Start(ctx); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	if _, err := executor.Add(ctx, durex.Spec{Name: "restart-test"}); err != nil {
		t.Fatalf("first Add: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	if err := executor.Stop(); err != nil {
		t.Fatalf("first Stop: %v", err)
	}

	if got := executed.Load(); got != 1 {
		t.Fatalf("after first run: expected 1 execution, got %d", got)
	}

	// Restart
	if err := executor.Start(ctx); err != nil {
		t.Fatalf("second Start: %v", err)
	}
	if _, err := executor.Add(ctx, durex.Spec{Name: "restart-test"}); err != nil {
		t.Fatalf("second Add: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	if err := executor.Stop(); err != nil {
		t.Fatalf("second Stop: %v", err)
	}

	if got := executed.Load(); got != 2 {
		t.Errorf("after restart: expected 2 executions, got %d", got)
	}
}
