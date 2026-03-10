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

func TestExecutor_DLQ_FailedCommandMovedToDLQ(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store,
		durex.WithParallelism(1),
		durex.WithDeadLetterQueue(),
	)

	executor.HandleFunc("fail", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.Empty(), errors.New("permanent failure")
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	inst, _ := executor.Add(ctx, durex.Spec{Name: "fail"})

	time.Sleep(200 * time.Millisecond)

	got, err := executor.Get(ctx, inst.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != durex.StatusDeadLetter {
		t.Errorf("Status = %s, want DEAD_LETTER", got.Status)
	}
}

func TestExecutor_DLQ_FindDeadLettered(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store,
		durex.WithParallelism(1),
		durex.WithDeadLetterQueue(),
	)

	executor.HandleFunc("fail", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.Empty(), errors.New("permanent failure")
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	executor.Add(ctx, durex.Spec{Name: "fail"})
	executor.Add(ctx, durex.Spec{Name: "fail"})

	time.Sleep(200 * time.Millisecond)

	dlq, err := executor.FindDeadLettered(ctx)
	if err != nil {
		t.Fatalf("FindDeadLettered: %v", err)
	}
	if len(dlq) != 2 {
		t.Errorf("FindDeadLettered = %d, want 2", len(dlq))
	}
}

func TestExecutor_DLQ_ReplayFromDLQ(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store,
		durex.WithParallelism(1),
		durex.WithDeadLetterQueue(),
	)

	var attempts atomic.Int32
	executor.HandleFunc("flaky", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		count := attempts.Add(1)
		if count <= 1 {
			return durex.Empty(), errors.New("temporary failure")
		}
		return durex.Empty(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	inst, _ := executor.Add(ctx, durex.Spec{Name: "flaky"})
	time.Sleep(200 * time.Millisecond)

	// Should be in DLQ
	got, _ := executor.Get(ctx, inst.ID)
	if got.Status != durex.StatusDeadLetter {
		t.Fatalf("Status = %s, want DEAD_LETTER", got.Status)
	}

	// Replay
	if err := executor.ReplayFromDLQ(ctx, inst.ID); err != nil {
		t.Fatalf("ReplayFromDLQ: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	got, _ = executor.Get(ctx, inst.ID)
	if got.Status != durex.StatusCompleted {
		t.Errorf("After replay: Status = %s, want COMPLETED", got.Status)
	}
}

func TestExecutor_DLQ_ReplayNonDLQCommand(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store,
		durex.WithParallelism(1),
		durex.WithDeadLetterQueue(),
	)

	executor.HandleFunc("ok", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.Empty(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	inst, _ := executor.Add(ctx, durex.Spec{Name: "ok"})
	time.Sleep(200 * time.Millisecond)

	err := executor.ReplayFromDLQ(ctx, inst.ID)
	if err == nil {
		t.Error("Expected error when replaying non-DLQ command")
	}
}

func TestExecutor_DLQ_PurgeDLQ(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store,
		durex.WithParallelism(1),
		durex.WithDeadLetterQueue(),
	)

	executor.HandleFunc("fail", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.Empty(), errors.New("permanent failure")
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	executor.Add(ctx, durex.Spec{Name: "fail"})
	time.Sleep(200 * time.Millisecond)

	// Purge with 0 age (remove all)
	count, err := executor.PurgeDLQ(ctx, 0)
	if err != nil {
		t.Fatalf("PurgeDLQ: %v", err)
	}
	if count != 1 {
		t.Errorf("PurgeDLQ = %d, want 1", count)
	}

	// Should be empty now
	dlq, _ := executor.FindDeadLettered(ctx)
	if len(dlq) != 0 {
		t.Errorf("After purge: %d in DLQ, want 0", len(dlq))
	}
}

func TestExecutor_DLQ_PurgeRespectAge(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store,
		durex.WithParallelism(1),
		durex.WithDeadLetterQueue(),
	)

	executor.HandleFunc("fail", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.Empty(), errors.New("permanent failure")
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	executor.Add(ctx, durex.Spec{Name: "fail"})
	time.Sleep(200 * time.Millisecond)

	// Purge with 1 hour age — nothing should be purged
	count, err := executor.PurgeDLQ(ctx, time.Hour)
	if err != nil {
		t.Fatalf("PurgeDLQ: %v", err)
	}
	if count != 0 {
		t.Errorf("PurgeDLQ = %d, want 0 (too recent)", count)
	}
}
