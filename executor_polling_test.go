package durex_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/simonovic86/durex"
	"github.com/simonovic86/durex/storage"
)

// lockingMemory wraps Memory to implement LockingStorage for testing polling mode.
type lockingMemory struct {
	*storage.Memory
	mu sync.Mutex
}

func newLockingMemory() *lockingMemory {
	return &lockingMemory{Memory: storage.NewMemory()}
}

func (l *lockingMemory) ClaimPending(ctx context.Context, limit int) ([]*durex.Instance, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	pending, err := l.Memory.FindPending(ctx)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	var claimed []*durex.Instance
	for _, inst := range pending {
		if inst.Status != durex.StatusPending && inst.Status != durex.StatusRepeating {
			continue
		}
		if len(claimed) >= limit {
			break
		}
		inst.Status = durex.StatusStarted
		inst.StartedAt = &now
		inst.Attempt++
		if err := l.Memory.Update(ctx, inst); err != nil {
			continue
		}
		claimed = append(claimed, inst)
	}

	return claimed, nil
}

// Compile-time check
var _ durex.LockingStorage = (*lockingMemory)(nil)

func TestExecutor_PollingMode_BasicExecution(t *testing.T) {
	store := newLockingMemory()
	executor := durex.New(store,
		durex.WithParallelism(2),
		durex.WithPollInterval(50*time.Millisecond),
		durex.WithClaimBatchSize(5),
	)

	var executed atomic.Int32
	executor.HandleFunc("task", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		executed.Add(1)
		return durex.Empty(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	executor.Add(ctx, durex.Spec{Name: "task"})
	executor.Add(ctx, durex.Spec{Name: "task"})

	time.Sleep(500 * time.Millisecond)

	if got := executed.Load(); got != 2 {
		t.Errorf("executed = %d, want 2", got)
	}
}

func TestExecutor_PollingMode_StopsDuringShutdown(t *testing.T) {
	store := newLockingMemory()
	executor := durex.New(store,
		durex.WithParallelism(1),
		durex.WithPollInterval(50*time.Millisecond),
	)

	var started atomic.Int32
	executor.HandleFunc("task", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		started.Add(1)
		<-ctx.Done()
		return durex.Empty(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)

	// Add a command that will block
	executor.Add(ctx, durex.Spec{Name: "task"})
	time.Sleep(200 * time.Millisecond)

	// Stop should signal workers
	executor.Stop()

	// Should not accept new commands after stop
	_, err := executor.Add(ctx, durex.Spec{Name: "task"})
	if err == nil {
		t.Error("Expected error when adding after stop")
	}
}

func TestExecutor_PollingMode_RetryOnError(t *testing.T) {
	store := newLockingMemory()
	executor := durex.New(store,
		durex.WithParallelism(1),
		durex.WithPollInterval(50*time.Millisecond),
	)

	var attempts atomic.Int32
	executor.HandleFunc("flaky", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		count := attempts.Add(1)
		if count < 3 {
			return durex.Empty(), context.DeadlineExceeded
		}
		return durex.Empty(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	inst, _ := executor.Add(ctx, durex.Spec{Name: "flaky", Retries: 3})

	time.Sleep(time.Second)

	got, _ := executor.Get(ctx, inst.ID)
	if got.Status != durex.StatusCompleted {
		t.Errorf("Status = %s, want COMPLETED (after retries)", got.Status)
	}
	if got := attempts.Load(); got < 3 {
		t.Errorf("attempts = %d, want >= 3", got)
	}
}
