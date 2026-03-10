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

func TestRateLimiter_PerCommandLimit(t *testing.T) {
	rl := durex.NewRateLimiter()
	rl.SetLimit("email", 2)

	ctx := context.Background()

	// Acquire 2 slots
	r1, err := rl.Acquire(ctx, "email")
	if err != nil {
		t.Fatalf("Acquire 1: %v", err)
	}
	r2, err := rl.Acquire(ctx, "email")
	if err != nil {
		t.Fatalf("Acquire 2: %v", err)
	}

	if got := rl.Current("email"); got != 2 {
		t.Errorf("Current = %d, want 2", got)
	}

	// Third should block; use TryAcquire to check non-blocking
	_, ok := rl.TryAcquire("email")
	if ok {
		t.Error("TryAcquire should have failed when at limit")
	}

	// Release one, then TryAcquire should succeed
	r1()
	r3, ok := rl.TryAcquire("email")
	if !ok {
		t.Error("TryAcquire should succeed after release")
	}
	r2()
	r3()

	if got := rl.Current("email"); got != 0 {
		t.Errorf("Current = %d, want 0 after all releases", got)
	}
}

func TestRateLimiter_GlobalLimit(t *testing.T) {
	rl := durex.NewRateLimiter()
	rl.SetGlobalLimit(3)

	// Acquire 3 across different commands
	r1, _ := rl.TryAcquire("a")
	r2, _ := rl.TryAcquire("b")
	r3, _ := rl.TryAcquire("c")

	if got := rl.GlobalCurrent(); got != 3 {
		t.Errorf("GlobalCurrent = %d, want 3", got)
	}

	_, ok := rl.TryAcquire("d")
	if ok {
		t.Error("TryAcquire should fail at global limit")
	}

	r1()
	r2()
	r3()
}

func TestRateLimiter_AcquireBlocks(t *testing.T) {
	rl := durex.NewRateLimiter()
	rl.SetLimit("task", 1)

	ctx := context.Background()
	release, _ := rl.Acquire(ctx, "task")

	acquired := make(chan struct{})
	go func() {
		r, err := rl.Acquire(ctx, "task")
		if err != nil {
			t.Errorf("Acquire: %v", err)
			return
		}
		close(acquired)
		r()
	}()

	// Should not have acquired yet
	select {
	case <-acquired:
		t.Fatal("Should not have acquired while slot is held")
	case <-time.After(50 * time.Millisecond):
	}

	release()

	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("Should have acquired after release")
	}
}

func TestRateLimiter_AcquireContextCancel(t *testing.T) {
	rl := durex.NewRateLimiter()
	rl.SetLimit("task", 1)

	ctx := context.Background()
	release, _ := rl.Acquire(ctx, "task")
	defer release()

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := rl.Acquire(cancelCtx, "task")
	if err == nil {
		t.Error("Expected error from cancelled context")
	}
}

func TestRateLimiter_RemoveLimit(t *testing.T) {
	rl := durex.NewRateLimiter()
	rl.SetLimit("task", 1)

	release, _ := rl.TryAcquire("task")
	_, ok := rl.TryAcquire("task")
	if ok {
		t.Error("Should fail at limit=1")
	}
	release()

	// Remove limit
	rl.SetLimit("task", 0)

	// Should be unlimited now
	var releases []func()
	for i := 0; i < 10; i++ {
		r, ok := rl.TryAcquire("task")
		if !ok {
			t.Errorf("TryAcquire %d should succeed with no limit", i)
		}
		releases = append(releases, r)
	}
	for _, r := range releases {
		r()
	}
}

func TestRateLimiter_Stats(t *testing.T) {
	rl := durex.NewRateLimiter()
	rl.SetLimit("email", 5)
	rl.SetGlobalLimit(10)

	r1, _ := rl.TryAcquire("email")
	r2, _ := rl.TryAcquire("email")

	stats := rl.Stats()
	if stats.GlobalLimit != 10 {
		t.Errorf("GlobalLimit = %d, want 10", stats.GlobalLimit)
	}
	if stats.GlobalCurrent != 2 {
		t.Errorf("GlobalCurrent = %d, want 2", stats.GlobalCurrent)
	}
	if cs, ok := stats.Commands["email"]; !ok {
		t.Error("Missing email in stats")
	} else {
		if cs.Limit != 5 {
			t.Errorf("email Limit = %d, want 5", cs.Limit)
		}
		if cs.Current != 2 {
			t.Errorf("email Current = %d, want 2", cs.Current)
		}
	}

	r1()
	r2()
}

func TestRateLimiter_Concurrent(t *testing.T) {
	rl := durex.NewRateLimiter()
	rl.SetLimit("task", 5)

	ctx := context.Background()
	var wg sync.WaitGroup
	var maxConcurrent atomic.Int32

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := rl.Acquire(ctx, "task")
			if err != nil {
				return
			}
			cur := int32(rl.Current("task"))
			for {
				old := maxConcurrent.Load()
				if cur <= old || maxConcurrent.CompareAndSwap(old, cur) {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
			release()
		}()
	}
	wg.Wait()

	if got := maxConcurrent.Load(); got > 5 {
		t.Errorf("Max concurrent = %d, want <= 5", got)
	}
}

func TestRateLimitMiddleware(t *testing.T) {
	store := storage.NewMemory()
	limiter := durex.NewRateLimiter()
	limiter.SetLimit("slow", 1)

	executor := durex.New(store,
		durex.WithParallelism(4),
		durex.WithMiddleware(durex.RateLimitMiddleware(limiter, 5*time.Second)),
	)

	var maxConcurrent atomic.Int32
	var currentCount atomic.Int32
	var totalExecuted atomic.Int32

	executor.HandleFunc("slow", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		cur := currentCount.Add(1)
		defer currentCount.Add(-1)
		totalExecuted.Add(1)

		for {
			old := maxConcurrent.Load()
			if cur <= old || maxConcurrent.CompareAndSwap(old, cur) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		return durex.Empty(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	for i := 0; i < 5; i++ {
		executor.Add(ctx, durex.Spec{Name: "slow"})
	}

	time.Sleep(500 * time.Millisecond)

	if got := maxConcurrent.Load(); got > 1 {
		t.Errorf("Max concurrent = %d, want <= 1 (rate limited)", got)
	}
	if got := totalExecuted.Load(); got < 5 {
		t.Errorf("Total executed = %d, want 5", got)
	}
}
