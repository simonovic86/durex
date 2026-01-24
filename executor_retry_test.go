package durex_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/simonovic86/durex"
	"github.com/simonovic86/durex/storage"
)

// TestRetry_CountDecrement verifies that retries count decrements correctly.
func TestRetry_CountDecrement(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(1))

	var mu sync.Mutex
	retriesObserved := []int{}
	done := make(chan struct{})

	executor.HandleFunc("retryCount", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		mu.Lock()
		retriesObserved = append(retriesObserved, cmd.Retries)
		if cmd.Retries == 0 {
			defer close(done)
		}
		mu.Unlock()
		return durex.Empty(), errors.New("always fail")
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	_, err := executor.Add(ctx, durex.Spec{
		Name:    "retryCount",
		Retries: 3,
	})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// Wait for all retries to complete
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for retries")
	}

	// Small sleep to let final state settle
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	// Should have 4 attempts: initial + 3 retries
	if len(retriesObserved) != 4 {
		t.Errorf("Expected 4 attempts, got %d: %v", len(retriesObserved), retriesObserved)
	}

	// Retries should decrement: 3, 2, 1, 0
	expected := []int{3, 2, 1, 0}
	for i, exp := range expected {
		if i < len(retriesObserved) && retriesObserved[i] != exp {
			t.Errorf("Attempt %d: expected retries=%d, got %d", i+1, exp, retriesObserved[i])
		}
	}
}

// TestRetry_AttemptIncrement verifies that attempt number increments correctly.
func TestRetry_AttemptIncrement(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(1))

	var mu sync.Mutex
	attemptsObserved := []int{}

	executor.HandleFunc("attemptIncrement", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		mu.Lock()
		attemptsObserved = append(attemptsObserved, cmd.Attempt)
		mu.Unlock()
		return durex.Empty(), errors.New("always fail")
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	_, err := executor.Add(ctx, durex.Spec{
		Name:    "attemptIncrement",
		Retries: 2,
	})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// Wait for all retries
	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	// Should have 3 attempts: initial + 2 retries
	// Attempts should be: 1, 2, 3
	expected := []int{1, 2, 3}
	if len(attemptsObserved) != len(expected) {
		t.Errorf("Expected %d attempts, got %d: %v", len(expected), len(attemptsObserved), attemptsObserved)
	}

	for i, exp := range expected {
		if i < len(attemptsObserved) && attemptsObserved[i] != exp {
			t.Errorf("Observed attempt %d: expected %d, got %d", i+1, exp, attemptsObserved[i])
		}
	}
}

// TestRetry_SuccessOnRetry verifies that a command can succeed after initial failures.
func TestRetry_SuccessOnRetry(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(1))

	attempts := atomic.Int32{}
	succeeded := atomic.Bool{}

	executor.HandleFunc("succeedOnRetry", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		attempt := attempts.Add(1)
		if attempt < 3 {
			return durex.Empty(), errors.New("not yet")
		}
		succeeded.Store(true)
		return durex.Empty(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	instance, err := executor.Add(ctx, durex.Spec{
		Name:    "succeedOnRetry",
		Retries: 5,
	})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// Wait for execution
	time.Sleep(300 * time.Millisecond)

	if !succeeded.Load() {
		t.Error("Command should have succeeded on retry")
	}

	if got := attempts.Load(); got != 3 {
		t.Errorf("Expected 3 attempts, got %d", got)
	}

	// Verify final status is completed
	final, err := store.Get(ctx, instance.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if final.Status != durex.StatusCompleted {
		t.Errorf("Expected COMPLETED status, got %s", final.Status)
	}
}

// TestRetry_ExhaustsRetries verifies that a command fails after exhausting retries.
func TestRetry_ExhaustsRetries(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(1))

	attempts := atomic.Int32{}

	executor.HandleFunc("exhaustRetries", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		attempts.Add(1)
		return durex.Empty(), errors.New("always fail")
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	instance, err := executor.Add(ctx, durex.Spec{
		Name:    "exhaustRetries",
		Retries: 2,
	})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// Wait for all retries
	time.Sleep(300 * time.Millisecond)

	// Should have 3 attempts: initial + 2 retries
	if got := attempts.Load(); got != 3 {
		t.Errorf("Expected 3 attempts, got %d", got)
	}

	// Verify final status is failed
	final, err := store.Get(ctx, instance.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if final.Status != durex.StatusFailed {
		t.Errorf("Expected FAILED status, got %s", final.Status)
	}

	if final.Error != "always fail" {
		t.Errorf("Expected error 'always fail', got %q", final.Error)
	}
}

// TestRetry_WithConstantBackoff verifies that backoff delays are applied.
func TestRetry_WithConstantBackoff(t *testing.T) {
	store := storage.NewMemory()
	backoffDelay := 100 * time.Millisecond

	executor := durex.New(store,
		durex.WithParallelism(1),
		durex.WithBackoff(durex.ConstantBackoff{Delay: backoffDelay}),
	)

	var mu sync.Mutex
	attemptTimes := []time.Time{}

	executor.HandleFunc("constantBackoff", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		mu.Lock()
		attemptTimes = append(attemptTimes, time.Now())
		mu.Unlock()
		return durex.Empty(), errors.New("fail for backoff test")
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	_, err := executor.Add(ctx, durex.Spec{
		Name:    "constantBackoff",
		Retries: 2,
	})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// Wait for retries with backoff
	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(attemptTimes) < 3 {
		t.Fatalf("Expected at least 3 attempts, got %d", len(attemptTimes))
	}

	// Verify delay between attempts
	for i := 1; i < len(attemptTimes); i++ {
		delay := attemptTimes[i].Sub(attemptTimes[i-1])
		// Allow 50ms tolerance
		minDelay := backoffDelay - 50*time.Millisecond
		if delay < minDelay {
			t.Errorf("Attempt %d to %d: delay %v < expected %v",
				i, i+1, delay, backoffDelay)
		}
	}
}

// TestRetry_WithLinearBackoff verifies linear backoff increases delay.
func TestRetry_WithLinearBackoff(t *testing.T) {
	store := storage.NewMemory()
	initialDelay := 50 * time.Millisecond

	executor := durex.New(store,
		durex.WithParallelism(1),
		durex.WithBackoff(durex.LinearBackoff{
			InitialDelay: initialDelay,
			MaxDelay:     500 * time.Millisecond,
		}),
	)

	var mu sync.Mutex
	attemptTimes := []time.Time{}

	executor.HandleFunc("linearBackoff", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		mu.Lock()
		attemptTimes = append(attemptTimes, time.Now())
		mu.Unlock()
		return durex.Empty(), errors.New("fail for linear backoff test")
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	_, err := executor.Add(ctx, durex.Spec{
		Name:    "linearBackoff",
		Retries: 3,
	})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// Wait for retries with increasing backoff
	time.Sleep(800 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(attemptTimes) < 4 {
		t.Fatalf("Expected at least 4 attempts, got %d", len(attemptTimes))
	}

	// Linear backoff: 50ms, 100ms, 150ms (initialDelay * attempt)
	expectedDelays := []time.Duration{
		50 * time.Millisecond,  // After attempt 1
		100 * time.Millisecond, // After attempt 2
		150 * time.Millisecond, // After attempt 3
	}

	for i := 1; i < len(attemptTimes); i++ {
		delay := attemptTimes[i].Sub(attemptTimes[i-1])
		expected := expectedDelays[i-1]
		minDelay := expected - 30*time.Millisecond // Tolerance

		if delay < minDelay {
			t.Errorf("Attempt %d: delay %v < expected ~%v", i, delay, expected)
		}
	}
}

// TestRetry_WithExponentialBackoff verifies exponential backoff doubles delay.
func TestRetry_WithExponentialBackoff(t *testing.T) {
	store := storage.NewMemory()
	initialDelay := 25 * time.Millisecond

	executor := durex.New(store,
		durex.WithParallelism(1),
		durex.WithBackoff(durex.ExponentialBackoff{
			InitialDelay: initialDelay,
			MaxDelay:     1 * time.Second,
			Multiplier:   2.0,
		}),
	)

	var mu sync.Mutex
	attemptTimes := []time.Time{}

	executor.HandleFunc("exponentialBackoff", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		mu.Lock()
		attemptTimes = append(attemptTimes, time.Now())
		mu.Unlock()
		return durex.Empty(), errors.New("fail for exponential backoff test")
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	_, err := executor.Add(ctx, durex.Spec{
		Name:    "exponentialBackoff",
		Retries: 3,
	})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// Wait for retries with exponential backoff: 25ms, 50ms, 100ms = 175ms minimum
	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(attemptTimes) < 4 {
		t.Fatalf("Expected at least 4 attempts, got %d", len(attemptTimes))
	}

	// Exponential backoff: 25ms, 50ms, 100ms
	expectedDelays := []time.Duration{
		25 * time.Millisecond,  // After attempt 1: 25 * 2^0
		50 * time.Millisecond,  // After attempt 2: 25 * 2^1
		100 * time.Millisecond, // After attempt 3: 25 * 2^2
	}

	for i := 1; i < len(attemptTimes); i++ {
		delay := attemptTimes[i].Sub(attemptTimes[i-1])
		expected := expectedDelays[i-1]
		minDelay := expected - 20*time.Millisecond // Tolerance

		if delay < minDelay {
			t.Errorf("Attempt %d: delay %v < expected ~%v", i, delay, expected)
		}
	}
}

// TestRetry_MaxDelayRespected verifies that MaxDelay caps the backoff.
func TestRetry_MaxDelayRespected(t *testing.T) {
	store := storage.NewMemory()
	maxDelay := 50 * time.Millisecond

	executor := durex.New(store,
		durex.WithParallelism(1),
		durex.WithBackoff(durex.ExponentialBackoff{
			InitialDelay: 100 * time.Millisecond, // Would be 100, 200, 400...
			MaxDelay:     maxDelay,               // But capped at 50
			Multiplier:   2.0,
		}),
	)

	var mu sync.Mutex
	attemptTimes := []time.Time{}

	executor.HandleFunc("maxDelay", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		mu.Lock()
		attemptTimes = append(attemptTimes, time.Now())
		mu.Unlock()
		return durex.Empty(), errors.New("fail")
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	_, err := executor.Add(ctx, durex.Spec{
		Name:    "maxDelay",
		Retries: 2,
	})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// With MaxDelay=50ms, should complete faster than without
	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(attemptTimes) < 3 {
		t.Fatalf("Expected at least 3 attempts, got %d", len(attemptTimes))
	}

	// All delays should be capped at ~maxDelay (with tolerance)
	for i := 1; i < len(attemptTimes); i++ {
		delay := attemptTimes[i].Sub(attemptTimes[i-1])
		maxExpected := maxDelay + 30*time.Millisecond // Tolerance

		if delay > maxExpected {
			t.Errorf("Attempt %d: delay %v > maxDelay %v", i, delay, maxDelay)
		}
	}
}

// TestRetry_NoBackoff verifies immediate retry with NoBackoff.
func TestRetry_NoBackoff(t *testing.T) {
	store := storage.NewMemory()

	executor := durex.New(store,
		durex.WithParallelism(1),
		durex.WithBackoff(durex.NoBackoff()),
	)

	var mu sync.Mutex
	attemptTimes := []time.Time{}

	executor.HandleFunc("noBackoff", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		mu.Lock()
		attemptTimes = append(attemptTimes, time.Now())
		mu.Unlock()
		return durex.Empty(), errors.New("fail")
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	_, err := executor.Add(ctx, durex.Spec{
		Name:    "noBackoff",
		Retries: 2,
	})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// Should complete very quickly with no backoff
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(attemptTimes) < 3 {
		t.Fatalf("Expected at least 3 attempts, got %d", len(attemptTimes))
	}

	// Delays should be minimal (essentially just execution time)
	totalTime := attemptTimes[len(attemptTimes)-1].Sub(attemptTimes[0])
	if totalTime > 100*time.Millisecond {
		t.Errorf("Total time %v too long for NoBackoff", totalTime)
	}
}

// TestRetry_ErrorPreserved verifies the error message is preserved in storage.
func TestRetry_ErrorPreserved(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(1))

	errorMsg := "this is the final error message"

	executor.HandleFunc("errorPreserved", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.Empty(), errors.New(errorMsg)
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	instance, err := executor.Add(ctx, durex.Spec{
		Name:    "errorPreserved",
		Retries: 0, // No retries, fail immediately
	})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	final, err := store.Get(ctx, instance.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if final.Status != durex.StatusFailed {
		t.Errorf("Expected FAILED, got %s", final.Status)
	}

	if final.Error != errorMsg {
		t.Errorf("Expected error %q, got %q", errorMsg, final.Error)
	}
}

// TestRetry_WithDefaultRetries verifies executor-level default retries.
func TestRetry_WithDefaultRetries(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store,
		durex.WithParallelism(1),
		durex.WithDefaultRetries(2), // Default 2 retries for all commands
	)

	attempts := atomic.Int32{}

	executor.HandleFunc("defaultRetries", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		attempts.Add(1)
		return durex.Empty(), errors.New("fail")
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	_, err := executor.Add(ctx, durex.Spec{
		Name: "defaultRetries",
		// No Retries specified, should use default
	})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(300 * time.Millisecond)

	// Should have 3 attempts: initial + 2 default retries
	if got := attempts.Load(); got != 3 {
		t.Errorf("Expected 3 attempts (1 + 2 default retries), got %d", got)
	}
}

// TestRetry_SpecOverridesDefault verifies spec retries override default.
func TestRetry_SpecOverridesDefault(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store,
		durex.WithParallelism(1),
		durex.WithDefaultRetries(5), // High default
	)

	attempts := atomic.Int32{}

	executor.HandleFunc("overrideDefault", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		attempts.Add(1)
		return durex.Empty(), errors.New("fail")
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	_, err := executor.Add(ctx, durex.Spec{
		Name:    "overrideDefault",
		Retries: 1, // Override with lower value
	})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Should have 2 attempts: initial + 1 spec retry
	if got := attempts.Load(); got != 2 {
		t.Errorf("Expected 2 attempts (1 + 1 spec retry), got %d", got)
	}
}
