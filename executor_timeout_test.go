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

// TimeoutTestCommand is a test command for timeout tests.
type TimeoutTestCommand struct {
	durex.BaseCommand
	name       string
	executeFn  func(ctx context.Context, cmd *durex.Instance) (durex.Result, error)
}

func (c *TimeoutTestCommand) Name() string { return c.name }

func (c *TimeoutTestCommand) Execute(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
	if c.executeFn != nil {
		return c.executeFn(ctx, cmd)
	}
	return durex.Empty(), nil
}

// TestTimeout_CommandExceedsTimeout tests that commands exceeding their timeout are failed.
func TestTimeout_CommandExceedsTimeout(t *testing.T) {
	store := storage.NewMemory()
	var handlerCalled atomic.Bool
	var ctxCancelled atomic.Bool

	cmd := &TimeoutTestCommand{
		name: "slow-task",
		executeFn: func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
			handlerCalled.Store(true)
			select {
			case <-ctx.Done():
				ctxCancelled.Store(true)
				return durex.Empty(), ctx.Err()
			case <-time.After(5 * time.Second):
				return durex.Empty(), nil
			}
		},
	}

	executor := durex.New(store)
	executor.Register(cmd)

	ctx := context.Background()
	if err := executor.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer executor.Stop()

	// Add command with 100ms timeout
	_, err := executor.Add(ctx, durex.Spec{
		Name:    "slow-task",
		Timeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Wait for execution
	time.Sleep(300 * time.Millisecond)

	if !handlerCalled.Load() {
		t.Error("handler was not called")
	}
	if !ctxCancelled.Load() {
		t.Error("context was not cancelled when timeout exceeded")
	}
}

// TestTimeout_CommandCompletesWithinTimeout tests that commands completing within timeout succeed.
func TestTimeout_CommandCompletesWithinTimeout(t *testing.T) {
	store := storage.NewMemory()
	var completed atomic.Bool

	cmd := &TimeoutTestCommand{
		name: "fast-task",
		executeFn: func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
			time.Sleep(10 * time.Millisecond)
			completed.Store(true)
			return durex.Empty(), nil
		},
	}

	executor := durex.New(store)
	executor.Register(cmd)

	ctx := context.Background()
	if err := executor.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer executor.Stop()

	// Add command with 1s timeout
	instance, err := executor.Add(ctx, durex.Spec{
		Name:    "fast-task",
		Timeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Wait for execution
	time.Sleep(100 * time.Millisecond)

	if !completed.Load() {
		t.Error("handler did not complete")
	}

	// Check status
	result, err := store.Get(ctx, instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != durex.StatusCompleted {
		t.Errorf("expected status %v, got %v", durex.StatusCompleted, result.Status)
	}
}

// TestTimeout_DefaultTimeout tests that default timeout is applied.
func TestTimeout_DefaultTimeout(t *testing.T) {
	store := storage.NewMemory()
	var ctxCancelled atomic.Bool

	cmd := &TimeoutTestCommand{
		name: "slow-task",
		executeFn: func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
			select {
			case <-ctx.Done():
				ctxCancelled.Store(true)
				return durex.Empty(), ctx.Err()
			case <-time.After(5 * time.Second):
				return durex.Empty(), nil
			}
		},
	}

	executor := durex.New(store, durex.WithDefaultTimeout(100*time.Millisecond))
	executor.Register(cmd)

	ctx := context.Background()
	if err := executor.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer executor.Stop()

	// Add command without timeout (should use default)
	_, err := executor.Add(ctx, durex.Spec{
		Name: "slow-task",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Wait for execution
	time.Sleep(300 * time.Millisecond)

	if !ctxCancelled.Load() {
		t.Error("context was not cancelled (default timeout not applied)")
	}
}

// TestTimeout_SpecOverridesDefault tests that spec timeout overrides default.
func TestTimeout_SpecOverridesDefault(t *testing.T) {
	store := storage.NewMemory()
	var completed atomic.Bool

	cmd := &TimeoutTestCommand{
		name: "medium-task",
		executeFn: func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
			// Takes 50ms
			time.Sleep(50 * time.Millisecond)
			completed.Store(true)
			return durex.Empty(), nil
		},
	}

	executor := durex.New(store, durex.WithDefaultTimeout(10*time.Millisecond))
	executor.Register(cmd)

	ctx := context.Background()
	if err := executor.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer executor.Stop()

	// Add command with longer timeout than default
	_, err := executor.Add(ctx, durex.Spec{
		Name:    "medium-task",
		Timeout: time.Second, // Override the 10ms default
	})
	if err != nil {
		t.Fatal(err)
	}

	// Wait for execution
	time.Sleep(200 * time.Millisecond)

	if !completed.Load() {
		t.Error("handler did not complete (spec timeout should have overridden default)")
	}
}

// TestTimeout_RetryAfterTimeout tests that commands are retried after timeout.
func TestTimeout_RetryAfterTimeout(t *testing.T) {
	store := storage.NewMemory()
	var attempts atomic.Int32

	cmd := &TimeoutTestCommand{
		name: "flaky-task",
		executeFn: func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
			attempt := attempts.Add(1)
			if attempt == 1 {
				// First attempt: timeout
				<-ctx.Done()
				return durex.Empty(), ctx.Err()
			}
			// Second attempt: succeed
			return durex.Empty(), nil
		},
	}

	executor := durex.New(store)
	executor.Register(cmd)

	ctx := context.Background()
	if err := executor.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer executor.Stop()

	instance, err := executor.Add(ctx, durex.Spec{
		Name:    "flaky-task",
		Timeout: 50 * time.Millisecond,
		Retries: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Wait for both attempts
	time.Sleep(400 * time.Millisecond)

	if attempts.Load() != 2 {
		t.Errorf("expected 2 attempts, got %d", attempts.Load())
	}

	// Check final status
	result, err := store.Get(ctx, instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != durex.StatusCompleted {
		t.Errorf("expected status %v, got %v", durex.StatusCompleted, result.Status)
	}
}

// TestPanicRecovery tests that panics in handlers are recovered.
func TestPanicRecovery(t *testing.T) {
	store := storage.NewMemory()
	var errorHandlerCalled atomic.Bool
	var errorMessage string
	var mu sync.Mutex

	cmd := &TimeoutTestCommand{
		name: "panic-task",
		executeFn: func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
			panic("test panic")
		},
	}

	executor := durex.New(store, durex.WithErrorHandler(func(cmd *durex.Instance, err error) {
		mu.Lock()
		errorHandlerCalled.Store(true)
		errorMessage = err.Error()
		mu.Unlock()
	}))
	executor.Register(cmd)

	ctx := context.Background()
	if err := executor.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer executor.Stop()

	instance, err := executor.Add(ctx, durex.Spec{
		Name: "panic-task",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Wait for execution
	time.Sleep(100 * time.Millisecond)

	// Check that error handler was called
	mu.Lock()
	if !errorHandlerCalled.Load() {
		t.Error("error handler was not called after panic")
	}
	if errorMessage != "panic: test panic" {
		t.Errorf("expected panic message, got: %s", errorMessage)
	}
	mu.Unlock()

	// Check that command is marked as failed
	result, err := store.Get(ctx, instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != durex.StatusFailed {
		t.Errorf("expected status %v, got %v", durex.StatusFailed, result.Status)
	}
	if result.Error != "panic: test panic" {
		t.Errorf("expected error 'panic: test panic', got '%s'", result.Error)
	}
}

// TestPanicRecovery_WorkerContinues tests that workers continue after panic.
func TestPanicRecovery_WorkerContinues(t *testing.T) {
	store := storage.NewMemory()
	var normalTaskExecuted atomic.Bool

	panicCmd := &TimeoutTestCommand{
		name: "panic-task",
		executeFn: func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
			panic("test panic")
		},
	}

	normalCmd := &TimeoutTestCommand{
		name: "normal-task",
		executeFn: func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
			normalTaskExecuted.Store(true)
			return durex.Empty(), nil
		},
	}

	executor := durex.New(store)
	executor.Register(panicCmd)
	executor.Register(normalCmd)

	ctx := context.Background()
	if err := executor.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer executor.Stop()

	// Add panic task first
	_, err := executor.Add(ctx, durex.Spec{Name: "panic-task"})
	if err != nil {
		t.Fatal(err)
	}

	// Wait for panic to be handled
	time.Sleep(50 * time.Millisecond)

	// Add normal task - worker should still be running
	_, err = executor.Add(ctx, durex.Spec{Name: "normal-task"})
	if err != nil {
		t.Fatal(err)
	}

	// Wait for normal task
	time.Sleep(100 * time.Millisecond)

	if !normalTaskExecuted.Load() {
		t.Error("normal task was not executed after panic recovery")
	}
}
