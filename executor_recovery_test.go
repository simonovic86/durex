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

// TestRecovery_OnRecoverCalled verifies OnRecover is called after retries exhausted.
func TestRecovery_OnRecoverCalled(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(1))

	recoverCalled := atomic.Bool{}
	errorReceived := ""
	var mu sync.Mutex

	executor.HandleFunc("recoverTest", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.Empty(), errors.New("primary failure")
	},
		durex.Retries(2),
		durex.OnRecover(func(ctx context.Context, cmd *durex.Instance, err error) (durex.Result, error) {
			recoverCalled.Store(true)
			mu.Lock()
			errorReceived = err.Error()
			mu.Unlock()
			return durex.Empty(), nil
		}),
	)

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	_, err := executor.Add(ctx, durex.Spec{Name: "recoverTest"})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// Wait for execution and recovery
	time.Sleep(500 * time.Millisecond)

	if !recoverCalled.Load() {
		t.Error("OnRecover was not called")
	}

	mu.Lock()
	defer mu.Unlock()
	if errorReceived != "primary failure" {
		t.Errorf("Expected error 'primary failure', got %q", errorReceived)
	}
}

// TestRecovery_OnRecoverReceivesCommandData verifies recovery gets full command data.
func TestRecovery_OnRecoverReceivesCommandData(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(1))

	var mu sync.Mutex
	recoveredData := durex.M{}
	recoveredName := ""
	recoveredID := ""

	executor.HandleFunc("dataRecoverTest", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.Empty(), errors.New("fail with data")
	},
		durex.Retries(0),
		durex.OnRecover(func(ctx context.Context, cmd *durex.Instance, err error) (durex.Result, error) {
			mu.Lock()
			recoveredData = cmd.Data
			recoveredName = cmd.Name
			recoveredID = cmd.ID
			mu.Unlock()
			return durex.Empty(), nil
		}),
	)

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	instance, err := executor.Add(ctx, durex.Spec{
		Name: "dataRecoverTest",
		Data: durex.M{"user_id": "123", "action": "payment"},
	})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if recoveredData["user_id"] != "123" {
		t.Errorf("Expected user_id '123', got %v", recoveredData["user_id"])
	}
	if recoveredData["action"] != "payment" {
		t.Errorf("Expected action 'payment', got %v", recoveredData["action"])
	}
	if recoveredName != "dataRecoverTest" {
		t.Errorf("Expected name 'dataRecoverTest', got %q", recoveredName)
	}
	if recoveredID != instance.ID {
		t.Errorf("Expected ID %q, got %q", instance.ID, recoveredID)
	}
}

// TestRecovery_SpawnCompensationCommands verifies saga pattern compensation.
func TestRecovery_SpawnCompensationCommands(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(4))

	compensationExecuted := &sync.Map{}

	// Main command that fails
	executor.HandleFunc("processPayment", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.Empty(), errors.New("payment processor down")
	},
		durex.Retries(1),
		durex.OnRecover(func(ctx context.Context, cmd *durex.Instance, err error) (durex.Result, error) {
			// Spawn compensation commands (saga pattern)
			return durex.Spawn(
				durex.Spec{Name: "refundPayment", Data: cmd.Data},
				durex.Spec{Name: "releaseInventory", Data: cmd.Data},
				durex.Spec{Name: "notifyCustomer", Data: durex.M{
					"error":   err.Error(),
					"user_id": cmd.Data["user_id"],
				}},
			), nil
		}),
	)

	// Compensation commands
	executor.HandleFunc("refundPayment", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		compensationExecuted.Store("refundPayment", cmd.GetString("order_id"))
		return durex.Empty(), nil
	})

	executor.HandleFunc("releaseInventory", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		compensationExecuted.Store("releaseInventory", cmd.GetString("order_id"))
		return durex.Empty(), nil
	})

	executor.HandleFunc("notifyCustomer", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		compensationExecuted.Store("notifyCustomer", cmd.GetString("user_id"))
		compensationExecuted.Store("error_message", cmd.GetString("error"))
		return durex.Empty(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	_, err := executor.Add(ctx, durex.Spec{
		Name: "processPayment",
		Data: durex.M{"order_id": "order-789", "user_id": "user-456"},
	})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// Wait for main command retries and compensation
	time.Sleep(600 * time.Millisecond)

	// Verify all compensation commands executed
	if v, ok := compensationExecuted.Load("refundPayment"); !ok || v != "order-789" {
		t.Errorf("refundPayment not executed with correct data: %v", v)
	}
	if v, ok := compensationExecuted.Load("releaseInventory"); !ok || v != "order-789" {
		t.Errorf("releaseInventory not executed with correct data: %v", v)
	}
	if v, ok := compensationExecuted.Load("notifyCustomer"); !ok || v != "user-456" {
		t.Errorf("notifyCustomer not executed with correct data: %v", v)
	}
	if v, ok := compensationExecuted.Load("error_message"); !ok || v != "payment processor down" {
		t.Errorf("Error message not passed to notification: %v", v)
	}
}

// TestRecovery_NoRecoverIfSuccess verifies OnRecover not called on success.
func TestRecovery_NoRecoverIfSuccess(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(1))

	recoverCalled := atomic.Bool{}

	executor.HandleFunc("successNoRecover", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.Empty(), nil // Success
	},
		durex.OnRecover(func(ctx context.Context, cmd *durex.Instance, err error) (durex.Result, error) {
			recoverCalled.Store(true)
			return durex.Empty(), nil
		}),
	)

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	_, err := executor.Add(ctx, durex.Spec{Name: "successNoRecover"})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	if recoverCalled.Load() {
		t.Error("OnRecover should not be called on success")
	}
}

// TestRecovery_NoRecoverIfRetriesRemain verifies OnRecover only after exhaustion.
func TestRecovery_NoRecoverIfRetriesRemain(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(1))

	attempts := atomic.Int32{}
	recoverCalled := atomic.Bool{}

	executor.HandleFunc("retryBeforeRecover", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		attempt := attempts.Add(1)
		if attempt < 3 {
			return durex.Empty(), errors.New("temporary failure")
		}
		return durex.Empty(), nil // Succeed on 3rd attempt
	},
		durex.Retries(5), // More retries than needed
		durex.OnRecover(func(ctx context.Context, cmd *durex.Instance, err error) (durex.Result, error) {
			recoverCalled.Store(true)
			return durex.Empty(), nil
		}),
	)

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	_, err := executor.Add(ctx, durex.Spec{Name: "retryBeforeRecover"})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(400 * time.Millisecond)

	if got := attempts.Load(); got != 3 {
		t.Errorf("Expected 3 attempts, got %d", got)
	}

	if recoverCalled.Load() {
		t.Error("OnRecover should not be called when command eventually succeeds")
	}
}

// TestRecovery_WithStructCommand verifies recovery with Command struct.
func TestRecovery_WithStructCommand(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(1))

	recoverCalled := atomic.Bool{}

	// Register compensation command
	executor.HandleFunc("structCompensation", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.Empty(), nil
	})

	// Register main command
	cmd := &RecoverableCommand{
		onRecover: func(ctx context.Context, cmd *durex.Instance, err error) (durex.Result, error) {
			recoverCalled.Store(true)
			return durex.Spawn(durex.Spec{Name: "structCompensation"}), nil
		},
	}
	executor.Register(cmd)

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	_, err := executor.Add(ctx, durex.Spec{Name: "recoverableCmd"})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(300 * time.Millisecond)

	if !recoverCalled.Load() {
		t.Error("Recover should be called for struct command")
	}
}

// TestExpired_OnExpiredCalled verifies OnExpired is called when deadline passes.
func TestExpired_OnExpiredCalled(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(1))

	expiredCalled := atomic.Bool{}
	executeCalled := atomic.Bool{}

	executor.HandleFunc("expiredTest", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		executeCalled.Store(true)
		return durex.Empty(), nil
	},
		durex.OnExpired(func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
			expiredCalled.Store(true)
			return durex.Empty(), nil
		}),
	)

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	// Add with delay longer than deadline
	_, err := executor.Add(ctx, durex.Spec{
		Name:     "expiredTest",
		Delay:    200 * time.Millisecond,
		Deadline: 50 * time.Millisecond, // Deadline before delay completes
	})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(400 * time.Millisecond)

	if !expiredCalled.Load() {
		t.Error("OnExpired should be called when deadline passes")
	}

	// Execute might or might not be called depending on timing
	// The key test is that OnExpired IS called
}

// TestExpired_SpawnFollowUpCommands verifies expired can spawn new commands.
func TestExpired_SpawnFollowUpCommands(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(2))

	followUpExecuted := atomic.Bool{}

	executor.HandleFunc("willExpire", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		time.Sleep(500 * time.Millisecond) // Long-running (will be preempted by deadline check)
		return durex.Empty(), nil
	},
		durex.OnExpired(func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
			return durex.Spawn(durex.Spec{
				Name: "handleExpiredTask",
				Data: durex.M{"original_task": cmd.ID},
			}), nil
		}),
	)

	executor.HandleFunc("handleExpiredTask", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		followUpExecuted.Store(true)
		return durex.Empty(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	// Add with short deadline
	_, err := executor.Add(ctx, durex.Spec{
		Name:     "willExpire",
		Delay:    100 * time.Millisecond,
		Deadline: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(400 * time.Millisecond)

	if !followUpExecuted.Load() {
		t.Error("Follow-up command from OnExpired should execute")
	}
}

// TestRecovery_ErrorHandler verifies global error handler is called.
func TestRecovery_ErrorHandler(t *testing.T) {
	store := storage.NewMemory()

	errorHandlerCalled := atomic.Bool{}
	var handledError error
	var handledCmd *durex.Instance
	var mu sync.Mutex

	executor := durex.New(store,
		durex.WithParallelism(1),
		durex.WithErrorHandler(func(cmd *durex.Instance, err error) {
			errorHandlerCalled.Store(true)
			mu.Lock()
			handledError = err
			handledCmd = cmd
			mu.Unlock()
		}),
	)

	executor.HandleFunc("errorHandlerTest", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.Empty(), errors.New("global handler error")
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	_, err := executor.Add(ctx, durex.Spec{
		Name:    "errorHandlerTest",
		Retries: 0,
	})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	if !errorHandlerCalled.Load() {
		t.Error("Global error handler should be called")
	}

	mu.Lock()
	defer mu.Unlock()
	if handledError == nil || handledError.Error() != "global handler error" {
		t.Errorf("Expected 'global handler error', got %v", handledError)
	}
	if handledCmd == nil || handledCmd.Name != "errorHandlerTest" {
		t.Error("Error handler should receive the failed command")
	}
}

// TestRecovery_BothErrorHandlerAndOnRecover verifies both are called.
func TestRecovery_BothErrorHandlerAndOnRecover(t *testing.T) {
	store := storage.NewMemory()

	errorHandlerCalled := atomic.Bool{}
	onRecoverCalled := atomic.Bool{}

	executor := durex.New(store,
		durex.WithParallelism(1),
		durex.WithErrorHandler(func(cmd *durex.Instance, err error) {
			errorHandlerCalled.Store(true)
		}),
	)

	executor.HandleFunc("bothHandlers", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.Empty(), errors.New("fail")
	},
		durex.Retries(0),
		durex.OnRecover(func(ctx context.Context, cmd *durex.Instance, err error) (durex.Result, error) {
			onRecoverCalled.Store(true)
			return durex.Empty(), nil
		}),
	)

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	_, err := executor.Add(ctx, durex.Spec{Name: "bothHandlers"})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	if !errorHandlerCalled.Load() {
		t.Error("Global error handler should be called")
	}
	if !onRecoverCalled.Load() {
		t.Error("OnRecover should be called")
	}
}

// TestRecovery_CompensationChain verifies multi-level compensation.
func TestRecovery_CompensationChain(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(4))

	var mu sync.Mutex
	executionOrder := []string{}

	executor.HandleFunc("step1", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		mu.Lock()
		executionOrder = append(executionOrder, "step1")
		mu.Unlock()
		return cmd.ContinueSequence(nil), nil
	})

	executor.HandleFunc("step2", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		mu.Lock()
		executionOrder = append(executionOrder, "step2")
		mu.Unlock()
		return cmd.ContinueSequence(nil), nil
	})

	// Step 3 fails and triggers compensation
	executor.HandleFunc("step3", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		mu.Lock()
		executionOrder = append(executionOrder, "step3-fail")
		mu.Unlock()
		return durex.Empty(), errors.New("step 3 failed")
	},
		durex.Retries(0),
		durex.OnRecover(func(ctx context.Context, cmd *durex.Instance, err error) (durex.Result, error) {
			mu.Lock()
			executionOrder = append(executionOrder, "step3-recover")
			mu.Unlock()
			// Compensate for steps 1 and 2
			return durex.Spawn(
				durex.Spec{Name: "undoStep2"},
				durex.Spec{Name: "undoStep1"},
			), nil
		}),
	)

	executor.HandleFunc("undoStep2", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		mu.Lock()
		executionOrder = append(executionOrder, "undoStep2")
		mu.Unlock()
		return durex.Empty(), nil
	})

	executor.HandleFunc("undoStep1", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		mu.Lock()
		executionOrder = append(executionOrder, "undoStep1")
		mu.Unlock()
		return durex.Empty(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	_, err := executor.Add(ctx, durex.Spec{
		Name:     "step1",
		Sequence: []string{"step2", "step3"},
	})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(600 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	// Verify we have the expected events
	expectedEvents := map[string]bool{
		"step1":         true,
		"step2":         true,
		"step3-fail":    true,
		"step3-recover": true,
		"undoStep1":     true,
		"undoStep2":     true,
	}

	for _, event := range executionOrder {
		delete(expectedEvents, event)
	}

	if len(expectedEvents) > 0 {
		t.Errorf("Missing events: %v. Got order: %v", expectedEvents, executionOrder)
	}
}

// RecoverableCommand is a test command struct that implements Recoverable.
type RecoverableCommand struct {
	durex.BaseCommand
	onRecover func(ctx context.Context, cmd *durex.Instance, err error) (durex.Result, error)
}

func (c *RecoverableCommand) Name() string { return "recoverableCmd" }

func (c *RecoverableCommand) Execute(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
	return durex.Empty(), errors.New("intentional failure")
}

func (c *RecoverableCommand) Recover(ctx context.Context, cmd *durex.Instance, err error) (durex.Result, error) {
	if c.onRecover != nil {
		return c.onRecover(ctx, cmd, err)
	}
	return durex.Empty(), nil
}
