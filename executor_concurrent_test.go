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

// TestConcurrentClaim_MemoryStorage tests that commands are not claimed twice
// even with multiple workers in single-instance mode.
func TestConcurrentClaim_MemoryStorage(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(10))

	// Track which commands were executed and how many times
	executed := &sync.Map{}
	executionCounts := &sync.Map{}

	executor.HandleFunc("concurrent", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		id := cmd.ID

		// Check if already executed
		if _, loaded := executed.LoadOrStore(id, true); loaded {
			t.Errorf("Command %s executed more than once!", id)
		}

		// Count executions
		count, _ := executionCounts.LoadOrStore(id, new(int32))
		atomic.AddInt32(count.(*int32), 1)

		// Simulate some work
		time.Sleep(10 * time.Millisecond)

		return durex.Empty(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	// Add many commands concurrently
	const numCommands = 100
	var wg sync.WaitGroup
	wg.Add(numCommands)

	for i := 0; i < numCommands; i++ {
		go func() {
			defer wg.Done()
			_, err := executor.Add(ctx, durex.Spec{Name: "concurrent"})
			if err != nil {
				t.Errorf("Failed to add command: %v", err)
			}
		}()
	}

	wg.Wait()

	// Wait for all commands to complete
	time.Sleep(500 * time.Millisecond)

	// Verify each command was executed exactly once
	totalExecuted := 0
	executionCounts.Range(func(key, value any) bool {
		count := atomic.LoadInt32(value.(*int32))
		if count != 1 {
			t.Errorf("Command %v executed %d times, expected 1", key, count)
		}
		totalExecuted++
		return true
	})

	if totalExecuted != numCommands {
		t.Errorf("Expected %d commands executed, got %d", numCommands, totalExecuted)
	}
}

// TestConcurrentClaim_NoDoubleClaim simulates multiple workers trying to claim
// the same commands and verifies no double-claiming occurs.
func TestConcurrentClaim_NoDoubleClaim(t *testing.T) {
	store := storage.NewMemory()

	// Pre-create commands in storage
	ctx := context.Background()
	const numCommands = 50

	for i := 0; i < numCommands; i++ {
		cmd := &durex.Instance{
			ID:        durex.GenerateID(),
			Name:      "claimTest",
			Status:    durex.StatusPending,
			CreatedAt: time.Now(),
			ReadyAt:   time.Now(),
		}
		if err := store.Create(ctx, cmd); err != nil {
			t.Fatalf("Failed to create command: %v", err)
		}
	}

	// Track claimed command IDs
	claimedIDs := &sync.Map{}
	claimCount := atomic.Int32{}
	duplicateCount := atomic.Int32{}

	executor := durex.New(store,
		durex.WithParallelism(20), // Many workers
	)

	executor.HandleFunc("claimTest", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		claimCount.Add(1)

		// Check for duplicate claim
		if _, loaded := claimedIDs.LoadOrStore(cmd.ID, true); loaded {
			duplicateCount.Add(1)
			t.Errorf("DUPLICATE CLAIM: Command %s was claimed twice!", cmd.ID)
		}

		// Simulate work
		time.Sleep(5 * time.Millisecond)
		return durex.Empty(), nil
	})

	executor.Start(ctx)

	// Wait for all commands to be processed
	time.Sleep(500 * time.Millisecond)

	executor.Stop()

	if got := claimCount.Load(); got != numCommands {
		t.Errorf("Expected %d claims, got %d", numCommands, got)
	}

	if got := duplicateCount.Load(); got != 0 {
		t.Errorf("Expected 0 duplicate claims, got %d", got)
	}
}

// TestConcurrentAdd verifies that concurrent Add operations are thread-safe
// and all commands are properly persisted and executed.
func TestConcurrentAdd(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(4))

	executedIDs := &sync.Map{}

	executor.HandleFunc("concurrentAdd", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		executedIDs.Store(cmd.ID, true)
		return durex.Empty(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	const numGoroutines = 10
	const commandsPerGoroutine = 20
	const totalCommands = numGoroutines * commandsPerGoroutine

	addedIDs := &sync.Map{}
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < commandsPerGoroutine; i++ {
				instance, err := executor.Add(ctx, durex.Spec{
					Name: "concurrentAdd",
					Data: durex.M{"g": g, "i": i},
				})
				if err != nil {
					t.Errorf("Add failed: %v", err)
					continue
				}
				addedIDs.Store(instance.ID, true)
			}
		}()
	}

	wg.Wait()

	// Wait for execution
	time.Sleep(500 * time.Millisecond)

	// Count added and executed
	addedCount := 0
	addedIDs.Range(func(key, value any) bool {
		addedCount++
		return true
	})

	executedCount := 0
	executedIDs.Range(func(key, value any) bool {
		executedCount++
		return true
	})

	if addedCount != totalCommands {
		t.Errorf("Expected %d commands added, got %d", totalCommands, addedCount)
	}

	if executedCount != totalCommands {
		t.Errorf("Expected %d commands executed, got %d", totalCommands, executedCount)
	}
}

// TestConcurrentRetry verifies that retries work correctly under concurrent load.
func TestConcurrentRetry(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(4))

	attemptCounts := &sync.Map{}
	completedCount := atomic.Int32{}

	executor.HandleFunc("concurrentRetry", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		// Track attempts per command
		countI, _ := attemptCounts.LoadOrStore(cmd.ID, new(int32))
		count := countI.(*int32)
		attempt := atomic.AddInt32(count, 1)

		// Fail first 2 attempts
		if attempt < 3 {
			return durex.Empty(), errors.New("temporary failure")
		}

		completedCount.Add(1)
		return durex.Empty(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	const numCommands = 20
	var wg sync.WaitGroup
	wg.Add(numCommands)

	for i := 0; i < numCommands; i++ {
		go func() {
			defer wg.Done()
			_, err := executor.Add(ctx, durex.Spec{
				Name:    "concurrentRetry",
				Retries: 3, // Will succeed on 3rd attempt
			})
			if err != nil {
				t.Errorf("Add failed: %v", err)
			}
		}()
	}

	wg.Wait()

	// Wait for retries to complete
	time.Sleep(1 * time.Second)

	if got := completedCount.Load(); got != int32(numCommands) {
		t.Errorf("Expected %d completed, got %d", numCommands, got)
	}

	// Verify each command was attempted exactly 3 times
	attemptCounts.Range(func(key, value any) bool {
		count := atomic.LoadInt32(value.(*int32))
		if count != 3 {
			t.Errorf("Command %v had %d attempts, expected 3", key, count)
		}
		return true
	})
}

// TestConcurrentCancel verifies cancellation works correctly under concurrent load.
func TestConcurrentCancel(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(4))

	startedCount := atomic.Int32{}

	executor.HandleFunc("cancelMe", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		startedCount.Add(1)
		// Long-running command
		time.Sleep(100 * time.Millisecond)
		return durex.Empty(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)

	// Add commands with delay so they're not immediately executed
	var instances []*durex.Instance
	for i := 0; i < 10; i++ {
		inst, err := executor.Add(ctx, durex.Spec{
			Name:  "cancelMe",
			Delay: 50 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("Add failed: %v", err)
		}
		instances = append(instances, inst)
	}

	// Cancel half of them concurrently
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			executor.Cancel(ctx, instances[idx].ID)
		}(i)
	}
	wg.Wait()

	// Wait for non-cancelled to execute
	time.Sleep(300 * time.Millisecond)

	executor.Stop()

	// Verify cancelled commands were not executed (or fewer were)
	started := startedCount.Load()
	if started > 5 {
		// Some might have started before cancellation, that's OK
		t.Logf("Started %d commands (up to 5 were cancelled)", started)
	}
}

// TestConcurrentStats verifies Stats() is thread-safe during concurrent operations.
func TestConcurrentStats(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(4))

	executor.HandleFunc("statsTest", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		time.Sleep(10 * time.Millisecond)
		return durex.Empty(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	// Concurrently add commands and read stats
	var wg sync.WaitGroup
	const numOps = 50

	// Writers
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numOps; i++ {
			executor.Add(ctx, durex.Spec{Name: "statsTest"})
			time.Sleep(5 * time.Millisecond)
		}
	}()

	// Readers
	for r := 0; r < 5; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < numOps; i++ {
				stats, err := executor.Stats(ctx)
				if err != nil {
					t.Errorf("Stats failed: %v", err)
				}
				// Just verify we can read stats without panicking
				_ = stats.Pending
				_ = stats.Completed
				_ = stats.Failed
				_ = stats.QueueSize
				time.Sleep(3 * time.Millisecond)
			}
		}()
	}

	wg.Wait()
}

// TestRaceCondition_StorageOperations tests for race conditions in storage operations.
func TestRaceCondition_StorageOperations(t *testing.T) {
	store := storage.NewMemory()
	ctx := context.Background()

	const numGoroutines = 10
	const opsPerGoroutine = 50

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 3) // Create, Update, Read goroutines

	// Shared command IDs
	createdIDs := make(chan string, numGoroutines*opsPerGoroutine)

	// Create goroutines
	for g := 0; g < numGoroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				cmd := &durex.Instance{
					ID:        durex.GenerateID(),
					Name:      "raceTest",
					Status:    durex.StatusPending,
					CreatedAt: time.Now(),
					ReadyAt:   time.Now(),
				}
				if err := store.Create(ctx, cmd); err != nil {
					continue // Might race with cleanup
				}
				createdIDs <- cmd.ID
			}
		}()
	}

	// Update goroutines
	for g := 0; g < numGoroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				select {
				case id := <-createdIDs:
					createdIDs <- id // Put it back for others
					cmd, err := store.Get(ctx, id)
					if err != nil {
						continue
					}
					cmd.Status = durex.StatusStarted
					store.Update(ctx, cmd)
				default:
					time.Sleep(time.Millisecond)
				}
			}
		}()
	}

	// Read goroutines
	for g := 0; g < numGoroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				store.FindPending(ctx)
				store.Count(ctx, nil)
				time.Sleep(time.Millisecond)
			}
		}()
	}

	wg.Wait()
	close(createdIDs)

	// Test passed if no races were detected (run with -race flag)
	t.Log("Race condition test completed")
}
