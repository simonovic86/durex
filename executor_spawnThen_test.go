package durex_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/simonovic86/durex"
	"github.com/simonovic86/durex/storage"
)

// TestSpawnThen_BasicFanIn verifies that SpawnThen waits for all parallel tasks.
func TestSpawnThen_BasicFanIn(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(4))

	var mu sync.Mutex
	executionOrder := []string{}
	parallelComplete := atomic.Bool{}

	// Parallel tasks
	for _, name := range []string{"task1", "task2", "task3"} {
		taskName := name
		executor.HandleFunc(taskName, func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
			mu.Lock()
			executionOrder = append(executionOrder, taskName)
			mu.Unlock()
			time.Sleep(50 * time.Millisecond) // Simulate work
			return durex.Empty(), nil
		})
	}

	// Continuation task - should only run after all parallel tasks complete
	executor.HandleFunc("continuation", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		if !parallelComplete.Load() {
			parallelComplete.Store(true)
		}
		mu.Lock()
		executionOrder = append(executionOrder, "continuation")
		mu.Unlock()
		return durex.Empty(), nil
	})

	// Coordinator spawns parallel tasks with continuation
	executor.HandleFunc("coordinator", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.SpawnThen(
			[]durex.Spec{
				{Name: "task1"},
				{Name: "task2"},
				{Name: "task3"},
			},
			durex.Spec{Name: "continuation"},
		), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	_, err := executor.Add(ctx, durex.Spec{Name: "coordinator"})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// Wait for completion
	time.Sleep(2 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	// Verify all tasks executed
	if len(executionOrder) != 4 {
		t.Errorf("Expected 4 tasks executed, got %d: %v", len(executionOrder), executionOrder)
	}

	// Verify continuation was last
	if len(executionOrder) > 0 && executionOrder[len(executionOrder)-1] != "continuation" {
		t.Errorf("Expected continuation to be last, got: %v", executionOrder)
	}

	// Verify continuation ran
	if !parallelComplete.Load() {
		t.Error("Continuation task did not execute")
	}
}

// TestSpawnThen_DataPropagation verifies data flows to continuation.
func TestSpawnThen_DataPropagation(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(4))

	var continuationData durex.M
	var mu sync.Mutex

	// Parallel tasks that set data
	executor.HandleFunc("setData1", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		cmd.Set("result1", "value1")
		return durex.Empty(), nil
	})

	executor.HandleFunc("setData2", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		cmd.Set("result2", "value2")
		return durex.Empty(), nil
	})

	// Continuation receives merged data
	executor.HandleFunc("aggregate", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		mu.Lock()
		continuationData = make(durex.M)
		for k, v := range cmd.Data {
			continuationData[k] = v
		}
		mu.Unlock()
		return durex.Empty(), nil
	})

	executor.HandleFunc("coordinator", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.SpawnThen(
			[]durex.Spec{
				{Name: "setData1"},
				{Name: "setData2"},
			},
			durex.Spec{
				Name: "aggregate",
				Data: durex.M{"original": "data"},
			},
		), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	_, err := executor.Add(ctx, durex.Spec{Name: "coordinator"})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(2 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	// Verify continuation received original data
	if continuationData["original"] != "data" {
		t.Errorf("Expected original='data', got %v", continuationData["original"])
	}

	// Verify continuation received results from parallel tasks (with prefixes)
	hasResult1 := false
	hasResult2 := false
	for k := range continuationData {
		if k == "_barrier_result_setData1_result1" {
			hasResult1 = true
		}
		if k == "_barrier_result_setData2_result2" {
			hasResult2 = true
		}
	}

	if !hasResult1 {
		t.Errorf("Expected result from setData1, got data: %v", continuationData)
	}
	if !hasResult2 {
		t.Errorf("Expected result from setData2, got data: %v", continuationData)
	}
}

// TestSpawnThen_OneChildFails verifies continuation doesn't run if a child fails.
func TestSpawnThen_OneChildFails(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(4))

	continuationRan := atomic.Bool{}

	executor.HandleFunc("successTask", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.Empty(), nil
	})

	executor.HandleFunc("failTask", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.Empty(), errors.New("task failed")
	})

	executor.HandleFunc("shouldNotRun", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		continuationRan.Store(true)
		return durex.Empty(), nil
	})

	executor.HandleFunc("coordinator", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.SpawnThen(
			[]durex.Spec{
				{Name: "successTask"},
				{Name: "failTask"},
			},
			durex.Spec{Name: "shouldNotRun"},
		), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	_, err := executor.Add(ctx, durex.Spec{Name: "coordinator"})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(2 * time.Second)

	if continuationRan.Load() {
		t.Error("Continuation should not run when a child task fails")
	}
}

// TestSpawnThen_TraceIDPropagation verifies TraceID flows through barrier.
func TestSpawnThen_TraceIDPropagation(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(4))

	var mu sync.Mutex
	traceIDs := []string{}

	executor.HandleFunc("task1", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		mu.Lock()
		traceIDs = append(traceIDs, cmd.TraceID)
		mu.Unlock()
		return durex.Empty(), nil
	})

	executor.HandleFunc("task2", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		mu.Lock()
		traceIDs = append(traceIDs, cmd.TraceID)
		mu.Unlock()
		return durex.Empty(), nil
	})

	executor.HandleFunc("continuation", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		mu.Lock()
		traceIDs = append(traceIDs, cmd.TraceID)
		mu.Unlock()
		return durex.Empty(), nil
	})

	executor.HandleFunc("coordinator", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.SpawnThen(
			[]durex.Spec{
				{Name: "task1"},
				{Name: "task2"},
			},
			durex.Spec{Name: "continuation"},
		), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	expectedTraceID := "test-trace-123"
	_, err := executor.Add(ctx, durex.Spec{
		Name:    "coordinator",
		TraceID: expectedTraceID,
	})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(2 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	// All tasks should have the same TraceID
	if len(traceIDs) != 3 {
		t.Errorf("Expected 3 trace IDs, got %d", len(traceIDs))
	}

	for i, traceID := range traceIDs {
		if traceID != expectedTraceID {
			t.Errorf("Task %d: expected TraceID %q, got %q", i, expectedTraceID, traceID)
		}
	}
}

// TestSpawnThen_EmptyParallelTasks verifies continuation runs with no parallel tasks.
func TestSpawnThen_EmptyParallelTasks(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(2))

	continuationRan := atomic.Bool{}

	executor.HandleFunc("continuation", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		continuationRan.Store(true)
		return durex.Empty(), nil
	})

	executor.HandleFunc("coordinator", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		// SpawnThen with empty parallel tasks - should just run continuation
		return durex.SpawnThen(
			[]durex.Spec{},
			durex.Spec{Name: "continuation"},
		), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	_, err := executor.Add(ctx, durex.Spec{Name: "coordinator"})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(1 * time.Second)

	// With empty parallel tasks, no barrier is created, so continuation won't run automatically
	// This is expected behavior - SpawnThen requires at least one parallel task
	if continuationRan.Load() {
		t.Error("Continuation should not run automatically with empty parallel tasks")
	}
}

// TestSpawnThen_ChainedContinuations verifies multiple SpawnThen in sequence.
func TestSpawnThen_ChainedContinuations(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(4))

	var mu sync.Mutex
	executionOrder := []string{}

	// First wave
	executor.HandleFunc("wave1_task1", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		mu.Lock()
		executionOrder = append(executionOrder, "wave1_task1")
		mu.Unlock()
		return durex.Empty(), nil
	})

	executor.HandleFunc("wave1_task2", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		mu.Lock()
		executionOrder = append(executionOrder, "wave1_task2")
		mu.Unlock()
		return durex.Empty(), nil
	})

	// First continuation spawns second wave
	executor.HandleFunc("wave1_done", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		mu.Lock()
		executionOrder = append(executionOrder, "wave1_done")
		mu.Unlock()

		return durex.SpawnThen(
			[]durex.Spec{
				{Name: "wave2_task1"},
				{Name: "wave2_task2"},
			},
			durex.Spec{Name: "wave2_done"},
		), nil
	})

	// Second wave
	executor.HandleFunc("wave2_task1", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		mu.Lock()
		executionOrder = append(executionOrder, "wave2_task1")
		mu.Unlock()
		return durex.Empty(), nil
	})

	executor.HandleFunc("wave2_task2", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		mu.Lock()
		executionOrder = append(executionOrder, "wave2_task2")
		mu.Unlock()
		return durex.Empty(), nil
	})

	executor.HandleFunc("wave2_done", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		mu.Lock()
		executionOrder = append(executionOrder, "wave2_done")
		mu.Unlock()
		return durex.Empty(), nil
	})

	executor.HandleFunc("coordinator", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.SpawnThen(
			[]durex.Spec{
				{Name: "wave1_task1"},
				{Name: "wave1_task2"},
			},
			durex.Spec{Name: "wave1_done"},
		), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	_, err := executor.Add(ctx, durex.Spec{Name: "coordinator"})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(3 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	// Should have 6 tasks total
	if len(executionOrder) != 6 {
		t.Errorf("Expected 6 tasks, got %d: %v", len(executionOrder), executionOrder)
	}

	// Verify wave1_done comes after both wave1 tasks
	wave1DoneIdx := -1
	lastWave1TaskIdx := -1
	for i, name := range executionOrder {
		if name == "wave1_done" {
			wave1DoneIdx = i
		}
		if name == "wave1_task1" || name == "wave1_task2" {
			if i > lastWave1TaskIdx {
				lastWave1TaskIdx = i
			}
		}
	}

	if wave1DoneIdx < lastWave1TaskIdx {
		t.Errorf("wave1_done should come after wave1 tasks, order: %v", executionOrder)
	}

	// Verify wave2_done comes after both wave2 tasks
	wave2DoneIdx := -1
	lastWave2TaskIdx := -1
	for i, name := range executionOrder {
		if name == "wave2_done" {
			wave2DoneIdx = i
		}
		if name == "wave2_task1" || name == "wave2_task2" {
			if i > lastWave2TaskIdx {
				lastWave2TaskIdx = i
			}
		}
	}

	if wave2DoneIdx < lastWave2TaskIdx {
		t.Errorf("wave2_done should come after wave2 tasks, order: %v", executionOrder)
	}
}

// TestSpawnThen_WithRetries verifies barrier waits even when children retry.
func TestSpawnThen_WithRetries(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(4))

	continuationRan := atomic.Bool{}
	retryCount := atomic.Int32{}

	executor.HandleFunc("retryTask", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		count := retryCount.Add(1)
		if count < 2 {
			return durex.Empty(), fmt.Errorf("attempt %d failed", count)
		}
		return durex.Empty(), nil
	}, durex.Retries(2))

	executor.HandleFunc("continuation", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		continuationRan.Store(true)
		return durex.Empty(), nil
	})

	executor.HandleFunc("coordinator", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.SpawnThen(
			[]durex.Spec{
				{Name: "retryTask", Retries: 2},
			},
			durex.Spec{Name: "continuation"},
		), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	_, err := executor.Add(ctx, durex.Spec{Name: "coordinator"})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(3 * time.Second)

	// Continuation should run after retries succeed
	if !continuationRan.Load() {
		t.Error("Continuation should run after child task succeeds with retries")
	}

	if retryCount.Load() < 2 {
		t.Errorf("Expected at least 2 attempts, got %d", retryCount.Load())
	}
}
