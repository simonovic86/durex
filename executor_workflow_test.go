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

// TestWorkflow_SequenceExecution verifies that sequences execute in order.
func TestWorkflow_SequenceExecution(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(1))

	var mu sync.Mutex
	executionOrder := []string{}

	for _, name := range []string{"step1", "step2", "step3", "step4"} {
		cmdName := name
		executor.HandleFunc(cmdName, func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
			mu.Lock()
			executionOrder = append(executionOrder, cmdName)
			mu.Unlock()
			return cmd.ContinueSequence(nil), nil
		})
	}

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	_, err := executor.Add(ctx, durex.Spec{
		Name:     "step1",
		Sequence: []string{"step2", "step3", "step4"},
	})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	expected := []string{"step1", "step2", "step3", "step4"}
	if len(executionOrder) != len(expected) {
		t.Errorf("Expected %d steps, got %d: %v", len(expected), len(executionOrder), executionOrder)
	}

	for i, exp := range expected {
		if i < len(executionOrder) && executionOrder[i] != exp {
			t.Errorf("Step %d: expected %s, got %s", i, exp, executionOrder[i])
		}
	}
}

// TestWorkflow_DataPassesBetweenSteps verifies data flows through the sequence.
func TestWorkflow_DataPassesBetweenSteps(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(1))

	var mu sync.Mutex
	dataAtEachStep := make(map[string]durex.M)

	// Step 1: Set initial value
	executor.HandleFunc("dataStep1", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		mu.Lock()
		dataAtEachStep["step1"] = copyMap(cmd.Data)
		mu.Unlock()

		// Add data for next step
		return cmd.ContinueSequence(durex.M{"step1_result": "completed"}), nil
	})

	// Step 2: Read step1 data, add more
	executor.HandleFunc("dataStep2", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		mu.Lock()
		dataAtEachStep["step2"] = copyMap(cmd.Data)
		mu.Unlock()

		// Verify step1 data is present
		if cmd.GetString("step1_result") != "completed" {
			t.Error("Step2: step1_result not found")
		}

		return cmd.ContinueSequence(durex.M{"step2_result": "done"}), nil
	})

	// Step 3: Read all previous data
	executor.HandleFunc("dataStep3", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		mu.Lock()
		dataAtEachStep["step3"] = copyMap(cmd.Data)
		mu.Unlock()

		// Verify all previous data is present
		if cmd.GetString("initial") != "data" {
			t.Error("Step3: initial data not found")
		}
		if cmd.GetString("step1_result") != "completed" {
			t.Error("Step3: step1_result not found")
		}
		if cmd.GetString("step2_result") != "done" {
			t.Error("Step3: step2_result not found")
		}

		return durex.Empty(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	_, err := executor.Add(ctx, durex.Spec{
		Name:     "dataStep1",
		Sequence: []string{"dataStep2", "dataStep3"},
		Data:     durex.M{"initial": "data"},
	})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	// Verify step3 has accumulated all data
	step3Data := dataAtEachStep["step3"]
	if step3Data == nil {
		t.Fatal("Step3 did not execute")
	}

	if step3Data["initial"] != "data" {
		t.Error("Initial data not preserved through sequence")
	}
	if step3Data["step1_result"] != "completed" {
		t.Error("Step1 result not passed to step3")
	}
	if step3Data["step2_result"] != "done" {
		t.Error("Step2 result not passed to step3")
	}
}

// TestWorkflow_SetAndGetWithinStep verifies Set/Get within steps.
func TestWorkflow_SetAndGetWithinStep(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(1))

	var mu sync.Mutex
	receivedValue := ""

	executor.HandleFunc("setStep", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		// Use Set to modify data
		cmd.Set("computed_value", "calculated_result")
		return cmd.ContinueSequence(nil), nil
	})

	executor.HandleFunc("getStep", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		mu.Lock()
		receivedValue = cmd.GetString("computed_value")
		mu.Unlock()
		return durex.Empty(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	_, err := executor.Add(ctx, durex.Spec{
		Name:     "setStep",
		Sequence: []string{"getStep"},
	})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if receivedValue != "calculated_result" {
		t.Errorf("Expected 'calculated_result', got %q", receivedValue)
	}
}

// TestWorkflow_TraceIDPropagation verifies TraceID flows through sequence.
func TestWorkflow_TraceIDPropagation(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(1))

	var mu sync.Mutex
	traceIDsObserved := []string{}

	for _, name := range []string{"trace1", "trace2", "trace3"} {
		cmdName := name
		executor.HandleFunc(cmdName, func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
			mu.Lock()
			traceIDsObserved = append(traceIDsObserved, cmd.TraceID)
			mu.Unlock()
			return cmd.ContinueSequence(nil), nil
		})
	}

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	traceID := "trace-abc-123"
	_, err := executor.Add(ctx, durex.Spec{
		Name:     "trace1",
		Sequence: []string{"trace2", "trace3"},
		TraceID:  traceID,
	})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(400 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(traceIDsObserved) != 3 {
		t.Fatalf("Expected 3 observations, got %d", len(traceIDsObserved))
	}

	for i, observed := range traceIDsObserved {
		if observed != traceID {
			t.Errorf("Step %d: expected TraceID %q, got %q", i+1, traceID, observed)
		}
	}
}

// TestWorkflow_CorrelationIDPropagation verifies CorrelationID flows through.
func TestWorkflow_CorrelationIDPropagation(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(1))

	var mu sync.Mutex
	correlationIDsObserved := []string{}

	for _, name := range []string{"corr1", "corr2", "corr3"} {
		cmdName := name
		executor.HandleFunc(cmdName, func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
			mu.Lock()
			correlationIDsObserved = append(correlationIDsObserved, cmd.CorrelationID)
			mu.Unlock()
			return cmd.ContinueSequence(nil), nil
		})
	}

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	correlationID := "order-456"
	_, err := executor.Add(ctx, durex.Spec{
		Name:          "corr1",
		Sequence:      []string{"corr2", "corr3"},
		CorrelationID: correlationID,
	})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(400 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(correlationIDsObserved) != 3 {
		t.Fatalf("Expected 3 observations, got %d", len(correlationIDsObserved))
	}

	for i, observed := range correlationIDsObserved {
		if observed != correlationID {
			t.Errorf("Step %d: expected CorrelationID %q, got %q", i+1, correlationID, observed)
		}
	}
}

// TestWorkflow_EmptySequence verifies ContinueSequence with empty sequence.
func TestWorkflow_EmptySequence(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(1))

	completed := atomic.Bool{}

	executor.HandleFunc("emptySeq", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		result := cmd.ContinueSequence(nil)
		// Should return empty result for empty sequence
		if len(result.Commands) != 0 {
			t.Errorf("Expected 0 spawned commands for empty sequence, got %d", len(result.Commands))
		}
		completed.Store(true)
		return result, nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	instance, err := executor.Add(ctx, durex.Spec{
		Name:     "emptySeq",
		Sequence: nil, // Empty sequence
	})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	if !completed.Load() {
		t.Error("Command should have completed")
	}

	// Verify status is completed
	final, err := store.Get(ctx, instance.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if final.Status != durex.StatusCompleted {
		t.Errorf("Expected COMPLETED, got %s", final.Status)
	}
}

// TestWorkflow_SpawnMultipleChildren verifies Spawn creates multiple children.
func TestWorkflow_SpawnMultipleChildren(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(4))

	childCount := atomic.Int32{}
	childDataReceived := &sync.Map{}

	executor.HandleFunc("spawner", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.Spawn(
			durex.Spec{Name: "spawnedChild", Data: durex.M{"index": 1}},
			durex.Spec{Name: "spawnedChild", Data: durex.M{"index": 2}},
			durex.Spec{Name: "spawnedChild", Data: durex.M{"index": 3}},
		), nil
	})

	executor.HandleFunc("spawnedChild", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		childCount.Add(1)
		idx := cmd.GetInt("index")
		childDataReceived.Store(idx, true)
		return durex.Empty(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	parentInstance, err := executor.Add(ctx, durex.Spec{Name: "spawner"})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(400 * time.Millisecond)

	if got := childCount.Load(); got != 3 {
		t.Errorf("Expected 3 children executed, got %d", got)
	}

	// Verify all indices were received
	for i := 1; i <= 3; i++ {
		if _, ok := childDataReceived.Load(i); !ok {
			t.Errorf("Child with index %d was not executed", i)
		}
	}

	// Verify children have parent reference
	children, err := store.FindByParent(ctx, parentInstance.ID)
	if err != nil {
		t.Fatalf("FindByParent failed: %v", err)
	}

	if len(children) != 3 {
		t.Errorf("Expected 3 children in storage, got %d", len(children))
	}
}

// TestWorkflow_ChildInheritsTraceID verifies spawned children inherit tracing.
func TestWorkflow_ChildInheritsTraceID(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(2))

	var mu sync.Mutex
	childTraceIDs := []string{}

	executor.HandleFunc("traceParent", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.Spawn(
			durex.Spec{Name: "traceChild"},
			durex.Spec{Name: "traceChild"},
		), nil
	})

	executor.HandleFunc("traceChild", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		mu.Lock()
		childTraceIDs = append(childTraceIDs, cmd.TraceID)
		mu.Unlock()
		return durex.Empty(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	traceID := "parent-trace-xyz"
	_, err := executor.Add(ctx, durex.Spec{
		Name:    "traceParent",
		TraceID: traceID,
	})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(childTraceIDs) != 2 {
		t.Fatalf("Expected 2 children, got %d", len(childTraceIDs))
	}

	for i, childTrace := range childTraceIDs {
		if childTrace != traceID {
			t.Errorf("Child %d: expected TraceID %q, got %q", i, traceID, childTrace)
		}
	}
}

// TestWorkflow_SequenceReducesCorrectly verifies sequence shrinks at each step.
func TestWorkflow_SequenceReducesCorrectly(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(1))

	var mu sync.Mutex
	sequenceLengths := []int{}

	for _, name := range []string{"reduce1", "reduce2", "reduce3", "reduce4"} {
		cmdName := name
		executor.HandleFunc(cmdName, func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
			mu.Lock()
			sequenceLengths = append(sequenceLengths, len(cmd.Sequence))
			mu.Unlock()
			return cmd.ContinueSequence(nil), nil
		})
	}

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	_, err := executor.Add(ctx, durex.Spec{
		Name:     "reduce1",
		Sequence: []string{"reduce2", "reduce3", "reduce4"},
	})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	// Expected sequence lengths: 3, 2, 1, 0
	expected := []int{3, 2, 1, 0}
	if len(sequenceLengths) != len(expected) {
		t.Fatalf("Expected %d steps, got %d", len(expected), len(sequenceLengths))
	}

	for i, exp := range expected {
		if sequenceLengths[i] != exp {
			t.Errorf("Step %d: expected sequence length %d, got %d",
				i+1, exp, sequenceLengths[i])
		}
	}
}

// TestWorkflow_SequenceFailsAtStep verifies sequence stops on error.
func TestWorkflow_SequenceFailsAtStep(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(1))

	executedSteps := atomic.Int32{}

	executor.HandleFunc("failSeq1", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		executedSteps.Add(1)
		return cmd.ContinueSequence(nil), nil
	})

	executor.HandleFunc("failSeq2", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		executedSteps.Add(1)
		return durex.Empty(), errors.New("step 2 failed")
	})

	executor.HandleFunc("failSeq3", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		executedSteps.Add(1) // Should not reach here
		return durex.Empty(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	_, err := executor.Add(ctx, durex.Spec{
		Name:     "failSeq1",
		Sequence: []string{"failSeq2", "failSeq3"},
	})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(300 * time.Millisecond)

	// Only steps 1 and 2 should execute (step 2 fails)
	if got := executedSteps.Load(); got != 2 {
		t.Errorf("Expected 2 steps executed, got %d", got)
	}
}

// Helper function to copy map
func copyMap(m durex.M) durex.M {
	if m == nil {
		return nil
	}
	result := make(durex.M)
	for k, v := range m {
		result[k] = v
	}
	return result
}
