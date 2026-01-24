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

// TestCommand is a simple test command.
type TestCommand struct {
	durex.BaseCommand
	name      string
	executed  atomic.Int32
	executeFn func(ctx context.Context, cmd *durex.Instance) (durex.Result, error)
}

func (c *TestCommand) Name() string { return c.name }

func (c *TestCommand) Execute(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
	c.executed.Add(1)
	if c.executeFn != nil {
		return c.executeFn(ctx, cmd)
	}
	return durex.Empty(), nil
}

func TestExecutor_BasicExecution(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(1))

	cmd := &TestCommand{name: "testCmd"}
	executor.Register(cmd)

	ctx := context.Background()
	if err := executor.Start(ctx); err != nil {
		t.Fatalf("Failed to start executor: %v", err)
	}
	defer executor.Stop()

	_, err := executor.Add(ctx, durex.Spec{
		Name: "testCmd",
		Data: durex.M{"key": "value"},
	})
	if err != nil {
		t.Fatalf("Failed to add command: %v", err)
	}

	// Wait for execution
	time.Sleep(100 * time.Millisecond)

	if got := cmd.executed.Load(); got != 1 {
		t.Errorf("Expected 1 execution, got %d", got)
	}
}

func TestExecutor_Retry(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(1))

	attempts := atomic.Int32{}
	cmd := &TestCommand{
		name: "retryCmd",
		executeFn: func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
			count := attempts.Add(1)
			if count < 3 {
				return durex.Empty(), errors.New("temporary error")
			}
			return durex.Empty(), nil
		},
	}
	executor.Register(cmd)

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	_, err := executor.Add(ctx, durex.Spec{
		Name:    "retryCmd",
		Retries: 3,
	})
	if err != nil {
		t.Fatalf("Failed to add command: %v", err)
	}

	// Wait for retries
	time.Sleep(500 * time.Millisecond)

	if got := attempts.Load(); got != 3 {
		t.Errorf("Expected 3 attempts, got %d", got)
	}
}

func TestExecutor_Repeat(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(1))

	executions := atomic.Int32{}
	cmd := &TestCommand{
		name: "repeatCmd",
		executeFn: func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
			executions.Add(1)
			return durex.Repeat(), nil
		},
	}
	executor.Register(cmd)

	ctx := context.Background()
	executor.Start(ctx)

	_, err := executor.Add(ctx, durex.Spec{
		Name:   "repeatCmd",
		Period: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Failed to add command: %v", err)
	}

	// Wait for multiple executions
	time.Sleep(200 * time.Millisecond)
	executor.Stop()

	if got := executions.Load(); got < 2 {
		t.Errorf("Expected at least 2 executions, got %d", got)
	}
}

func TestExecutor_Sequence(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(1))

	var order []string
	var mu sync.Mutex

	for _, name := range []string{"step1", "step2", "step3"} {
		cmdName := name
		cmd := &TestCommand{
			name: cmdName,
			executeFn: func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
				mu.Lock()
				order = append(order, cmdName)
				mu.Unlock()
				return cmd.ContinueSequence(nil), nil
			},
		}
		executor.Register(cmd)
	}

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	_, err := executor.Add(ctx, durex.Spec{
		Name:     "step1",
		Sequence: []string{"step2", "step3"},
	})
	if err != nil {
		t.Fatalf("Failed to add command: %v", err)
	}

	// Wait for sequence
	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(order) != 3 {
		t.Errorf("Expected 3 steps, got %d: %v", len(order), order)
	}

	expected := []string{"step1", "step2", "step3"}
	for i, name := range expected {
		if i < len(order) && order[i] != name {
			t.Errorf("Step %d: expected %s, got %s", i, name, order[i])
		}
	}
}

func TestExecutor_Delay(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(1))

	var mu sync.Mutex
	var executedAt time.Time
	cmd := &TestCommand{
		name: "delayedCmd",
		executeFn: func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
			mu.Lock()
			executedAt = time.Now()
			mu.Unlock()
			return durex.Empty(), nil
		},
	}
	executor.Register(cmd)

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	startTime := time.Now()
	_, err := executor.Add(ctx, durex.Spec{
		Name:  "delayedCmd",
		Delay: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Failed to add command: %v", err)
	}

	// Wait for delayed execution
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	execAt := executedAt
	mu.Unlock()

	if execAt.IsZero() {
		t.Error("Command was not executed")
		return
	}

	delay := execAt.Sub(startTime)
	if delay < 90*time.Millisecond {
		t.Errorf("Command executed too early: %v", delay)
	}
}

func TestExecutor_SpawnChildren(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(2))

	childExecuted := atomic.Int32{}

	parentCmd := &TestCommand{
		name: "parent",
		executeFn: func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
			return durex.Spawn(
				durex.Spec{Name: "child", Data: durex.M{"n": 1}},
				durex.Spec{Name: "child", Data: durex.M{"n": 2}},
				durex.Spec{Name: "child", Data: durex.M{"n": 3}},
			), nil
		},
	}

	childCmd := &TestCommand{
		name: "child",
		executeFn: func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
			childExecuted.Add(1)
			return durex.Empty(), nil
		},
	}

	executor.Register(parentCmd)
	executor.Register(childCmd)

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	_, err := executor.Add(ctx, durex.Spec{Name: "parent"})
	if err != nil {
		t.Fatalf("Failed to add command: %v", err)
	}

	// Wait for children
	time.Sleep(200 * time.Millisecond)

	if got := childExecuted.Load(); got != 3 {
		t.Errorf("Expected 3 child executions, got %d", got)
	}
}

func TestInstance_DataAccessors(t *testing.T) {
	instance := &durex.Instance{
		Data: durex.M{
			"string": "hello",
			"int":    42,
			"float":  3.14,
			"bool":   true,
			"slice":  []any{"a", "b"},
			"map":    map[string]any{"nested": "value"},
		},
	}

	if got := instance.GetString("string"); got != "hello" {
		t.Errorf("GetString: expected 'hello', got %q", got)
	}

	if got := instance.GetInt("int"); got != 42 {
		t.Errorf("GetInt: expected 42, got %d", got)
	}

	if got := instance.GetBool("bool"); got != true {
		t.Errorf("GetBool: expected true, got %v", got)
	}

	if got := instance.GetSlice("slice"); len(got) != 2 {
		t.Errorf("GetSlice: expected 2 elements, got %d", len(got))
	}

	if got := instance.GetMap("map"); got["nested"] != "value" {
		t.Errorf("GetMap: expected nested value, got %v", got)
	}

	// Test missing keys return zero values
	if got := instance.GetString("missing"); got != "" {
		t.Errorf("GetString(missing): expected empty, got %q", got)
	}

	if got := instance.GetInt("missing"); got != 0 {
		t.Errorf("GetInt(missing): expected 0, got %d", got)
	}
}

func TestInstance_Set(t *testing.T) {
	instance := &durex.Instance{}

	instance.Set("key", "value")

	if instance.Data == nil {
		t.Error("Data should be initialized")
	}

	if got := instance.GetString("key"); got != "value" {
		t.Errorf("Expected 'value', got %q", got)
	}
}

func TestInstance_ContinueSequence(t *testing.T) {
	instance := &durex.Instance{
		Data:     durex.M{"existing": "data"},
		Sequence: []string{"next", "final"},
	}

	result := instance.ContinueSequence(durex.M{"added": "new"})

	if len(result.Commands) != 1 {
		t.Fatalf("Expected 1 command, got %d", len(result.Commands))
	}

	cmd := result.Commands[0]
	if cmd.Name != "next" {
		t.Errorf("Expected name 'next', got %q", cmd.Name)
	}

	if len(cmd.Sequence) != 1 || cmd.Sequence[0] != "final" {
		t.Errorf("Sequence not properly reduced: %v", cmd.Sequence)
	}

	if cmd.Data["existing"] != "data" {
		t.Error("Existing data not preserved")
	}

	if cmd.Data["added"] != "new" {
		t.Error("Additional data not added")
	}
}

func TestInstance_ContinueSequence_Empty(t *testing.T) {
	instance := &durex.Instance{
		Data:     durex.M{"data": "value"},
		Sequence: nil,
	}

	result := instance.ContinueSequence(nil)

	if len(result.Commands) != 0 {
		t.Errorf("Expected empty result for empty sequence, got %d commands", len(result.Commands))
	}
}

func TestStatus_IsTerminal(t *testing.T) {
	terminalStatuses := []durex.Status{
		durex.StatusCompleted,
		durex.StatusFailed,
		durex.StatusExpired,
		durex.StatusCancelled,
	}

	for _, s := range terminalStatuses {
		if !s.IsTerminal() {
			t.Errorf("%s should be terminal", s)
		}
	}

	activeStatuses := []durex.Status{
		durex.StatusPending,
		durex.StatusStarted,
		durex.StatusRepeating,
	}

	for _, s := range activeStatuses {
		if s.IsTerminal() {
			t.Errorf("%s should not be terminal", s)
		}
	}
}

func TestStatus_IsActive(t *testing.T) {
	activeStatuses := []durex.Status{
		durex.StatusPending,
		durex.StatusStarted,
		durex.StatusRepeating,
	}

	for _, s := range activeStatuses {
		if !s.IsActive() {
			t.Errorf("%s should be active", s)
		}
	}
}

func TestSpec_WithMethods(t *testing.T) {
	spec := durex.Spec{Name: "test"}

	spec = spec.WithData(durex.M{"key": "value"})
	if spec.Data["key"] != "value" {
		t.Error("WithData failed")
	}

	spec = spec.WithDelay(5 * time.Second)
	if spec.Delay != 5*time.Second {
		t.Error("WithDelay failed")
	}

	spec = spec.WithRetries(3)
	if spec.Retries != 3 {
		t.Error("WithRetries failed")
	}

	spec = spec.WithPriority(10)
	if spec.Priority != 10 {
		t.Error("WithPriority failed")
	}

	spec = spec.WithTags("urgent", "important")
	if len(spec.Tags) != 2 {
		t.Error("WithTags failed")
	}
}
