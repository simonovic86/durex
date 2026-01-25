package durex_test

import (
	"context"
	"fmt"
	"time"

	"github.com/simonovic86/durex"
	"github.com/simonovic86/durex/storage"
)

// GreetCommand sends a greeting.
type GreetCommand struct{}

func (c *GreetCommand) Name() string { return "greet" }

func (c *GreetCommand) Execute(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
	name := cmd.GetString("name")
	fmt.Printf("Hello, %s!\n", name)
	return durex.Empty(), nil
}

func Example() {
	// Create in-memory storage
	store := storage.NewMemory()

	// Create executor with options
	executor := durex.New(store,
		durex.WithParallelism(4),
		durex.WithDefaultRetries(3),
	)

	// Register command handlers
	executor.Register(&GreetCommand{})

	// Start processing
	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	// Add a command
	executor.Add(ctx, durex.Spec{
		Name: "greet",
		Data: durex.M{"name": "World"},
	})

	// Wait for processing
	time.Sleep(100 * time.Millisecond)
	// Output: Hello, World!
}

func ExampleExecutor_Add() {
	store := storage.NewMemory()
	executor := durex.New(store)
	executor.Register(&GreetCommand{})
	executor.Start(context.Background())
	defer executor.Stop()

	ctx := context.Background()

	// Add a simple command
	instance, _ := executor.Add(ctx, durex.Spec{
		Name: "greet",
		Data: durex.M{"name": "Alice"},
	})

	fmt.Println("Added command:", instance.Name)
	time.Sleep(100 * time.Millisecond)
	// Output:
	// Added command: greet
	// Hello, Alice!
}

func ExampleExecutor_Add_withRetries() {
	store := storage.NewMemory()
	executor := durex.New(store)
	executor.Register(&GreetCommand{})
	executor.Start(context.Background())
	defer executor.Stop()

	ctx := context.Background()

	// Add a command with retries and delay
	executor.Add(ctx, durex.Spec{
		Name:    "greet",
		Data:    durex.M{"name": "Bob"},
		Retries: 3,
		Delay:   time.Second,
	})
}

func ExampleExecutor_Add_withTags() {
	store := storage.NewMemory()
	executor := durex.New(store)
	executor.Register(&GreetCommand{})
	executor.Start(context.Background())
	defer executor.Stop()

	ctx := context.Background()

	// Add a command with tags for categorization
	executor.Add(ctx, durex.Spec{
		Name: "greet",
		Data: durex.M{"name": "Charlie"},
		Tags: []string{"priority:high", "batch:123"},
	})
}

func ExampleExecutor_HandleFunc() {
	store := storage.NewMemory()
	executor := durex.New(store)

	// Register using HandleFunc for simple handlers
	executor.HandleFunc("sendEmail", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		to := cmd.GetString("to")
		subject := cmd.GetString("subject")
		fmt.Printf("Sending email to %s: %s\n", to, subject)
		return durex.Empty(), nil
	})

	executor.Start(context.Background())
	defer executor.Stop()

	executor.Add(context.Background(), durex.Spec{
		Name: "sendEmail",
		Data: durex.M{
			"to":      "user@example.com",
			"subject": "Welcome!",
		},
	})

	time.Sleep(100 * time.Millisecond)
	// Output: Sending email to user@example.com: Welcome!
}

func ExampleNext() {
	store := storage.NewMemory()
	executor := durex.New(store)

	// Step 1 spawns Step 2
	executor.HandleFunc("step1", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		fmt.Println("Step 1 complete")
		return durex.Next(durex.Spec{
			Name: "step2",
			Data: durex.M{"from": "step1"},
		}), nil
	})

	executor.HandleFunc("step2", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		fmt.Println("Step 2 complete")
		return durex.Empty(), nil
	})

	executor.Start(context.Background())
	defer executor.Stop()

	executor.Add(context.Background(), durex.Spec{Name: "step1"})

	time.Sleep(100 * time.Millisecond)
	// Output:
	// Step 1 complete
	// Step 2 complete
}

func ExampleSpawn() {
	store := storage.NewMemory()
	executor := durex.New(store)

	// Fan-out: process multiple items in parallel
	executor.HandleFunc("fanOut", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		items := []string{"a", "b", "c"}
		specs := make([]durex.Spec, len(items))
		for i, item := range items {
			specs[i] = durex.Spec{
				Name: "process",
				Data: durex.M{"item": item},
			}
		}
		return durex.Spawn(specs...), nil
	})

	executor.HandleFunc("process", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		item := cmd.GetString("item")
		fmt.Printf("Processing: %s\n", item)
		return durex.Empty(), nil
	})

	executor.Start(context.Background())
	defer executor.Stop()

	executor.Add(context.Background(), durex.Spec{Name: "fanOut"})

	time.Sleep(100 * time.Millisecond)
}

func ExampleRepeat() {
	store := storage.NewMemory()
	executor := durex.New(store)

	count := 0
	executor.HandleFunc("heartbeat", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		count++
		fmt.Printf("Heartbeat #%d\n", count)
		if count >= 2 {
			return durex.Empty(), nil // Stop repeating
		}
		return durex.Repeat(), nil
	})

	executor.Start(context.Background())
	defer executor.Stop()

	// Add repeating command with 50ms period
	executor.Add(context.Background(), durex.Spec{
		Name:   "heartbeat",
		Period: 50 * time.Millisecond,
	})

	time.Sleep(150 * time.Millisecond)
	// Output:
	// Heartbeat #1
	// Heartbeat #2
}

func ExampleInstance_ContinueSequence() {
	store := storage.NewMemory()
	executor := durex.New(store)

	executor.HandleFunc("validate", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		fmt.Println("Validating order")
		cmd.Set("validated", true)
		return cmd.ContinueSequence(nil), nil
	})

	executor.HandleFunc("charge", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		fmt.Println("Charging payment")
		return cmd.ContinueSequence(nil), nil
	})

	executor.HandleFunc("ship", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		fmt.Println("Shipping order")
		return durex.Empty(), nil
	})

	executor.Start(context.Background())
	defer executor.Stop()

	// Create a sequence of commands
	executor.Add(context.Background(), durex.Spec{
		Name:     "validate",
		Sequence: []string{"charge", "ship"},
		Data:     durex.M{"order_id": "12345"},
	})

	time.Sleep(100 * time.Millisecond)
	// Output:
	// Validating order
	// Charging payment
	// Shipping order
}

func ExampleWithMiddleware() {
	store := storage.NewMemory()

	// Logging middleware
	loggingMiddleware := func(ctx durex.MiddlewareContext, next func() (durex.Result, error)) (durex.Result, error) {
		fmt.Printf("Starting: %s\n", ctx.Command.Name)
		result, err := next()
		fmt.Printf("Finished: %s\n", ctx.Command.Name)
		return result, err
	}

	executor := durex.New(store,
		durex.WithMiddleware(loggingMiddleware),
	)

	executor.HandleFunc("task", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		fmt.Println("Executing task")
		return durex.Empty(), nil
	})

	executor.Start(context.Background())
	defer executor.Stop()

	executor.Add(context.Background(), durex.Spec{Name: "task"})

	time.Sleep(100 * time.Millisecond)
	// Output:
	// Starting: task
	// Executing task
	// Finished: task
}
