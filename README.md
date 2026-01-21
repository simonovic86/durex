# Durex

[![Go Reference](https://pkg.go.dev/badge/github.com/simonovic86/durex.svg)](https://pkg.go.dev/github.com/simonovic86/durex)
[![Go Report Card](https://goreportcard.com/badge/github.com/simonovic86/durex)](https://goreportcard.com/report/github.com/simonovic86/durex)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Durable Execution Framework for Go**

Durex enables you to build reliable, persistent command/task execution systems with automatic retries, deadlines, and recovery from failures. It's inspired by patterns from job queues, workflow engines, and saga orchestrators.

## Features

- 🔄 **Persistent Commands** - Commands survive process restarts
- 🔁 **Automatic Retries** - Configurable retry logic with customizable handling
- ⏰ **Deadlines** - Time-bound execution with expiration handlers
- 🔗 **Command Chaining** - Build workflows with sequences
- 🛡️ **Recovery** - Custom error handling and compensation patterns
- 🔌 **Middleware** - Extensible execution pipeline
- 💾 **Multiple Backends** - PostgreSQL, SQLite, In-Memory

## Installation

```bash
go get github.com/simonovic86/durex
```

## Quick Start

### 1. Define a Command

```go
type SendEmailCommand struct {
    durex.BaseCommand
    mailer *MailService
}

func (c *SendEmailCommand) Name() string {
    return "sendEmail"
}

func (c *SendEmailCommand) Execute(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
    to := cmd.GetString("to")
    subject := cmd.GetString("subject")
    body := cmd.GetString("body")

    if err := c.mailer.Send(to, subject, body); err != nil {
        return durex.Empty(), err  // Will retry if retries > 0
    }

    return durex.Empty(), nil
}

// Optional: handle permanent failures
func (c *SendEmailCommand) Recover(ctx context.Context, cmd *durex.Instance, err error) (durex.Result, error) {
    log.Printf("Email to %s failed permanently: %v", cmd.GetString("to"), err)
    return durex.Empty(), nil
}

// Optional: provide defaults
func (c *SendEmailCommand) Default() durex.Spec {
    return durex.Spec{
        Retries: 3,
    }
}
```

### 2. Create an Executor

```go
import (
    "github.com/simonovic86/durex"
    "github.com/simonovic86/durex/storage"
)

// Use in-memory storage for development
store := storage.NewMemory()

// Or PostgreSQL for production
// db, _ := sql.Open("postgres", "postgres://...")
// store := storage.NewPostgres(db)
// store.Migrate(ctx)

executor := durex.New(store,
    durex.WithParallelism(4),
    durex.WithDefaultRetries(3),
    durex.WithLogger(slog.Default()),
)

executor.Register(&SendEmailCommand{mailer: mailerService})
executor.Start(ctx)
defer executor.Stop()
```

### 3. Add Commands

```go
// Simple command
executor.Add(ctx, durex.Spec{
    Name: "sendEmail",
    Data: durex.M{
        "to":      "user@example.com",
        "subject": "Welcome!",
        "body":    "Thanks for signing up.",
    },
})

// Delayed command
executor.Add(ctx, durex.Spec{
    Name:  "sendEmail",
    Delay: 5 * time.Minute,
    Data:  durex.M{"to": "user@example.com"},
})

// Command with deadline
executor.Add(ctx, durex.Spec{
    Name:     "processOrder",
    Deadline: 30 * time.Second,
    Data:     durex.M{"orderId": "12345"},
})
```

## Command Results

Commands return a `Result` that tells the executor what to do next:

| Result | Description |
|--------|-------------|
| `durex.Empty()` | Command completed, no follow-up |
| `durex.Repeat()` | Reschedule to run again after Period |
| `durex.Retry()` | Retry immediately (uses retry counter) |
| `durex.Next(spec)` | Spawn a single follow-up command |
| `durex.Spawn(specs...)` | Spawn multiple commands |

## Command Chaining (Workflows)

Build multi-step workflows by chaining commands:

```go
executor.Add(ctx, durex.Spec{
    Name:     "validateOrder",
    Sequence: []string{"processPayment", "shipOrder", "sendConfirmation"},
    Data:     durex.M{"orderId": "12345"},
})
```

Each command continues the sequence:

```go
func (c *ValidateOrderCommand) Execute(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
    // Validate...
    cmd.Set("validated", true)  // Data is passed to next command
    return cmd.ContinueSequence(nil), nil
}
```

## Repeating Commands

Create commands that run on a schedule:

```go
type CleanupCommand struct {
    durex.BaseCommand
}

func (c *CleanupCommand) Execute(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
    // Cleanup logic...
    return durex.Repeat(), nil  // Run again after Period
}

func (c *CleanupCommand) Default() durex.Spec {
    return durex.Spec{
        Period: time.Hour,
    }
}
```

## Error Handling & Recovery

Implement the `Recoverable` interface for custom error handling:

```go
func (c *ProcessPaymentCommand) Recover(ctx context.Context, cmd *durex.Instance, err error) (durex.Result, error) {
    // Spawn compensation commands (saga pattern)
    return durex.Spawn(
        durex.Spec{Name: "refundPayment", Data: cmd.Data},
        durex.Spec{Name: "notifyFailure", Data: durex.M{"error": err.Error()}},
    ), nil
}
```

## Deadlines

Set execution deadlines and handle expiration:

```go
executor.Add(ctx, durex.Spec{
    Name:     "timeoutSensitiveTask",
    Deadline: 5 * time.Minute,
})

// Implement Expirable to handle timeout
func (c *MyCommand) Expired(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
    log.Printf("Command %s expired", cmd.ID)
    return durex.Next(durex.Spec{Name: "handleTimeout"}), nil
}
```

## Middleware

Add cross-cutting concerns:

```go
executor := durex.New(store,
    durex.WithMiddleware(
        // Logging middleware
        func(ctx durex.MiddlewareContext, next func() (durex.Result, error)) (durex.Result, error) {
            start := time.Now()
            result, err := next()
            slog.Info("Command executed",
                "name", ctx.Command.Name,
                "duration", time.Since(start),
            )
            return result, err
        },
        // Metrics middleware
        func(ctx durex.MiddlewareContext, next func() (durex.Result, error)) (durex.Result, error) {
            metrics.CommandStarted(ctx.Command.Name)
            result, err := next()
            if err != nil {
                metrics.CommandFailed(ctx.Command.Name)
            } else {
                metrics.CommandCompleted(ctx.Command.Name)
            }
            return result, err
        },
    ),
)
```

## Storage Backends

### In-Memory (Testing/Development)

```go
store := storage.NewMemory()
```

### SQLite (Single Instance)

```go
store, err := storage.OpenSQLite("commands.db")
if err != nil {
    log.Fatal(err)
}
store.Migrate(ctx)
```

### PostgreSQL (Production)

```go
db, err := sql.Open("postgres", "postgres://user:pass@localhost/db")
if err != nil {
    log.Fatal(err)
}

store := storage.NewPostgres(db)
store.Migrate(ctx)
```

## Configuration Options

```go
executor := durex.New(store,
    durex.WithParallelism(8),              // Worker count
    durex.WithLogger(slog.Default()),       // Custom logger
    durex.WithDefaultRetries(3),            // Default retry count
    durex.WithDefaultRepeatInterval(time.Minute), // Default repeat period
    durex.WithMaxDelay(24 * time.Hour),     // Max scheduling delay
    durex.WithCleanupInterval(time.Hour),   // Auto-cleanup interval
    durex.WithCleanupAge(7 * 24 * time.Hour), // Cleanup age threshold
    durex.WithGracefulShutdown(30 * time.Second), // Shutdown timeout
    durex.WithPermanentCommands("cleanup", "monitor"), // Always-running commands
    durex.WithErrorHandler(func(cmd *durex.Instance, err error) {
        alerting.Notify(err)
    }),
)
```

## Command Instance Data Access

```go
func (c *MyCommand) Execute(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
    // Type-safe accessors
    str := cmd.GetString("key")
    num := cmd.GetInt("count")
    flag := cmd.GetBool("enabled")
    list := cmd.GetSlice("items")
    obj := cmd.GetMap("nested")

    // Set data (for passing to sequence)
    cmd.Set("result", "value")

    // Check metadata
    if cmd.IsOverdue() { ... }
    if cmd.HasTag("priority") { ... }

    return durex.Empty(), nil
}
```

## Best Practices

1. **Idempotency**: Design commands to be safely re-executable
2. **Small Commands**: Keep commands focused on single responsibilities
3. **Proper Retries**: Use retries for transient failures, not logic errors
4. **Deadlines**: Set realistic deadlines for time-sensitive operations
5. **Compensation**: Implement Recover for proper rollback/cleanup
6. **Monitoring**: Use middleware for observability

## Examples

See the [examples](./examples) directory for complete working examples:

- `basic/` - Simple commands with retries and delays
- `workflow/` - E-commerce order processing pipeline

## License

MIT License - see [LICENSE](LICENSE) for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
