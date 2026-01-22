# Durex

[![Go Reference](https://pkg.go.dev/badge/github.com/simonovic86/durex.svg)](https://pkg.go.dev/github.com/simonovic86/durex)
[![Go Report Card](https://goreportcard.com/badge/github.com/simonovic86/durex)](https://goreportcard.com/report/github.com/simonovic86/durex)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Durable Execution Framework for Go**

Durex enables you to build reliable, persistent command/task execution systems with automatic retries, deadlines, and recovery from failures.

## Features

- 🔄 **Persistent Commands** - Commands survive process restarts
- 🔁 **Automatic Retries** - Configurable retry logic
- ⏰ **Deadlines** - Time-bound execution with expiration handlers
- 🔗 **Workflows** - Chain commands together with sequences
- 🛡️ **Recovery** - Custom error handling and compensation (saga pattern)
- 🎯 **Type Safety** - Generic typed commands with `HandleTyped[T]`
- 💾 **Multiple Backends** - PostgreSQL, SQLite, In-Memory

## Architecture

```mermaid
graph TB
    subgraph Your Application
        A[App Code]
    end
    
    subgraph Durex
        B[Executor]
        C[Command Registry]
        D[Worker Pool]
    end
    
    subgraph Storage Backends
        E[(PostgreSQL)]
        F[(SQLite)]
        G[(Memory)]
    end
    
    A -->|Add/HandleFunc| B
    B --> C
    B --> D
    D -->|Execute| C
    B <-->|Persist/Fetch| E
    B <-->|Persist/Fetch| F
    B <-->|Persist/Fetch| G
```

## Installation

```bash
go get github.com/simonovic86/durex
```

## Quick Start

```go
package main

import (
    "context"
    "github.com/simonovic86/durex"
    "github.com/simonovic86/durex/storage"
)

func main() {
    // Create executor
    executor := durex.New(storage.NewMemory())

    // Register a command - just a function!
    executor.HandleFunc("greet", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
        name := cmd.GetString("name")
        fmt.Printf("Hello, %s!\n", name)
        return durex.Empty(), nil
    })

    // Start processing
    executor.Start(context.Background())
    defer executor.Stop()

    // Add a command
    executor.Add(ctx, durex.Spec{
        Name: "greet",
        Data: durex.M{"name": "World"},
    })
}
```

## Three Ways to Create Commands

### 1. Simple Function (Recommended)

```go
// Basic - just a function
executor.HandleFunc("sendEmail", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
    to := cmd.GetString("to")
    return mailer.Send(to), nil
})

// With options
executor.HandleFunc("sendEmail", sendEmailFn,
    durex.Retries(3),
    durex.OnRecover(handleFailure),
    durex.OnExpired(handleTimeout),
)
```

### 2. Typed Function (Type-Safe Data)

```go
type EmailData struct {
    To      string `json:"to"`
    Subject string `json:"subject"`
}

// No more GetString() - data is typed!
durex.HandleTyped(executor, "sendEmail", func(ctx context.Context, data EmailData, cmd *durex.Instance) (durex.Result, error) {
    return mailer.Send(data.To, data.Subject), nil
}, durex.WithRetries[EmailData](3))

// Add with typed data
executor.Add(ctx, durex.Typed("sendEmail", EmailData{
    To:      "user@example.com",
    Subject: "Welcome!",
}))
```

### 3. Struct (When You Need Dependencies)

```go
type SendEmailCommand struct {
    durex.BaseCommand
    mailer *MailService
}

func (c *SendEmailCommand) Name() string { return "sendEmail" }

func (c *SendEmailCommand) Execute(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
    return c.mailer.Send(cmd.GetString("to")), nil
}

executor.Register(&SendEmailCommand{mailer: mailerService})
```

## Command Results

| Result | Description |
|--------|-------------|
| `durex.Empty()` | Done, no follow-up |
| `durex.Repeat()` | Run again after Period |
| `durex.Retry()` | Retry immediately |
| `durex.Next(spec)` | Spawn one command |
| `durex.Spawn(specs...)` | Spawn multiple commands |

### Command Lifecycle

```mermaid
stateDiagram-v2
    [*] --> Pending: Add()
    Pending --> Running: Worker picks up
    Running --> Completed: Success
    Running --> Failed: Error (retries exhausted)
    Running --> Pending: Retry / Repeat
    Running --> Expired: Deadline exceeded
    Expired --> [*]: OnExpired handler
    Failed --> [*]: OnRecover handler
    Completed --> [*]
```

## Workflows (Command Chaining)

```mermaid
sequenceDiagram
    participant App
    participant Executor
    participant step1
    participant step2
    participant step3
    
    App->>Executor: Add(Spec with Sequence)
    Executor->>step1: Execute
    Note right of step1: Set("validated", true)
    step1-->>Executor: ContinueSequence()
    Executor->>step2: Execute (data passed)
    Note right of step2: GetBool("validated")
    step2-->>Executor: ContinueSequence()
    Executor->>step3: Execute
    step3-->>Executor: Empty()
```

```go
// Register steps
executor.HandleFunc("step1", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
    cmd.Set("validated", true)  // Pass data to next step
    return cmd.ContinueSequence(nil), nil
})

executor.HandleFunc("step2", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
    validated := cmd.GetBool("validated")  // Receive data from previous step
    return cmd.ContinueSequence(nil), nil
})

// Execute workflow: step1 → step2 → step3
executor.Add(ctx, durex.Spec{
    Name:     "step1",
    Sequence: []string{"step2", "step3"},
    Data:     durex.M{"orderId": "123"},
})
```

## Repeating Commands (Cron-like)

```go
executor.HandleFunc("cleanup", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
    // cleanup logic...
    return durex.Repeat(), nil  // Run again after period
}, durex.Period(time.Hour))
```

## Error Recovery (Saga Pattern)

```mermaid
graph TD
    A[processPayment] -->|retries exhausted| B[OnRecover Handler]
    B -->|Spawn| C[refundPayment]
    B -->|Spawn| D[releaseInventory]
    B -->|Spawn| E[notifyCustomer]
    
    style A fill:#ff6b6b,color:#fff
    style B fill:#ffd93d,color:#333
    style C fill:#6bcb77,color:#fff
    style D fill:#6bcb77,color:#fff
    style E fill:#6bcb77,color:#fff
```

```go
executor.HandleFunc("processPayment", processPayment,
    durex.Retries(3),
    durex.OnRecover(func(ctx context.Context, cmd *durex.Instance, err error) (durex.Result, error) {
        // Payment failed - spawn compensation commands
        return durex.Spawn(
            durex.Spec{Name: "refundPayment", Data: cmd.Data},
            durex.Spec{Name: "releaseInventory", Data: cmd.Data},
            durex.Spec{Name: "notifyCustomer", Data: durex.M{"error": err.Error()}},
        ), nil
    }),
)
```

## Delayed Execution

```go
executor.Add(ctx, durex.Spec{
    Name:  "sendReminder",
    Delay: 24 * time.Hour,  // Run tomorrow
    Data:  durex.M{"userId": "123"},
})
```

## Deadlines

```go
executor.HandleFunc("timeoutTask", taskFn,
    durex.Deadline(5*time.Minute),
    durex.OnExpired(func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
        log.Println("Task timed out!")
        return durex.Empty(), nil
    }),
)
```

## Storage Backends

```go
// In-memory (development/testing)
store := storage.NewMemory()

// SQLite (single instance)
store, _ := storage.OpenSQLite("commands.db")
store.Migrate(ctx)

// PostgreSQL (production)
db, _ := sql.Open("postgres", "postgres://...")
store := storage.NewPostgres(db)
store.Migrate(ctx)
```

## Configuration

```go
executor := durex.New(store,
    durex.WithParallelism(8),              // Worker count
    durex.WithDefaultRetries(3),            // Default retries
    durex.WithCleanupInterval(time.Hour),   // Auto-cleanup
    durex.WithGracefulShutdown(30*time.Second),
    durex.WithMiddleware(loggingMiddleware),
)
```

## Middleware

```go
func loggingMiddleware(ctx durex.MiddlewareContext, next func() (durex.Result, error)) (durex.Result, error) {
    start := time.Now()
    result, err := next()
    slog.Info("Command executed", "name", ctx.Command.Name, "duration", time.Since(start))
    return result, err
}
```

## Examples

See [examples/basic](./examples/basic) and [examples/workflow](./examples/workflow) for complete working examples.

```bash
# Run basic example
go run ./examples/basic

# Run e-commerce workflow example  
go run ./examples/workflow
```

## License

MIT License - see [LICENSE](LICENSE)
