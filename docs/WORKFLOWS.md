# Command Workflows & Chaining

This guide explains how to build complex workflows by chaining commands together in Durex.

## Table of Contents

- [Overview](#overview)
- [Command Lifecycle](#command-lifecycle)
- [Chaining Patterns](#chaining-patterns)
  - [Sequences (Linear Workflows)](#sequences-linear-workflows)
  - [Fan-Out (Parallel Execution)](#fan-out-parallel-execution)
  - [Fan-In (Aggregation)](#fan-in-aggregation)
  - [Dynamic Branching](#dynamic-branching)
- [Data Flow Between Commands](#data-flow-between-commands)
- [Error Handling & Recovery](#error-handling--recovery)
- [Saga Pattern (Compensation)](#saga-pattern-compensation)
- [Parent-Child Relationships](#parent-child-relationships)
- [Tracing Workflows](#tracing-workflows)
- [Best Practices](#best-practices)

---

## Overview

Durex enables you to build complex, durable workflows by composing simple commands. Each command is:

- **Persistent** - Survives process restarts
- **Retriable** - Automatically retries on failure
- **Composable** - Can spawn child commands or continue sequences
- **Traceable** - Linked via parent IDs and correlation IDs

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Validate   │────▶│   Process   │────▶│   Notify    │
│   Order     │     │   Payment   │     │  Customer   │
└─────────────┘     └─────────────┘     └─────────────┘
                           │
                           ▼ (on failure)
                    ┌─────────────┐
                    │   Refund    │
                    │   Payment   │
                    └─────────────┘
```

---

## Command Lifecycle

```
                    ┌──────────────────────────────────────────┐
                    │                                          │
                    ▼                                          │
┌─────────┐    ┌─────────┐    ┌─────────────┐                  │
│   Add   │───▶│ PENDING │───▶│   STARTED   │──────────────────┤
└─────────┘    └─────────┘    └─────────────┘                  │
                    ▲              │    │                      │
                    │              │    │                      │
                    │    ┌─────────┘    └──────────┐           │
                    │    ▼                         ▼           │
                    │ Success                   Error          │
                    │    │                         │           │
                    │    ▼                         ▼           │
                    │ ┌─────────────┐    ┌─────────────────┐   │
                    │ │  COMPLETED  │    │ Retries left?   │   │
                    │ └─────────────┘    └─────────────────┘   │
                    │        │                  │    │         │
                    │        │              Yes │    │ No      │
                    │        │                  │    ▼         │
                    │        │                  │ ┌────────┐   │
                    │        │                  │ │ FAILED │   │
                    │        │                  │ └────────┘   │
                    │        │                  │      │       │
                    │        ▼                  │      ▼       │
                    │   Spawn children?         │  Recover()   │
                    │        │                  │      │       │
                    │    Yes │                  │      │       │
                    │        ▼                  │      │       │
                    └────[Children]─────────────┴──────┘       │
                                                               │
                    ┌──────────────────────────────────────────┘
                    │ (Repeat or Retry result)
                    │
               ┌────┴─────┐
               │ REPEATING│
               └──────────┘
```

### Command States

| State | Description |
|-------|-------------|
| `PENDING` | Waiting to be picked up by a worker |
| `STARTED` | Currently executing |
| `COMPLETED` | Finished successfully |
| `FAILED` | Failed after all retries exhausted |
| `EXPIRED` | Deadline exceeded before completion |
| `CANCELLED` | Manually cancelled |
| `REPEATING` | Waiting to run again (periodic commands) |

---

## Chaining Patterns

### Sequences (Linear Workflows)

The simplest chaining pattern. Commands execute one after another, with data flowing through.

```go
// Define the workflow steps
executor.HandleFunc("validateOrder", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
    orderId := cmd.GetString("orderId")
    
    // Validate...
    cmd.Set("validated", true)
    cmd.Set("validatedAt", time.Now().Format(time.RFC3339))
    
    // Continue to next step in sequence
    return cmd.ContinueSequence(nil), nil
})

executor.HandleFunc("processPayment", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
    // Access data from previous step
    validated := cmd.GetBool("validated")
    if !validated {
        return durex.Empty(), errors.New("order not validated")
    }
    
    // Process payment...
    cmd.Set("paymentId", "PAY-12345")
    
    return cmd.ContinueSequence(nil), nil
})

executor.HandleFunc("shipOrder", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
    paymentId := cmd.GetString("paymentId")
    // Ship...
    return durex.Empty(), nil  // End of sequence
})

// Start the workflow
executor.Add(ctx, durex.Spec{
    Name:     "validateOrder",
    Sequence: []string{"processPayment", "shipOrder"},
    Data:     durex.M{"orderId": "ORD-001", "amount": 99.99},
})
```

**How it works:**

1. `validateOrder` runs first
2. `ContinueSequence()` spawns `processPayment` with all accumulated data
3. `processPayment` runs, then spawns `shipOrder`
4. `shipOrder` returns `Empty()` to end the chain

```
validateOrder ──▶ processPayment ──▶ shipOrder
     │                  │                │
     └── Data flows through all steps ───┘
```

### Fan-Out (Parallel Execution)

Spawn multiple commands to run concurrently.

```go
executor.HandleFunc("notifyAll", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
    users := cmd.GetSlice("users")
    
    specs := make([]durex.Spec, len(users))
    for i, user := range users {
        specs[i] = durex.Spec{
            Name: "sendNotification",
            Data: durex.M{
                "userId":  user,
                "message": cmd.GetString("message"),
            },
        }
    }
    
    return durex.Spawn(specs...), nil
})

// Trigger fan-out
executor.Add(ctx, durex.Spec{
    Name: "notifyAll",
    Data: durex.M{
        "users":   []string{"user1", "user2", "user3"},
        "message": "System maintenance tonight",
    },
})
```

```
                    ┌─────────────────┐
                    │   notifyAll     │
                    └────────┬────────┘
                             │
           ┌─────────────────┼─────────────────┐
           ▼                 ▼                 ▼
    ┌────────────┐    ┌────────────┐    ┌────────────┐
    │ notify     │    │ notify     │    │ notify     │
    │ user1      │    │ user2      │    │ user3      │
    └────────────┘    └────────────┘    └────────────┘
```

### Fan-In (Aggregation)

Collect results from multiple parallel commands. Use a coordination pattern with shared state.

```go
// Worker commands that process items
executor.HandleFunc("processItem", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
    itemId := cmd.GetString("itemId")
    batchId := cmd.GetString("batchId")
    
    // Process item...
    result := processItem(itemId)
    
    // Store result (use external storage or command data)
    storeResult(batchId, itemId, result)
    
    // Check if all items done, then trigger aggregation
    if allItemsComplete(batchId) {
        return durex.Next(durex.Spec{
            Name: "aggregateResults",
            Data: durex.M{"batchId": batchId},
        }), nil
    }
    
    return durex.Empty(), nil
})

executor.HandleFunc("aggregateResults", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
    batchId := cmd.GetString("batchId")
    results := loadResults(batchId)
    
    // Aggregate all results...
    summary := aggregate(results)
    
    return durex.Empty(), nil
})
```

### Dynamic Branching

Choose the next command based on runtime conditions.

```go
executor.HandleFunc("routeOrder", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
    orderType := cmd.GetString("type")
    
    var nextCommand string
    switch orderType {
    case "digital":
        nextCommand = "deliverDigital"
    case "physical":
        nextCommand = "shipPhysical"
    case "subscription":
        nextCommand = "activateSubscription"
    default:
        return durex.Empty(), fmt.Errorf("unknown order type: %s", orderType)
    }
    
    return durex.Next(durex.Spec{
        Name: nextCommand,
        Data: cmd.Data,
    }), nil
})
```

```
                    ┌─────────────────┐
                    │   routeOrder    │
                    └────────┬────────┘
                             │
         ┌───────────────────┼───────────────────┐
         │                   │                   │
         ▼                   ▼                   ▼
   type="digital"      type="physical"    type="subscription"
         │                   │                   │
         ▼                   ▼                   ▼
  ┌──────────────┐   ┌──────────────┐   ┌─────────────────┐
  │deliverDigital│   │ shipPhysical │   │activateSubscr.  │
  └──────────────┘   └──────────────┘   └─────────────────┘
```

---

## Data Flow Between Commands

### Passing Data via Instance

Data set on the instance flows to child commands:

```go
// Parent command
executor.HandleFunc("parent", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
    cmd.Set("computedValue", 42)
    cmd.Set("processedAt", time.Now())
    
    return durex.Next(durex.Spec{
        Name: "child",
        // Child inherits cmd.Data automatically via ContinueSequence
        // Or explicitly pass data:
        Data: cmd.Data,
    }), nil
})

// Child command
executor.HandleFunc("child", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
    value := cmd.GetInt("computedValue")  // 42
    return durex.Empty(), nil
})
```

### Typed Data Flow

Use typed commands for compile-time safety:

```go
type OrderData struct {
    OrderID    string  `json:"orderId"`
    Amount     float64 `json:"amount"`
    CustomerID string  `json:"customerId"`
}

durex.HandleTyped(executor, "processOrder", 
    func(ctx context.Context, data OrderData, cmd *durex.Instance) (durex.Result, error) {
        // data is already typed!
        fmt.Printf("Processing order %s for $%.2f\n", data.OrderID, data.Amount)
        
        return cmd.ContinueSequence(nil), nil
    })
```

### Data Accumulation in Sequences

Each step can add to the shared data:

```go
// Step 1: Add validation info
cmd.Set("validated", true)
cmd.Set("validatedBy", "system")

// Step 2: Add payment info
cmd.Set("paymentId", "PAY-123")
cmd.Set("chargedAmount", 99.99)

// Step 3: Has access to ALL previous data
validated := cmd.GetBool("validated")      // true
paymentId := cmd.GetString("paymentId")    // "PAY-123"
```

---

## Error Handling & Recovery

### Automatic Retries

Commands automatically retry on error:

```go
executor.HandleFunc("unreliableTask", taskFn, durex.Retries(3))

// Or with typed commands
durex.HandleTyped(executor, "unreliableTask", taskFn, 
    durex.WithRetries[MyData](3))
```

### Retry Behavior

```
Attempt 1 ──▶ Error ──▶ Retry (retries: 3→2)
Attempt 2 ──▶ Error ──▶ Retry (retries: 2→1)
Attempt 3 ──▶ Error ──▶ Retry (retries: 1→0)
Attempt 4 ──▶ Error ──▶ FAILED (no retries left) ──▶ Recover()
```

### Explicit Retry Control

Return `Retry()` to retry without consuming from the retry counter:

```go
executor.HandleFunc("conditionalRetry", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
    err := doWork()
    
    if isTransientError(err) {
        // Retry without triggering Recover
        return durex.Retry(), nil
    }
    
    if err != nil {
        // Normal error - uses retry counter, eventually triggers Recover
        return durex.Empty(), err
    }
    
    return durex.Empty(), nil
})
```

### Recovery Handler

Called after all retries are exhausted:

```go
executor.HandleFunc("criticalTask", taskFn,
    durex.Retries(3),
    durex.OnRecover(func(ctx context.Context, cmd *durex.Instance, err error) (durex.Result, error) {
        log.Printf("Task failed permanently: %v", err)
        
        // Option 1: Just log and complete
        return durex.Empty(), nil
        
        // Option 2: Spawn compensation/notification
        return durex.Spawn(
            durex.Spec{Name: "alertOps", Data: durex.M{"error": err.Error()}},
            durex.Spec{Name: "rollback", Data: cmd.Data},
        ), nil
    }),
)
```

---

## Saga Pattern (Compensation)

For distributed transactions, implement the saga pattern with compensation commands:

```go
// Forward flow
executor.HandleFunc("reserveInventory", reserveInventoryFn,
    durex.Retries(3),
    durex.OnRecover(func(ctx context.Context, cmd *durex.Instance, err error) (durex.Result, error) {
        // No compensation needed - nothing was done
        return durex.Next(durex.Spec{
            Name: "notifyOrderFailed",
            Data: durex.M{"reason": "inventory", "error": err.Error()},
        }), nil
    }),
)

executor.HandleFunc("chargePayment", chargePaymentFn,
    durex.Retries(3),
    durex.OnRecover(func(ctx context.Context, cmd *durex.Instance, err error) (durex.Result, error) {
        // Compensate: release the reserved inventory
        return durex.Spawn(
            durex.Spec{Name: "releaseInventory", Data: cmd.Data},
            durex.Spec{Name: "notifyOrderFailed", Data: durex.M{"reason": "payment"}},
        ), nil
    }),
)

executor.HandleFunc("shipOrder", shipOrderFn,
    durex.Retries(3),
    durex.OnRecover(func(ctx context.Context, cmd *durex.Instance, err error) (durex.Result, error) {
        // Compensate: refund payment and release inventory
        return durex.Spawn(
            durex.Spec{Name: "refundPayment", Data: cmd.Data},
            durex.Spec{Name: "releaseInventory", Data: cmd.Data},
            durex.Spec{Name: "notifyOrderFailed", Data: durex.M{"reason": "shipping"}},
        ), nil
    }),
)

// Compensation commands
executor.HandleFunc("releaseInventory", releaseInventoryFn)
executor.HandleFunc("refundPayment", refundPaymentFn)
executor.HandleFunc("notifyOrderFailed", notifyFailureFn)
```

```
Forward Flow (Happy Path):
reserveInventory ──▶ chargePayment ──▶ shipOrder ──▶ Complete!

Compensation (shipOrder fails):
                                            ┌──────────────────┐
                                            │   shipOrder      │
                                            │     FAILED       │
                                            └────────┬─────────┘
                                                     │
                   ┌─────────────────────────────────┼─────────────────┐
                   ▼                                 ▼                 ▼
           ┌──────────────┐                  ┌──────────────┐   ┌────────────┐
           │refundPayment │                  │releaseInvent.│   │notifyFailed│
           └──────────────┘                  └──────────────┘   └────────────┘
```

---

## Parent-Child Relationships

Every spawned command has a reference to its parent:

```go
executor.HandleFunc("child", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
    if cmd.ParentID != nil {
        fmt.Printf("I was spawned by: %s\n", *cmd.ParentID)
    }
    return durex.Empty(), nil
})
```

### Querying Children

```go
// Find all children of a command
children, err := storage.FindByParent(ctx, parentID)
for _, child := range children {
    fmt.Printf("Child: %s, Status: %s\n", child.ID, child.Status)
}
```

---

## Tracing Workflows

Use trace and correlation IDs to track related commands:

```go
// Set IDs on the root command
executor.Add(ctx, durex.Spec{
    Name:          "startWorkflow",
    TraceID:       "trace-abc-123",      // From your tracing system (e.g., OpenTelemetry)
    CorrelationID: "order-456",          // Business correlation ID
    Sequence:      []string{"step1", "step2", "step3"},
})
```

### Automatic Propagation

Trace and correlation IDs automatically propagate to:
- Sequence continuations (`ContinueSequence`)
- Spawned children (`Next`, `Spawn`)

```go
executor.HandleFunc("anyStep", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
    // These are available in every command in the chain
    log.Printf("TraceID: %s, CorrelationID: %s", cmd.TraceID, cmd.CorrelationID)
    
    // Child commands automatically inherit these
    return durex.Spawn(
        durex.Spec{Name: "child1"},  // Gets same TraceID & CorrelationID
        durex.Spec{Name: "child2"},  // Gets same TraceID & CorrelationID
    ), nil
})
```

### Querying by Correlation ID

```go
// Find all commands in a workflow
commands, _ := storage.FindByCorrelationID(ctx, "order-456")

fmt.Printf("Workflow has %d commands:\n", len(commands))
for _, cmd := range commands {
    fmt.Printf("  - %s (%s): %s\n", cmd.Name, cmd.ID, cmd.Status)
}
```

---

## Best Practices

### 1. Keep Commands Small and Focused

```go
// ❌ Bad: One giant command
executor.HandleFunc("processOrder", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
    validate()
    reserveInventory()
    chargePayment()
    ship()
    notify()
    return durex.Empty(), nil
})

// ✅ Good: Small, focused commands
executor.HandleFunc("validateOrder", validateFn)
executor.HandleFunc("reserveInventory", reserveFn)
executor.HandleFunc("chargePayment", chargeFn)
executor.HandleFunc("shipOrder", shipFn)
executor.HandleFunc("notifyCustomer", notifyFn)

executor.Add(ctx, durex.Spec{
    Name:     "validateOrder",
    Sequence: []string{"reserveInventory", "chargePayment", "shipOrder", "notifyCustomer"},
})
```

### 2. Make Commands Idempotent

Commands may retry. Design them to be safe to run multiple times:

```go
executor.HandleFunc("chargePayment", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
    orderId := cmd.GetString("orderId")
    
    // Check if already charged (idempotency)
    if existingPayment := findPayment(orderId); existingPayment != nil {
        cmd.Set("paymentId", existingPayment.ID)
        return cmd.ContinueSequence(nil), nil
    }
    
    // Process new payment
    payment := chargeCard(orderId, cmd.GetFloat64("amount"))
    cmd.Set("paymentId", payment.ID)
    
    return cmd.ContinueSequence(nil), nil
})
```

### 3. Use Unique Keys for Deduplication

Prevent duplicate workflows:

```go
executor.Add(ctx, durex.Spec{
    Name:      "processOrder",
    UniqueKey: fmt.Sprintf("order:%s", orderId),  // Only one active per order
    Data:      orderData,
})
```

### 4. Set Appropriate Deadlines

Prevent workflows from running forever:

```go
executor.Add(ctx, durex.Spec{
    Name:     "timeoutSensitiveTask",
    Deadline: 5 * time.Minute,
})

// Handle expiration
executor.HandleFunc("timeoutSensitiveTask", taskFn,
    durex.Deadline(5*time.Minute),
    durex.OnExpired(func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
        log.Printf("Task timed out: %s", cmd.ID)
        return durex.Empty(), nil
    }),
)
```

### 5. Use Tags for Categorization

```go
executor.Add(ctx, durex.Spec{
    Name: "processOrder",
    Tags: []string{"orders", "high-priority", "region:us-east"},
})

// Query by tags
orders, _ := storage.Find(ctx, durex.Query{
    Tags: []string{"orders", "high-priority"},
})
```

### 6. Log with Context

```go
executor.HandleFunc("myTask", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
    log := slog.With(
        "command_id", cmd.ID,
        "command_name", cmd.Name,
        "trace_id", cmd.TraceID,
        "correlation_id", cmd.CorrelationID,
        "attempt", cmd.Attempt,
    )
    
    log.Info("Starting task")
    // ...
    log.Info("Task completed")
    
    return durex.Empty(), nil
})
```

---

## Summary

| Pattern | Use Case | Method |
|---------|----------|--------|
| **Sequence** | Linear workflow | `Spec.Sequence` + `ContinueSequence()` |
| **Fan-Out** | Parallel processing | `Spawn(specs...)` |
| **Fan-In** | Aggregate results | External coordination + `Next()` |
| **Branching** | Conditional flow | `Next(spec)` based on logic |
| **Saga** | Distributed transactions | `OnRecover` + compensation commands |
| **Retry** | Transient failures | `Retries(n)` + `Retry()` |
| **Repeat** | Periodic tasks | `Period(d)` + `Repeat()` |
