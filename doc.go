/*
Package durex provides a durable background job queue and workflow engine for Go.

Durex is a lightweight, embeddable task queue with persistence, automatic retries,
workflow sequences, and saga pattern support. It's an alternative to Asynq, River,
and Temporal for teams who want workflow capabilities without infrastructure complexity.

Use SQLite for development, PostgreSQL for production. No Redis or Kafka required.

# Quick Start

	store := storage.NewMemory()
	exec := durex.New(store, durex.WithParallelism(4))

	exec.HandleFunc("greet", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		fmt.Println("Hello,", cmd.GetString("name"))
		return durex.Empty(), nil
	})

	exec.Start(context.Background())
	defer exec.Stop()

	exec.Add(ctx, durex.Spec{
		Name: "greet",
		Data: durex.M{"name": "World"},
	})

# Key Features

  - Persistent Commands: Commands survive process restarts
  - Automatic Retries: Configurable retry logic with backoff strategies
  - Deadlines: Time-bound execution with expiration handling
  - Command Chaining: Build workflows with sequences
  - Recovery: Custom error handling and compensation (saga pattern)
  - Middleware: Extensible execution pipeline
  - Multiple Storage Backends: PostgreSQL, SQLite, Memory
  - Rate Limiting: Control concurrent execution per command type
  - Deduplication: Prevent duplicate commands with unique keys
  - Context Propagation: Trace and correlation IDs across command chains
  - Execution History: Full audit trail of command lifecycle events
  - Prometheus Metrics: Built-in metrics for monitoring
  - Web Dashboard: Real-time monitoring UI with retry/cancel actions
  - Dead Letter Queue: Preserve failed commands for inspection and replay
  - Panic Recovery: Workers survive panics and continue processing
  - Stuck Command Recovery: Automatic detection and recovery of stuck commands
  - Health Endpoint: /api/health for load balancer health checks

# Basic Usage

Define a command by implementing the Command interface:

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
			return durex.Empty(), err // Will retry if retries > 0
		}

		return durex.Empty(), nil
	}

Create an executor and register commands:

	storage := storage.NewMemory()
	executor := durex.New(storage,
		durex.WithParallelism(4),
		durex.WithDefaultRetries(3),
	)

	executor.Register(&SendEmailCommand{mailer: mailerService})
	executor.Start(ctx)
	defer executor.Stop()

Add commands for execution:

	executor.Add(ctx, durex.Spec{
		Name: "sendEmail",
		Data: durex.M{
			"to":      "user@example.com",
			"subject": "Welcome!",
			"body":    "Thanks for signing up.",
		},
		Retries: 3,
	})

# Command Results

Commands return a Result that tells the executor what to do next:

  - durex.Empty(): Command completed, no follow-up actions
  - durex.Repeat(): Reschedule this command to run again after its Period
  - durex.Retry(): Retry immediately (uses retry counter, doesn't trigger Recover)
  - durex.Next(spec): Spawn a single follow-up command
  - durex.Spawn(specs...): Spawn multiple follow-up commands

# Command Chaining

Build workflows by chaining commands:

	executor.Add(ctx, durex.Spec{
		Name:     "validateOrder",
		Sequence: []string{"processPayment", "shipOrder", "sendConfirmation"},
		Data:     durex.M{"orderId": "12345"},
	})

Each command can continue the sequence:

	func (c *ValidateOrderCommand) Execute(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		// Validate order...
		cmd.Set("validated", true)
		return cmd.ContinueSequence(nil), nil
	}

# Error Handling

Commands can implement the Recoverable interface for custom error handling:

	func (c *SendEmailCommand) Recover(ctx context.Context, cmd *durex.Instance, err error) (durex.Result, error) {
		// Log the failure, notify ops, spawn compensation commands
		return durex.Next(durex.Spec{
			Name: "notifyFailure",
			Data: durex.M{"error": err.Error()},
		}), nil
	}

# Deadlines

Set execution deadlines:

	executor.Add(ctx, durex.Spec{
		Name:     "processOrder",
		Deadline: 5 * time.Minute,
	})

Commands can implement Expirable to handle deadline expiration:

	func (c *ProcessOrderCommand) Expired(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		// Handle timeout - refund, notify, etc.
		return durex.Empty(), nil
	}

# Middleware

Add cross-cutting concerns:

	executor := durex.New(storage,
		durex.WithMiddleware(
			func(ctx durex.MiddlewareContext, next func() (durex.Result, error)) (durex.Result, error) {
				start := time.Now()
				result, err := next()
				log.Printf("Command %s took %v", ctx.Command.Name, time.Since(start))
				return result, err
			},
		),
	)

# Storage Backends

Durex supports multiple storage backends:

	// In-memory (for testing)
	storage := storage.NewMemory()

	// SQLite (for single-instance deployments)
	storage, _ := storage.OpenSQLite("commands.db")
	storage.Migrate(ctx)

	// PostgreSQL (for production)
	db, _ := sql.Open("postgres", "postgres://...")
	storage := storage.NewPostgres(db)
	storage.Migrate(ctx)

# Backoff Strategies

Configure retry backoff behavior:

	executor := durex.New(storage,
		durex.WithBackoff(durex.DefaultExponentialBackoff()),
	)

Available strategies:

  - durex.NoBackoff(): Immediate retry (default)
  - durex.ConstantBackoff{Delay: 5 * time.Second}: Fixed delay
  - durex.LinearBackoff{InitialDelay: time.Second, MaxDelay: time.Minute}
  - durex.ExponentialBackoff{InitialDelay: time.Second, MaxDelay: 5 * time.Minute, Multiplier: 2.0}
  - durex.JitteredBackoff{Strategy: ..., JitterRate: 0.1}: Add randomness to prevent thundering herd

# Deduplication

Prevent duplicate commands with unique keys:

	executor.Add(ctx, durex.Spec{
		Name:      "sendEmail",
		UniqueKey: "email:user123:welcome",  // Only one active command with this key
	})

If a non-terminal command with the same UniqueKey exists, Add() returns ErrDuplicateCommand.

# Rate Limiting

Control concurrent command execution:

	executor := durex.New(storage,
		durex.WithRateLimit("sendEmail", 10),     // Max 10 concurrent emails
		durex.WithRateLimit("apiCall", 5),        // Max 5 concurrent API calls
		durex.WithGlobalRateLimit(100),           // Max 100 total concurrent commands
	)

# Tracing and Correlation

Commands automatically propagate trace and correlation IDs to child commands:

	executor.Add(ctx, durex.Spec{
		Name:          "workflow",
		TraceID:       "trace-123",      // Propagated to all children
		CorrelationID: "correlation-456", // Links related commands
	})

Access these in your command:

	func (c *MyCommand) Execute(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		log.Printf("TraceID: %s, CorrelationID: %s", cmd.TraceID, cmd.CorrelationID)
		// ...
	}

# Web Dashboard

Enable the built-in monitoring dashboard:

	// Simple standalone server
	go executor.ServeDashboard(":8080")

	// Or integrate with existing HTTP server
	http.Handle("/durex/", http.StripPrefix("/durex", executor.DashboardHandler()))

	// Or enable via option (auto-starts with executor)
	executor := durex.New(store, durex.WithDashboard(":8080"))

The dashboard provides:
  - Live command counts (pending, completed, failed, repeating)
  - Recent commands with status, attempts, and timing
  - Retry and cancel actions for individual commands
  - Health endpoint at /api/health

# Execution History

Every command tracks its execution history for debugging and auditing:

	history, _ := executor.History(ctx, "cmd_abc123")
	for _, event := range history {
		fmt.Printf("%s: %s (attempt %d)\n", event.Timestamp, event.Type, event.Attempt)
	}

Event types: created, started, completed, failed, retrying, expired, cancelled, repeating, recovered.

History is also available via the dashboard API: GET /api/commands/history?id=<command_id>

# Prometheus Metrics

Enable Prometheus metrics for monitoring:

	metrics := durex.NewPrometheusMetrics(prometheus.DefaultRegisterer)
	executor := durex.New(store, durex.WithMetrics(metrics))

Exported metrics:
  - durex_commands_started_total: Counter per command name
  - durex_commands_completed_total: Counter per command name
  - durex_commands_failed_total: Counter per command name
  - durex_commands_retried_total: Counter per command name
  - durex_command_duration_seconds: Histogram per command name
  - durex_queue_size: Gauge for current queue size

# Dead Letter Queue

Enable DLQ to preserve failed commands for inspection and replay:

	executor := durex.New(store, durex.WithDeadLetterQueue())

	// Inspect failed commands
	deadLettered, _ := executor.FindDeadLettered(ctx)

	// Replay a command
	executor.ReplayFromDLQ(ctx, "cmd_abc123")

	// Purge old entries
	executor.PurgeDLQ(ctx, 7*24*time.Hour)

# Reliability Features

Durex includes several reliability features:

Panic Recovery: Workers automatically recover from panics in command handlers.
The command is marked as failed, and workers continue processing other commands.

Stuck Command Recovery: Commands stuck in STARTED status (e.g., after a crash)
are automatically detected and reset to PENDING:

	executor := durex.New(store,
		durex.WithStuckCommandRecovery(
			time.Minute,    // Check every minute
			5*time.Minute,  // Reset commands stuck >5 min
		),
	)

# Multi-Instance Deployment

For horizontal scaling with PostgreSQL, Durex automatically uses row-level locking:

	db, _ := sql.Open("postgres", "postgres://...")
	store := storage.NewPostgres(db)
	store.Migrate(ctx)

	executor := durex.New(store,
		durex.WithPollInterval(500*time.Millisecond),
		durex.WithClaimBatchSize(20),
	)

Multiple executor instances can safely run concurrently - each will claim different commands.

# Production Considerations

For production deployments:

  - Use PostgreSQL for durability and multi-instance support
  - Configure appropriate parallelism based on workload
  - Set up monitoring using the MetricsCollector interface
  - Implement proper error handling and alerting
  - Use deadlines to prevent runaway commands
  - Consider idempotency in command implementations

See the examples directory for complete working examples.
*/
package durex
