/*
Package durex provides a durable execution framework for Go applications.

Durex enables you to build reliable, persistent command/task execution systems
with automatic retries, deadlines, and recovery from failures. It's inspired by
patterns from job queues, workflow engines, and saga orchestrators.

# Key Features

  - Persistent Commands: Commands survive process restarts
  - Automatic Retries: Configurable retry logic with backoff
  - Deadlines: Time-bound execution with expiration handling
  - Command Chaining: Build workflows with sequences
  - Recovery: Custom error handling and compensation
  - Middleware: Extensible execution pipeline
  - Multiple Storage Backends: PostgreSQL, SQLite, Memory

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
