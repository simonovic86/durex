package durex

import (
	"log/slog"
	"time"
)

// Option configures an Executor.
type Option func(*Executor)

// WithParallelism sets the number of concurrent command workers.
// Default is 4.
func WithParallelism(n int) Option {
	return func(e *Executor) {
		if n > 0 {
			e.parallelism = n
		}
	}
}

// WithLogger sets the logger for the executor.
// Default is slog.Default().
func WithLogger(logger *slog.Logger) Option {
	return func(e *Executor) {
		if logger != nil {
			e.logger = logger
		}
	}
}

// WithQueueSize sets the internal queue buffer size.
// Default is 1000.
func WithQueueSize(size int) Option {
	return func(e *Executor) {
		if size > 0 {
			e.queueSize = size
		}
	}
}

// WithDefaultRetries sets the default retry count for commands
// that don't specify their own.
// Default is 0 (no retries).
func WithDefaultRetries(n int) Option {
	return func(e *Executor) {
		e.defaultRetries = max(n, 0)
	}
}

// WithDefaultTimeout sets the default execution timeout for commands
// that don't specify their own.
// Default is 0 (no timeout).
//
// The timeout limits how long each execution attempt can take.
// If the handler doesn't complete within this duration, the context is cancelled
// and the command is treated as failed (will retry if retries remain).
func WithDefaultTimeout(d time.Duration) Option {
	return func(e *Executor) {
		if d > 0 {
			e.defaultTimeout = d
		}
	}
}

// WithDefaultPeriod sets the default period for repeating commands.
// Default is 1 minute.
func WithDefaultPeriod(d time.Duration) Option {
	return func(e *Executor) {
		if d > 0 {
			e.defaultRepeatInterval = d
		}
	}
}

// WithDefaultRepeatInterval sets the default period for repeating commands.
//
// Deprecated: Use WithDefaultPeriod for consistency with Spec.Period.
func WithDefaultRepeatInterval(d time.Duration) Option {
	return WithDefaultPeriod(d)
}

// WithMaxDelay sets the maximum delay for scheduled commands.
// Commands scheduled further out will be re-queued periodically.
// Default is 24 hours.
func WithMaxDelay(d time.Duration) Option {
	return func(e *Executor) {
		if d > 0 {
			e.maxDelay = d
		}
	}
}

// WithCleanupInterval sets how often completed commands are cleaned up.
// Set to 0 to disable automatic cleanup.
// Default is 1 hour.
func WithCleanupInterval(d time.Duration) Option {
	return func(e *Executor) {
		e.cleanupInterval = d
	}
}

// WithCleanupAge sets how old completed commands must be before cleanup.
// Default is 24 hours.
func WithCleanupAge(d time.Duration) Option {
	return func(e *Executor) {
		if d > 0 {
			e.cleanupAge = d
		}
	}
}

// WithStuckCommandRecovery enables automatic recovery of stuck commands.
// Stuck commands are those in STARTED status for longer than threshold,
// which may indicate a worker crash or process restart.
//
// checkInterval: how often to check for stuck commands
// threshold: how long a command must be in STARTED status to be considered stuck
//
// By default, stuck command recovery is disabled.
// Recommended settings: checkInterval=1m, threshold=5m
func WithStuckCommandRecovery(checkInterval, threshold time.Duration) Option {
	return func(e *Executor) {
		e.stuckCheckInterval = checkInterval
		if threshold > 0 {
			e.stuckThreshold = threshold
		}
	}
}

// WithGracefulShutdown sets the timeout for graceful shutdown.
// Default is 30 seconds.
func WithGracefulShutdown(d time.Duration) Option {
	return func(e *Executor) {
		if d > 0 {
			e.shutdownTimeout = d
		}
	}
}

// WithDashboard enables the web dashboard on the specified address.
// The dashboard provides a UI for monitoring commands and executor stats.
//
// Example:
//
//	executor := durex.New(store, durex.WithDashboard(":8080"))
//
// The dashboard will be available at http://localhost:8080 after Start().
func WithDashboard(addr string) Option {
	return func(e *Executor) {
		e.dashboardAddr = addr
	}
}

// WithDeadLetterQueue enables the dead letter queue.
// When enabled, commands that fail after exhausting all retries are moved
// to DEAD_LETTER status instead of FAILED. This allows for manual inspection
// and replay of failed commands.
//
// Use ReplayFromDLQ() to retry a dead-lettered command,
// and PurgeDLQ() to remove old dead-lettered commands.
func WithDeadLetterQueue() Option {
	return func(e *Executor) {
		e.deadLetterEnabled = true
	}
}

// WithMetrics enables metrics collection.
// The provided MetricsCollector will receive execution metrics.
func WithMetrics(collector MetricsCollector) Option {
	return func(e *Executor) {
		e.metrics = collector
	}
}

// WithMiddleware adds middleware to the execution pipeline.
// Middleware is executed in the order added.
func WithMiddleware(mw ...Middleware) Option {
	return func(e *Executor) {
		e.middleware = append(e.middleware, mw...)
	}
}

// WithPermanentCommands sets commands that should always be running.
// These commands are started on executor Start() and restarted if they fail.
func WithPermanentCommands(names ...string) Option {
	return func(e *Executor) {
		e.permanentCommands = append(e.permanentCommands, names...)
	}
}

// WithErrorHandler sets a custom error handler for command failures.
// This is called in addition to the command's Recover method.
func WithErrorHandler(handler func(cmd *Instance, err error)) Option {
	return func(e *Executor) {
		e.errorHandler = handler
	}
}

// WithBackoff sets the backoff strategy for retries.
// Default is NoBackoff() (immediate retry).
// Use DefaultExponentialBackoff() for production workloads.
func WithBackoff(strategy BackoffStrategy) Option {
	return func(e *Executor) {
		if strategy != nil {
			e.backoff = strategy
		}
	}
}

// WithRateLimiter sets a custom rate limiter for the executor.
// Use this when you need fine-grained control over rate limiting.
func WithRateLimiter(limiter *RateLimiter) Option {
	return func(e *Executor) {
		e.rateLimiter = limiter
	}
}

// WithRateLimit sets the maximum concurrent executions for a specific command type.
// This creates or updates the executor's rate limiter.
//
// Example:
//
//	executor := durex.New(storage,
//		durex.WithRateLimit("sendEmail", 10),    // max 10 concurrent emails
//		durex.WithRateLimit("processOrder", 5),  // max 5 concurrent orders
//	)
func WithRateLimit(commandName string, maxConcurrent int) Option {
	return func(e *Executor) {
		if e.rateLimiter == nil {
			e.rateLimiter = NewRateLimiter()
		}
		e.rateLimiter.SetLimit(commandName, maxConcurrent)
	}
}

// WithGlobalRateLimit sets the maximum total concurrent executions across all commands.
// This is useful for limiting overall system load.
//
// Example:
//
//	executor := durex.New(storage,
//		durex.WithGlobalRateLimit(100),  // max 100 total concurrent commands
//	)
func WithGlobalRateLimit(maxConcurrent int) Option {
	return func(e *Executor) {
		if e.rateLimiter == nil {
			e.rateLimiter = NewRateLimiter()
		}
		e.rateLimiter.SetGlobalLimit(maxConcurrent)
	}
}

// WithPollInterval sets how often workers poll for new commands when using
// LockingStorage (multi-instance mode). Default is 1 second.
func WithPollInterval(d time.Duration) Option {
	return func(e *Executor) {
		if d > 0 {
			e.pollInterval = d
		}
	}
}

// WithClaimBatchSize sets how many commands each worker claims per poll cycle
// when using LockingStorage. Default is 10.
func WithClaimBatchSize(size int) Option {
	return func(e *Executor) {
		if size > 0 {
			e.claimBatchSize = size
		}
	}
}

// MetricsCollector receives execution metrics.
type MetricsCollector interface {
	// CommandStarted is called when a command begins execution.
	CommandStarted(name string)

	// CommandCompleted is called when a command finishes successfully.
	CommandCompleted(name string, duration time.Duration)

	// CommandFailed is called when a command fails.
	CommandFailed(name string, err error)

	// CommandRetried is called when a command is retried.
	CommandRetried(name string, attempt int)

	// QueueSize reports the current queue size.
	QueueSize(size int)
}

// Middleware wraps command execution.
// Return the result of calling next() to continue the chain,
// or return early to short-circuit execution.
type Middleware func(ctx MiddlewareContext, next func() (Result, error)) (Result, error)

// MiddlewareContext provides context for middleware.
type MiddlewareContext struct {
	// Command is the command being executed.
	Command *Instance

	// Handler is the command handler.
	Handler Command

	// Executor is the executor running the command.
	Executor *Executor
}
