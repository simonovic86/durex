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
		e.defaultRetries = n
	}
}

// WithDefaultRepeatInterval sets the default period for repeating commands.
// Default is 1 minute.
func WithDefaultRepeatInterval(d time.Duration) Option {
	return func(e *Executor) {
		if d > 0 {
			e.defaultRepeatInterval = d
		}
	}
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

// WithGracefulShutdown sets the timeout for graceful shutdown.
// Default is 30 seconds.
func WithGracefulShutdown(d time.Duration) Option {
	return func(e *Executor) {
		if d > 0 {
			e.shutdownTimeout = d
		}
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
