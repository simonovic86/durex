package durex

import (
	"time"
)

// Spec defines the specification for creating a command instance.
// Use this when adding new commands to the executor.
type Spec struct {
	// Name is the command type identifier.
	// Must match a registered command handler's Name().
	Name string `json:"name"`

	// Data contains the command payload.
	// This data is persisted and available during execution.
	Data M `json:"data,omitempty"`

	// Delay postpones command execution by the specified duration.
	// The command will be scheduled to run after this delay.
	Delay time.Duration `json:"delay,omitempty"`

	// Period sets the interval for repeating commands.
	// When a command returns Repeat(), it will be rescheduled after this duration.
	// If not set, defaults to executor's DefaultRepeatInterval.
	// Note: If Cron is set, it takes precedence over Period.
	Period time.Duration `json:"period,omitempty"`

	// Cron sets a cron expression for scheduled commands.
	// When a command returns Repeat(), it will be rescheduled based on this expression.
	// Uses standard cron format: "minute hour day-of-month month day-of-week"
	// Examples:
	//   - "0 0 * * *"     - Daily at midnight
	//   - "*/15 * * * *"  - Every 15 minutes
	//   - "0 9 * * 1-5"   - Weekdays at 9 AM
	// If both Cron and Period are set, Cron takes precedence.
	Cron string `json:"cron,omitempty"`

	// Timeout sets the maximum execution time per attempt.
	// If the handler doesn't complete within this duration, the context is cancelled
	// and the command is treated as failed (will retry if retries remain).
	// This is different from Deadline which prevents starting after a time.
	Timeout time.Duration `json:"timeout,omitempty"`

	// Deadline sets a relative deadline from now.
	// If the command hasn't started by this time, Expired() is called instead.
	// Takes precedence over DeadlineAt if both are set.
	Deadline time.Duration `json:"deadline,omitempty"`

	// DeadlineAt sets an absolute deadline.
	// If the command hasn't started by this time, Expired() is called instead.
	DeadlineAt *time.Time `json:"deadline_at,omitempty"`

	// Retries is the number of retry attempts on failure.
	// When a command returns an error, it will be retried up to this many times.
	// After retries are exhausted, Recover() is called.
	Retries int `json:"retries,omitempty"`

	// Sequence defines a chain of commands to execute in order.
	// When the current command completes, the next command in the sequence
	// is automatically spawned with the accumulated data.
	Sequence []string `json:"sequence,omitempty"`

	// Priority sets the command's priority (higher = more important).
	// Commands with higher priority are executed first when multiple
	// commands are ready. Default is 0.
	Priority int `json:"priority,omitempty"`

	// Tags are optional labels for categorization and filtering.
	Tags []string `json:"tags,omitempty"`

	// UniqueKey prevents duplicate commands with the same key.
	// If a non-terminal command with this key already exists, Add() returns
	// ErrDuplicateCommand instead of creating a new instance.
	// Useful for ensuring idempotency (e.g., "send-email:user123").
	UniqueKey string `json:"unique_key,omitempty"`

	// TraceID is used for distributed tracing.
	// Automatically propagated to child commands.
	TraceID string `json:"trace_id,omitempty"`

	// CorrelationID links related commands together.
	// Automatically propagated to child commands.
	// If not set, defaults to the root command's ID.
	CorrelationID string `json:"correlation_id,omitempty"`
}

// WithData returns a copy of the Spec with the given data merged in.
func (s Spec) WithData(data M) Spec {
	if s.Data == nil {
		s.Data = make(M)
	}
	for k, v := range data {
		s.Data[k] = v
	}
	return s
}

// WithDelay returns a copy of the Spec with the given delay.
func (s Spec) WithDelay(d time.Duration) Spec {
	s.Delay = d
	return s
}

// WithRetries returns a copy of the Spec with the given retry count.
func (s Spec) WithRetries(n int) Spec {
	s.Retries = n
	return s
}

// WithTimeout returns a copy of the Spec with the given execution timeout.
// The timeout limits how long each execution attempt can take.
func (s Spec) WithTimeout(d time.Duration) Spec {
	s.Timeout = d
	return s
}

// WithDeadline returns a copy of the Spec with the given deadline.
func (s Spec) WithDeadline(d time.Duration) Spec {
	s.Deadline = d
	return s
}

// WithPriority returns a copy of the Spec with the given priority.
func (s Spec) WithPriority(p int) Spec {
	s.Priority = p
	return s
}

// WithTags returns a copy of the Spec with the given tags.
func (s Spec) WithTags(tags ...string) Spec {
	s.Tags = append(s.Tags, tags...)
	return s
}

// WithUniqueKey returns a copy of the Spec with the given unique key.
// Commands with the same unique key cannot be added while one is still active.
func (s Spec) WithUniqueKey(key string) Spec {
	s.UniqueKey = key
	return s
}

// WithTraceID returns a copy of the Spec with the given trace ID.
// The trace ID is propagated to all child commands.
func (s Spec) WithTraceID(traceID string) Spec {
	s.TraceID = traceID
	return s
}

// WithCorrelationID returns a copy of the Spec with the given correlation ID.
// The correlation ID is propagated to all child commands.
func (s Spec) WithCorrelationID(correlationID string) Spec {
	s.CorrelationID = correlationID
	return s
}

// WithPeriod returns a copy of the Spec with the given repeat period.
// When a command returns Repeat(), it will be rescheduled after this duration.
// If both Cron and Period are set, Cron takes precedence.
func (s Spec) WithPeriod(d time.Duration) Spec {
	s.Period = d
	return s
}

// WithCron returns a copy of the Spec with the given cron expression.
// When a command returns Repeat(), it will be rescheduled based on this expression.
// Uses standard cron format: "minute hour day-of-month month day-of-week"
// If both Cron and Period are set, Cron takes precedence.
func (s Spec) WithCron(expr string) Spec {
	s.Cron = expr
	return s
}

// WithSequence returns a copy of the Spec with the given sequence chain.
// Commands in the sequence are executed in order after this command completes.
func (s Spec) WithSequence(names ...string) Spec {
	s.Sequence = names
	return s
}

// WithDeadlineAt returns a copy of the Spec with the given absolute deadline.
// If the command hasn't started by this time, Expired() is called instead.
func (s Spec) WithDeadlineAt(t time.Time) Spec {
	s.DeadlineAt = &t
	return s
}
