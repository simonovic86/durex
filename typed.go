package durex

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Duration is an alias for time.Duration that allows FuncOption to work
// without import cycles in user code.
type Duration = time.Duration

// TypedExecuteFunc is a function that receives typed data.
type TypedExecuteFunc[T any] func(ctx context.Context, data T, cmd *Instance) (Result, error)

// TypedRecoverFunc is a recovery function that receives typed data.
type TypedRecoverFunc[T any] func(ctx context.Context, data T, cmd *Instance, err error) (Result, error)

// TypedExpiredFunc is an expiration handler that receives typed data.
type TypedExpiredFunc[T any] func(ctx context.Context, data T, cmd *Instance) (Result, error)

// TypedCommand wraps a typed function as a Command.
type TypedCommand[T any] struct {
	name        string
	executeFn   TypedExecuteFunc[T]
	recoverFn   TypedRecoverFunc[T]
	expiredFn   TypedExpiredFunc[T]
	defaultSpec Spec
}

// TypedOption configures a TypedCommand.
type TypedOption[T any] func(*TypedCommand[T])

// WithRetries sets the default retry count for typed commands.
// Negative values are clamped to 0.
func WithRetries[T any](n int) TypedOption[T] {
	return func(c *TypedCommand[T]) {
		c.defaultSpec.Retries = max(n, 0)
	}
}

// WithRecover sets the recovery function for typed commands.
func WithRecover[T any](fn TypedRecoverFunc[T]) TypedOption[T] {
	return func(c *TypedCommand[T]) {
		c.recoverFn = fn
	}
}

// WithPeriod sets the repeat period for typed commands.
// Non-positive durations are ignored.
func WithPeriod[T any](d time.Duration) TypedOption[T] {
	return func(c *TypedCommand[T]) {
		if d > 0 {
			c.defaultSpec.Period = d
		}
	}
}

// WithCron sets a cron expression for scheduled typed commands.
// When a command returns Repeat(), it will be rescheduled based on this expression.
// Uses standard cron format: "minute hour day-of-month month day-of-week"
//
// Examples:
//
//	durex.WithCron[MyData]("0 0 * * *")     // Daily at midnight
//	durex.WithCron[MyData]("*/15 * * * *")  // Every 15 minutes
//	durex.WithCron[MyData]("0 9 * * 1-5")   // Weekdays at 9 AM
//
// If both WithCron and WithPeriod are set, Cron takes precedence.
func WithCron[T any](expr string) TypedOption[T] {
	return func(c *TypedCommand[T]) {
		c.defaultSpec.Cron = expr
	}
}

// WithDeadline sets the default deadline for typed commands.
// Non-positive durations are ignored.
func WithDeadline[T any](d time.Duration) TypedOption[T] {
	return func(c *TypedCommand[T]) {
		if d > 0 {
			c.defaultSpec.Deadline = d
		}
	}
}

// WithExpired sets the expiration handler for typed commands.
// Called when a command's deadline expires before execution begins.
func WithExpired[T any](fn TypedExpiredFunc[T]) TypedOption[T] {
	return func(c *TypedCommand[T]) {
		c.expiredFn = fn
	}
}

// WithTags sets default tags for typed commands.
func WithTags[T any](tags ...string) TypedOption[T] {
	return func(c *TypedCommand[T]) {
		c.defaultSpec.Tags = tags
	}
}

// Name implements Command.
func (c *TypedCommand[T]) Name() string {
	return c.name
}

// Execute implements Command.
func (c *TypedCommand[T]) Execute(ctx context.Context, cmd *Instance) (Result, error) {
	var data T
	if err := c.unmarshalData(cmd.Data, &data); err != nil {
		return Empty(), fmt.Errorf("failed to unmarshal command data: %w", err)
	}
	return c.executeFn(ctx, data, cmd)
}

// Recover implements Recoverable.
func (c *TypedCommand[T]) Recover(ctx context.Context, cmd *Instance, err error) (Result, error) {
	if c.recoverFn == nil {
		return Empty(), nil
	}
	var data T
	if unmarshalErr := c.unmarshalData(cmd.Data, &data); unmarshalErr != nil {
		return Empty(), fmt.Errorf("failed to unmarshal recovery data: %w", unmarshalErr)
	}
	return c.recoverFn(ctx, data, cmd, err)
}

// Expired implements Expirable.
func (c *TypedCommand[T]) Expired(ctx context.Context, cmd *Instance) (Result, error) {
	if c.expiredFn == nil {
		return Empty(), nil
	}
	var data T
	if err := c.unmarshalData(cmd.Data, &data); err != nil {
		return Empty(), fmt.Errorf("failed to unmarshal expired data: %w", err)
	}
	return c.expiredFn(ctx, data, cmd)
}

// Default implements Defaulter.
func (c *TypedCommand[T]) Default() Spec {
	spec := c.defaultSpec
	spec.Name = c.name
	return spec
}

func (c *TypedCommand[T]) unmarshalData(data M, target *T) error {
	// Convert map to JSON then unmarshal to struct
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return json.Unmarshal(jsonBytes, target)
}

// NewTyped creates a new typed command.
//
// Example:
//
//	type EmailData struct {
//	    To      string `json:"to"`
//	    Subject string `json:"subject"`
//	}
//
//	cmd := durex.NewTyped("sendEmail", func(ctx context.Context, data EmailData, cmd *durex.Instance) (durex.Result, error) {
//	    return mailer.Send(data.To, data.Subject), nil
//	})
func NewTyped[T any](name string, fn TypedExecuteFunc[T], opts ...TypedOption[T]) *TypedCommand[T] {
	cmd := &TypedCommand[T]{
		name:      name,
		executeFn: fn,
	}
	for _, opt := range opts {
		opt(cmd)
	}
	return cmd
}

// HandleTyped registers a typed command handler.
// The data from the command instance is automatically unmarshaled to the type T.
//
// Example:
//
//	type OrderData struct {
//	    OrderID string  `json:"orderId"`
//	    Amount  float64 `json:"amount"`
//	}
//
//	durex.HandleTyped(executor, "processOrder", func(ctx context.Context, data OrderData, cmd *durex.Instance) (durex.Result, error) {
//	    // data is already typed - no GetString() needed!
//	    log.Printf("Processing order %s for $%.2f", data.OrderID, data.Amount)
//	    return durex.Empty(), nil
//	})
func HandleTyped[T any](e *Executor, name string, fn TypedExecuteFunc[T], opts ...TypedOption[T]) *Executor {
	cmd := NewTyped(name, fn, opts...)
	e.registry.MustRegister(cmd)
	return e
}

// Typed creates a Spec with typed data.
// The data struct is automatically converted to a map.
// Returns an error if the data cannot be marshaled to JSON.
//
// Example:
//
//	spec, err := durex.Typed("sendEmail", EmailData{
//	    To:      "user@example.com",
//	    Subject: "Welcome!",
//	})
func Typed[T any](name string, data T) (Spec, error) {
	// Convert struct to map
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return Spec{}, fmt.Errorf("failed to marshal data: %w", err)
	}
	var m M
	if err := json.Unmarshal(jsonBytes, &m); err != nil {
		return Spec{}, fmt.Errorf("failed to unmarshal data to map: %w", err)
	}

	return Spec{
		Name: name,
		Data: m,
	}, nil
}

// MustTyped is like Typed but panics on error.
func MustTyped[T any](name string, data T) Spec {
	spec, err := Typed(name, data)
	if err != nil {
		panic(fmt.Sprintf("durex: %v", err))
	}
	return spec
}

// Ensure TypedCommand implements interfaces.
var (
	_ Command     = (*TypedCommand[any])(nil)
	_ Recoverable = (*TypedCommand[any])(nil)
	_ Expirable   = (*TypedCommand[any])(nil)
	_ Defaulter   = (*TypedCommand[any])(nil)
)
