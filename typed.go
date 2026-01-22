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

// TypedCommand wraps a typed function as a Command.
type TypedCommand[T any] struct {
	name        string
	executeFn   TypedExecuteFunc[T]
	recoverFn   TypedRecoverFunc[T]
	defaultSpec Spec
}

// TypedOption configures a TypedCommand.
type TypedOption[T any] func(*TypedCommand[T])

// WithRetries sets the default retry count for typed commands.
func WithRetries[T any](n int) TypedOption[T] {
	return func(c *TypedCommand[T]) {
		c.defaultSpec.Retries = n
	}
}

// WithRecover sets the recovery function for typed commands.
func WithRecover[T any](fn TypedRecoverFunc[T]) TypedOption[T] {
	return func(c *TypedCommand[T]) {
		c.recoverFn = fn
	}
}

// WithPeriod sets the repeat period for typed commands.
func WithPeriod[T any](d time.Duration) TypedOption[T] {
	return func(c *TypedCommand[T]) {
		c.defaultSpec.Period = d
	}
}

// WithDeadline sets the default deadline for typed commands.
func WithDeadline[T any](d time.Duration) TypedOption[T] {
	return func(c *TypedCommand[T]) {
		c.defaultSpec.Deadline = d
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
		return Empty(), nil
	}
	return c.recoverFn(ctx, data, cmd, err)
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
func HandleTyped[T any](e *Executor, name string, fn TypedExecuteFunc[T], opts ...TypedOption[T]) {
	cmd := NewTyped(name, fn, opts...)
	e.registry.Register(cmd)
}

// Typed creates a Spec with typed data.
// The data struct is automatically converted to a map.
//
// Example:
//
//	executor.Add(ctx, durex.Typed("sendEmail", EmailData{
//	    To:      "user@example.com",
//	    Subject: "Welcome!",
//	}))
func Typed[T any](name string, data T) Spec {
	// Convert struct to map
	jsonBytes, _ := json.Marshal(data)
	var m M
	json.Unmarshal(jsonBytes, &m)

	return Spec{
		Name: name,
		Data: m,
	}
}

// Ensure TypedCommand implements interfaces.
var (
	_ Command     = (*TypedCommand[any])(nil)
	_ Recoverable = (*TypedCommand[any])(nil)
	_ Defaulter   = (*TypedCommand[any])(nil)
)
