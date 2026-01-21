// Package durex provides a durable execution framework for Go.
//
// Durex is a production-grade command pattern implementation that provides
// persistent task execution with retries, deadlines, and recovery mechanisms.
//
// Basic usage:
//
//	executor := durex.New(storage,
//		durex.WithParallelism(4),
//		durex.WithLogger(slog.Default()),
//	)
//
//	executor.Register(&MyCommand{})
//	executor.Start(ctx)
//
//	executor.Add(ctx, durex.Spec{
//		Name: "myCommand",
//		Data: durex.M{"key": "value"},
//	})
package durex

import (
	"context"
)

// M is a convenience alias for command data maps.
type M = map[string]any

// Command defines the interface that all command handlers must implement.
//
// Commands encapsulate business logic that can be executed asynchronously,
// persisted, retried on failure, and recovered on system restart.
type Command interface {
	// Name returns the unique identifier for this command type.
	// This name is used to register and resolve command handlers.
	Name() string

	// Execute runs the command logic.
	// Returns a Result indicating what should happen next.
	// If an error is returned, the command will be retried (if retries > 0)
	// or marked as failed and Recover will be called.
	Execute(ctx context.Context, cmd *Instance) (Result, error)
}

// Recoverable is an optional interface commands can implement
// to handle failures after all retries are exhausted.
type Recoverable interface {
	// Recover is called when a command fails after exhausting all retries.
	// Use this to clean up resources, notify systems, or spawn compensating commands.
	Recover(ctx context.Context, cmd *Instance, err error) (Result, error)
}

// Expirable is an optional interface commands can implement
// to handle deadline expiration.
type Expirable interface {
	// Expired is called when a command's deadline has passed before execution.
	// Use this to handle timeout scenarios gracefully.
	Expired(ctx context.Context, cmd *Instance) (Result, error)
}

// Defaulter is an optional interface commands can implement
// to provide default specification values.
type Defaulter interface {
	// Default returns the default Spec for this command type.
	// These values are merged with user-provided values when creating instances.
	Default() Spec
}

// BaseCommand provides a minimal Command implementation.
// Embed this in your commands to satisfy the Command interface
// with sensible defaults, then override only what you need.
//
// Example:
//
//	type MyCommand struct {
//		durex.BaseCommand
//		db *sql.DB
//	}
//
//	func (c *MyCommand) Name() string { return "myCommand" }
//
//	func (c *MyCommand) Execute(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
//		// your logic here
//		return durex.Empty(), nil
//	}
type BaseCommand struct{}

// Name returns an empty string. Override this in your command.
func (b BaseCommand) Name() string { return "" }

// Execute returns Empty. Override this in your command.
func (b BaseCommand) Execute(ctx context.Context, cmd *Instance) (Result, error) {
	return Empty(), nil
}

// Recover returns Empty. Override this to handle failures.
func (b BaseCommand) Recover(ctx context.Context, cmd *Instance, err error) (Result, error) {
	return Empty(), nil
}

// Expired returns Empty. Override this to handle deadline expiration.
func (b BaseCommand) Expired(ctx context.Context, cmd *Instance) (Result, error) {
	return Empty(), nil
}

// Default returns an empty Spec. Override this to provide defaults.
func (b BaseCommand) Default() Spec {
	return Spec{}
}
