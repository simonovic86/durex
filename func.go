package durex

import (
	"context"
)

// ExecuteFunc is the function signature for command execution.
type ExecuteFunc func(ctx context.Context, cmd *Instance) (Result, error)

// RecoverFunc is the function signature for error recovery.
type RecoverFunc func(ctx context.Context, cmd *Instance, err error) (Result, error)

// ExpiredFunc is the function signature for deadline expiration.
type ExpiredFunc func(ctx context.Context, cmd *Instance) (Result, error)

// FuncCommand wraps a function as a Command.
// Created via HandleFunc or NewFunc.
type FuncCommand struct {
	name        string
	executeFn   ExecuteFunc
	recoverFn   RecoverFunc
	expiredFn   ExpiredFunc
	defaultSpec Spec
}

// Name implements Command.
func (f *FuncCommand) Name() string {
	return f.name
}

// Execute implements Command.
func (f *FuncCommand) Execute(ctx context.Context, cmd *Instance) (Result, error) {
	return f.executeFn(ctx, cmd)
}

// Recover implements Recoverable.
func (f *FuncCommand) Recover(ctx context.Context, cmd *Instance, err error) (Result, error) {
	if f.recoverFn != nil {
		return f.recoverFn(ctx, cmd, err)
	}
	return Empty(), nil
}

// Expired implements Expirable.
func (f *FuncCommand) Expired(ctx context.Context, cmd *Instance) (Result, error) {
	if f.expiredFn != nil {
		return f.expiredFn(ctx, cmd)
	}
	return Empty(), nil
}

// Default implements Defaulter.
func (f *FuncCommand) Default() Spec {
	spec := f.defaultSpec
	spec.Name = f.name
	return spec
}

// FuncOption configures a FuncCommand.
type FuncOption func(*FuncCommand)

// Retries sets the default retry count.
func Retries(n int) FuncOption {
	return func(f *FuncCommand) {
		f.defaultSpec.Retries = n
	}
}

// OnRecover sets the recovery function.
func OnRecover(fn RecoverFunc) FuncOption {
	return func(f *FuncCommand) {
		f.recoverFn = fn
	}
}

// OnExpired sets the expiration handler.
func OnExpired(fn ExpiredFunc) FuncOption {
	return func(f *FuncCommand) {
		f.expiredFn = fn
	}
}

// Period sets the repeat period for recurring commands.
func Period(d Duration) FuncOption {
	return func(f *FuncCommand) {
		f.defaultSpec.Period = d
	}
}

// Cron sets a cron expression for scheduled commands.
// When a command returns Repeat(), it will be rescheduled based on this expression.
// Uses standard cron format: "minute hour day-of-month month day-of-week"
//
// Examples:
//
//	durex.Cron("0 0 * * *")     // Daily at midnight
//	durex.Cron("*/15 * * * *")  // Every 15 minutes
//	durex.Cron("0 9 * * 1-5")   // Weekdays at 9 AM
//
// If both Cron and Period are set, Cron takes precedence.
func Cron(expr string) FuncOption {
	return func(f *FuncCommand) {
		f.defaultSpec.Cron = expr
	}
}

// Deadline sets the default deadline.
func Deadline(d Duration) FuncOption {
	return func(f *FuncCommand) {
		f.defaultSpec.Deadline = d
	}
}

// Tags sets default tags.
func Tags(tags ...string) FuncOption {
	return func(f *FuncCommand) {
		f.defaultSpec.Tags = tags
	}
}

// NewFunc creates a new FuncCommand.
// Use this when you need to configure the command before registering.
//
// Example:
//
//	cmd := durex.NewFunc("sendEmail", sendEmailFn,
//	    durex.Retries(3),
//	    durex.OnRecover(handleFailure),
//	)
//	executor.Handle(cmd)
func NewFunc(name string, fn ExecuteFunc, opts ...FuncOption) *FuncCommand {
	cmd := &FuncCommand{
		name:      name,
		executeFn: fn,
	}
	for _, opt := range opts {
		opt(cmd)
	}
	return cmd
}

// HandleFunc registers a function as a command handler.
// This is the simplest way to create a command.
//
// Example:
//
//	executor.HandleFunc("sendEmail", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
//	    to := cmd.GetString("to")
//	    if err := mailer.Send(to); err != nil {
//	        return durex.Empty(), err
//	    }
//	    return durex.Empty(), nil
//	})
//
// With options:
//
//	executor.HandleFunc("sendEmail", sendEmailFn,
//	    durex.Retries(3),
//	    durex.OnRecover(handleFailure),
//	)
func (e *Executor) HandleFunc(name string, fn ExecuteFunc, opts ...FuncOption) *Executor {
	cmd := NewFunc(name, fn, opts...)
	e.registry.Register(cmd)
	return e
}

// Ensure FuncCommand implements all optional interfaces.
var (
	_ Command     = (*FuncCommand)(nil)
	_ Recoverable = (*FuncCommand)(nil)
	_ Expirable   = (*FuncCommand)(nil)
	_ Defaulter   = (*FuncCommand)(nil)
)
