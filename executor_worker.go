package durex

import (
	"context"
	"fmt"
	"time"
)

// worker processes commands from the queue (single-instance mode).
func (e *Executor) worker(_ int) {
	defer e.wg.Done()

	for {
		select {
		case <-e.ctx.Done():
			return
		case instance, ok := <-e.queue:
			if !ok {
				return
			}
			e.safeExecute(instance, false)
		}
	}
}

// safeExecute wraps execute with panic recovery.
// If claimed is true, the instance was already transitioned to STARTED by ClaimPending().
func (e *Executor) safeExecute(instance *Instance, claimed bool) {
	defer func() {
		if r := recover(); r != nil {
			e.logger.Error("durex: panic in command execution",
				"id", instance.ID,
				"name", instance.Name,
				"panic", r,
			)

			// Mark the command as failed
			instance.Status = StatusFailed
			instance.Error = fmt.Sprintf("panic: %v", r)
			now := time.Now()
			instance.CompletedAt = &now

			// Use a separate context with timeout for panic recovery updates
			// to ensure the update succeeds even during shutdown when e.ctx is cancelled
			updateCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if err := e.storage.Update(updateCtx, instance); err != nil {
				e.logger.Error("durex: failed to update panicked command",
					"id", instance.ID,
					"error", err,
				)
			}

			// Call error handler if set
			if e.errorHandler != nil {
				e.errorHandler(instance, fmt.Errorf("panic: %v", r))
			}
		}
	}()

	if err := e.execute(instance, claimed); err != nil {
		e.logger.Error("durex: command execution failed",
			"id", instance.ID,
			"name", instance.Name,
			"error", err,
		)
	}
}

// pollingWorker claims commands directly from storage (multi-instance safe).
// Uses row-level locking to prevent multiple executors from claiming the same command.
func (e *Executor) pollingWorker(_ int) {
	defer e.wg.Done()

	lockingStorage := e.storage.(LockingStorage)
	ticker := time.NewTicker(e.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.claimAndExecute(lockingStorage)
		}
	}
}

// claimAndExecute claims pending commands and executes them.
func (e *Executor) claimAndExecute(storage LockingStorage) {
	if e.stopping.Load() {
		return
	}

	// Claim a batch of commands
	commands, err := storage.ClaimPending(e.ctx, e.claimBatchSize)
	if err != nil {
		e.logger.Error("durex: failed to claim commands", "error", err)
		return
	}

	// Execute each claimed command
	for _, instance := range commands {
		if e.stopping.Load() {
			return
		}

		e.safeExecute(instance, true)
	}
}

// execute runs a single command instance.
// If claimed is true, the instance was already transitioned to STARTED status
// by ClaimPending() (polling/locking mode). Otherwise, execute handles the
// status transition itself (queue mode).
func (e *Executor) execute(instance *Instance, claimed bool) error {
	baseCtx := e.ctx
	now := time.Now()

	// Check if cancelled
	if e.stopping.Load() {
		return nil
	}

	// Check deadline
	if instance.DeadlineAt != nil && now.After(*instance.DeadlineAt) {
		return e.handleExpired(baseCtx, instance)
	}

	// Queue mode: check if ready (polling mode never has this issue —
	// ClaimPending only returns ready instances)
	if !claimed && now.Before(instance.ReadyAt) {
		e.scheduleDelayed(instance)
		return nil
	}

	// Resolve handler
	handler, err := e.registry.Resolve(instance.Name)
	if err != nil {
		instance.Status = StatusFailed
		instance.Error = err.Error()
		return e.storage.Update(baseCtx, instance)
	}

	// Apply rate limiting
	if e.rateLimiter != nil {
		release, err := e.rateLimiter.Acquire(baseCtx, instance.Name)
		if err != nil {
			if claimed {
				// Polling mode: reset to PENDING for next poll cycle
				instance.Status = StatusPending
				return e.storage.Update(baseCtx, instance)
			}
			// Queue mode: re-queue with delay
			e.scheduleDelayed(instance)
			return nil
		}
		defer release()
	}

	// Queue mode: transition to STARTED (polling mode already did this in ClaimPending)
	if !claimed {
		instance.Status = StatusStarted
		instance.StartedAt = &now
		instance.Attempt++
		instance.RecordEvent(EventStarted, "")
		if err := e.storage.Update(baseCtx, instance); err != nil {
			return err
		}
	}

	// Collect metrics
	if e.metrics != nil {
		e.metrics.CommandStarted(instance.Name)
	}

	e.logger.Debug("durex: executing command",
		"id", instance.ID,
		"name", instance.Name,
		"attempt", instance.Attempt,
	)

	// Create execution context with timeout if specified
	execCtx := baseCtx
	var cancel context.CancelFunc
	if instance.Timeout > 0 {
		execCtx, cancel = context.WithTimeout(baseCtx, instance.Timeout)
		defer cancel()
	}

	// Execute with middleware
	result, err := e.executeWithMiddleware(execCtx, instance, handler)

	// Record duration
	duration := time.Since(now)

	// Check for timeout
	if err == nil && execCtx.Err() == context.DeadlineExceeded {
		err = fmt.Errorf("durex: command execution timed out after %v", instance.Timeout)
	}

	if err != nil {
		if e.metrics != nil {
			e.metrics.CommandFailed(instance.Name, err)
		}
		return e.handleError(baseCtx, instance, handler, err)
	}

	if e.metrics != nil {
		e.metrics.CommandCompleted(instance.Name, duration)
	}

	return e.handleResult(baseCtx, instance, handler, result)
}

// executeWithMiddleware runs the command through the middleware chain.
func (e *Executor) executeWithMiddleware(ctx context.Context, instance *Instance, handler Command) (Result, error) {
	if len(e.middleware) == 0 {
		return handler.Execute(ctx, instance)
	}

	// Build middleware chain
	var chain func() (Result, error)
	chain = func() (Result, error) {
		return handler.Execute(ctx, instance)
	}

	for i := len(e.middleware) - 1; i >= 0; i-- {
		mw := e.middleware[i]
		next := chain
		mwCtx := MiddlewareContext{
			Command:  instance,
			Handler:  handler,
			Executor: e,
		}
		chain = func() (Result, error) {
			return mw(mwCtx, next)
		}
	}

	return chain()
}
