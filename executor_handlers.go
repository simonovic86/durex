package durex

import (
	"context"
	"fmt"
	"time"
)

// handleResult processes the execution result.
func (e *Executor) handleResult(ctx context.Context, instance *Instance, _ Command, result Result) error {
	now := time.Now()

	// Handle repeat
	if result.Repeat {
		instance.Status = StatusRepeating

		// Calculate next run time: cron expression takes precedence over period
		var nextRun time.Time
		var scheduleInfo string

		if instance.Cron != "" {
			nextRun = NextCronTime(instance.Cron, now)
			if nextRun.IsZero() {
				// Invalid cron expression, fall back to period
				period := instance.Period
				if period == 0 {
					period = e.defaultRepeatInterval
				}
				nextRun = now.Add(period)
				scheduleInfo = fmt.Sprintf("next run in %v (cron fallback)", period)
				e.logger.Warn("durex: invalid cron expression, using period",
					"id", instance.ID,
					"cron", instance.Cron,
				)
			} else {
				scheduleInfo = fmt.Sprintf("next run at %s (cron: %s)", nextRun.Format(time.RFC3339), instance.Cron)
			}
		} else {
			period := instance.Period
			if period == 0 {
				period = e.defaultRepeatInterval
			}
			nextRun = now.Add(period)
			scheduleInfo = fmt.Sprintf("next run in %v", period)
		}

		instance.ReadyAt = nextRun
		instance.RecordEvent(EventRepeating, scheduleInfo)
		if err := e.storage.Update(ctx, instance); err != nil {
			return err
		}
		e.scheduleFn(instance)
		return nil
	}

	// Handle retry
	if result.Retry {
		if instance.Retries > 0 {
			instance.Retries--
			instance.Status = StatusPending
			instance.RecordEvent(EventRetrying, fmt.Sprintf("retries left: %d", instance.Retries))
			if e.metrics != nil {
				e.metrics.CommandRetried(instance.Name, instance.Attempt)
			}
			if err := e.storage.Update(ctx, instance); err != nil {
				return err
			}
			e.scheduleFn(instance)
			return nil
		}
	}

	// Spawn children and barrier
	childIDs := e.spawnChildren(ctx, instance, result.Commands)

	if result.Continuation != nil && len(childIDs) > 0 {
		continuation := *result.Continuation
		propagateIDs(&continuation, instance)
		e.spawnBarrier(ctx, instance, continuation, childIDs)
	}

	// Mark completed
	instance.Status = StatusCompleted
	instance.CompletedAt = &now
	instance.RecordEvent(EventCompleted, "")
	return e.storage.Update(ctx, instance)
}

// spawnChildren creates and schedules child command instances.
// Returns the IDs of successfully created children.
func (e *Executor) spawnChildren(ctx context.Context, parent *Instance, specs []Spec) []string {
	childIDs := make([]string, 0, len(specs))
	for _, spec := range specs {
		propagateIDs(&spec, parent)

		child, err := e.createInstance(spec)
		if err != nil {
			e.logger.Error("durex: failed to create child command",
				"parent_id", parent.ID,
				"child_name", spec.Name,
				"error", err,
			)
			continue
		}
		child.ParentID = &parent.ID

		if err := e.storage.Create(ctx, child); err != nil {
			e.logger.Error("durex: failed to persist child command",
				"parent_id", parent.ID,
				"child_id", child.ID,
				"error", err,
			)
			continue
		}
		childIDs = append(childIDs, child.ID)
		e.scheduleFn(child)
	}
	return childIDs
}

// spawnBarrier creates the internal barrier command that triggers
// a continuation after all children complete (fan-in pattern).
func (e *Executor) spawnBarrier(ctx context.Context, parent *Instance, continuation Spec, childIDs []string) {
	barrierSpec := Spec{
		Name: barrierCommandName,
		Data: M{
			"coordinator_id": parent.ID,
			"expected_count": len(childIDs),
			"continuation":   continuation,
			"child_ids":      childIDs,
		},
		TraceID:       parent.TraceID,
		CorrelationID: parent.CorrelationID,
		Delay:         time.Second, // Give children time to be created
	}

	barrier, err := e.createInstance(barrierSpec)
	if err != nil {
		e.logger.Error("durex: failed to create barrier command",
			"parent_id", parent.ID,
			"error", err,
		)
		return
	}

	barrier.ParentID = &parent.ID
	if err := e.storage.Create(ctx, barrier); err != nil {
		e.logger.Error("durex: failed to persist barrier command",
			"parent_id", parent.ID,
			"barrier_id", barrier.ID,
			"error", err,
		)
		return
	}

	e.scheduleFn(barrier)
	e.logger.Debug("durex: barrier command created",
		"parent_id", parent.ID,
		"barrier_id", barrier.ID,
		"children_count", len(childIDs),
	)
}

// propagateIDs copies trace and correlation IDs from a parent instance to a child spec
// when the child spec does not already have them set.
func propagateIDs(spec *Spec, parent *Instance) {
	if spec.TraceID == "" && parent.TraceID != "" {
		spec.TraceID = parent.TraceID
	}
	if spec.CorrelationID == "" {
		if parent.CorrelationID != "" {
			spec.CorrelationID = parent.CorrelationID
		} else {
			// Use root command's ID as correlation ID
			spec.CorrelationID = parent.ID
		}
	}
}

// handleError processes execution errors.
func (e *Executor) handleError(ctx context.Context, instance *Instance, handler Command, err error) error {
	now := time.Now()

	e.logger.Warn("durex: command failed",
		"id", instance.ID,
		"name", instance.Name,
		"attempt", instance.Attempt,
		"retries_left", instance.Retries,
		"error", err,
	)

	// Retry if possible
	if instance.Retries > 0 {
		instance.Retries--
		instance.Status = StatusPending
		instance.Error = err.Error()

		// Apply backoff delay
		backoffDelay := e.backoff.NextDelay(instance.Attempt)
		instance.ReadyAt = now.Add(backoffDelay)

		instance.RecordError(EventRetrying, err)

		if e.metrics != nil {
			e.metrics.CommandRetried(instance.Name, instance.Attempt)
		}

		e.logger.Debug("durex: scheduling retry with backoff",
			"id", instance.ID,
			"name", instance.Name,
			"attempt", instance.Attempt,
			"backoff", backoffDelay,
		)

		if err := e.storage.Update(ctx, instance); err != nil {
			return err
		}
		e.scheduleFn(instance)
		return nil
	}

	// Mark failed - use DLQ status if enabled
	if e.deadLetterEnabled {
		instance.Status = StatusDeadLetter
		instance.RecordError(EventRecovered, err)
	} else {
		instance.Status = StatusFailed
		instance.RecordError(EventFailed, err)
	}
	instance.Error = err.Error()
	instance.CompletedAt = &now
	if updateErr := e.storage.Update(ctx, instance); updateErr != nil {
		return updateErr
	}

	// Call error handler
	if e.errorHandler != nil {
		e.errorHandler(instance, err)
	}

	// Call Recover if implemented
	if recoverable, ok := handler.(Recoverable); ok {
		result, recoverErr := recoverable.Recover(ctx, instance, err)
		if recoverErr != nil {
			e.logger.Error("durex: recover failed",
				"id", instance.ID,
				"name", instance.Name,
				"error", recoverErr,
			)
			return nil
		}

		// Spawn recovery commands
		for _, spec := range result.Commands {
			if _, err := e.Add(ctx, spec); err != nil {
				e.logger.Error("durex: failed to add recovery command",
					"error", err,
				)
			}
		}
	}

	return nil
}

// handleExpired processes deadline expiration.
func (e *Executor) handleExpired(ctx context.Context, instance *Instance) error {
	now := time.Now()

	e.logger.Warn("durex: command expired",
		"id", instance.ID,
		"name", instance.Name,
		"deadline", instance.DeadlineAt,
	)

	instance.Status = StatusExpired
	instance.CompletedAt = &now
	instance.RecordEvent(EventExpired, "deadline exceeded")
	if err := e.storage.Update(ctx, instance); err != nil {
		return err
	}

	// Resolve handler for Expired callback
	handler, err := e.registry.Resolve(instance.Name)
	if err != nil {
		return nil
	}

	// Call Expired if implemented
	if expirable, ok := handler.(Expirable); ok {
		result, expiredErr := expirable.Expired(ctx, instance)
		if expiredErr != nil {
			e.logger.Error("durex: expired handler failed",
				"id", instance.ID,
				"name", instance.Name,
				"error", expiredErr,
			)
			return nil
		}

		// Spawn follow-up commands
		for _, spec := range result.Commands {
			if _, err := e.Add(ctx, spec); err != nil {
				e.logger.Error("durex: failed to add expired follow-up command",
					"error", err,
				)
			}
		}
	}

	return nil
}
