package durex

import (
	"errors"
	"time"
)

// cleanupLoop periodically cleans up old commands.
func (e *Executor) cleanupLoop() {
	defer e.wg.Done()
	ticker := time.NewTicker(e.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			count, err := e.storage.Cleanup(e.ctx, e.cleanupAge)
			if err != nil {
				e.logger.Error("durex: cleanup failed", "error", err)
			} else if count > 0 {
				e.logger.Debug("durex: cleaned up old commands", "count", count)
			}
		}
	}
}

// stuckCommandRecoveryLoop periodically recovers stuck commands.
// Stuck commands are those in STARTED status for longer than stuckThreshold,
// which may indicate a worker crash or process restart.
func (e *Executor) stuckCommandRecoveryLoop() {
	defer e.wg.Done()
	ticker := time.NewTicker(e.stuckCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.recoverStuckCommands()
		}
	}
}

// recoverStuckCommands finds and resets stuck commands.
func (e *Executor) recoverStuckCommands() {
	ctx := e.ctx

	// Find commands in STARTED status
	commands, err := e.storage.FindByStatus(ctx, StatusStarted)
	if err != nil {
		e.logger.Error("durex: failed to find started commands", "error", err)
		return
	}

	now := time.Now()
	recovered := 0

	for _, cmd := range commands {
		// Check if the command has been stuck for too long
		if cmd.StartedAt == nil {
			continue
		}

		stuckDuration := now.Sub(*cmd.StartedAt)
		if stuckDuration < e.stuckThreshold {
			continue
		}

		e.logger.Warn("durex: recovering stuck command",
			"id", cmd.ID,
			"name", cmd.Name,
			"started_at", cmd.StartedAt,
			"stuck_duration", stuckDuration,
		)

		// Reset to pending so it can be picked up again
		cmd.Status = StatusPending
		cmd.StartedAt = nil
		// Don't increment attempt - this is recovery, not a retry

		if err := e.storage.Update(ctx, cmd); err != nil {
			e.logger.Error("durex: failed to recover stuck command",
				"id", cmd.ID,
				"error", err,
			)
			continue
		}

		e.scheduleFn(cmd)

		recovered++
	}

	if recovered > 0 {
		e.logger.Info("durex: recovered stuck commands", "count", recovered)
	}
}

// createInstance creates a new Instance from a Spec.
func (e *Executor) createInstance(spec Spec) (*Instance, error) {
	// Validate
	if spec.Name == "" {
		return nil, errors.New("durex: command name is required")
	}

	// Get defaults from handler
	if handler, err := e.registry.Resolve(spec.Name); err == nil {
		if defaulter, ok := handler.(Defaulter); ok {
			defaults := defaulter.Default()
			spec = mergeSpecs(defaults, spec)
		}
	}

	now := time.Now()
	readyAt := now.Add(spec.Delay)

	instance := &Instance{
		ID:            e.idGen.Generate(),
		Name:          spec.Name,
		Data:          spec.Data,
		Status:        StatusPending,
		Retries:       spec.Retries,
		Sequence:      spec.Sequence,
		Priority:      spec.Priority,
		Tags:          spec.Tags,
		UniqueKey:     spec.UniqueKey,
		TraceID:       spec.TraceID,
		CorrelationID: spec.CorrelationID,
		CreatedAt:     now,
		ReadyAt:       readyAt,
		Period:        spec.Period,
		Cron:          spec.Cron,
		Timeout:       spec.Timeout,
		Attempt:       0,
	}

	// Apply default retries if not specified
	if instance.Retries == 0 && e.defaultRetries > 0 {
		instance.Retries = e.defaultRetries
	}

	// Apply default timeout if not specified
	if instance.Timeout == 0 && e.defaultTimeout > 0 {
		instance.Timeout = e.defaultTimeout
	}

	// Set deadline
	if spec.DeadlineAt != nil {
		instance.DeadlineAt = spec.DeadlineAt
	} else if spec.Deadline > 0 {
		deadline := now.Add(spec.Deadline)
		instance.DeadlineAt = &deadline
	}

	// Record creation event
	instance.RecordEvent(EventCreated, "")

	return instance, nil
}

// mergeSpecs merges default spec with user spec (user takes precedence).
func mergeSpecs(defaults, user Spec) Spec {
	result := defaults

	if user.Name != "" {
		result.Name = user.Name
	}
	if user.Data != nil {
		if result.Data == nil {
			result.Data = make(M)
		}
		for k, v := range user.Data {
			result.Data[k] = v
		}
	}
	if user.Delay != 0 {
		result.Delay = user.Delay
	}
	if user.Period != 0 {
		result.Period = user.Period
	}
	if user.Cron != "" {
		result.Cron = user.Cron
	}
	if user.Timeout != 0 {
		result.Timeout = user.Timeout
	}
	if user.Deadline != 0 {
		result.Deadline = user.Deadline
	}
	if user.DeadlineAt != nil {
		result.DeadlineAt = user.DeadlineAt
	}
	if user.Retries != 0 {
		result.Retries = user.Retries
	}
	if len(user.Sequence) > 0 {
		result.Sequence = user.Sequence
	}
	if user.Priority != 0 {
		result.Priority = user.Priority
	}
	if len(user.Tags) > 0 {
		result.Tags = user.Tags
	}

	if user.UniqueKey != "" {
		result.UniqueKey = user.UniqueKey
	}

	if user.TraceID != "" {
		result.TraceID = user.TraceID
	}

	if user.CorrelationID != "" {
		result.CorrelationID = user.CorrelationID
	}

	return result
}

// ptr returns a pointer to the given value.
func ptr[T any](v T) *T {
	return &v
}
