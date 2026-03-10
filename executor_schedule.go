package durex

import (
	"context"
	"time"
)

// queueSchedule adds an instance to the execution queue (queue mode only).
func (e *Executor) queueSchedule(instance *Instance) {
	delay := time.Until(instance.ReadyAt)

	if delay <= 0 {
		select {
		case e.queue <- instance:
		case <-e.ctx.Done():
		}
		return
	}

	e.scheduleDelayed(instance)
}

// scheduleDelayed schedules an instance for future execution.
func (e *Executor) scheduleDelayed(instance *Instance) {
	delay := time.Until(instance.ReadyAt)

	// Cap delay to prevent timer issues
	if delay > e.maxDelay {
		delay = e.maxDelay
	}

	// Minimum delay to avoid tight loops
	if delay < 0 {
		delay = 0
	}

	timer := time.AfterFunc(delay, func() {
		// Remove from tracking when timer fires
		e.delayedTimersMu.Lock()
		delete(e.delayedTimers, instance.ID)
		e.delayedTimersMu.Unlock()

		if e.stopping.Load() {
			return
		}
		select {
		case e.queue <- instance:
		case <-e.ctx.Done():
		}
	})

	// Track the timer for cleanup on shutdown
	e.delayedTimersMu.Lock()
	// Cancel any existing timer for this instance (e.g., if rescheduled)
	if existing, ok := e.delayedTimers[instance.ID]; ok {
		existing.Stop()
	}
	e.delayedTimers[instance.ID] = timer
	e.delayedTimersMu.Unlock()
}

// cancelDelayedTimers stops all pending delayed timers.
func (e *Executor) cancelDelayedTimers() {
	e.delayedTimersMu.Lock()
	defer e.delayedTimersMu.Unlock()

	count := len(e.delayedTimers)
	for id, timer := range e.delayedTimers {
		timer.Stop()
		delete(e.delayedTimers, id)
	}

	if count > 0 {
		e.logger.Debug("durex: cancelled delayed timers", "count", count)
	}
}

// replay loads pending commands from storage.
func (e *Executor) replay(ctx context.Context) error {
	commands, err := e.storage.FindPending(ctx)
	if err != nil {
		return err
	}

	for _, cmd := range commands {
		e.scheduleFn(cmd)
	}

	e.logger.Info("durex: replayed pending commands", "count", len(commands))
	return nil
}

// startPermanentCommand starts a permanent command.
func (e *Executor) startPermanentCommand(name string) error {
	handler, err := e.registry.Resolve(name)
	if err != nil {
		return err
	}

	// Get default spec
	var spec Spec
	if defaulter, ok := handler.(Defaulter); ok {
		spec = defaulter.Default()
	}
	spec.Name = name

	_, err = e.Add(e.ctx, spec)
	return err
}
