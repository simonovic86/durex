package durex

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Add queues a new command for execution.
func (e *Executor) Add(ctx context.Context, spec Spec) (*Instance, error) {
	if !e.started.Load() {
		return nil, ErrExecutorNotReady
	}

	instance, err := e.createInstance(spec)
	if err != nil {
		return nil, err
	}

	// Check for duplicate unique key
	if instance.UniqueKey != "" {
		existing, err := e.storage.FindByUniqueKey(ctx, instance.UniqueKey)
		if err == nil && existing != nil {
			e.logger.Debug("durex: duplicate command blocked",
				"unique_key", instance.UniqueKey,
				"existing_id", existing.ID,
			)
			return nil, ErrDuplicateCommand
		}
		// ErrNotFound is expected and means we can proceed
		if err != nil && !errors.Is(err, ErrNotFound) {
			return nil, fmt.Errorf("durex: failed to check unique key: %w", err)
		}
	}

	if err := e.storage.Create(ctx, instance); err != nil {
		return nil, fmt.Errorf("durex: failed to persist command: %w", err)
	}

	// Clone before scheduling to avoid data races - once scheduled, workers
	// may immediately start modifying the instance (retries, status, etc.)
	result := instance.Clone()

	e.scheduleFn(instance)

	e.logger.Debug("durex: command added",
		"id", result.ID,
		"name", result.Name,
		"delay", time.Until(result.ReadyAt),
	)

	return result, nil
}

// AddMany queues multiple commands for execution.
func (e *Executor) AddMany(ctx context.Context, specs ...Spec) ([]*Instance, error) {
	instances := make([]*Instance, len(specs))
	for i, spec := range specs {
		instance, err := e.Add(ctx, spec)
		if err != nil {
			return instances[:i], err
		}
		instances[i] = instance
	}
	return instances, nil
}

// Get retrieves a command instance by ID.
func (e *Executor) Get(ctx context.Context, id string) (*Instance, error) {
	return e.storage.Get(ctx, id)
}

// History returns the execution history for a command.
func (e *Executor) History(ctx context.Context, id string) ([]Event, error) {
	instance, err := e.storage.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return instance.History, nil
}

// Cancel cancels a pending command.
func (e *Executor) Cancel(ctx context.Context, id string) error {
	instance, err := e.storage.Get(ctx, id)
	if err != nil {
		return err
	}

	if instance.Status.IsTerminal() {
		return fmt.Errorf("durex: cannot cancel command in status %s", instance.Status)
	}

	instance.Status = StatusCancelled
	now := time.Now()
	instance.CompletedAt = &now
	instance.RecordEvent(EventCancelled, "")
	return e.storage.Update(ctx, instance)
}

// CancelByTag cancels all pending commands with the given tag.
// Returns the number of commands cancelled.
// Requires QueryableStorage to support tag queries.
func (e *Executor) CancelByTag(ctx context.Context, tag string) (int, error) {
	qs, ok := e.storage.(QueryableStorage)
	if !ok {
		return 0, fmt.Errorf("durex: storage does not support tag queries")
	}

	// Find commands with this tag
	commands, err := qs.Find(ctx, Query{
		Tags: []string{tag},
	})
	if err != nil {
		return 0, err
	}

	cancelled := 0
	now := time.Now()
	for _, cmd := range commands {
		if cmd.Status.IsTerminal() {
			continue
		}
		cmd.Status = StatusCancelled
		cmd.CompletedAt = &now
		if err := e.storage.Update(ctx, cmd); err != nil {
			e.logger.Error("durex: failed to cancel command",
				"id", cmd.ID,
				"error", err,
			)
			continue
		}
		cancelled++
	}

	return cancelled, nil
}

// ReplayFromDLQ replays a dead-lettered command.
// The command is reset to PENDING status and will be executed again.
func (e *Executor) ReplayFromDLQ(ctx context.Context, id string) error {
	instance, err := e.storage.Get(ctx, id)
	if err != nil {
		return err
	}

	if instance.Status != StatusDeadLetter {
		return fmt.Errorf("durex: command is not in dead letter queue (status: %s)", instance.Status)
	}

	// Reset for retry
	instance.Status = StatusPending
	instance.Error = ""
	instance.StartedAt = nil
	instance.CompletedAt = nil
	instance.Attempt = 0
	instance.ReadyAt = time.Now()

	if err := e.storage.Update(ctx, instance); err != nil {
		return err
	}

	// Schedule for execution
	e.scheduleFn(instance)

	e.logger.Info("durex: replayed command from DLQ",
		"id", instance.ID,
		"name", instance.Name,
	)

	return nil
}

// FindDeadLettered returns all commands in the dead letter queue.
func (e *Executor) FindDeadLettered(ctx context.Context) ([]*Instance, error) {
	return e.storage.FindByStatus(ctx, StatusDeadLetter)
}

// PurgeDLQ removes dead-lettered commands older than the specified age.
// Returns the number of commands purged.
func (e *Executor) PurgeDLQ(ctx context.Context, age time.Duration) (int, error) {
	commands, err := e.storage.FindByStatus(ctx, StatusDeadLetter)
	if err != nil {
		return 0, err
	}

	cutoff := time.Now().Add(-age)
	purged := 0

	for _, cmd := range commands {
		if cmd.CompletedAt != nil && cmd.CompletedAt.Before(cutoff) {
			if err := e.storage.Delete(ctx, cmd.ID); err != nil {
				e.logger.Error("durex: failed to purge DLQ command",
					"id", cmd.ID,
					"error", err,
				)
				continue
			}
			purged++
		}
	}

	if purged > 0 {
		e.logger.Info("durex: purged dead letter queue", "count", purged)
	}

	return purged, nil
}

// Stats returns current executor statistics.
func (e *Executor) Stats(ctx context.Context) (*Stats, error) {
	pending, err := e.storage.Count(ctx, ptr(StatusPending))
	if err != nil {
		return nil, err
	}

	completed, err := e.storage.Count(ctx, ptr(StatusCompleted))
	if err != nil {
		return nil, err
	}

	failed, err := e.storage.Count(ctx, ptr(StatusFailed))
	if err != nil {
		return nil, err
	}

	deadLetter, err := e.storage.Count(ctx, ptr(StatusDeadLetter))
	if err != nil {
		return nil, err
	}

	repeating, err := e.storage.Count(ctx, ptr(StatusRepeating))
	if err != nil {
		return nil, err
	}

	stats := &Stats{
		Pending:            pending,
		Completed:          completed,
		Failed:             failed,
		DeadLetter:         deadLetter,
		Repeating:          repeating,
		QueueSize:          len(e.queue),
		RegisteredCommands: e.registry.Count(),
		WorkerCount:        e.parallelism,
	}

	if e.rateLimiter != nil {
		rlStats := e.rateLimiter.Stats()
		stats.RateLimit = &rlStats
	}

	return stats, nil
}

// Stats holds executor statistics.
type Stats struct {
	Pending            int64
	Completed          int64
	Failed             int64
	DeadLetter         int64
	Repeating          int64
	QueueSize          int
	RegisteredCommands int
	WorkerCount        int
	RateLimit          *RateLimitStats
}
