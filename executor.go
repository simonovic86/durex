package durex

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Common executor errors.
var (
	ErrExecutorStopped  = errors.New("durex: executor is stopped")
	ErrExecutorNotReady = errors.New("durex: executor is not started")
)

// Executor manages command execution with persistence, retries, and scheduling.
type Executor struct {
	// Core components
	registry *Registry
	storage  Storage
	logger   *slog.Logger
	idGen    IDGenerator

	// Configuration
	parallelism           int
	queueSize             int
	defaultRetries        int
	defaultTimeout        time.Duration
	defaultRepeatInterval time.Duration
	maxDelay              time.Duration
	cleanupInterval       time.Duration
	cleanupAge            time.Duration
	shutdownTimeout       time.Duration
	permanentCommands     []string
	backoff               BackoffStrategy
	pollInterval          time.Duration
	claimBatchSize        int

	// Stuck command recovery
	stuckCheckInterval time.Duration
	stuckThreshold     time.Duration

	// Dashboard
	dashboardAddr string

	// Dead Letter Queue
	deadLetterEnabled bool

	// Extensibility
	middleware   []Middleware
	metrics      MetricsCollector
	errorHandler func(cmd *Instance, err error)
	rateLimiter  *RateLimiter

	// Runtime state
	queue    chan *Instance
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
	started  atomic.Bool
	stopping atomic.Bool

	// Timer tracking for delayed commands
	delayedTimers   map[string]*time.Timer
	delayedTimersMu sync.Mutex
}

// New creates a new Executor with the given storage and options.
func New(storage Storage, opts ...Option) *Executor {
	ctx, cancel := context.WithCancel(context.Background())

	e := &Executor{
		registry:              NewRegistry(),
		storage:               storage,
		logger:                slog.Default(),
		idGen:                 &DefaultIDGenerator{},
		parallelism:           4,
		queueSize:             1000,
		defaultRetries:        0,
		defaultTimeout:        0, // No default timeout
		defaultRepeatInterval: time.Minute,
		maxDelay:              24 * time.Hour,
		cleanupInterval:       time.Hour,
		cleanupAge:            24 * time.Hour,
		shutdownTimeout:       30 * time.Second,
		stuckCheckInterval:    0, // Disabled by default
		stuckThreshold:        5 * time.Minute,
		backoff:               NoBackoff(),
		pollInterval:          time.Second,
		claimBatchSize:        10,
		ctx:                   ctx,
		cancel:                cancel,
		delayedTimers:         make(map[string]*time.Timer),
	}

	for _, opt := range opts {
		opt(e)
	}

	e.queue = make(chan *Instance, e.queueSize)
	return e
}

// Register adds a command handler to the executor.
// Must be called before Start.
func (e *Executor) Register(cmd Command) *Executor {
	e.registry.Register(cmd)
	return e
}

// Start begins processing commands.
// It replays pending commands from storage and starts worker goroutines.
func (e *Executor) Start(ctx context.Context) error {
	if e.started.Load() {
		return nil
	}

	// Register internal barrier command
	e.registerBarrierCommand()

	// Check if storage supports locking (safe for multi-instance)
	_, useLocking := e.storage.(LockingStorage)

	e.logger.Info("durex: starting executor",
		"parallelism", e.parallelism,
		"registered_commands", e.registry.Count(),
		"locking_mode", useLocking,
	)

	if useLocking {
		// Use polling workers that claim directly from storage
		// This is safe for multi-instance deployments
		for i := 0; i < e.parallelism; i++ {
			e.wg.Add(1)
			go e.pollingWorker(i)
		}
	} else {
		// Use queue-based workers (single instance only)
		for i := 0; i < e.parallelism; i++ {
			e.wg.Add(1)
			go e.worker(i)
		}

		// Replay pending commands into the queue
		if err := e.replay(ctx); err != nil {
			e.logger.Error("durex: failed to replay pending commands", "error", err)
			return err
		}
	}

	// Start permanent commands
	for _, name := range e.permanentCommands {
		if err := e.startPermanentCommand(name); err != nil {
			e.logger.Error("durex: failed to start permanent command",
				"name", name,
				"error", err,
			)
		}
	}

	// Start cleanup routine
	if e.cleanupInterval > 0 {
		go e.cleanupLoop()
	}

	// Start stuck command recovery routine
	if e.stuckCheckInterval > 0 {
		go e.stuckCommandRecoveryLoop()
	}

	// Start dashboard if configured
	if e.dashboardAddr != "" {
		go func() {
			e.logger.Info("durex: starting dashboard", "addr", e.dashboardAddr)
			if err := e.ServeDashboard(e.dashboardAddr); err != nil {
				e.logger.Error("durex: dashboard server error", "error", err)
			}
		}()
	}

	e.started.Store(true)

	return nil
}

// Stop gracefully shuts down the executor.
// It waits for in-flight commands to complete up to the shutdown timeout.
func (e *Executor) Stop() error {
	if !e.started.Load() || e.stopping.Load() {
		return nil
	}

	e.stopping.Store(true)
	e.logger.Info("durex: stopping executor")

	// Cancel all pending delayed timers to release resources
	e.cancelDelayedTimers()

	// Signal shutdown
	e.cancel()

	// Wait for workers with timeout
	done := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		e.logger.Info("durex: executor stopped gracefully")
	case <-time.After(e.shutdownTimeout):
		e.logger.Warn("durex: executor shutdown timed out")
	}

	e.started.Store(false)
	return nil
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

	e.schedule(instance)

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
	e.schedule(instance)

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
			e.safeExecute(instance)
		}
	}
}

// safeExecute wraps execute with panic recovery.
func (e *Executor) safeExecute(instance *Instance) {
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

			if err := e.storage.Update(e.ctx, instance); err != nil {
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

	if err := e.execute(instance); err != nil {
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

		e.safeExecuteClaimed(instance)
	}
}

// safeExecuteClaimed wraps executeClaimedCommand with panic recovery.
func (e *Executor) safeExecuteClaimed(instance *Instance) {
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

			if err := e.storage.Update(e.ctx, instance); err != nil {
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

	if err := e.executeClaimedCommand(instance); err != nil {
		e.logger.Error("durex: command execution failed",
			"id", instance.ID,
			"name", instance.Name,
			"error", err,
		)
	}
}

// executeClaimedCommand executes a command that was already claimed (status=STARTED).
func (e *Executor) executeClaimedCommand(instance *Instance) error {
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
			// Context cancelled, reschedule
			instance.Status = StatusPending
			return e.storage.Update(baseCtx, instance)
		}
		defer release()
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

// execute runs a single command instance.
func (e *Executor) execute(instance *Instance) error {
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

	// Check if ready
	if now.Before(instance.ReadyAt) {
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
			// Context cancelled, reschedule
			e.scheduleDelayed(instance)
			return nil
		}
		defer release()
	}

	// Update status to started
	instance.Status = StatusStarted
	instance.StartedAt = &now
	instance.Attempt++
	instance.RecordEvent(EventStarted, "")
	if err := e.storage.Update(baseCtx, instance); err != nil {
		return err
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
		e.schedule(instance)
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
			e.schedule(instance)
			return nil
		}
	}

	// Spawn children
	childIDs := make([]string, 0, len(result.Commands))
	for _, spec := range result.Commands {
		// Propagate trace and correlation IDs to children
		if spec.TraceID == "" && instance.TraceID != "" {
			spec.TraceID = instance.TraceID
		}
		if spec.CorrelationID == "" {
			if instance.CorrelationID != "" {
				spec.CorrelationID = instance.CorrelationID
			} else {
				// Use root command's ID as correlation ID
				spec.CorrelationID = instance.ID
			}
		}

		child, err := e.createInstance(spec)
		if err != nil {
			e.logger.Error("durex: failed to create child command",
				"parent_id", instance.ID,
				"child_name", spec.Name,
				"error", err,
			)
			continue
		}
		child.ParentID = &instance.ID

		if err := e.storage.Create(ctx, child); err != nil {
			e.logger.Error("durex: failed to persist child command",
				"parent_id", instance.ID,
				"child_id", child.ID,
				"error", err,
			)
			continue
		}
		childIDs = append(childIDs, child.ID)
		e.schedule(child)
	}

	// Spawn barrier command if there's a continuation
	if result.Continuation != nil && len(childIDs) > 0 {
		// Propagate trace and correlation IDs to continuation
		continuation := *result.Continuation
		if continuation.TraceID == "" && instance.TraceID != "" {
			continuation.TraceID = instance.TraceID
		}
		if continuation.CorrelationID == "" {
			if instance.CorrelationID != "" {
				continuation.CorrelationID = instance.CorrelationID
			} else {
				continuation.CorrelationID = instance.ID
			}
		}

		// Create barrier command that waits for all children
		barrierSpec := Spec{
			Name: barrierCommandName,
			Data: M{
				"coordinator_id": instance.ID,
				"expected_count": len(childIDs),
				"continuation":   continuation,
				"child_ids":      childIDs,
			},
			TraceID:       instance.TraceID,
			CorrelationID: instance.CorrelationID,
			Delay:         time.Second, // Give children time to be created
		}

		barrier, err := e.createInstance(barrierSpec)
		if err != nil {
			e.logger.Error("durex: failed to create barrier command",
				"parent_id", instance.ID,
				"error", err,
			)
		} else {
			barrier.ParentID = &instance.ID
			if err := e.storage.Create(ctx, barrier); err != nil {
				e.logger.Error("durex: failed to persist barrier command",
					"parent_id", instance.ID,
					"barrier_id", barrier.ID,
					"error", err,
				)
			} else {
				e.schedule(barrier)
				e.logger.Debug("durex: barrier command created",
					"parent_id", instance.ID,
					"barrier_id", barrier.ID,
					"children_count", len(childIDs),
				)
			}
		}
	}

	// Mark completed
	instance.Status = StatusCompleted
	instance.CompletedAt = &now
	instance.RecordEvent(EventCompleted, "")
	return e.storage.Update(ctx, instance)
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
		e.schedule(instance)
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

// schedule adds an instance to the execution queue.
func (e *Executor) schedule(instance *Instance) {
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

// replay loads pending commands from storage.
func (e *Executor) replay(ctx context.Context) error {
	commands, err := e.storage.FindPending(ctx)
	if err != nil {
		return err
	}

	for _, cmd := range commands {
		e.schedule(cmd)
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

// cleanupLoop periodically cleans up old commands.
func (e *Executor) cleanupLoop() {
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

		// Schedule for execution (only in non-locking mode)
		if _, ok := e.storage.(LockingStorage); !ok {
			e.schedule(cmd)
		}

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
