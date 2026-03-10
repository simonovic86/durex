package durex

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
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

	// Mode-specific scheduling (set in Start based on storage type)
	scheduleFn func(instance *Instance)

	// Dashboard server for graceful shutdown
	dashboardServer *http.Server
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
		scheduleFn:            func(_ *Instance) {}, // no-op until Start() sets the real implementation
	}

	for _, opt := range opts {
		opt(e)
	}

	e.queue = make(chan *Instance, e.queueSize)
	return e
}

// Register adds a command handler to the executor.
// Must be called before Start. Panics if the name is empty or already registered.
func (e *Executor) Register(cmd Command) *Executor {
	e.registry.MustRegister(cmd)
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
		// Polling mode: scheduling is a no-op (polling workers discover pending instances)
		e.scheduleFn = func(_ *Instance) {}

		// Use polling workers that claim directly from storage
		// This is safe for multi-instance deployments
		for i := 0; i < e.parallelism; i++ {
			e.wg.Add(1)
			go e.pollingWorker(i)
		}
	} else {
		// Queue mode: use channel-based scheduling
		e.scheduleFn = e.queueSchedule

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

	// Mark as started before launching permanent commands (they call Add which checks this flag)
	e.started.Store(true)

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
		e.wg.Add(1)
		go e.cleanupLoop()
	}

	// Start stuck command recovery routine
	if e.stuckCheckInterval > 0 {
		e.wg.Add(1)
		go e.stuckCommandRecoveryLoop()
	}

	// Start dashboard if configured
	if e.dashboardAddr != "" {
		e.startDashboard()
	}

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

	// Shut down dashboard server if running
	if e.dashboardServer != nil {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := e.dashboardServer.Shutdown(shutdownCtx); err != nil {
			e.logger.Error("durex: dashboard shutdown error", "error", err)
		}
	}

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

	var err error
	select {
	case <-done:
		e.logger.Info("durex: executor stopped gracefully")
	case <-time.After(e.shutdownTimeout):
		e.logger.Warn("durex: executor shutdown timed out")
		err = fmt.Errorf("durex: shutdown timed out after %v", e.shutdownTimeout)
	}

	e.started.Store(false)
	return err
}

// startDashboard starts the dashboard HTTP server in a tracked goroutine.
func (e *Executor) startDashboard() {
	e.dashboardServer = &http.Server{
		Addr:         e.dashboardAddr,
		Handler:      e.DashboardHandler(),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		e.logger.Info("durex: starting dashboard", "addr", e.dashboardAddr)
		if err := e.dashboardServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			e.logger.Error("durex: dashboard server error", "error", err)
		}
	}()
}
