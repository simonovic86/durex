package durex

import (
	"context"
	"errors"
	"time"
)

// Common storage errors.
var (
	ErrNotFound         = errors.New("durex: command not found")
	ErrAlreadyExists    = errors.New("durex: command already exists")
	ErrStorageClosed    = errors.New("durex: storage is closed")
	ErrDuplicateCommand = errors.New("durex: command with this unique key already exists")
)

// Storage defines the interface for command persistence.
// Implementations must be safe for concurrent use.
type Storage interface {
	// Create persists a new command instance.
	// Returns ErrAlreadyExists if an instance with the same ID exists.
	Create(ctx context.Context, cmd *Instance) error

	// Update saves changes to an existing command instance.
	// Returns ErrNotFound if the instance doesn't exist.
	Update(ctx context.Context, cmd *Instance) error

	// Delete removes a command instance by ID.
	// Returns nil if the instance doesn't exist (idempotent).
	Delete(ctx context.Context, id string) error

	// Get retrieves a command instance by ID.
	// Returns ErrNotFound if the instance doesn't exist.
	Get(ctx context.Context, id string) (*Instance, error)

	// FindPending returns all commands that are ready for execution.
	// This includes commands with status PENDING, STARTED, or REPEATING
	// where ReadyAt <= now.
	FindPending(ctx context.Context) ([]*Instance, error)

	// FindByStatus returns commands with the given status.
	FindByStatus(ctx context.Context, status Status) ([]*Instance, error)

	// FindByParent returns all child commands of the given parent.
	FindByParent(ctx context.Context, parentID string) ([]*Instance, error)

	// FindByUniqueKey returns an active command with the given unique key.
	// Returns ErrNotFound if no active command with this key exists.
	// Only searches non-terminal statuses (PENDING, STARTED, REPEATING).
	FindByUniqueKey(ctx context.Context, key string) (*Instance, error)

	// Cleanup removes completed/failed/expired commands older than the given age.
	// Returns the number of commands deleted.
	Cleanup(ctx context.Context, olderThan time.Duration) (int64, error)

	// Count returns the total number of commands, optionally filtered by status.
	Count(ctx context.Context, status *Status) (int64, error)

	// Close releases any resources held by the storage.
	Close() error
}

// Query represents a flexible query for finding commands.
type Query struct {
	// Status filters by command status.
	Status *Status

	// Name filters by command name.
	Name *string

	// ParentID filters by parent command.
	ParentID *string

	// Tags filters by tags (commands must have all specified tags).
	Tags []string

	// CreatedAfter filters commands created after this time.
	CreatedAfter *time.Time

	// CreatedBefore filters commands created before this time.
	CreatedBefore *time.Time

	// Limit restricts the number of results.
	Limit int

	// Offset skips the first N results.
	Offset int

	// OrderBy specifies the sort field.
	OrderBy string

	// OrderDesc sorts in descending order.
	OrderDesc bool
}

// QueryableStorage extends Storage with advanced query capabilities.
// Implement this interface for full-featured storage backends.
type QueryableStorage interface {
	Storage

	// Find returns commands matching the query.
	Find(ctx context.Context, query Query) ([]*Instance, error)
}

// TransactionalStorage extends Storage with transaction support.
// Implement this interface for ACID-compliant storage backends.
type TransactionalStorage interface {
	Storage

	// Begin starts a new transaction.
	Begin(ctx context.Context) (Transaction, error)
}

// Transaction represents a storage transaction.
type Transaction interface {
	Storage

	// Commit commits the transaction.
	Commit() error

	// Rollback aborts the transaction.
	Rollback() error
}

// Hooks allows observing storage operations.
// Useful for logging, metrics, and debugging.
type Hooks struct {
	// AfterCreate is called after a command is created.
	AfterCreate func(ctx context.Context, cmd *Instance)

	// AfterUpdate is called after a command is updated.
	AfterUpdate func(ctx context.Context, cmd *Instance)

	// AfterDelete is called after a command is deleted.
	AfterDelete func(ctx context.Context, id string)

	// OnError is called when a storage operation fails.
	OnError func(ctx context.Context, op string, err error)
}

// HookedStorage wraps a Storage with lifecycle hooks.
type HookedStorage struct {
	Storage
	Hooks Hooks
}

// Create implements Storage.
func (h *HookedStorage) Create(ctx context.Context, cmd *Instance) error {
	err := h.Storage.Create(ctx, cmd)
	if err != nil {
		if h.Hooks.OnError != nil {
			h.Hooks.OnError(ctx, "Create", err)
		}
		return err
	}
	if h.Hooks.AfterCreate != nil {
		h.Hooks.AfterCreate(ctx, cmd)
	}
	return nil
}

// Update implements Storage.
func (h *HookedStorage) Update(ctx context.Context, cmd *Instance) error {
	err := h.Storage.Update(ctx, cmd)
	if err != nil {
		if h.Hooks.OnError != nil {
			h.Hooks.OnError(ctx, "Update", err)
		}
		return err
	}
	if h.Hooks.AfterUpdate != nil {
		h.Hooks.AfterUpdate(ctx, cmd)
	}
	return nil
}

// Delete implements Storage.
func (h *HookedStorage) Delete(ctx context.Context, id string) error {
	err := h.Storage.Delete(ctx, id)
	if err != nil {
		if h.Hooks.OnError != nil {
			h.Hooks.OnError(ctx, "Delete", err)
		}
		return err
	}
	if h.Hooks.AfterDelete != nil {
		h.Hooks.AfterDelete(ctx, id)
	}
	return nil
}
