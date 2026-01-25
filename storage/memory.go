// Package storage provides storage implementations for durex.
package storage

import (
	"context"
	"sync"
	"time"

	"github.com/simonovic86/durex"
)

// Compile-time interface assertions.
var (
	_ durex.Storage          = (*Memory)(nil)
	_ durex.QueryableStorage = (*Memory)(nil)
)

// Memory is an in-memory storage implementation.
// Useful for testing and development.
// Not recommended for production as data is lost on restart.
type Memory struct {
	mu       sync.RWMutex
	commands map[string]*durex.Instance
	closed   bool
}

// NewMemory creates a new in-memory storage.
func NewMemory() *Memory {
	return &Memory{
		commands: make(map[string]*durex.Instance),
	}
}

// Create implements durex.Storage.
func (m *Memory) Create(ctx context.Context, cmd *durex.Instance) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return durex.ErrStorageClosed
	}

	if _, exists := m.commands[cmd.ID]; exists {
		return durex.ErrAlreadyExists
	}

	// Store a clone to prevent external mutations
	m.commands[cmd.ID] = cmd.Clone()
	return nil
}

// Update implements durex.Storage.
func (m *Memory) Update(ctx context.Context, cmd *durex.Instance) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return durex.ErrStorageClosed
	}

	if _, exists := m.commands[cmd.ID]; !exists {
		return durex.ErrNotFound
	}

	m.commands[cmd.ID] = cmd.Clone()
	return nil
}

// Delete implements durex.Storage.
func (m *Memory) Delete(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return durex.ErrStorageClosed
	}

	delete(m.commands, id)
	return nil
}

// Get implements durex.Storage.
func (m *Memory) Get(ctx context.Context, id string) (*durex.Instance, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, durex.ErrStorageClosed
	}

	cmd, exists := m.commands[id]
	if !exists {
		return nil, durex.ErrNotFound
	}

	return cmd.Clone(), nil
}

// FindPending implements durex.Storage.
func (m *Memory) FindPending(ctx context.Context) ([]*durex.Instance, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, durex.ErrStorageClosed
	}

	var result []*durex.Instance
	for _, cmd := range m.commands {
		if cmd.Status.IsActive() {
			result = append(result, cmd.Clone())
		}
	}

	return result, nil
}

// FindByStatus implements durex.Storage.
func (m *Memory) FindByStatus(ctx context.Context, status durex.Status) ([]*durex.Instance, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, durex.ErrStorageClosed
	}

	var result []*durex.Instance
	for _, cmd := range m.commands {
		if cmd.Status == status {
			result = append(result, cmd.Clone())
		}
	}

	return result, nil
}

// FindByParent implements durex.Storage.
func (m *Memory) FindByParent(ctx context.Context, parentID string) ([]*durex.Instance, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, durex.ErrStorageClosed
	}

	var result []*durex.Instance
	for _, cmd := range m.commands {
		if cmd.ParentID != nil && *cmd.ParentID == parentID {
			result = append(result, cmd.Clone())
		}
	}

	return result, nil
}

// FindByUniqueKey implements durex.Storage.
func (m *Memory) FindByUniqueKey(ctx context.Context, key string) (*durex.Instance, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, durex.ErrStorageClosed
	}

	for _, cmd := range m.commands {
		if cmd.UniqueKey == key && cmd.Status.IsActive() {
			return cmd.Clone(), nil
		}
	}

	return nil, durex.ErrNotFound
}

// Cleanup implements durex.Storage.
func (m *Memory) Cleanup(ctx context.Context, olderThan time.Duration) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return 0, durex.ErrStorageClosed
	}

	cutoff := time.Now().Add(-olderThan)
	var count int64

	for id, cmd := range m.commands {
		if cmd.Status.IsTerminal() && cmd.CompletedAt != nil && cmd.CompletedAt.Before(cutoff) {
			delete(m.commands, id)
			count++
		}
	}

	return count, nil
}

// Count implements durex.Storage.
func (m *Memory) Count(ctx context.Context, status *durex.Status) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return 0, durex.ErrStorageClosed
	}

	var count int64
	for _, cmd := range m.commands {
		if status == nil || cmd.Status == *status {
			count++
		}
	}

	return count, nil
}

// Close implements durex.Storage.
func (m *Memory) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closed = true
	m.commands = nil
	return nil
}

// Find implements durex.QueryableStorage.
func (m *Memory) Find(ctx context.Context, query durex.Query) ([]*durex.Instance, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, durex.ErrStorageClosed
	}

	var result []*durex.Instance
	for _, cmd := range m.commands {
		if m.matchesQuery(cmd, query) {
			result = append(result, cmd.Clone())
		}
	}

	// Apply offset and limit
	if query.Offset > 0 {
		if query.Offset >= len(result) {
			return nil, nil
		}
		result = result[query.Offset:]
	}

	if query.Limit > 0 && query.Limit < len(result) {
		result = result[:query.Limit]
	}

	return result, nil
}

func (m *Memory) matchesQuery(cmd *durex.Instance, query durex.Query) bool {
	if query.Status != nil && cmd.Status != *query.Status {
		return false
	}

	if query.Name != nil && cmd.Name != *query.Name {
		return false
	}

	if query.ParentID != nil {
		if cmd.ParentID == nil || *cmd.ParentID != *query.ParentID {
			return false
		}
	}

	if len(query.Tags) > 0 {
		for _, tag := range query.Tags {
			if !cmd.HasTag(tag) {
				return false
			}
		}
	}

	if query.CreatedAfter != nil && cmd.CreatedAt.Before(*query.CreatedAfter) {
		return false
	}

	if query.CreatedBefore != nil && cmd.CreatedAt.After(*query.CreatedBefore) {
		return false
	}

	return true
}

// Reset clears all commands. Useful for testing.
func (m *Memory) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.commands = make(map[string]*durex.Instance)
}

// All returns all commands. Useful for debugging.
func (m *Memory) All() []*durex.Instance {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*durex.Instance, 0, len(m.commands))
	for _, cmd := range m.commands {
		result = append(result, cmd.Clone())
	}
	return result
}
