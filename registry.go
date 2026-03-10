package durex

import (
	"fmt"
	"sync"
)

// Registry manages command handler registration and resolution.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Command
}

// NewRegistry creates a new command registry.
func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[string]Command),
	}
}

// Register adds a command handler to the registry.
// Returns an error if the name is empty or a handler with the same name is already registered.
func (r *Registry) Register(cmd Command) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	name := cmd.Name()
	if name == "" {
		return fmt.Errorf("durex: command name cannot be empty")
	}

	if _, exists := r.handlers[name]; exists {
		return fmt.Errorf("durex: command %q already registered", name)
	}

	r.handlers[name] = cmd
	return nil
}

// MustRegister is like Register but panics on error.
func (r *Registry) MustRegister(cmd Command) {
	if err := r.Register(cmd); err != nil {
		panic(err.Error())
	}
}

// Overwrite adds or replaces a command handler in the registry.
// Useful for testing or dynamic reconfiguration.
func (r *Registry) Overwrite(cmd Command) {
	r.mu.Lock()
	defer r.mu.Unlock()

	name := cmd.Name()
	if name == "" {
		panic("durex: command name cannot be empty")
	}

	r.handlers[name] = cmd
}

// Resolve returns the command handler for the given name.
// Returns an error if no handler is registered.
func (r *Registry) Resolve(name string) (Command, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	cmd, exists := r.handlers[name]
	if !exists {
		return nil, fmt.Errorf("durex: no handler registered for command %q", name)
	}
	return cmd, nil
}

// Has returns true if a handler is registered for the given name.
func (r *Registry) Has(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, exists := r.handlers[name]
	return exists
}

// Names returns a list of all registered command names.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.handlers))
	for name := range r.handlers {
		names = append(names, name)
	}
	return names
}

// Count returns the number of registered handlers.
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.handlers)
}

// Unregister removes a command handler from the registry.
// Returns true if a handler was removed.
func (r *Registry) Unregister(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.handlers[name]; exists {
		delete(r.handlers, name)
		return true
	}
	return false
}

// Clear removes all registered handlers.
func (r *Registry) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers = make(map[string]Command)
}
