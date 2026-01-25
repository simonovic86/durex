package durex

import (
	"context"
	"sync"
	"time"
)

// RateLimiter controls concurrent execution of commands.
type RateLimiter struct {
	mu       sync.Mutex
	limits   map[string]int             // command name -> max concurrent
	current  map[string]int             // command name -> current count
	waiters  map[string][]chan struct{} // command name -> waiting goroutines
	global   int                        // global max concurrent (0 = unlimited)
	globalCt int                        // global current count
}

// NewRateLimiter creates a new rate limiter.
func NewRateLimiter() *RateLimiter {
	return &RateLimiter{
		limits:  make(map[string]int),
		current: make(map[string]int),
		waiters: make(map[string][]chan struct{}),
	}
}

// SetLimit sets the maximum concurrent executions for a command type.
// Use 0 to remove the limit.
func (r *RateLimiter) SetLimit(name string, limit int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if limit <= 0 {
		delete(r.limits, name)
	} else {
		r.limits[name] = limit
	}
}

// SetGlobalLimit sets the maximum total concurrent executions across all commands.
// Use 0 to remove the limit.
func (r *RateLimiter) SetGlobalLimit(limit int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.global = limit
}

// Acquire attempts to acquire a slot for the given command.
// It blocks until a slot is available or the context is canceled.
// Returns a release function that must be called when the command completes.
func (r *RateLimiter) Acquire(ctx context.Context, name string) (release func(), err error) {
	for {
		r.mu.Lock()

		// Check global limit
		if r.global > 0 && r.globalCt >= r.global {
			ch := make(chan struct{}, 1)
			r.waiters[""] = append(r.waiters[""], ch)
			r.mu.Unlock()

			select {
			case <-ctx.Done():
				r.removeWaiter("", ch)
				return nil, ctx.Err()
			case <-ch:
				continue // retry
			}
		}

		// Check per-command limit
		limit, hasLimit := r.limits[name]
		if hasLimit && r.current[name] >= limit {
			ch := make(chan struct{}, 1)
			r.waiters[name] = append(r.waiters[name], ch)
			r.mu.Unlock()

			select {
			case <-ctx.Done():
				r.removeWaiter(name, ch)
				return nil, ctx.Err()
			case <-ch:
				continue // retry
			}
		}

		// Acquire slot
		r.current[name]++
		r.globalCt++
		r.mu.Unlock()

		return func() { r.release(name) }, nil
	}
}

// TryAcquire attempts to acquire a slot without blocking.
// Returns the release function and true if successful, nil and false otherwise.
func (r *RateLimiter) TryAcquire(name string) (release func(), ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Check global limit
	if r.global > 0 && r.globalCt >= r.global {
		return nil, false
	}

	// Check per-command limit
	limit, hasLimit := r.limits[name]
	if hasLimit && r.current[name] >= limit {
		return nil, false
	}

	// Acquire slot
	r.current[name]++
	r.globalCt++

	return func() { r.release(name) }, true
}

// release releases a slot for the given command.
func (r *RateLimiter) release(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.current[name] > 0 {
		r.current[name]--
	}
	if r.globalCt > 0 {
		r.globalCt--
	}

	// Notify one waiter for this command
	if len(r.waiters[name]) > 0 {
		ch := r.waiters[name][0]
		r.waiters[name] = r.waiters[name][1:]
		select {
		case ch <- struct{}{}:
		default:
		}
	}

	// Notify one global waiter
	if len(r.waiters[""]) > 0 {
		ch := r.waiters[""][0]
		r.waiters[""] = r.waiters[""][1:]
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// removeWaiter removes a waiting channel from the waiters list.
func (r *RateLimiter) removeWaiter(name string, ch chan struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()

	waiters := r.waiters[name]
	for i, w := range waiters {
		if w == ch {
			r.waiters[name] = append(waiters[:i], waiters[i+1:]...)
			return
		}
	}
}

// Current returns the current concurrent count for a command.
func (r *RateLimiter) Current(name string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.current[name]
}

// GlobalCurrent returns the total concurrent count across all commands.
func (r *RateLimiter) GlobalCurrent() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.globalCt
}

// Stats returns rate limiter statistics.
func (r *RateLimiter) Stats() RateLimitStats {
	r.mu.Lock()
	defer r.mu.Unlock()

	stats := RateLimitStats{
		GlobalLimit:   r.global,
		GlobalCurrent: r.globalCt,
		Commands:      make(map[string]CommandRateStats),
	}

	for name, limit := range r.limits {
		stats.Commands[name] = CommandRateStats{
			Limit:   limit,
			Current: r.current[name],
			Waiting: len(r.waiters[name]),
		}
	}

	return stats
}

// RateLimitStats holds rate limiter statistics.
type RateLimitStats struct {
	GlobalLimit   int
	GlobalCurrent int
	Commands      map[string]CommandRateStats
}

// CommandRateStats holds per-command rate statistics.
type CommandRateStats struct {
	Limit   int
	Current int
	Waiting int
}

// RateLimitMiddleware creates middleware that applies rate limiting.
// The timeout specifies how long to wait for a slot before giving up.
func RateLimitMiddleware(limiter *RateLimiter, timeout time.Duration) Middleware {
	return func(ctx MiddlewareContext, next func() (Result, error)) (Result, error) {
		// Create context with timeout for waiting
		waitCtx := context.Background()
		if timeout > 0 {
			var cancel context.CancelFunc
			waitCtx, cancel = context.WithTimeout(waitCtx, timeout)
			defer cancel()
		}

		release, err := limiter.Acquire(waitCtx, ctx.Command.Name)
		if err != nil {
			// Rate limit timeout - retry later
			return Retry(), nil
		}
		defer release()

		return next()
	}
}
