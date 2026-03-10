package durex_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/simonovic86/durex"
	"github.com/simonovic86/durex/storage"
)

func TestExecutor_Middleware_ExecutionOrder(t *testing.T) {
	store := storage.NewMemory()

	var order []string
	var mu sync.Mutex
	appendOrder := func(s string) {
		mu.Lock()
		order = append(order, s)
		mu.Unlock()
	}

	mw1 := func(ctx durex.MiddlewareContext, next func() (durex.Result, error)) (durex.Result, error) {
		appendOrder("mw1-before")
		r, err := next()
		appendOrder("mw1-after")
		return r, err
	}

	mw2 := func(ctx durex.MiddlewareContext, next func() (durex.Result, error)) (durex.Result, error) {
		appendOrder("mw2-before")
		r, err := next()
		appendOrder("mw2-after")
		return r, err
	}

	executor := durex.New(store,
		durex.WithParallelism(1),
		durex.WithMiddleware(mw1, mw2),
	)

	executor.HandleFunc("test", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		appendOrder("handler")
		return durex.Empty(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	executor.Add(ctx, durex.Spec{Name: "test"})
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	expected := []string{"mw1-before", "mw2-before", "handler", "mw2-after", "mw1-after"}
	if len(order) != len(expected) {
		t.Fatalf("order = %v, want %v", order, expected)
	}
	for i, v := range expected {
		if order[i] != v {
			t.Errorf("order[%d] = %q, want %q", i, order[i], v)
		}
	}
}

func TestExecutor_Middleware_ShortCircuit(t *testing.T) {
	store := storage.NewMemory()
	var handlerCalled atomic.Int32

	// Middleware that short-circuits and returns Retry
	mw := func(ctx durex.MiddlewareContext, next func() (durex.Result, error)) (durex.Result, error) {
		return durex.Retry(), nil // Don't call next()
	}

	executor := durex.New(store,
		durex.WithParallelism(1),
		durex.WithMiddleware(mw),
	)

	executor.HandleFunc("test", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		handlerCalled.Add(1)
		return durex.Empty(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	executor.Add(ctx, durex.Spec{Name: "test", Retries: 1})
	time.Sleep(200 * time.Millisecond)

	if got := handlerCalled.Load(); got != 0 {
		t.Errorf("Handler called %d times, want 0 (middleware short-circuited)", got)
	}
}

func TestExecutor_Middleware_HasContext(t *testing.T) {
	store := storage.NewMemory()
	var sawName atomic.Value

	mw := func(ctx durex.MiddlewareContext, next func() (durex.Result, error)) (durex.Result, error) {
		sawName.Store(ctx.Command.Name)
		return next()
	}

	executor := durex.New(store,
		durex.WithParallelism(1),
		durex.WithMiddleware(mw),
	)

	executor.HandleFunc("myCmd", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.Empty(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	executor.Add(ctx, durex.Spec{Name: "myCmd"})
	time.Sleep(200 * time.Millisecond)

	if got, _ := sawName.Load().(string); got != "myCmd" {
		t.Errorf("Middleware saw name %q, want myCmd", got)
	}
}

func TestExecutor_NoMiddleware(t *testing.T) {
	store := storage.NewMemory()
	var executed atomic.Int32

	executor := durex.New(store, durex.WithParallelism(1))

	executor.HandleFunc("test", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		executed.Add(1)
		return durex.Empty(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	executor.Add(ctx, durex.Spec{Name: "test"})
	time.Sleep(200 * time.Millisecond)

	if got := executed.Load(); got != 1 {
		t.Errorf("executed = %d, want 1", got)
	}
}
