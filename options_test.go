package durex_test

import (
	"context"
	"testing"
	"time"

	"github.com/simonovic86/durex"
	"github.com/simonovic86/durex/storage"
)

func TestRetries_NegativeClampedToZero(t *testing.T) {
	cmd := durex.NewFunc("test", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.Empty(), nil
	}, durex.Retries(-5))

	spec := cmd.Default()
	if spec.Retries != 0 {
		t.Errorf("expected retries clamped to 0, got %d", spec.Retries)
	}
}

func TestPeriod_NegativeIgnored(t *testing.T) {
	cmd := durex.NewFunc("test", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.Empty(), nil
	}, durex.Period(-1*time.Second))

	spec := cmd.Default()
	if spec.Period != 0 {
		t.Errorf("expected period to remain 0, got %v", spec.Period)
	}
}

func TestDeadline_NegativeIgnored(t *testing.T) {
	cmd := durex.NewFunc("test", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.Empty(), nil
	}, durex.Deadline(-1*time.Second))

	spec := cmd.Default()
	if spec.Deadline != 0 {
		t.Errorf("expected deadline to remain 0, got %v", spec.Deadline)
	}
}

func TestWithRetries_NegativeClampedToZero(t *testing.T) {
	type MyData struct {
		Value string `json:"value"`
	}

	cmd := durex.NewTyped("test",
		func(ctx context.Context, data MyData, cmd *durex.Instance) (durex.Result, error) {
			return durex.Empty(), nil
		},
		durex.WithRetries[MyData](-3),
	)

	spec := cmd.Default()
	if spec.Retries != 0 {
		t.Errorf("expected retries clamped to 0, got %d", spec.Retries)
	}
}

func TestWithDefaultRetries_NegativeClampedToZero(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithDefaultRetries(-10))

	executor.HandleFunc("test", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.Empty(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)

	inst, err := executor.Add(ctx, durex.Spec{Name: "test"})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	if inst.Retries < 0 {
		t.Errorf("expected non-negative retries, got %d", inst.Retries)
	}

	executor.Stop()
}
