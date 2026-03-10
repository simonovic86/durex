package durex_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/simonovic86/durex"
	"github.com/simonovic86/durex/storage"
)

func TestTyped_Success(t *testing.T) {
	type EmailData struct {
		To      string `json:"to"`
		Subject string `json:"subject"`
	}

	spec, err := durex.Typed("sendEmail", EmailData{
		To:      "user@example.com",
		Subject: "Welcome!",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if spec.Name != "sendEmail" {
		t.Errorf("expected name 'sendEmail', got %q", spec.Name)
	}
	if spec.Data["to"] != "user@example.com" {
		t.Errorf("expected to 'user@example.com', got %v", spec.Data["to"])
	}
	if spec.Data["subject"] != "Welcome!" {
		t.Errorf("expected subject 'Welcome!', got %v", spec.Data["subject"])
	}
}

func TestTyped_MarshalError(t *testing.T) {
	type BadData struct {
		Ch chan int `json:"ch"`
	}
	_, err := durex.Typed("test", BadData{Ch: make(chan int)})
	if err == nil {
		t.Fatal("expected error for unmarshalable data")
	}
	if !strings.Contains(err.Error(), "failed to marshal data") {
		t.Errorf("expected marshal error, got: %v", err)
	}
}

func TestMustTyped_Success(t *testing.T) {
	type Data struct {
		Value string `json:"value"`
	}
	spec := durex.MustTyped("test", Data{Value: "hello"})
	if spec.Name != "test" {
		t.Errorf("expected name 'test', got %q", spec.Name)
	}
}

func TestMustTyped_Panics(t *testing.T) {
	type BadData struct {
		Ch chan int `json:"ch"`
	}
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for unmarshalable data")
		}
	}()
	durex.MustTyped("test", BadData{Ch: make(chan int)})
}

func TestTypedCommand_Recover_UnmarshalError(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithParallelism(1))

	recoverCalled := atomic.Bool{}

	type MyData struct {
		Value string `json:"value"`
	}

	durex.HandleTyped(executor, "recoverTest",
		func(ctx context.Context, data MyData, cmd *durex.Instance) (durex.Result, error) {
			return durex.Empty(), nil
		},
		durex.WithRecover(func(ctx context.Context, data MyData, cmd *durex.Instance, err error) (durex.Result, error) {
			recoverCalled.Store(true)
			return durex.Empty(), nil
		}),
	)

	// Test the Recover method directly with bad data via a TypedCommand
	cmd := durex.NewTyped("directRecoverTest",
		func(ctx context.Context, data MyData, cmd *durex.Instance) (durex.Result, error) {
			return durex.Empty(), nil
		},
		durex.WithRecover(func(ctx context.Context, data MyData, cmd *durex.Instance, err error) (durex.Result, error) {
			recoverCalled.Store(true)
			return durex.Empty(), nil
		}),
	)

	// Create an instance with data that can't unmarshal to MyData
	instance := &durex.Instance{
		ID:        "test-1",
		Name:      "directRecoverTest",
		Status:    durex.StatusFailed,
		Data:      durex.M{"value": map[string]any{"nested": "object"}}, // string field with non-string value
		CreatedAt: time.Now(),
		ReadyAt:   time.Now(),
	}

	_, recoverErr := cmd.Recover(context.Background(), instance, nil)
	if recoverErr == nil {
		t.Error("expected error from Recover with bad data")
	}
	if recoverCalled.Load() {
		t.Error("recovery function should not have been called with bad data")
	}

	_ = executor
}

func TestTypedCommand_Expired(t *testing.T) {
	type MyData struct {
		Value string `json:"value"`
	}

	expiredCalled := atomic.Bool{}
	var receivedValue string

	cmd := durex.NewTyped("expireTest",
		func(ctx context.Context, data MyData, cmd *durex.Instance) (durex.Result, error) {
			return durex.Empty(), nil
		},
		durex.WithExpired(func(ctx context.Context, data MyData, cmd *durex.Instance) (durex.Result, error) {
			expiredCalled.Store(true)
			receivedValue = data.Value
			return durex.Empty(), nil
		}),
	)

	instance := &durex.Instance{
		ID:        "test-expired-1",
		Name:      "expireTest",
		Status:    durex.StatusPending,
		Data:      durex.M{"value": "hello"},
		CreatedAt: time.Now(),
		ReadyAt:   time.Now(),
	}

	result, err := cmd.Expired(context.Background(), instance)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !expiredCalled.Load() {
		t.Error("expired function should have been called")
	}
	if receivedValue != "hello" {
		t.Errorf("expected value 'hello', got %q", receivedValue)
	}
	if len(result.Commands) != 0 {
		t.Error("expected empty result")
	}
}

func TestTypedCommand_Expired_Nil(t *testing.T) {
	type MyData struct {
		Value string `json:"value"`
	}

	cmd := durex.NewTyped("expireNilTest",
		func(ctx context.Context, data MyData, cmd *durex.Instance) (durex.Result, error) {
			return durex.Empty(), nil
		},
	)

	instance := &durex.Instance{
		ID:        "test-expired-nil",
		Name:      "expireNilTest",
		Status:    durex.StatusPending,
		Data:      durex.M{"value": "hello"},
		CreatedAt: time.Now(),
		ReadyAt:   time.Now(),
	}

	result, err := cmd.Expired(context.Background(), instance)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Commands) != 0 {
		t.Error("expected empty result from nil handler")
	}
}

func TestTypedCommand_Expired_UnmarshalError(t *testing.T) {
	type MyData struct {
		Value string `json:"value"`
	}

	expiredCalled := atomic.Bool{}

	cmd := durex.NewTyped("expireUnmarshalTest",
		func(ctx context.Context, data MyData, cmd *durex.Instance) (durex.Result, error) {
			return durex.Empty(), nil
		},
		durex.WithExpired(func(ctx context.Context, data MyData, cmd *durex.Instance) (durex.Result, error) {
			expiredCalled.Store(true)
			return durex.Empty(), nil
		}),
	)

	instance := &durex.Instance{
		ID:        "test-expired-bad",
		Name:      "expireUnmarshalTest",
		Status:    durex.StatusPending,
		Data:      durex.M{"value": map[string]any{"nested": "object"}},
		CreatedAt: time.Now(),
		ReadyAt:   time.Now(),
	}

	_, err := cmd.Expired(context.Background(), instance)
	if err == nil {
		t.Error("expected error from Expired with bad data")
	}
	if expiredCalled.Load() {
		t.Error("expired function should not have been called with bad data")
	}
}

func TestTypedCommand_WithTags(t *testing.T) {
	type MyData struct {
		Value string `json:"value"`
	}

	cmd := durex.NewTyped("tagTest",
		func(ctx context.Context, data MyData, cmd *durex.Instance) (durex.Result, error) {
			return durex.Empty(), nil
		},
		durex.WithTags[MyData]("billing", "urgent"),
	)

	spec := cmd.Default()
	if len(spec.Tags) != 2 {
		t.Fatalf("expected 2 tags, got %d", len(spec.Tags))
	}
	if spec.Tags[0] != "billing" || spec.Tags[1] != "urgent" {
		t.Errorf("expected tags [billing urgent], got %v", spec.Tags)
	}
}

func TestHandleTyped_ReturnsExecutor(t *testing.T) {
	type MyData struct {
		Value string `json:"value"`
	}

	store := storage.NewMemory()
	executor := durex.New(store)

	returned := durex.HandleTyped(executor, "returnTest",
		func(ctx context.Context, data MyData, cmd *durex.Instance) (durex.Result, error) {
			return durex.Empty(), nil
		},
	)

	if returned != executor {
		t.Error("HandleTyped should return the same executor for chaining")
	}
}
