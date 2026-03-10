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
