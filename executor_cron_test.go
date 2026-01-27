package durex_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/simonovic86/durex"
	"github.com/simonovic86/durex/storage"
)

func TestExecutor_CronScheduling(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store)

	var executionCount atomic.Int32

	// Register command with cron expression (every minute)
	executor.HandleFunc("cronJob", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		executionCount.Add(1)
		// Stop after first execution for test
		if executionCount.Load() >= 1 {
			return durex.Empty(), nil
		}
		return durex.Repeat(), nil
	}, durex.Cron("* * * * *"))

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	// Add the cron job
	instance, err := executor.Add(ctx, durex.Spec{
		Name: "cronJob",
		Cron: "*/1 * * * *",
	})
	if err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	// Wait for execution
	time.Sleep(100 * time.Millisecond)

	// Verify it executed
	if executionCount.Load() != 1 {
		t.Errorf("Expected 1 execution, got %d", executionCount.Load())
	}

	// Verify the instance has cron set
	refreshed, err := store.Get(ctx, instance.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if refreshed.Cron != "*/1 * * * *" {
		t.Errorf("Instance.Cron = %q, want %q", refreshed.Cron, "*/1 * * * *")
	}
}

func TestExecutor_CronRepeatScheduling(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store)

	var executionCount atomic.Int32

	// Register command with cron expression
	executor.HandleFunc("cronRepeat", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		executionCount.Add(1)
		// Always repeat
		return durex.Repeat(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	// Add the cron job with hourly schedule
	_, err := executor.Add(ctx, durex.Spec{
		Name: "cronRepeat",
		Cron: "0 * * * *", // Every hour at minute 0
	})
	if err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	// Wait for execution
	time.Sleep(100 * time.Millisecond)

	// Verify it executed
	if executionCount.Load() != 1 {
		t.Errorf("Expected 1 execution, got %d", executionCount.Load())
	}

	// Get the command to check the next scheduled time
	commands := store.All()
	if len(commands) == 0 {
		t.Fatal("Expected at least one command in storage")
	}

	cmd := commands[0]
	if cmd.Status != durex.StatusRepeating {
		t.Errorf("Status = %v, want %v", cmd.Status, durex.StatusRepeating)
	}

	// The ReadyAt should be scheduled to the next hour boundary
	now := time.Now()
	expectedNext := durex.NextCronTime("0 * * * *", now.Add(-time.Second))
	if cmd.ReadyAt.Before(now) {
		t.Errorf("ReadyAt = %v should be in the future", cmd.ReadyAt)
	}

	// Should be within the next hour
	if cmd.ReadyAt.After(now.Add(time.Hour + time.Minute)) {
		t.Errorf("ReadyAt = %v, expected within next hour (around %v)", cmd.ReadyAt, expectedNext)
	}
}

func TestExecutor_CronTakesPrecedenceOverPeriod(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store, durex.WithDefaultRepeatInterval(time.Second))

	var executionCount atomic.Int32

	// Register command with both cron and period
	executor.HandleFunc("cronVsPeriod", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		executionCount.Add(1)
		return durex.Repeat(), nil
	})

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	// Add with both cron and period - cron should take precedence
	_, err := executor.Add(ctx, durex.Spec{
		Name:   "cronVsPeriod",
		Cron:   "0 0 * * *", // Daily at midnight
		Period: time.Second, // This should be ignored
	})
	if err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	// Wait for execution
	time.Sleep(100 * time.Millisecond)

	// Get the command
	commands := store.All()
	if len(commands) == 0 {
		t.Fatal("Expected at least one command in storage")
	}

	cmd := commands[0]

	// The ReadyAt should be set based on cron (next midnight), not period (1 second from now)
	now := time.Now()
	expectedNextMidnight := durex.NextCronTime("0 0 * * *", now)

	// Should be close to next midnight, not 1 second from now
	diff := cmd.ReadyAt.Sub(expectedNextMidnight)
	if diff < -time.Second || diff > time.Second {
		t.Errorf("ReadyAt = %v, expected around %v (diff: %v)", cmd.ReadyAt, expectedNextMidnight, diff)
	}
}

func TestCronOption(t *testing.T) {
	store := storage.NewMemory()
	executor := durex.New(store)

	// Use Cron option
	executor.HandleFunc("testCron", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.Empty(), nil
	}, durex.Cron("*/5 * * * *"))

	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	// Add a command and check it has the cron set
	instance, err := executor.Add(ctx, durex.Spec{Name: "testCron"})
	if err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	refreshed, err := store.Get(ctx, instance.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if refreshed.Cron != "*/5 * * * *" {
		t.Errorf("Instance.Cron = %q, want %q", refreshed.Cron, "*/5 * * * *")
	}
}
