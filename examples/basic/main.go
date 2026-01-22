// Example: Basic durex usage with functional commands
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/simonovic86/durex"
	"github.com/simonovic86/durex/storage"
)

func main() {
	// Pretty logging
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))

	// Create executor with in-memory storage
	executor := durex.New(storage.NewMemory(),
		durex.WithParallelism(4),
	)

	// ============================================
	// Register commands using simple functions
	// ============================================

	// Simple command - just a function!
	executor.HandleFunc("greet", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		name := cmd.GetString("name")
		slog.Info("👋 Hello!", "name", name)
		return durex.Empty(), nil
	})

	// Command with retries
	executor.HandleFunc("sendEmail", sendEmail,
		durex.Retries(3),
		durex.OnRecover(func(ctx context.Context, cmd *durex.Instance, err error) (durex.Result, error) {
			slog.Error("📧 Email permanently failed", "to", cmd.GetString("to"), "error", err)
			return durex.Empty(), nil
		}),
	)

	// Repeating command (like cron)
	executor.HandleFunc("heartbeat", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		slog.Info("💓 Heartbeat", "time", time.Now().Format("15:04:05"))
		return durex.Repeat(), nil
	}, durex.Period(5*time.Second))

	// Command that spawns children
	executor.HandleFunc("notifyAll", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		users := []string{"alice@example.com", "bob@example.com", "carol@example.com"}

		specs := make([]durex.Spec, len(users))
		for i, user := range users {
			specs[i] = durex.Spec{
				Name: "sendEmail",
				Data: durex.M{"to": user, "subject": "Announcement!"},
			}
		}

		slog.Info("📢 Notifying all users", "count", len(users))
		return durex.Spawn(specs...), nil
	})

	// Chained workflow using sequence
	executor.HandleFunc("step1", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		slog.Info("🔷 Step 1: Validating", "orderId", cmd.GetString("orderId"))
		cmd.Set("validated", true)
		return cmd.ContinueSequence(nil), nil
	})

	executor.HandleFunc("step2", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		slog.Info("🔷 Step 2: Processing", "orderId", cmd.GetString("orderId"), "validated", cmd.GetBool("validated"))
		cmd.Set("processed", true)
		return cmd.ContinueSequence(nil), nil
	})

	executor.HandleFunc("step3", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		slog.Info("🔷 Step 3: Complete!", "orderId", cmd.GetString("orderId"))
		return durex.Empty(), nil
	})

	// Start executor
	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	slog.Info("🚀 Durex started! Adding some commands...")

	// ============================================
	// Add commands to execute
	// ============================================

	// Simple greeting
	executor.Add(ctx, durex.Spec{
		Name: "greet",
		Data: durex.M{"name": "World"},
	})

	// Email with retries (will fail twice, succeed on third)
	executor.Add(ctx, durex.Spec{
		Name: "sendEmail",
		Data: durex.M{"to": "test@example.com", "subject": "Hello!"},
	})

	// Fan-out to multiple users
	executor.Add(ctx, durex.Spec{Name: "notifyAll"})

	// Workflow: step1 → step2 → step3
	executor.Add(ctx, durex.Spec{
		Name:     "step1",
		Sequence: []string{"step2", "step3"},
		Data:     durex.M{"orderId": "ORD-001"},
	})

	// Delayed command
	executor.Add(ctx, durex.Spec{
		Name:  "greet",
		Data:  durex.M{"name": "Delayed User"},
		Delay: 3 * time.Second,
	})

	// Start heartbeat
	executor.Add(ctx, durex.Spec{Name: "heartbeat"})

	// Wait for Ctrl+C
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	slog.Info("👋 Shutting down...")
}

// sendEmail simulates sending an email with occasional failures
func sendEmail(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
	to := cmd.GetString("to")
	subject := cmd.GetString("subject")

	slog.Info("📧 Sending email", "to", to, "subject", subject, "attempt", cmd.Attempt)

	// Simulate failure on first 2 attempts
	if cmd.Attempt < 3 {
		return durex.Empty(), fmt.Errorf("temporary SMTP error")
	}

	slog.Info("✅ Email sent!", "to", to)
	return durex.Empty(), nil
}
