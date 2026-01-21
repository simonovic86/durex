// Package main demonstrates basic usage of the durex framework.
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

// SendEmailCommand demonstrates a simple command with retry logic.
type SendEmailCommand struct {
	durex.BaseCommand
}

func (c *SendEmailCommand) Name() string {
	return "sendEmail"
}

func (c *SendEmailCommand) Execute(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
	to := cmd.GetString("to")
	subject := cmd.GetString("subject")

	slog.Info("Sending email",
		"to", to,
		"subject", subject,
		"attempt", cmd.Attempt,
	)

	// Simulate occasional failure
	if cmd.Attempt < 2 {
		return durex.Empty(), fmt.Errorf("simulated failure")
	}

	slog.Info("Email sent successfully", "to", to)
	return durex.Empty(), nil
}

func (c *SendEmailCommand) Recover(ctx context.Context, cmd *durex.Instance, err error) (durex.Result, error) {
	slog.Error("Email sending failed permanently",
		"to", cmd.GetString("to"),
		"error", err,
	)
	return durex.Empty(), nil
}

func (c *SendEmailCommand) Default() durex.Spec {
	return durex.Spec{
		Name:    "sendEmail",
		Retries: 3,
	}
}

// CleanupCommand demonstrates a repeating command.
type CleanupCommand struct {
	durex.BaseCommand
}

func (c *CleanupCommand) Name() string {
	return "cleanup"
}

func (c *CleanupCommand) Execute(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
	slog.Info("Running cleanup task", "time", time.Now().Format(time.RFC3339))
	return durex.Repeat(), nil
}

func (c *CleanupCommand) Default() durex.Spec {
	return durex.Spec{
		Name:   "cleanup",
		Period: 10 * time.Second,
	}
}

// ProcessOrderCommand demonstrates command chaining with sequences.
type ProcessOrderCommand struct {
	durex.BaseCommand
}

func (c *ProcessOrderCommand) Name() string {
	return "processOrder"
}

func (c *ProcessOrderCommand) Execute(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
	orderID := cmd.GetString("orderId")
	slog.Info("Processing order", "orderId", orderID)

	// Add some computed data for the next step
	cmd.Set("processedAt", time.Now().Format(time.RFC3339))

	return cmd.ContinueSequence(nil), nil
}

// ValidatePaymentCommand is the second step in the order sequence.
type ValidatePaymentCommand struct {
	durex.BaseCommand
}

func (c *ValidatePaymentCommand) Name() string {
	return "validatePayment"
}

func (c *ValidatePaymentCommand) Execute(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
	orderID := cmd.GetString("orderId")
	processedAt := cmd.GetString("processedAt")

	slog.Info("Validating payment",
		"orderId", orderID,
		"processedAt", processedAt,
	)

	cmd.Set("paymentValidated", true)
	return cmd.ContinueSequence(nil), nil
}

// ShipOrderCommand is the final step in the order sequence.
type ShipOrderCommand struct {
	durex.BaseCommand
}

func (c *ShipOrderCommand) Name() string {
	return "shipOrder"
}

func (c *ShipOrderCommand) Execute(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
	orderID := cmd.GetString("orderId")

	slog.Info("Order shipped!", "orderId", orderID)
	return durex.Empty(), nil
}

func main() {
	// Configure structured logging
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))

	// Create in-memory storage (use PostgreSQL or SQLite for production)
	store := storage.NewMemory()

	// Create executor with options
	executor := durex.New(store,
		durex.WithParallelism(4),
		durex.WithLogger(slog.Default()),
		durex.WithDefaultRetries(2),
		durex.WithCleanupInterval(time.Minute),
	)

	// Register command handlers
	executor.Register(&SendEmailCommand{})
	executor.Register(&CleanupCommand{})
	executor.Register(&ProcessOrderCommand{})
	executor.Register(&ValidatePaymentCommand{})
	executor.Register(&ShipOrderCommand{})

	// Start the executor
	ctx := context.Background()
	if err := executor.Start(ctx); err != nil {
		slog.Error("Failed to start executor", "error", err)
		os.Exit(1)
	}

	slog.Info("Durex executor started")

	// Add some example commands
	//
	// 1. Simple email command with retries
	_, err := executor.Add(ctx, durex.Spec{
		Name: "sendEmail",
		Data: durex.M{
			"to":      "user@example.com",
			"subject": "Welcome to Durex!",
			"body":    "Thanks for trying out our framework.",
		},
	})
	if err != nil {
		slog.Error("Failed to add email command", "error", err)
	}

	// 2. Command chain (order processing workflow)
	_, err = executor.Add(ctx, durex.Spec{
		Name:     "processOrder",
		Sequence: []string{"validatePayment", "shipOrder"},
		Data: durex.M{
			"orderId": "ORD-12345",
			"amount":  99.99,
		},
	})
	if err != nil {
		slog.Error("Failed to add order command", "error", err)
	}

	// 3. Scheduled command with delay
	_, err = executor.Add(ctx, durex.Spec{
		Name:  "sendEmail",
		Delay: 5 * time.Second,
		Data: durex.M{
			"to":      "delayed@example.com",
			"subject": "Delayed email",
		},
	})
	if err != nil {
		slog.Error("Failed to add delayed command", "error", err)
	}

	// 4. Repeating cleanup command
	_, err = executor.Add(ctx, durex.Spec{
		Name: "cleanup",
	})
	if err != nil {
		slog.Error("Failed to add cleanup command", "error", err)
	}

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	<-sigCh
	slog.Info("Shutting down...")

	if err := executor.Stop(); err != nil {
		slog.Error("Error stopping executor", "error", err)
	}

	slog.Info("Goodbye!")
}
