// Package main demonstrates a more complex workflow using durex.
// This example shows an e-commerce order processing pipeline.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/simonovic86/durex"
	"github.com/simonovic86/durex/storage"
)

// Order represents an e-commerce order.
type Order struct {
	ID         string
	CustomerID string
	Amount     float64
	Items      []string
}

// === Commands ===

// ValidateOrderCommand validates the order data.
type ValidateOrderCommand struct {
	durex.BaseCommand
}

func (c *ValidateOrderCommand) Name() string { return "validateOrder" }

func (c *ValidateOrderCommand) Execute(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
	orderID := cmd.GetString("orderId")
	amount := cmd.GetInt("amount")

	slog.Info("Validating order", "orderId", orderID, "amount", amount)

	// Simulate validation
	if amount <= 0 {
		return durex.Empty(), fmt.Errorf("invalid order amount: %d", amount)
	}

	cmd.Set("validated", true)
	cmd.Set("validatedAt", time.Now().Format(time.RFC3339))

	return cmd.ContinueSequence(nil), nil
}

func (c *ValidateOrderCommand) Recover(ctx context.Context, cmd *durex.Instance, err error) (durex.Result, error) {
	orderID := cmd.GetString("orderId")
	slog.Error("Order validation failed permanently", "orderId", orderID, "error", err)

	// Spawn a notification command
	return durex.Next(durex.Spec{
		Name: "notifyOrderFailed",
		Data: durex.M{
			"orderId": orderID,
			"reason":  err.Error(),
			"stage":   "validation",
		},
	}), nil
}

// ReserveInventoryCommand reserves items in inventory.
type ReserveInventoryCommand struct {
	durex.BaseCommand
}

func (c *ReserveInventoryCommand) Name() string { return "reserveInventory" }

func (c *ReserveInventoryCommand) Execute(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
	orderID := cmd.GetString("orderId")
	items := cmd.GetSlice("items")

	slog.Info("Reserving inventory", "orderId", orderID, "itemCount", len(items))

	// Simulate random inventory failure
	if rand.Float32() < 0.3 && cmd.Attempt < 2 {
		return durex.Empty(), fmt.Errorf("inventory service temporarily unavailable")
	}

	cmd.Set("inventoryReserved", true)
	cmd.Set("reservationId", fmt.Sprintf("RES-%s-%d", orderID, time.Now().Unix()))

	return cmd.ContinueSequence(nil), nil
}

func (c *ReserveInventoryCommand) Default() durex.Spec {
	return durex.Spec{
		Name:    "reserveInventory",
		Retries: 3,
	}
}

// ProcessPaymentCommand handles payment processing.
type ProcessPaymentCommand struct {
	durex.BaseCommand
}

func (c *ProcessPaymentCommand) Name() string { return "processPayment" }

func (c *ProcessPaymentCommand) Execute(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
	orderID := cmd.GetString("orderId")
	amount := cmd.GetInt("amount")

	slog.Info("Processing payment", "orderId", orderID, "amount", amount)

	// Simulate payment processing
	time.Sleep(100 * time.Millisecond)

	cmd.Set("paymentId", fmt.Sprintf("PAY-%s-%d", orderID, time.Now().Unix()))
	cmd.Set("paymentProcessed", true)

	return cmd.ContinueSequence(nil), nil
}

func (c *ProcessPaymentCommand) Recover(ctx context.Context, cmd *durex.Instance, err error) (durex.Result, error) {
	orderID := cmd.GetString("orderId")
	reservationId := cmd.GetString("reservationId")

	slog.Error("Payment failed, initiating rollback", "orderId", orderID)

	// Spawn compensation commands
	return durex.Spawn(
		durex.Spec{
			Name: "releaseInventory",
			Data: durex.M{
				"orderId":       orderID,
				"reservationId": reservationId,
			},
		},
		durex.Spec{
			Name: "notifyOrderFailed",
			Data: durex.M{
				"orderId": orderID,
				"reason":  err.Error(),
				"stage":   "payment",
			},
		},
	), nil
}

func (c *ProcessPaymentCommand) Default() durex.Spec {
	return durex.Spec{
		Name:     "processPayment",
		Retries:  2,
		Deadline: 30 * time.Second,
	}
}

// ShipOrderCommand initiates order shipping.
type ShipOrderCommand struct {
	durex.BaseCommand
}

func (c *ShipOrderCommand) Name() string { return "shipOrder" }

func (c *ShipOrderCommand) Execute(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
	orderID := cmd.GetString("orderId")

	slog.Info("Initiating shipping", "orderId", orderID)

	cmd.Set("trackingNumber", fmt.Sprintf("TRK%d", rand.Intn(1000000)))
	cmd.Set("shippedAt", time.Now().Format(time.RFC3339))

	return cmd.ContinueSequence(nil), nil
}

// SendConfirmationCommand sends order confirmation to customer.
type SendConfirmationCommand struct {
	durex.BaseCommand
}

func (c *SendConfirmationCommand) Name() string { return "sendConfirmation" }

func (c *SendConfirmationCommand) Execute(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
	orderID := cmd.GetString("orderId")
	trackingNumber := cmd.GetString("trackingNumber")

	slog.Info("Sending confirmation email",
		"orderId", orderID,
		"trackingNumber", trackingNumber,
	)

	// All done!
	slog.Info("Order completed successfully!", "orderId", orderID)
	return durex.Empty(), nil
}

// === Compensation Commands ===

// ReleaseInventoryCommand releases previously reserved inventory.
type ReleaseInventoryCommand struct {
	durex.BaseCommand
}

func (c *ReleaseInventoryCommand) Name() string { return "releaseInventory" }

func (c *ReleaseInventoryCommand) Execute(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
	reservationId := cmd.GetString("reservationId")

	slog.Info("Releasing inventory reservation", "reservationId", reservationId)
	return durex.Empty(), nil
}

// NotifyOrderFailedCommand sends failure notification.
type NotifyOrderFailedCommand struct {
	durex.BaseCommand
}

func (c *NotifyOrderFailedCommand) Name() string { return "notifyOrderFailed" }

func (c *NotifyOrderFailedCommand) Execute(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
	orderID := cmd.GetString("orderId")
	reason := cmd.GetString("reason")
	stage := cmd.GetString("stage")

	slog.Warn("Order failed notification sent",
		"orderId", orderID,
		"reason", reason,
		"failedAtStage", stage,
	)
	return durex.Empty(), nil
}

// === Middleware ===

// loggingMiddleware logs command execution.
func loggingMiddleware(ctx durex.MiddlewareContext, next func() (durex.Result, error)) (durex.Result, error) {
	start := time.Now()
	result, err := next()
	duration := time.Since(start)

	if err != nil {
		slog.Debug("Command failed",
			"name", ctx.Command.Name,
			"id", ctx.Command.ID,
			"duration", duration,
			"error", err,
		)
	} else {
		slog.Debug("Command completed",
			"name", ctx.Command.Name,
			"id", ctx.Command.ID,
			"duration", duration,
		)
	}

	return result, err
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))

	store := storage.NewMemory()

	executor := durex.New(store,
		durex.WithParallelism(8),
		durex.WithLogger(slog.Default()),
		durex.WithMiddleware(loggingMiddleware),
	)

	// Register all commands
	executor.Register(&ValidateOrderCommand{})
	executor.Register(&ReserveInventoryCommand{})
	executor.Register(&ProcessPaymentCommand{})
	executor.Register(&ShipOrderCommand{})
	executor.Register(&SendConfirmationCommand{})
	executor.Register(&ReleaseInventoryCommand{})
	executor.Register(&NotifyOrderFailedCommand{})

	ctx := context.Background()
	if err := executor.Start(ctx); err != nil {
		slog.Error("Failed to start", "error", err)
		os.Exit(1)
	}

	slog.Info("E-commerce workflow engine started")

	// Simulate incoming orders
	go func() {
		orderNum := 1
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
				orderID := fmt.Sprintf("ORD-%04d", orderNum)
				orderNum++

				_, err := executor.Add(ctx, durex.Spec{
					Name: "validateOrder",
					Sequence: []string{
						"reserveInventory",
						"processPayment",
						"shipOrder",
						"sendConfirmation",
					},
					Data: durex.M{
						"orderId":    orderID,
						"customerId": fmt.Sprintf("CUST-%03d", rand.Intn(100)),
						"amount":     rand.Intn(500) + 50,
						"items":      []string{"item1", "item2", "item3"},
					},
				})

				if err != nil {
					slog.Error("Failed to create order", "error", err)
				} else {
					slog.Info("New order created", "orderId", orderID)
				}
			}
		}
	}()

	// Wait for shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	slog.Info("Shutting down...")
	executor.Stop()
}
