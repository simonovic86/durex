// Example: Fan-in pattern with SpawnThen
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/simonovic86/durex"
	"github.com/simonovic86/durex/storage"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	executor := durex.New(storage.NewMemory(),
		durex.WithParallelism(4),
	)

	// Parallel tasks
	executor.HandleFunc("chargePayment", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		orderId := cmd.GetString("orderId")
		amount := cmd.Get("amount")
		slog.Info("💳 Charging payment", "orderId", orderId, "amount", amount)
		time.Sleep(100 * time.Millisecond) // Simulate payment API
		cmd.Set("paymentId", fmt.Sprintf("PAY-%s", orderId))
		return durex.Empty(), nil
	})

	executor.HandleFunc("reserveInventory", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		orderId := cmd.GetString("orderId")
		slog.Info("📦 Reserving inventory", "orderId", orderId)
		time.Sleep(150 * time.Millisecond) // Simulate inventory check
		cmd.Set("reservationId", fmt.Sprintf("RES-%s", orderId))
		return durex.Empty(), nil
	})

	executor.HandleFunc("sendEmail", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		orderId := cmd.GetString("orderId")
		email := cmd.GetString("email")
		slog.Info("✉️  Sending confirmation email", "orderId", orderId, "email", email)
		time.Sleep(80 * time.Millisecond) // Simulate email service
		return durex.Empty(), nil
	})

	// Continuation - runs only after ALL parallel tasks complete
	executor.HandleFunc("shipOrder", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		orderId := cmd.GetString("orderId")

		// Access results from parallel tasks (with prefixes)
		slog.Info("🚚 Shipping order (all prerequisites met!)", "orderId", orderId)

		// You can access child results if needed
		for k, v := range cmd.Data {
			if k != "orderId" && k != "amount" && k != "email" {
				slog.Debug("Received data from parallel task", "key", k, "value", v)
			}
		}

		slog.Info("✅ Order completed!", "orderId", orderId)
		return durex.Empty(), nil
	})

	// Main workflow coordinator
	executor.HandleFunc("processOrder", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		orderId := cmd.GetString("orderId")
		slog.Info("🛒 Processing order", "orderId", orderId)

		// SpawnThen: Run payment, inventory, and email in parallel
		// Then ship the order ONLY after all three complete successfully
		return durex.SpawnThen(
			[]durex.Spec{
				{Name: "chargePayment", Data: cmd.Data},
				{Name: "reserveInventory", Data: cmd.Data},
				{Name: "sendEmail", Data: cmd.Data},
			},
			durex.Spec{Name: "shipOrder", Data: cmd.Data},
		), nil
	})

	// Start executor
	ctx := context.Background()
	if err := executor.Start(ctx); err != nil {
		slog.Error("Failed to start executor", "error", err)
		return
	}
	defer func() {
		if err := executor.Stop(); err != nil {
			slog.Error("Failed to stop executor", "error", err)
		}
	}()

	// Submit an order
	_, err := executor.Add(ctx, durex.Spec{
		Name: "processOrder",
		Data: durex.M{
			"orderId": "ORD-12345",
			"amount":  99.99,
			"email":   "customer@example.com",
		},
	})
	if err != nil {
		slog.Error("Failed to add order", "error", err)
		return
	}

	// Wait for completion
	slog.Info("⏳ Waiting for order to complete...")
	time.Sleep(3 * time.Second)

	slog.Info("👋 Done!")
}
