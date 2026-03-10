// Example: E-commerce order processing with typed commands
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

// ============================================
// Data types for type-safe commands
// ============================================

type OrderData struct {
	OrderID    string   `json:"orderId"`
	CustomerID string   `json:"customerId"`
	Amount     float64  `json:"amount"`
	Items      []string `json:"items"`
	// Fields added during workflow
	ValidatedAt   string `json:"validatedAt,omitempty"`
	ReservationID string `json:"reservationId,omitempty"`
	PaymentID     string `json:"paymentId,omitempty"`
	TrackingNum   string `json:"trackingNum,omitempty"`
}

type FailureData struct {
	OrderID string `json:"orderId"`
	Reason  string `json:"reason"`
	Stage   string `json:"stage"`
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	executor := durex.New(storage.NewMemory(),
		durex.WithParallelism(8),
	)

	// ============================================
	// Register typed commands - no more GetString()!
	// ============================================

	// Step 1: Validate Order
	durex.HandleTyped(executor, "validateOrder", func(ctx context.Context, data OrderData, cmd *durex.Instance) (durex.Result, error) {
		slog.Info("📋 Validating order", "orderId", data.OrderID, "amount", data.Amount)

		if data.Amount <= 0 {
			return durex.Empty(), fmt.Errorf("invalid order amount: %.2f", data.Amount)
		}

		// Pass data to next step
		cmd.Set("validatedAt", time.Now().Format(time.RFC3339))
		return cmd.ContinueSequence(nil), nil
	}, durex.WithRetries[OrderData](2))

	// Step 2: Reserve Inventory
	durex.HandleTyped(executor, "reserveInventory", func(ctx context.Context, data OrderData, cmd *durex.Instance) (durex.Result, error) {
		slog.Info("📦 Reserving inventory", "orderId", data.OrderID, "items", len(data.Items))

		// Simulate occasional failure
		if rand.Float32() < 0.3 && cmd.Attempt < 2 {
			return durex.Empty(), fmt.Errorf("inventory service timeout")
		}

		cmd.Set("reservationId", fmt.Sprintf("RES-%s", data.OrderID))
		return cmd.ContinueSequence(nil), nil
	}, durex.WithRetries[OrderData](3))

	// Step 3: Process Payment
	durex.HandleTyped(executor, "processPayment", func(ctx context.Context, data OrderData, cmd *durex.Instance) (durex.Result, error) {
		slog.Info("💳 Processing payment", "orderId", data.OrderID, "amount", data.Amount)

		time.Sleep(50 * time.Millisecond) // Simulate API call

		cmd.Set("paymentId", fmt.Sprintf("PAY-%d", rand.Intn(100000)))
		return cmd.ContinueSequence(nil), nil
	},
		durex.WithRetries[OrderData](2),
		durex.WithDeadline[OrderData](30*time.Second),
		durex.WithRecover(func(ctx context.Context, data OrderData, cmd *durex.Instance, err error) (durex.Result, error) {
			slog.Error("💳 Payment failed - rolling back", "orderId", data.OrderID)

			// Spawn compensation commands
			return durex.Spawn(
				durex.MustTyped("releaseInventory", data),
				durex.MustTyped("notifyFailure", FailureData{
					OrderID: data.OrderID,
					Reason:  err.Error(),
					Stage:   "payment",
				}),
			), nil
		}),
	)

	// Step 4: Ship Order
	durex.HandleTyped(executor, "shipOrder", func(ctx context.Context, data OrderData, cmd *durex.Instance) (durex.Result, error) {
		trackingNum := fmt.Sprintf("TRK%d", rand.Intn(1000000))
		slog.Info("🚚 Shipping order", "orderId", data.OrderID, "tracking", trackingNum)

		cmd.Set("trackingNum", trackingNum)
		return cmd.ContinueSequence(nil), nil
	})

	// Step 5: Send Confirmation
	durex.HandleTyped(executor, "sendConfirmation", func(ctx context.Context, data OrderData, cmd *durex.Instance) (durex.Result, error) {
		slog.Info("✉️  Confirmation sent!",
			"orderId", data.OrderID,
			"customer", data.CustomerID,
			"tracking", cmd.GetString("trackingNum"),
		)
		slog.Info("🎉 Order complete!", "orderId", data.OrderID)
		return durex.Empty(), nil
	})

	// Compensation commands
	durex.HandleTyped(executor, "releaseInventory", func(ctx context.Context, data OrderData, cmd *durex.Instance) (durex.Result, error) {
		slog.Warn("↩️  Releasing inventory", "orderId", data.OrderID)
		return durex.Empty(), nil
	})

	durex.HandleTyped(executor, "notifyFailure", func(ctx context.Context, data FailureData, cmd *durex.Instance) (durex.Result, error) {
		slog.Error("🚨 Order failed notification",
			"orderId", data.OrderID,
			"stage", data.Stage,
			"reason", data.Reason,
		)
		return durex.Empty(), nil
	})

	// Start
	ctx := context.Background()
	executor.Start(ctx)
	defer executor.Stop()

	slog.Info("🛒 E-commerce workflow engine started!")

	// ============================================
	// Simulate incoming orders
	// ============================================

	go func() {
		orderNum := 1
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
				order := OrderData{
					OrderID:    fmt.Sprintf("ORD-%04d", orderNum),
					CustomerID: fmt.Sprintf("CUST-%03d", rand.Intn(100)),
					Amount:     float64(rand.Intn(500) + 50),
					Items:      []string{"Widget", "Gadget", "Gizmo"},
				}
				orderNum++

				// Add order workflow: validate → reserve → pay → ship → confirm
				spec := durex.MustTyped("validateOrder", order).
					WithRetries(2).
					WithSequence("reserveInventory", "processPayment", "shipOrder", "sendConfirmation")

				executor.Add(ctx, spec)
				slog.Info("📥 New order received", "orderId", order.OrderID, "amount", order.Amount)
			}
		}
	}()

	// Wait for shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	slog.Info("👋 Shutting down...")
}
