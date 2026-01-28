// Test helper to create a database with sample commands for CLI testing
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/simonovic86/durex"
	"github.com/simonovic86/durex/storage"

	_ "github.com/mattn/go-sqlite3"
)

func main() {
	// Create test database
	db, err := sql.Open("sqlite3", "test_cli.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	store := storage.NewSQLite(db)

	// Migrate
	ctx := context.Background()
	if err := store.Migrate(ctx); err != nil {
		log.Fatal(err)
	}

	// Create sample commands
	executor := durex.New(store)

	// Some handlers
	executor.HandleFunc("sendEmail", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.Empty(), nil
	})

	executor.HandleFunc("processPayment", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.Empty(), nil
	})

	executor.HandleFunc("failingTask", func(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
		return durex.Empty(), fmt.Errorf("task always fails")
	})

	if err := executor.Start(ctx); err != nil {
		log.Fatal(err)
	}

	// Add some sample commands
	fmt.Println("Creating sample commands...")

	// Completed commands
	for i := 0; i < 5; i++ {
		executor.Add(ctx, durex.Spec{
			Name: "sendEmail",
			Data: durex.M{
				"to":      fmt.Sprintf("user%d@example.com", i),
				"subject": "Test Email",
			},
			Tags: []string{"email", "test"},
		})
	}

	// Failed commands
	for i := 0; i < 3; i++ {
		executor.Add(ctx, durex.Spec{
			Name: "failingTask",
			Data: durex.M{
				"attempt": i,
			},
			Tags: []string{"failing", "test"},
		})
	}

	// Pending commands
	for i := 0; i < 2; i++ {
		executor.Add(ctx, durex.Spec{
			Name:  "processPayment",
			Delay: 10 * time.Hour, // Won't run for a while
			Data: durex.M{
				"orderId": fmt.Sprintf("ORD-%d", i),
				"amount":  99.99,
			},
			Tags: []string{"payment", "test"},
		})
	}

	// Wait for processing
	time.Sleep(2 * time.Second)

	if err := executor.Stop(); err != nil {
		log.Fatal(err)
	}

	fmt.Println("✅ Test database created: test_cli.db")
	fmt.Println("\nTry these commands:")
	fmt.Println("  ./bin/durex list --db=test_cli.db")
	fmt.Println("  ./bin/durex stats --db=test_cli.db --detailed")
	fmt.Println("  ./bin/durex list --db=test_cli.db --status=failed")
	fmt.Println("  ./bin/durex dashboard --db=test_cli.db --port=8080")
}
