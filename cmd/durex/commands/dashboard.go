package commands

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/simonovic86/durex"
	"github.com/simonovic86/durex/storage"

	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

type DashboardCommand struct {
	flags  *flag.FlagSet
	dbPath *string
	dbType *string
	port   *string
	host   *string
}

func NewDashboardCommand() *DashboardCommand {
	cmd := &DashboardCommand{
		flags: flag.NewFlagSet("dashboard", flag.ExitOnError),
	}

	cmd.dbPath = cmd.flags.String("db", "", "Database path (required)")
	cmd.dbType = cmd.flags.String("db-type", "sqlite", "Database type: sqlite or postgres")
	cmd.port = cmd.flags.String("port", "8080", "Port to listen on")
	cmd.host = cmd.flags.String("host", "localhost", "Host to bind to")

	return cmd
}

func (c *DashboardCommand) Parse(args []string) error {
	return c.flags.Parse(args)
}

func (c *DashboardCommand) Run(ctx context.Context) error {
	if *c.dbPath == "" {
		return fmt.Errorf("--db flag is required")
	}

	// Connect to storage
	var store durex.Storage

	switch *c.dbType {
	case "sqlite", "sqlite3":
		if _, err := os.Stat(*c.dbPath); os.IsNotExist(err) {
			return fmt.Errorf("database file does not exist: %s", *c.dbPath)
		}

		db, err := sql.Open("sqlite3", *c.dbPath)
		if err != nil {
			return fmt.Errorf("failed to open sqlite database: %w", err)
		}
		defer db.Close()

		store = storage.NewSQLite(db)

	case "postgres", "postgresql":
		db, err := sql.Open("postgres", *c.dbPath)
		if err != nil {
			return fmt.Errorf("failed to open postgres database: %w", err)
		}
		defer db.Close()

		if err := db.Ping(); err != nil {
			return fmt.Errorf("failed to connect to postgres: %w", err)
		}

		store = storage.NewPostgres(db)

	default:
		return fmt.Errorf("unsupported database type: %s", *c.dbType)
	}

	// Create executor with dashboard (read-only mode)
	addr := fmt.Sprintf("%s:%s", *c.host, *c.port)
	executor := durex.New(store, durex.WithDashboard(addr))

	fmt.Printf("🚀 Starting Durex Dashboard on http://%s\n", addr)
	fmt.Printf("📊 Database: %s (%s)\n", *c.dbPath, *c.dbType)
	fmt.Println("\nPress Ctrl+C to stop")

	// Start executor (won't process commands, just serves dashboard)
	if err := executor.Start(ctx); err != nil {
		return fmt.Errorf("failed to start executor: %w", err)
	}

	// Wait for interrupt
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\n👋 Shutting down...")
	if err := executor.Stop(); err != nil {
		slog.Error("Failed to stop executor", "error", err)
	}

	return nil
}
