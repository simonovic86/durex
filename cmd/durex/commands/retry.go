package commands

import (
	"context"
	"flag"
	"fmt"

	"github.com/simonovic86/durex"
)

type RetryCommand struct {
	flags  *flag.FlagSet
	dbPath *string
	dbType *string
	id     string
}

func NewRetryCommand() *RetryCommand {
	cmd := &RetryCommand{
		flags: flag.NewFlagSet("retry", flag.ExitOnError),
	}

	cmd.dbPath = cmd.flags.String("db", "", "Database path (required)")
	cmd.dbType = cmd.flags.String("db-type", "sqlite", "Database type: sqlite or postgres")

	return cmd
}

func (c *RetryCommand) Parse(args []string) error {
	if err := c.flags.Parse(args); err != nil {
		return err
	}

	// Get command ID from remaining args
	remaining := c.flags.Args()
	if len(remaining) == 0 {
		return fmt.Errorf("command ID is required")
	}

	c.id = remaining[0]
	return nil
}

func (c *RetryCommand) Run(ctx context.Context) error {
	store, err := ConnectStorage(*c.dbType, *c.dbPath)
	if err != nil {
		return err
	}
	defer store.Close()

	// Get the command
	instance, err := store.Get(ctx, c.id)
	if err != nil {
		return fmt.Errorf("failed to get command: %w", err)
	}

	// Check if it's in a retryable state
	if instance.Status != durex.StatusFailed && instance.Status != durex.StatusDeadLetter {
		return fmt.Errorf("command is in status %s (can only retry FAILED or DEAD_LETTER commands)", instance.Status)
	}

	fmt.Printf("Retrying command: %s (%s)\n", instance.ID, instance.Name)
	fmt.Printf("Previous status: %s\n", instance.Status)
	if instance.Error != "" {
		fmt.Printf("Previous error: %s\n", instance.Error)
	}

	// Reset the command for retry
	instance.Status = durex.StatusPending
	instance.Error = ""
	instance.StartedAt = nil
	instance.CompletedAt = nil
	instance.Attempt = 0
	instance.ReadyAt = instance.CreatedAt // Run immediately
	instance.RecordEvent(durex.EventRetrying, "manual retry via CLI")

	if err := store.Update(ctx, instance); err != nil {
		return fmt.Errorf("failed to update command: %w", err)
	}

	fmt.Println("✅ Command reset to PENDING status")
	fmt.Println("\nNote: The command will be picked up by running executor instances.")
	fmt.Println("If no executor is running, start one to process the retried command.")

	return nil
}
