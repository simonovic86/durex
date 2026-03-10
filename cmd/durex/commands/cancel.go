package commands

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/simonovic86/durex"
)

type CancelCommand struct {
	flags  *flag.FlagSet
	dbPath *string
	dbType *string
	tag    *string
	id     string
}

func NewCancelCommand() *CancelCommand {
	cmd := &CancelCommand{
		flags: flag.NewFlagSet("cancel", flag.ExitOnError),
	}

	cmd.dbPath = cmd.flags.String("db", "", "Database path (required)")
	cmd.dbType = cmd.flags.String("db-type", "sqlite", "Database type: sqlite or postgres")
	cmd.tag = cmd.flags.String("tag", "", "Cancel all commands with this tag")

	return cmd
}

func (c *CancelCommand) Parse(args []string) error {
	if err := c.flags.Parse(args); err != nil {
		return err
	}

	// Get command ID from remaining args (unless using --tag)
	remaining := c.flags.Args()
	if *c.tag == "" {
		if len(remaining) == 0 {
			return fmt.Errorf("command ID is required (or use --tag)")
		}
		c.id = remaining[0]
	}

	return nil
}

func (c *CancelCommand) Run(ctx context.Context) error {
	store, err := ConnectStorage(*c.dbType, *c.dbPath)
	if err != nil {
		return err
	}
	defer store.Close()

	// Cancel by tag
	if *c.tag != "" {
		return c.cancelByTag(ctx, store)
	}

	// Cancel by ID
	return c.cancelByID(ctx, store)
}

func (c *CancelCommand) cancelByID(ctx context.Context, store durex.Storage) error {
	instance, err := store.Get(ctx, c.id)
	if err != nil {
		return fmt.Errorf("failed to get command: %w", err)
	}

	if instance.Status.IsTerminal() {
		return fmt.Errorf("command is already in terminal status: %s", instance.Status)
	}

	fmt.Printf("Cancelling command: %s (%s)\n", instance.ID, instance.Name)
	fmt.Printf("Current status: %s\n", instance.Status)

	instance.Status = durex.StatusCancelled
	now := time.Now()
	instance.CompletedAt = &now
	instance.RecordEvent(durex.EventCancelled, "cancelled via CLI")

	if err := store.Update(ctx, instance); err != nil {
		return fmt.Errorf("failed to cancel command: %w", err)
	}

	fmt.Println("✅ Command cancelled successfully")
	return nil
}

func (c *CancelCommand) cancelByTag(ctx context.Context, store durex.Storage) error {
	qs, ok := store.(durex.QueryableStorage)
	if !ok {
		return fmt.Errorf("cancelling by tag requires QueryableStorage (Postgres or SQLite)")
	}

	// Find commands with this tag
	commands, err := qs.Find(ctx, durex.Query{
		Tags: []string{*c.tag},
	})
	if err != nil {
		return fmt.Errorf("failed to find commands: %w", err)
	}

	if len(commands) == 0 {
		fmt.Printf("No commands found with tag: %s\n", *c.tag)
		return nil
	}

	fmt.Printf("Found %d commands with tag '%s'\n", len(commands), *c.tag)

	cancelled := 0
	now := time.Now()
	for _, cmd := range commands {
		if cmd.Status.IsTerminal() {
			continue
		}

		cmd.Status = durex.StatusCancelled
		cmd.CompletedAt = &now
		cmd.RecordEvent(durex.EventCancelled, fmt.Sprintf("cancelled via CLI (tag: %s)", *c.tag))

		if err := store.Update(ctx, cmd); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to cancel %s: %v\n", cmd.ID, err)
			continue
		}
		cancelled++
	}

	fmt.Printf("✅ Cancelled %d commands\n", cancelled)
	return nil
}
