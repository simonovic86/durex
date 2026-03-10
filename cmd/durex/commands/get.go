package commands

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/simonovic86/durex"
)

type GetCommand struct {
	flags   *flag.FlagSet
	dbPath  *string
	dbType  *string
	format  *string
	history *bool
	id      string
}

func NewGetCommand() *GetCommand {
	cmd := &GetCommand{
		flags: flag.NewFlagSet("get", flag.ExitOnError),
	}

	cmd.dbPath = cmd.flags.String("db", "", "Database path (required)")
	cmd.dbType = cmd.flags.String("db-type", "sqlite", "Database type: sqlite or postgres")
	cmd.format = cmd.flags.String("format", "table", "Output format: table or json")
	cmd.history = cmd.flags.Bool("history", false, "Show execution history")

	return cmd
}

func (c *GetCommand) Parse(args []string) error {
	if err := c.flags.Parse(args); err != nil {
		return err
	}

	remaining := c.flags.Args()
	if len(remaining) == 0 {
		return fmt.Errorf("command ID is required")
	}

	c.id = remaining[0]
	return nil
}

func (c *GetCommand) Run(ctx context.Context) error {
	store, err := ConnectStorage(*c.dbType, *c.dbPath)
	if err != nil {
		return err
	}
	defer store.Close()

	instance, err := store.Get(ctx, c.id)
	if err != nil {
		return fmt.Errorf("failed to get command: %w", err)
	}

	switch *c.format {
	case "table":
		return c.printTable(instance)
	case "json":
		return c.printJSON(instance)
	default:
		return fmt.Errorf("unknown format: %s", *c.format)
	}
}

func (c *GetCommand) printTable(cmd *durex.Instance) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)

	fmt.Println("=== Command Details ===\n")
	fmt.Fprintf(w, "ID:\t%s\n", cmd.ID)
	fmt.Fprintf(w, "Name:\t%s\n", cmd.Name)
	fmt.Fprintf(w, "Status:\t%s\n", cmd.Status)
	fmt.Fprintf(w, "Attempt:\t%d\n", cmd.Attempt)
	fmt.Fprintf(w, "Retries:\t%d\n", cmd.Retries)

	if cmd.Priority != 0 {
		fmt.Fprintf(w, "Priority:\t%d\n", cmd.Priority)
	}

	if len(cmd.Tags) > 0 {
		fmt.Fprintf(w, "Tags:\t%s\n", strings.Join(cmd.Tags, ", "))
	}

	if cmd.UniqueKey != "" {
		fmt.Fprintf(w, "Unique Key:\t%s\n", cmd.UniqueKey)
	}

	if cmd.TraceID != "" {
		fmt.Fprintf(w, "Trace ID:\t%s\n", cmd.TraceID)
	}

	if cmd.CorrelationID != "" {
		fmt.Fprintf(w, "Correlation ID:\t%s\n", cmd.CorrelationID)
	}

	if cmd.ParentID != nil {
		fmt.Fprintf(w, "Parent ID:\t%s\n", *cmd.ParentID)
	}

	fmt.Fprintf(w, "\n")
	fmt.Fprintf(w, "Created:\t%s\n", cmd.CreatedAt.Format("2006-01-02 15:04:05"))
	fmt.Fprintf(w, "Ready At:\t%s\n", cmd.ReadyAt.Format("2006-01-02 15:04:05"))

	if cmd.StartedAt != nil {
		fmt.Fprintf(w, "Started:\t%s\n", cmd.StartedAt.Format("2006-01-02 15:04:05"))
	}

	if cmd.CompletedAt != nil {
		fmt.Fprintf(w, "Completed:\t%s\n", cmd.CompletedAt.Format("2006-01-02 15:04:05"))
		if cmd.StartedAt != nil {
			duration := cmd.CompletedAt.Sub(*cmd.StartedAt)
			fmt.Fprintf(w, "Duration:\t%v\n", duration)
		}
	}

	if cmd.DeadlineAt != nil {
		fmt.Fprintf(w, "Deadline:\t%s\n", cmd.DeadlineAt.Format("2006-01-02 15:04:05"))
	}

	if cmd.Timeout > 0 {
		fmt.Fprintf(w, "Timeout:\t%v\n", cmd.Timeout)
	}

	if cmd.Period > 0 {
		fmt.Fprintf(w, "Period:\t%v\n", cmd.Period)
	}

	if cmd.Cron != "" {
		fmt.Fprintf(w, "Cron:\t%s\n", cmd.Cron)
	}

	if len(cmd.Sequence) > 0 {
		fmt.Fprintf(w, "Sequence:\t%s\n", strings.Join(cmd.Sequence, " → "))
	}

	if cmd.Error != "" {
		fmt.Fprintf(w, "\nError:\t%s\n", cmd.Error)
	}

	w.Flush()

	// Print data
	if len(cmd.Data) > 0 {
		fmt.Println("\n=== Data ===")
		dataJSON, _ := json.MarshalIndent(cmd.Data, "", "  ")
		fmt.Println(string(dataJSON))
	}

	// Print metadata
	if len(cmd.Metadata) > 0 {
		fmt.Println("\n=== Metadata ===")
		metaJSON, _ := json.MarshalIndent(cmd.Metadata, "", "  ")
		fmt.Println(string(metaJSON))
	}

	// Print history if requested
	if *c.history && len(cmd.History) > 0 {
		fmt.Println("\n=== Execution History ===\n")
		hw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(hw, "TIME\tEVENT\tATTEMPT\tDURATION\tMESSAGE")
		fmt.Fprintln(hw, "----\t-----\t-------\t--------\t-------")

		for _, event := range cmd.History {
			timestamp := event.Timestamp.Format("15:04:05")
			duration := ""
			if event.DurationMs > 0 {
				duration = FormatDuration(event.DurationMs)
			}
			message := event.Message
			if event.Error != "" {
				message = event.Error
			}

			fmt.Fprintf(hw, "%s\t%s\t%d\t%s\t%s\n",
				timestamp, event.Type, event.Attempt, duration, TruncateString(message, 50))
		}
		hw.Flush()
	}

	return nil
}

func (c *GetCommand) printJSON(cmd *durex.Instance) error {
	data, err := json.MarshalIndent(cmd, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}
	fmt.Println(string(data))
	return nil
}
