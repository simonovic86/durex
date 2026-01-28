package commands

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/simonovic86/durex"
)

type ListCommand struct {
	flags    *flag.FlagSet
	dbPath   *string
	dbType   *string
	status   *string
	name     *string
	tag      *string
	limit    *int
	format   *string
}

func NewListCommand() *ListCommand {
	cmd := &ListCommand{
		flags: flag.NewFlagSet("list", flag.ExitOnError),
	}
	
	cmd.dbPath = cmd.flags.String("db", "", "Database path (required)")
	cmd.dbType = cmd.flags.String("db-type", "sqlite", "Database type: sqlite or postgres")
	cmd.status = cmd.flags.String("status", "", "Filter by status (pending, started, completed, failed, expired, cancelled, repeating)")
	cmd.name = cmd.flags.String("command", "", "Filter by command name")
	cmd.tag = cmd.flags.String("tag", "", "Filter by tag")
	cmd.limit = cmd.flags.Int("limit", 50, "Maximum number of commands to show")
	cmd.format = cmd.flags.String("format", "table", "Output format: table, json, or csv")
	
	return cmd
}

func (c *ListCommand) Parse(args []string) error {
	return c.flags.Parse(args)
}

func (c *ListCommand) Run(ctx context.Context) error {
	store, err := ConnectStorage(*c.dbType, *c.dbPath)
	if err != nil {
		return err
	}
	defer store.Close()

	// Build query
	var commands []*durex.Instance
	
	// Use queryable storage if available
	if qs, ok := store.(durex.QueryableStorage); ok {
		query := durex.Query{
			Limit: *c.limit,
		}
		
		if *c.status != "" {
			status := durex.Status(*c.status)
			query.Status = &status
		}
		
		if *c.name != "" {
			query.Name = c.name
		}
		
		if *c.tag != "" {
			query.Tags = []string{*c.tag}
		}
		
		commands, err = qs.Find(ctx, query)
		if err != nil {
			return fmt.Errorf("failed to query commands: %w", err)
		}
	} else {
		// Fallback: get by status
		if *c.status != "" {
			status := durex.Status(strings.ToUpper(*c.status))
			commands, err = store.FindByStatus(ctx, status)
			if err != nil {
				return fmt.Errorf("failed to list commands: %w", err)
			}
		} else {
			// Get all pending by default
			commands, err = store.FindPending(ctx)
			if err != nil {
				return fmt.Errorf("failed to list commands: %w", err)
			}
		}
		
		// Apply additional filters
		filtered := make([]*durex.Instance, 0, len(commands))
		for _, cmd := range commands {
			if *c.name != "" && cmd.Name != *c.name {
				continue
			}
			if *c.tag != "" && !cmd.HasTag(*c.tag) {
				continue
			}
			filtered = append(filtered, cmd)
			if len(filtered) >= *c.limit {
				break
			}
		}
		commands = filtered
	}

	// Output results
	switch *c.format {
	case "table":
		return c.printTable(commands)
	case "json":
		return c.printJSON(commands)
	case "csv":
		return c.printCSV(commands)
	default:
		return fmt.Errorf("unknown format: %s", *c.format)
	}
}

func (c *ListCommand) printTable(commands []*durex.Instance) error {
	if len(commands) == 0 {
		fmt.Println("No commands found")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tNAME\tSTATUS\tCREATED\tATTEMPT\tERROR")
	fmt.Fprintln(w, strings.Repeat("-", 80))
	
	for _, cmd := range commands {
		id := TruncateString(cmd.ID, 20)
		name := TruncateString(cmd.Name, 20)
		status := string(cmd.Status)
		created := cmd.CreatedAt.Format("2006-01-02 15:04")
		attempt := fmt.Sprintf("%d", cmd.Attempt)
		errMsg := TruncateString(cmd.Error, 30)
		
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
			id, name, status, created, attempt, errMsg)
	}
	
	w.Flush()
	
	fmt.Printf("\nTotal: %d commands\n", len(commands))
	return nil
}

func (c *ListCommand) printJSON(commands []*durex.Instance) error {
	fmt.Println("[")
	for i, cmd := range commands {
		fmt.Printf("  {\n")
		fmt.Printf("    \"id\": %q,\n", cmd.ID)
		fmt.Printf("    \"name\": %q,\n", cmd.Name)
		fmt.Printf("    \"status\": %q,\n", cmd.Status)
		fmt.Printf("    \"created_at\": %q,\n", cmd.CreatedAt.Format(time.RFC3339))
		fmt.Printf("    \"attempt\": %d,\n", cmd.Attempt)
		fmt.Printf("    \"error\": %q\n", cmd.Error)
		if i < len(commands)-1 {
			fmt.Printf("  },\n")
		} else {
			fmt.Printf("  }\n")
		}
	}
	fmt.Println("]")
	return nil
}

func (c *ListCommand) printCSV(commands []*durex.Instance) error {
	fmt.Println("ID,NAME,STATUS,CREATED,ATTEMPT,ERROR")
	for _, cmd := range commands {
		fmt.Printf("%s,%s,%s,%s,%d,%q\n",
			cmd.ID,
			cmd.Name,
			cmd.Status,
			cmd.CreatedAt.Format(time.RFC3339),
			cmd.Attempt,
			cmd.Error,
		)
	}
	return nil
}
