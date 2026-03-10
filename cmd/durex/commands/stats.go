package commands

import (
	"context"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/simonovic86/durex"
)

type StatsCommand struct {
	flags    *flag.FlagSet
	dbPath   *string
	dbType   *string
	command  *string
	detailed *bool
}

func NewStatsCommand() *StatsCommand {
	cmd := &StatsCommand{
		flags: flag.NewFlagSet("stats", flag.ExitOnError),
	}

	cmd.dbPath = cmd.flags.String("db", "", "Database path (required)")
	cmd.dbType = cmd.flags.String("db-type", "sqlite", "Database type: sqlite or postgres")
	cmd.command = cmd.flags.String("command", "", "Show stats for specific command name")
	cmd.detailed = cmd.flags.Bool("detailed", false, "Show detailed breakdown by command")

	return cmd
}

func (c *StatsCommand) Parse(args []string) error {
	return c.flags.Parse(args)
}

func (c *StatsCommand) Run(ctx context.Context) error {
	store, err := ConnectStorage(*c.dbType, *c.dbPath)
	if err != nil {
		return err
	}
	defer store.Close()

	// Get overall stats
	pending, err := store.Count(ctx, ptr(durex.StatusPending))
	if err != nil {
		return fmt.Errorf("failed to count pending: %w", err)
	}

	started, err := store.Count(ctx, ptr(durex.StatusStarted))
	if err != nil {
		return fmt.Errorf("failed to count started: %w", err)
	}

	completed, err := store.Count(ctx, ptr(durex.StatusCompleted))
	if err != nil {
		return fmt.Errorf("failed to count completed: %w", err)
	}

	failed, err := store.Count(ctx, ptr(durex.StatusFailed))
	if err != nil {
		return fmt.Errorf("failed to count failed: %w", err)
	}

	expired, err := store.Count(ctx, ptr(durex.StatusExpired))
	if err != nil {
		return fmt.Errorf("failed to count expired: %w", err)
	}

	cancelled, err := store.Count(ctx, ptr(durex.StatusCancelled))
	if err != nil {
		return fmt.Errorf("failed to count cancelled: %w", err)
	}

	repeating, err := store.Count(ctx, ptr(durex.StatusRepeating))
	if err != nil {
		return fmt.Errorf("failed to count repeating: %w", err)
	}

	deadLetter, err := store.Count(ctx, ptr(durex.StatusDeadLetter))
	if err != nil {
		return fmt.Errorf("failed to count dead letter: %w", err)
	}

	total, err := store.Count(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to count total: %w", err)
	}

	// Print overall stats
	fmt.Println("=== Durex Command Statistics ===\n")

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "STATUS\tCOUNT\tPERCENTAGE")
	fmt.Fprintln(w, "------\t-----\t----------")

	printStat := func(name string, count int64) {
		pct := float64(0)
		if total > 0 {
			pct = float64(count) / float64(total) * 100
		}
		fmt.Fprintf(w, "%s\t%d\t%.1f%%\n", name, count, pct)
	}

	printStat("Pending", pending)
	printStat("Started", started)
	printStat("Completed", completed)
	printStat("Failed", failed)
	printStat("Expired", expired)
	printStat("Cancelled", cancelled)
	printStat("Repeating", repeating)
	if deadLetter > 0 {
		printStat("Dead Letter", deadLetter)
	}
	fmt.Fprintln(w, "------\t-----\t----------")
	printStat("TOTAL", total)

	w.Flush()

	// Show detailed breakdown by command name if requested
	if *c.detailed {
		fmt.Println("\n=== Breakdown by Command ===\n")
		if err := c.printDetailedStats(ctx, store); err != nil {
			return err
		}
	}

	// Show stats for specific command if requested
	if *c.command != "" {
		fmt.Printf("\n=== Stats for '%s' ===\n\n", *c.command)
		if err := c.printCommandStats(ctx, store, *c.command); err != nil {
			return err
		}
	}

	return nil
}

func (c *StatsCommand) printDetailedStats(ctx context.Context, store durex.Storage) error {
	qs, ok := store.(durex.QueryableStorage)
	if !ok {
		fmt.Println("Detailed stats require QueryableStorage (Postgres or SQLite)")
		return nil
	}

	// Get all commands grouped by name
	commands, err := qs.Find(ctx, durex.Query{})
	if err != nil {
		return fmt.Errorf("failed to fetch commands: %w", err)
	}

	// Group by name and status
	type stats struct {
		pending   int
		started   int
		completed int
		failed    int
		total     int
	}

	commandStats := make(map[string]*stats)
	for _, cmd := range commands {
		if _, ok := commandStats[cmd.Name]; !ok {
			commandStats[cmd.Name] = &stats{}
		}
		s := commandStats[cmd.Name]
		s.total++

		switch cmd.Status {
		case durex.StatusPending:
			s.pending++
		case durex.StatusStarted:
			s.started++
		case durex.StatusCompleted:
			s.completed++
		case durex.StatusFailed, durex.StatusDeadLetter:
			s.failed++
		}
	}

	// Print breakdown
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "COMMAND\tTOTAL\tPENDING\tSTARTED\tCOMPLETED\tFAILED")
	fmt.Fprintln(w, "-------\t-----\t-------\t-------\t---------\t------")

	for name, s := range commandStats {
		fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%d\t%d\n",
			name, s.total, s.pending, s.started, s.completed, s.failed)
	}

	w.Flush()
	return nil
}

func (c *StatsCommand) printCommandStats(ctx context.Context, store durex.Storage, name string) error {
	qs, ok := store.(durex.QueryableStorage)
	if !ok {
		fmt.Println("Command stats require QueryableStorage (Postgres or SQLite)")
		return nil
	}

	commands, err := qs.Find(ctx, durex.Query{Name: &name})
	if err != nil {
		return fmt.Errorf("failed to fetch commands: %w", err)
	}

	if len(commands) == 0 {
		fmt.Printf("No commands found with name: %s\n", name)
		return nil
	}

	// Calculate stats
	var (
		totalAttempts int64
		totalDuration int64
		maxDuration   int64
		minDuration   int64 = 999999999
		successCount  int
		failureCount  int
	)

	for _, cmd := range commands {
		totalAttempts += int64(cmd.Attempt)

		if cmd.Status == durex.StatusCompleted {
			successCount++
			if cmd.StartedAt != nil && cmd.CompletedAt != nil {
				duration := cmd.CompletedAt.Sub(*cmd.StartedAt).Milliseconds()
				totalDuration += duration
				if duration > maxDuration {
					maxDuration = duration
				}
				if duration < minDuration {
					minDuration = duration
				}
			}
		} else if cmd.Status == durex.StatusFailed || cmd.Status == durex.StatusDeadLetter {
			failureCount++
		}
	}

	avgAttempts := float64(totalAttempts) / float64(len(commands))
	avgDuration := int64(0)
	if successCount > 0 {
		avgDuration = totalDuration / int64(successCount)
	}

	successRate := float64(0)
	if successCount+failureCount > 0 {
		successRate = float64(successCount) / float64(successCount+failureCount) * 100
	}

	// Print stats
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "Total executions:\t%d\n", len(commands))
	fmt.Fprintf(w, "Successful:\t%d\n", successCount)
	fmt.Fprintf(w, "Failed:\t%d\n", failureCount)
	fmt.Fprintf(w, "Success rate:\t%.1f%%\n", successRate)
	fmt.Fprintf(w, "Avg attempts:\t%.1f\n", avgAttempts)
	if successCount > 0 {
		fmt.Fprintf(w, "Avg duration:\t%s\n", FormatDuration(avgDuration))
		fmt.Fprintf(w, "Min duration:\t%s\n", FormatDuration(minDuration))
		fmt.Fprintf(w, "Max duration:\t%s\n", FormatDuration(maxDuration))
	}
	w.Flush()

	return nil
}

func ptr[T any](v T) *T {
	return &v
}
