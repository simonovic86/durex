// Durex CLI - Command-line interface for managing durex workflows
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/simonovic86/durex/cmd/durex/commands"
)

const version = "0.10.0"

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	// Get subcommand
	cmd := os.Args[1]
	
	ctx := context.Background()

	switch cmd {
	case "list", "ls":
		listCmd := commands.NewListCommand()
		if err := listCmd.Parse(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		if err := listCmd.Run(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

	case "stats":
		statsCmd := commands.NewStatsCommand()
		if err := statsCmd.Parse(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		if err := statsCmd.Run(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

	case "retry":
		retryCmd := commands.NewRetryCommand()
		if err := retryCmd.Parse(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		if err := retryCmd.Run(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

	case "cancel":
		cancelCmd := commands.NewCancelCommand()
		if err := cancelCmd.Parse(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		if err := cancelCmd.Run(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

	case "dashboard":
		dashboardCmd := commands.NewDashboardCommand()
		if err := dashboardCmd.Parse(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		if err := dashboardCmd.Run(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

	case "get":
		getCmd := commands.NewGetCommand()
		if err := getCmd.Parse(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		if err := getCmd.Run(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

	case "version", "-v", "--version":
		fmt.Printf("durex version %s\n", version)

	case "help", "-h", "--help":
		printUsage()

	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `durex - Command-line interface for managing durex workflows

Usage:
  durex <command> [options]

Commands:
  list, ls           List commands with optional filtering
  stats              Show statistics for commands
  retry <id>         Retry a failed command
  cancel <id>        Cancel a pending command
  get <id>           Get detailed information about a command
  dashboard          Start the web dashboard
  version            Show version information
  help               Show this help message

Examples:
  # List all failed commands
  durex list --db=/path/to/durex.db --status=failed

  # Show stats for a specific command type
  durex stats --db=/path/to/durex.db --command=sendEmail

  # Retry a failed command
  durex retry cmd_abc123 --db=/path/to/durex.db

  # Start dashboard on custom port
  durex dashboard --db=/path/to/durex.db --port=9090

  # List commands with postgres
  durex list --db-type=postgres --db="postgres://user:pass@localhost/durex?sslmode=disable"

For more information: https://github.com/simonovic86/durex
`)
}
