# Durex CLI

Command-line interface for managing durex workflows and inspecting command execution.

## Installation

```bash
go install github.com/simonovic86/durex/cmd/durex@latest
```

Or build from source:

```bash
cd cmd/durex
go build -o durex .
```

## Usage

### List Commands

```bash
# List all pending commands
durex list --db=/path/to/durex.db

# List failed commands
durex list --db=/path/to/durex.db --status=failed

# Filter by command name
durex list --db=/path/to/durex.db --command=sendEmail

# Filter by tag
durex list --db=/path/to/durex.db --tag=priority:high

# Output as JSON
durex list --db=/path/to/durex.db --format=json

# With PostgreSQL
durex list --db-type=postgres --db="postgres://user:pass@localhost/durex?sslmode=disable"
```

### Statistics

```bash
# Show overall statistics
durex stats --db=/path/to/durex.db

# Show breakdown by command type
durex stats --db=/path/to/durex.db --detailed

# Show stats for specific command
durex stats --db=/path/to/durex.db --command=sendEmail
```

### Get Command Details

```bash
# Get detailed information about a command
durex get cmd_abc123 --db=/path/to/durex.db

# Include execution history
durex get cmd_abc123 --db=/path/to/durex.db --history

# Output as JSON
durex get cmd_abc123 --db=/path/to/durex.db --format=json
```

### Retry Failed Commands

```bash
# Retry a specific failed command
durex retry cmd_abc123 --db=/path/to/durex.db
```

**Note:** The command is reset to PENDING status. You need a running executor instance to pick it up and execute it.

### Cancel Commands

```bash
# Cancel a specific command
durex cancel cmd_abc123 --db=/path/to/durex.db

# Cancel all commands with a tag
durex cancel --tag=batch:old --db=/path/to/durex.db
```

### Start Dashboard

```bash
# Start dashboard on default port (8080)
durex dashboard --db=/path/to/durex.db

# Custom port
durex dashboard --db=/path/to/durex.db --port=9090

# Custom host
durex dashboard --db=/path/to/durex.db --host=0.0.0.0 --port=8080
```

## Supported Databases

- **SQLite** (default): `--db-type=sqlite --db=/path/to/file.db`
- **PostgreSQL**: `--db-type=postgres --db="postgres://user:pass@host:port/db"`

## Global Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--db` | Database connection string | (required) |
| `--db-type` | Database type: sqlite or postgres | sqlite |

## Command-Specific Flags

### list

| Flag | Description | Default |
|------|-------------|---------|
| `--status` | Filter by status | (all) |
| `--command` | Filter by command name | (all) |
| `--tag` | Filter by tag | (all) |
| `--limit` | Maximum results to show | 50 |
| `--format` | Output format: table, json, csv | table |

### stats

| Flag | Description | Default |
|------|-------------|---------|
| `--command` | Show stats for specific command | (all) |
| `--detailed` | Show breakdown by command type | false |

### get

| Flag | Description | Default |
|------|-------------|---------|
| `--history` | Show execution history | false |
| `--format` | Output format: table or json | table |

### dashboard

| Flag | Description | Default |
|------|-------------|---------|
| `--port` | Port to listen on | 8080 |
| `--host` | Host to bind to | localhost |

## Examples

```bash
# Daily operations: check failed jobs
durex list --db=./data/durex.db --status=failed | wc -l

# Debugging: inspect a specific command
durex get cmd_abc123 --db=./data/durex.db --history

# Monitoring: get stats
durex stats --db=./data/durex.db --command=processOrder

# Operations: retry all failed emails from yesterday
durex list --db=./data/durex.db --status=failed --command=sendEmail --format=json | \
  jq -r '.[].id' | \
  xargs -I {} durex retry {} --db=./data/durex.db

# Development: quick dashboard access
durex dashboard --db=./data/durex.db
```

## Architecture

The CLI is a separate Go module that imports the durex library. This keeps the main durex library lightweight (no CGO, no database drivers) while the CLI tool can have whatever dependencies it needs.

```
durex/                    # Main library (lightweight)
├── go.mod                # Only durex core dependencies
└── cmd/
    └── durex/            # CLI tool (separate module)
        ├── go.mod        # CLI dependencies (DB drivers, etc.)
        └── commands/     # CLI command implementations
```

## Development

```bash
# Run tests
cd cmd/durex
go test ./...

# Build
go build -o durex .

# Create test database
go run test_cli.go
```

## Notes

- The CLI requires the database to exist and be migrated
- The CLI doesn't register command handlers - it only inspects/manages existing commands
- For command execution, you still need to run your application with the durex executor
