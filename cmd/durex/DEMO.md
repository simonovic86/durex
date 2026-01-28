# Durex CLI Demo

This guide shows the CLI tool in action with real examples.

## Setup Demo Database

```bash
# Create a test database with sample commands
cd cmd/durex
go run testdata/create_test_db.go
```

Output:
```
✅ Test database created: test_cli.db
```

## List Commands

### Show all pending commands

```bash
durex list --db=test_cli.db
```

Output:
```
ID                    NAME            STATUS   CREATED          ATTEMPT  ERROR
--------------------------------------------------------------------------------
cmd_19c06eeb28b_9...  processPayment  PENDING  2026-01-29 00:27  0       
cmd_19c06eeb28c_a...  processPayment  PENDING  2026-01-29 00:27  0       

Total: 2 commands
```

### List failed commands

```bash
durex list --db=test_cli.db --status=failed
```

Output:
```
ID                    NAME         STATUS  CREATED          ATTEMPT  ERROR
--------------------------------------------------------------------------------
cmd_19c06eeb28a_8...  failingTask  FAILED  2026-01-29 00:27  1       task always fails
cmd_19c06eeb28a_7...  failingTask  FAILED  2026-01-29 00:27  1       task always fails
cmd_19c06eeb288_6...  failingTask  FAILED  2026-01-29 00:27  1       task always fails

Total: 3 commands
```

### List completed commands

```bash
durex list --db=test_cli.db --status=completed
```

Output:
```
ID                    NAME       STATUS     CREATED          ATTEMPT  ERROR
--------------------------------------------------------------------------------
cmd_19c06eeb286_5...  sendEmail  COMPLETED  2026-01-29 00:27  1       
cmd_19c06eeb284_4...  sendEmail  COMPLETED  2026-01-29 00:27  1       
cmd_19c06eeb282_3...  sendEmail  COMPLETED  2026-01-29 00:27  1       
cmd_19c06eeb27f_2...  sendEmail  COMPLETED  2026-01-29 00:27  1       
cmd_19c06eeb27d_1...  sendEmail  COMPLETED  2026-01-29 00:27  1       

Total: 5 commands
```

## Statistics

### Overall statistics

```bash
durex stats --db=test_cli.db
```

Output:
```
=== Durex Command Statistics ===

STATUS     COUNT  PERCENTAGE
------     -----  ----------
Pending    2      20.0%
Started    0      0.0%
Completed  5      50.0%
Failed     3      30.0%
Expired    0      0.0%
Cancelled  0      0.0%
Repeating  0      0.0%
------     -----  ----------
TOTAL      10     100.0%
```

## Get Command Details

```bash
durex get --db=test_cli.db cmd_19c06eeb27d_1_c9213393d34bae64 --history
```

Output:
```
=== Command Details ===

ID:         cmd_19c06eeb27d_1_c9213393d34bae64
Name:       sendEmail
Status:     COMPLETED
Attempt:    1
Retries:    0
Tags:       email, test

Created:    2026-01-29 00:27:12
Ready At:   2026-01-29 00:27:12
Started:    2026-01-29 00:27:12
Completed:  2026-01-29 00:27:12
Duration:   37.427ms

=== Data ===
{
  "subject": "Test Email",
  "to": "user0@example.com"
}

=== Execution History ===

TIME      EVENT      ATTEMPT  DURATION  MESSAGE
----      -----      -------  --------  -------
00:27:12  CREATED    0                  
00:27:12  STARTED    1                  
00:27:12  COMPLETED  1        37ms      
```

## Retry Failed Commands

```bash
durex retry --db=test_cli.db cmd_19c06eeb288_6_5fc314901de1cd75
```

Output:
```
Retrying command: cmd_19c06eeb288_6_5fc314901de1cd75 (failingTask)
Previous status: FAILED
Previous error: task always fails
✅ Command reset to PENDING status

Note: The command will be picked up by running executor instances.
If no executor is running, start one to process the retried command.
```

## Cancel Commands

```bash
# Cancel by ID
durex cancel --db=test_cli.db cmd_abc123

# Cancel by tag
durex cancel --db=test_cli.db --tag=batch:old
```

## Start Dashboard

```bash
durex dashboard --db=test_cli.db --port=8080
```

Output:
```
🚀 Starting Durex Dashboard on http://localhost:8080
📊 Database: test_cli.db (sqlite)

Press Ctrl+C to stop
```

Open http://localhost:8080 in your browser to access the web UI.

## Real-World Scenarios

### Daily Operations: Check Failed Jobs

```bash
#!/bin/bash
# Check if any jobs failed overnight
FAILED_COUNT=$(durex list --db=/var/lib/durex/prod.db --status=failed | grep -c "FAILED")

if [ "$FAILED_COUNT" -gt 0 ]; then
    echo "⚠️  $FAILED_COUNT failed jobs found"
    durex list --db=/var/lib/durex/prod.db --status=failed
    # Send alert...
fi
```

### Debugging Production Issues

```bash
# Find slow commands
durex stats --db=/var/lib/durex/prod.db --command=processPayment

# Inspect specific failure
durex get --db=/var/lib/durex/prod.db cmd_xyz --history

# Check trace across workflow
durex list --db=/var/lib/durex/prod.db --format=json | \
    jq '.[] | select(.trace_id == "trace-123")'
```

### Bulk Operations

```bash
# Retry all failed emails from today
durex list --db=/var/lib/durex/prod.db --status=failed --command=sendEmail --format=json | \
    jq -r '.[].id' | \
    while read id; do
        durex retry --db=/var/lib/durex/prod.db "$id"
        echo "Retried $id"
    done
```

### Monitoring Integration

```bash
# Export metrics for monitoring system
durex stats --db=/var/lib/durex/prod.db --format=json > /tmp/durex_stats.json

# Prometheus textfile collector
cat << EOF > /var/lib/node_exporter/textfile_collector/durex.prom
durex_commands_pending $(durex stats --db=/var/lib/durex/prod.db | grep Pending | awk '{print $2}')
durex_commands_failed $(durex stats --db=/var/lib/durex/prod.db | grep Failed | awk '{print $2}')
EOF
```

## Tips

1. **Use aliases** for common operations:
   ```bash
   alias dx='durex --db=/var/lib/durex/prod.db'
   dx list --status=failed
   dx stats
   ```

2. **Combine with watch** for monitoring:
   ```bash
   watch -n 5 'durex stats --db=/var/lib/durex/prod.db'
   ```

3. **Use jq** for advanced filtering:
   ```bash
   durex list --db=/var/lib/durex/prod.db --format=json | \
       jq '.[] | select(.attempt > 3)'
   ```

4. **Quick dashboard access** during development:
   ```bash
   durex dashboard --db=./durex.db &
   open http://localhost:8080
   ```
