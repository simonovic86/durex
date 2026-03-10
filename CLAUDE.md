# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

**Durex** is a lightweight, embeddable durable task queue and workflow engine for Go — "Temporal for the rest of us". It provides persistent background job execution, automatic retries, workflow orchestration (sequences, fan-out, fan-in), and a web dashboard, with no external infrastructure required beyond a database.

## Commands

```bash
make all            # fmt + lint + test + build
make test           # go test with race detector and coverage
make test-short     # Quick tests (skips slow ones)
make lint           # go vet + staticcheck
make fmt            # gofmt
make build          # go build ./...
make cli            # Build the CLI tool
make coverage       # Generate HTML coverage report
make bench          # Run benchmarks
```

Run a single test:
```bash
go test -run TestName ./...
go test -run TestName -v -race .
```

The CLI tool lives in `cmd/durex/` and is a **separate Go module** (to keep the main library dependency-free of DB drivers).

## Architecture

### Core Abstractions

- **`Command`** (`command.go`): Interface for task handlers. Optional interfaces: `Recoverable` (saga compensation), `Expirable` (deadline handling), `Defaulter` (default spec).
- **`Spec`** (`spec.go`): Blueprint for creating a command instance — name, payload data, delay, retries, timeout, cron/period, sequence chain, etc.
- **`Instance`** (`instance.go`): The persisted, running state of a command. `Get/Set/GetString/GetBool` access typed data from JSON payload.
- **`Result`** (`result.go`): What a handler returns — `Empty()`, `Repeat()`, `Retry()`, `Next(spec)`, `Spawn(specs...)`, `SpawnThen(specs, continuation)`.
- **`Executor`** (`executor.go`): Worker pool engine. Polls storage, dispatches commands to handlers, manages goroutine lifecycle.
- **`Registry`** (`registry.go`): Maps command names to handlers.

### Instance Lifecycle
```
PENDING → STARTED → COMPLETED
                 ↓
            error + retries → re-PENDING (with backoff delay)
                 ↓
            error + no retries → FAILED → Recoverable.OnRecover()

PENDING → EXPIRED  (if deadline passed before execution)
COMPLETED → REPEATING → PENDING  (for periodic/cron commands)
```

### Workflow Patterns

- **Sequences**: Linear chain — set `Spec.Sequence = []string{"step2", "step3"}` and call `instance.ContinueSequence(data)` from the result.
- **Fan-out**: `durex.Spawn(spec1, spec2, ...)` — spawns parallel child commands.
- **Fan-in**: `durex.SpawnThen([]Spec{...}, continuationSpec)` — spawns parallel commands, then triggers continuation when all complete. Uses an internal `__durex_barrier` command (`barrier.go`) that tracks child IDs.
- **Saga**: Return `durex.Spawn(compensationSpec)` from `OnRecover()`.

### Storage Layer

`Storage` interface (`storage.go`) has three implementations in `storage/`:
- `memory.go` — in-memory, for tests
- `sqlite.go` — SQLite, schema auto-migrated on open
- `postgres.go` — PostgreSQL with `FOR UPDATE SKIP LOCKED` for multi-instance safety

### Registration Styles

```go
// Simple function
exec.HandleFunc("send-email", func(ctx context.Context, inst durex.Instance) (durex.Result, error) { ... })

// Type-safe generic
durex.HandleTyped[MyPayload](exec, "send-email", func(ctx context.Context, inst durex.Instance, p MyPayload) (durex.Result, error) { ... })

// Struct (implements Command interface)
exec.Register(&MyCommand{})
```

### Key Design Points

- Workers run as goroutines; panics are recovered and the worker continues.
- `barrier.go` implements fan-in by tracking child command IDs directly, checking completion on each child's callback.
- Deep copy of `Data` and `Metadata` uses JSON round-trip (`instance.go:deepCopyMap`) to handle nested maps/slices correctly.
- PostgreSQL uses row-level locking for distributed multi-instance safety; SQLite is single-instance only.
- The web dashboard (`dashboard.go`) serves an embedded SPA; the CLI (`cmd/durex/`) queries storage directly.

## Dependencies

Main library (`go.mod`): only `prometheus/client_golang` and `robfig/cron/v3`.

CLI (`cmd/durex/go.mod`): adds SQLite and PostgreSQL drivers — kept separate to avoid pulling DB drivers into library consumers.
