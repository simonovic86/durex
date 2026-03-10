# Durex Roadmap

Prioritized list of improvements identified from a full codebase audit (March 2026).

Items marked **DONE** were addressed in commit `90b6e6e`.

## Bugs

- [x] **DONE** Permanent commands always fail to start — `e.started` flag set after `e.Add()` call
- [x] **DONE** Barrier fan-in data collision — same-name children overwrite each other's results
- [ ] `CancelByTag` missing `RecordEvent` — unlike `Cancel()`, no `EventCancelled` is recorded in history (`executor.go:351-363`)
- [ ] Postgres `Find()` ignores tag filters — `Query.Tags` never translated into SQL conditions (`postgres.go:481-549`)
- [ ] Cleanup ignores `DEAD_LETTER` status — dead-lettered commands accumulate forever in SQLite/Postgres; Memory uses `IsTerminal()` which correctly includes it (`sqlite.go:335`, `postgres.go:446`)
- [ ] Workflow example has duplicate `Add` call — each iteration adds two commands, one without sequence (`examples/workflow/main.go:167-175`)
- [ ] `NewFunc` doc references nonexistent `executor.Handle()` — should say `executor.Register()` (`func.go:130`)

## Security

- [x] **DONE** SQL injection via table name — `fmt.Sprintf` interpolation without validation
- [x] **DONE** SQL injection via `Query.OrderBy` — free-form string in `ORDER BY` clause

## Design / Correctness

- [x] **DONE** Dashboard goroutine leaks on shutdown — no shutdown mechanism, port stays bound
- [x] **DONE** Instance.Clone() shallow-copies Data/Metadata — nested maps share references
- [ ] Partial child spawn failures corrupt fan-in — if some children fail to create, barrier gets fewer `childIDs` than intended, may trigger continuation prematurely (`executor.go:911-947`)
- [ ] Shutdown timeout returns nil, leaks goroutines — if `shutdownTimeout` triggers, `Stop()` returns no error but workers keep running (`executor.go:207-222`)
- [ ] `execute`/`executeClaimedCommand` massive duplication — two ~80-line methods share >90% identical code; only difference is "not ready" check and initial status update (`executor.go:648-816`)
- [ ] Queue channel dead code in polling mode — `schedule()` pushes to `e.queue` but no `worker()` goroutines consume it in `LockingStorage` mode
- [ ] Executor not reusable after Stop — context created once in `New()`, never recreated
- [ ] `ContinueSequence` shallow-copies data — nested maps share references with original instance (`instance.go:165-172`)
- [ ] JSON deep copy silently coerces `int` → `float64` — `deepCopyMap` via JSON round-trip
- [ ] `Typed()` helper silently swallows marshal errors — creates empty-data specs on failure (`typed.go:170-180`)
- [ ] Recover silently skips on unmarshal failure in TypedCommand — user's compensation function never called (`typed.go:91-100`)
- [ ] `replay` can block `Start()` indefinitely — no context check between iterations when queue is full (`executor.go:1199-1211`)

## Storage Layer

- [ ] SQLite missing `QueryableStorage`/`LockingStorage` — only implements base `Storage`, CLI falls back to client-side filtering
- [ ] `FindPending` doesn't filter by `ReadyAt` in Memory/SQLite — returns commands that aren't ready yet
- [ ] Missing `unique_key` UNIQUE constraint at DB level — concurrent creates can race past `FindByUniqueKey`
- [ ] SQLite missing `SetMaxOpenConns(1)` and `busy_timeout` PRAGMA — concurrent writes get "database is locked"
- [ ] Missing composite index `(status, priority DESC, ready_at ASC)` for FindPending performance
- [ ] `postgresTx` stubs out 6 of 10 Storage methods — `Get`, `Find*`, `Cleanup`, `Count` all return hard errors
- [ ] Silenced `json.Marshal` errors in SQLite Create/Update and `postgresTx` — data corruption on failure (`sqlite.go:116-119`, `postgres.go:807-810`)
- [ ] Latent `argNum++` missing after `CreatedBefore` in Postgres Find — **DONE** (fixed alongside OrderBy)
- [ ] Duplicate compile-time assertion (`sqlite.go:16` and `595`)

## API / Developer Experience

- [ ] Missing Spec builder methods — no `WithPeriod`, `WithCron`, `WithSequence`, `WithDeadlineAt`
- [ ] Inconsistent option naming — `FuncOption` uses `Retries(n)` / `Period(d)` / `OnRecover(fn)`; `TypedOption` uses `WithRetries(n)` / `WithPeriod(d)` / `WithRecover(fn)`
- [ ] `TypedCommand` missing `Expirable` support — no `WithExpired` option, typed commands can't handle deadline expiration
- [ ] `HandleTyped` returns void — unlike `Register()` and `HandleFunc()` which return `*Executor` for chaining
- [ ] `Register` panics, `MustRegister` doesn't — backwards from Go convention (`Must*` = panics)
- [ ] `Period` vs `RepeatInterval` terminology drift — Spec field is `Period`, executor option is `WithDefaultRepeatInterval`
- [ ] Options missing bounds checks — negative retries, negative timeout, zero/negative check interval accepted
- [ ] Rate limiter wasted wakeups under dual limits — TOCTOU between global and per-command checks

## Test Coverage

- [x] **DONE** SQLite conformance tests — extracted `storagetest` package, 23/23 passing
- [ ] Postgres conformance tests — test exists but requires `DUREX_TEST_POSTGRES_DSN` env var
- [ ] Dashboard API — 0 tests for 6 HTTP endpoints
- [ ] CLI commands — 0 tests for 7 command files
- [ ] Dead Letter Queue — 0 tests for `ReplayFromDLQ`, `FindDeadLettered`, `PurgeDLQ`
- [ ] Middleware chains — 0 tests for `executeWithMiddleware`
- [ ] Rate limiter — 0 tests for entire `ratelimit.go`
- [ ] Graceful shutdown — 0 tests for timeout, double-stop, concurrent add+stop
- [ ] Polling worker mode — 0 tests for `pollingWorker`, `claimAndExecute`
- [ ] `AddMany`, `CancelByTag` — 0 tests at executor level
- [ ] `HookedStorage` — 0 tests
- [ ] Stuck command recovery — 0 tests
- [ ] Permanent commands — 0 tests
- [ ] Storage error injection — no tests for `Update` failing during `handleResult`/`handleError`
