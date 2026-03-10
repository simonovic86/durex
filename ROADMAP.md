# Durex Roadmap

Prioritized list of improvements identified from a full codebase audit (March 2026).

Items marked **DONE** were addressed in commits `90b6e6e`, `72d49e3`, or subsequent fixes.

## Bugs

- [x] **DONE** Permanent commands always fail to start — `e.started` flag set after `e.Add()` call
- [x] **DONE** Barrier fan-in data collision — same-name children overwrite each other's results
- [x] **DONE** `CancelByTag` missing `RecordEvent` — unlike `Cancel()`, no `EventCancelled` is recorded in history
- [x] **DONE** Postgres `Find()` ignores tag filters — `Query.Tags` never translated into SQL conditions
- [x] **DONE** Cleanup ignores `DEAD_LETTER` status — dead-lettered commands accumulate forever in SQLite/Postgres
- [x] **DONE** Workflow example has duplicate `Add` call — each iteration adds two commands, one without sequence
- [x] **DONE** `NewFunc` doc references nonexistent `executor.Handle()` — should say `executor.Register()`

## Security

- [x] **DONE** SQL injection via table name — `fmt.Sprintf` interpolation without validation
- [x] **DONE** SQL injection via `Query.OrderBy` — free-form string in `ORDER BY` clause

## Design / Correctness

- [x] **DONE** Dashboard goroutine leaks on shutdown — no shutdown mechanism, port stays bound
- [x] **DONE** Instance.Clone() shallow-copies Data/Metadata — nested maps share references
- [ ] Partial child spawn failures corrupt fan-in — if some children fail to create, barrier gets fewer `childIDs` than intended, may trigger continuation prematurely (`executor.go:911-947`)
- [x] **DONE** Shutdown timeout returns nil, leaks goroutines — `Stop()` now returns error on timeout
- [x] **DONE** `execute`/`executeClaimedCommand` massive duplication — unified into single execution path
- [ ] Queue channel dead code in polling mode — `schedule()` pushes to `e.queue` but no `worker()` goroutines consume it in `LockingStorage` mode
- [ ] Executor not reusable after Stop — context created once in `New()`, never recreated
- [x] **DONE** `ContinueSequence` shallow-copies data — now uses `deepCopyMap` for nested maps
- [ ] JSON deep copy silently coerces `int` → `float64` — `deepCopyMap` via JSON round-trip
- [ ] `Typed()` helper silently swallows marshal errors — creates empty-data specs on failure (`typed.go:170-180`)
- [ ] Recover silently skips on unmarshal failure in TypedCommand — user's compensation function never called (`typed.go:91-100`)
- [ ] `replay` can block `Start()` indefinitely — no context check between iterations when queue is full (`executor.go:1199-1211`)

## Storage Layer

- [x] **DONE** SQLite missing `QueryableStorage` — now implements `QueryableStorage` with `Find()` method
- [ ] SQLite missing `LockingStorage` — only single-instance mode supported
- [x] **DONE** `FindPending` doesn't filter by `ReadyAt` in Memory/SQLite/Postgres — now filters `ready_at <= now`
- [ ] Missing `unique_key` UNIQUE constraint at DB level — concurrent creates can race past `FindByUniqueKey`
- [ ] SQLite missing `SetMaxOpenConns(1)` and `busy_timeout` PRAGMA — concurrent writes get "database is locked"
- [ ] Missing composite index `(status, priority DESC, ready_at ASC)` for FindPending performance
- [ ] `postgresTx` stubs out 6 of 10 Storage methods — `Get`, `Find*`, `Cleanup`, `Count` all return hard errors
- [ ] Silenced `json.Marshal` errors in SQLite Create/Update and `postgresTx` — data corruption on failure (`sqlite.go:116-119`, `postgres.go:807-810`)
- [x] **DONE** Latent `argNum++` missing after `CreatedBefore` in Postgres Find
- [x] **DONE** Duplicate compile-time assertion (`sqlite.go:16` and `595`) — removed duplicate

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
- [x] **DONE** Dashboard API — tests for all HTTP endpoints (stats, commands, health, retry, cancel, history, index)
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
