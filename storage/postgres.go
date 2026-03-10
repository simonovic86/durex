package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/simonovic86/durex"
)

// validIdentifier matches only safe SQL identifiers (alphanumeric and underscores).
var validIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// Compile-time interface assertions.
var (
	_ durex.Storage          = (*Postgres)(nil)
	_ durex.QueryableStorage = (*Postgres)(nil)
	_ durex.LockingStorage   = (*Postgres)(nil)
)

// Postgres is a PostgreSQL storage implementation.
type Postgres struct {
	db        *sql.DB
	tableName string
}

// PostgresOption configures the Postgres storage.
type PostgresOption func(*Postgres)

// WithTableName sets the table name for command storage.
func WithTableName(name string) PostgresOption {
	return func(p *Postgres) {
		p.tableName = name
	}
}

// NewPostgres creates a new PostgreSQL storage.
// The db connection should already be opened and configured.
// Panics if a custom table name contains invalid characters.
func NewPostgres(db *sql.DB, opts ...PostgresOption) *Postgres {
	p := &Postgres{
		db:        db,
		tableName: "durex_commands",
	}

	for _, opt := range opts {
		opt(p)
	}

	if !validIdentifier.MatchString(p.tableName) {
		panic(fmt.Sprintf("durex: invalid table name %q: must match [a-zA-Z_][a-zA-Z0-9_]*", p.tableName))
	}

	return p
}

// Migrate creates the commands table if it doesn't exist.
func (p *Postgres) Migrate(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			data JSONB,
			status TEXT NOT NULL DEFAULT 'PENDING',
			retries INTEGER NOT NULL DEFAULT 0,
			sequence JSONB,
			parent_id TEXT REFERENCES %s(id) ON DELETE SET NULL,
			priority INTEGER NOT NULL DEFAULT 0,
			tags JSONB,
			unique_key TEXT,
			trace_id TEXT,
			correlation_id TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			ready_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			started_at TIMESTAMPTZ,
			completed_at TIMESTAMPTZ,
			deadline_at TIMESTAMPTZ,
			period_ns BIGINT NOT NULL DEFAULT 0,
			cron TEXT,
			error TEXT,
			attempt INTEGER NOT NULL DEFAULT 0,
			metadata JSONB
		);

		CREATE INDEX IF NOT EXISTS idx_%s_status ON %s(status);
		CREATE INDEX IF NOT EXISTS idx_%s_ready_at ON %s(ready_at);
		CREATE INDEX IF NOT EXISTS idx_%s_name ON %s(name);
		CREATE INDEX IF NOT EXISTS idx_%s_parent_id ON %s(parent_id);
		CREATE INDEX IF NOT EXISTS idx_%s_unique_key ON %s(unique_key) WHERE unique_key IS NOT NULL;
		CREATE INDEX IF NOT EXISTS idx_%s_correlation_id ON %s(correlation_id) WHERE correlation_id IS NOT NULL;
	`, p.tableName, p.tableName,
		p.tableName, p.tableName,
		p.tableName, p.tableName,
		p.tableName, p.tableName,
		p.tableName, p.tableName,
		p.tableName, p.tableName,
		p.tableName, p.tableName)

	_, err := p.db.ExecContext(ctx, query)
	return err
}

// Create implements durex.Storage.
func (p *Postgres) Create(ctx context.Context, cmd *durex.Instance) error {
	data, err := json.Marshal(cmd.Data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	sequence, err := json.Marshal(cmd.Sequence)
	if err != nil {
		return fmt.Errorf("failed to marshal sequence: %w", err)
	}

	tags, err := json.Marshal(cmd.Tags)
	if err != nil {
		return fmt.Errorf("failed to marshal tags: %w", err)
	}

	metadata, err := json.Marshal(cmd.Metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	query := fmt.Sprintf(`
		INSERT INTO %s (
			id, name, data, status, retries, sequence, parent_id, priority,
			tags, unique_key, trace_id, correlation_id, created_at, ready_at, started_at, completed_at, deadline_at,
			period_ns, cron, error, attempt, metadata
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22
		)
	`, p.tableName)

	_, err = p.db.ExecContext(ctx, query,
		cmd.ID,
		cmd.Name,
		data,
		cmd.Status,
		cmd.Retries,
		sequence,
		cmd.ParentID,
		cmd.Priority,
		tags,
		nullStr(cmd.UniqueKey),
		nullStr(cmd.TraceID),
		nullStr(cmd.CorrelationID),
		cmd.CreatedAt,
		cmd.ReadyAt,
		cmd.StartedAt,
		cmd.CompletedAt,
		cmd.DeadlineAt,
		int64(cmd.Period),
		nullStr(cmd.Cron),
		cmd.Error,
		cmd.Attempt,
		metadata,
	)

	if err != nil {
		if strings.Contains(err.Error(), "duplicate key") ||
			strings.Contains(err.Error(), "unique constraint") {
			return durex.ErrAlreadyExists
		}
		return err
	}

	return nil
}

// Update implements durex.Storage.
func (p *Postgres) Update(ctx context.Context, cmd *durex.Instance) error {
	data, err := json.Marshal(cmd.Data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	sequence, err := json.Marshal(cmd.Sequence)
	if err != nil {
		return fmt.Errorf("failed to marshal sequence: %w", err)
	}

	tags, err := json.Marshal(cmd.Tags)
	if err != nil {
		return fmt.Errorf("failed to marshal tags: %w", err)
	}

	metadata, err := json.Marshal(cmd.Metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	query := fmt.Sprintf(`
		UPDATE %s SET
			name = $2,
			data = $3,
			status = $4,
			retries = $5,
			sequence = $6,
			parent_id = $7,
			priority = $8,
			tags = $9,
			unique_key = $10,
			trace_id = $11,
			correlation_id = $12,
			ready_at = $13,
			started_at = $14,
			completed_at = $15,
			deadline_at = $16,
			period_ns = $17,
			cron = $18,
			error = $19,
			attempt = $20,
			metadata = $21
		WHERE id = $1
	`, p.tableName)

	result, err := p.db.ExecContext(ctx, query,
		cmd.ID,
		cmd.Name,
		data,
		cmd.Status,
		cmd.Retries,
		sequence,
		cmd.ParentID,
		cmd.Priority,
		tags,
		nullStr(cmd.UniqueKey),
		nullStr(cmd.TraceID),
		nullStr(cmd.CorrelationID),
		cmd.ReadyAt,
		cmd.StartedAt,
		cmd.CompletedAt,
		cmd.DeadlineAt,
		int64(cmd.Period),
		nullStr(cmd.Cron),
		cmd.Error,
		cmd.Attempt,
		metadata,
	)

	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rows == 0 {
		return durex.ErrNotFound
	}

	return nil
}

// Delete implements durex.Storage.
func (p *Postgres) Delete(ctx context.Context, id string) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", p.tableName)
	_, err := p.db.ExecContext(ctx, query, id)
	return err
}

// Get implements durex.Storage.
func (p *Postgres) Get(ctx context.Context, id string) (*durex.Instance, error) {
	query := fmt.Sprintf(`
		SELECT id, name, data, status, retries, sequence, parent_id, priority,
			tags, unique_key, trace_id, correlation_id, created_at, ready_at, started_at, completed_at, deadline_at,
			period_ns, cron, error, attempt, metadata
		FROM %s WHERE id = $1
	`, p.tableName)

	row := p.db.QueryRowContext(ctx, query, id)
	return p.scanInstance(row)
}

// FindPending implements durex.Storage.
func (p *Postgres) FindPending(ctx context.Context) ([]*durex.Instance, error) {
	query := fmt.Sprintf(`
		SELECT id, name, data, status, retries, sequence, parent_id, priority,
			tags, unique_key, trace_id, correlation_id, created_at, ready_at, started_at, completed_at, deadline_at,
			period_ns, cron, error, attempt, metadata
		FROM %s
		WHERE status IN ('PENDING', 'STARTED', 'REPEATING')
		AND ready_at <= $1
		ORDER BY priority DESC, ready_at ASC
	`, p.tableName)

	return p.queryInstances(ctx, query, time.Now())
}

// ClaimPending implements durex.LockingStorage.
// Atomically finds and claims pending commands using FOR UPDATE SKIP LOCKED.
// This prevents multiple executor instances from claiming the same command.
func (p *Postgres) ClaimPending(ctx context.Context, limit int) ([]*durex.Instance, error) {
	if limit <= 0 {
		limit = 100
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Select and lock rows atomically
	// FOR UPDATE SKIP LOCKED ensures we don't block on locked rows
	selectQuery := fmt.Sprintf(`
		SELECT id, name, data, status, retries, sequence, parent_id, priority,
			tags, unique_key, trace_id, correlation_id, created_at, ready_at, started_at, completed_at, deadline_at,
			period_ns, cron, error, attempt, metadata
		FROM %s
		WHERE status IN ('PENDING', 'REPEATING')
		  AND ready_at <= NOW()
		ORDER BY priority DESC, ready_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, p.tableName)

	rows, err := tx.QueryContext(ctx, selectQuery, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query pending commands: %w", err)
	}

	var instances []*durex.Instance
	var ids []string

	for rows.Next() {
		instance, err := p.scanInstanceFromTxRows(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		instances = append(instances, instance)
		ids = append(ids, instance.ID)
	}
	rows.Close()

	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(ids) == 0 {
		return nil, nil
	}

	// Update status to STARTED for all claimed commands
	// Build the IN clause with placeholders
	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}

	updateQuery := fmt.Sprintf(`
		UPDATE %s
		SET status = 'STARTED', started_at = NOW(), attempt = attempt + 1
		WHERE id IN (%s)
	`, p.tableName, strings.Join(placeholders, ", "))

	_, err = tx.ExecContext(ctx, updateQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to update command status: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Update the returned instances to reflect the changes
	now := time.Now()
	for _, inst := range instances {
		inst.Status = durex.StatusStarted
		inst.StartedAt = &now
		inst.Attempt++
	}

	return instances, nil
}

// scanInstanceFromTxRows scans a single instance from transaction rows.
func (p *Postgres) scanInstanceFromTxRows(rows *sql.Rows) (*durex.Instance, error) {
	return p.scanInstanceFromRows(rows)
}

// FindByStatus implements durex.Storage.
func (p *Postgres) FindByStatus(ctx context.Context, status durex.Status) ([]*durex.Instance, error) {
	query := fmt.Sprintf(`
		SELECT id, name, data, status, retries, sequence, parent_id, priority,
			tags, unique_key, trace_id, correlation_id, created_at, ready_at, started_at, completed_at, deadline_at,
			period_ns, cron, error, attempt, metadata
		FROM %s
		WHERE status = $1
		ORDER BY created_at DESC
	`, p.tableName)

	return p.queryInstances(ctx, query, status)
}

// FindByParent implements durex.Storage.
func (p *Postgres) FindByParent(ctx context.Context, parentID string) ([]*durex.Instance, error) {
	query := fmt.Sprintf(`
		SELECT id, name, data, status, retries, sequence, parent_id, priority,
			tags, unique_key, trace_id, correlation_id, created_at, ready_at, started_at, completed_at, deadline_at,
			period_ns, cron, error, attempt, metadata
		FROM %s
		WHERE parent_id = $1
		ORDER BY created_at ASC
	`, p.tableName)

	return p.queryInstances(ctx, query, parentID)
}

// FindByUniqueKey implements durex.Storage.
func (p *Postgres) FindByUniqueKey(ctx context.Context, key string) (*durex.Instance, error) {
	query := fmt.Sprintf(`
		SELECT id, name, data, status, retries, sequence, parent_id, priority,
			tags, unique_key, trace_id, correlation_id, created_at, ready_at, started_at, completed_at, deadline_at,
			period_ns, cron, error, attempt, metadata
		FROM %s
		WHERE unique_key = $1 AND status IN ('PENDING', 'STARTED', 'REPEATING')
		LIMIT 1
	`, p.tableName)

	row := p.db.QueryRowContext(ctx, query, key)
	return p.scanInstance(row)
}

// FindByCorrelationID returns all commands with the given correlation ID.
func (p *Postgres) FindByCorrelationID(ctx context.Context, correlationID string) ([]*durex.Instance, error) {
	query := fmt.Sprintf(`
		SELECT id, name, data, status, retries, sequence, parent_id, priority,
			tags, unique_key, trace_id, correlation_id, created_at, ready_at, started_at, completed_at, deadline_at,
			period_ns, cron, error, attempt, metadata
		FROM %s
		WHERE correlation_id = $1
		ORDER BY created_at ASC
	`, p.tableName)

	return p.queryInstances(ctx, query, correlationID)
}

// Cleanup implements durex.Storage.
func (p *Postgres) Cleanup(ctx context.Context, olderThan time.Duration) (int64, error) {
	cutoff := time.Now().Add(-olderThan)

	query := fmt.Sprintf(`
		DELETE FROM %s
		WHERE status IN ('COMPLETED', 'FAILED', 'EXPIRED', 'CANCELLED', 'DEAD_LETTER')
		AND completed_at < $1
	`, p.tableName)

	result, err := p.db.ExecContext(ctx, query, cutoff)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}

// Count implements durex.Storage.
func (p *Postgres) Count(ctx context.Context, status *durex.Status) (int64, error) {
	var query string
	var args []any

	if status != nil {
		query = fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE status = $1", p.tableName)
		args = []any{*status}
	} else {
		query = fmt.Sprintf("SELECT COUNT(*) FROM %s", p.tableName)
	}

	var count int64
	err := p.db.QueryRowContext(ctx, query, args...).Scan(&count)
	return count, err
}

// Close implements durex.Storage.
func (p *Postgres) Close() error {
	return p.db.Close()
}

// Find implements durex.QueryableStorage.
func (p *Postgres) Find(ctx context.Context, query durex.Query) ([]*durex.Instance, error) {
	var conditions []string
	var args []any
	argNum := 1

	if query.Status != nil {
		conditions = append(conditions, fmt.Sprintf("status = $%d", argNum))
		args = append(args, *query.Status)
		argNum++
	}

	if query.Name != nil {
		conditions = append(conditions, fmt.Sprintf("name = $%d", argNum))
		args = append(args, *query.Name)
		argNum++
	}

	if query.ParentID != nil {
		conditions = append(conditions, fmt.Sprintf("parent_id = $%d", argNum))
		args = append(args, *query.ParentID)
		argNum++
	}

	if len(query.Tags) > 0 {
		for _, tag := range query.Tags {
			conditions = append(conditions, fmt.Sprintf("tags @> $%d::jsonb", argNum))
			args = append(args, fmt.Sprintf("[%q]", tag))
			argNum++
		}
	}

	if query.CreatedAfter != nil {
		conditions = append(conditions, fmt.Sprintf("created_at > $%d", argNum))
		args = append(args, *query.CreatedAfter)
		argNum++
	}

	if query.CreatedBefore != nil {
		conditions = append(conditions, fmt.Sprintf("created_at < $%d", argNum))
		args = append(args, *query.CreatedBefore)
		argNum++
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	orderBy := "created_at"
	if query.OrderBy != "" {
		// Validate against allowed column names to prevent SQL injection
		allowedColumns := map[string]bool{
			"id": true, "name": true, "status": true, "priority": true,
			"created_at": true, "ready_at": true, "started_at": true,
			"completed_at": true, "attempt": true, "retries": true,
		}
		if !allowedColumns[query.OrderBy] {
			return nil, fmt.Errorf("durex: invalid order_by column %q", query.OrderBy)
		}
		orderBy = query.OrderBy
	}
	orderDir := "ASC"
	if query.OrderDesc {
		orderDir = "DESC"
	}

	limitClause := ""
	if query.Limit > 0 {
		limitClause = fmt.Sprintf("LIMIT %d", query.Limit)
	}

	offsetClause := ""
	if query.Offset > 0 {
		offsetClause = fmt.Sprintf("OFFSET %d", query.Offset)
	}

	sqlQuery := fmt.Sprintf(`
		SELECT id, name, data, status, retries, sequence, parent_id, priority,
			tags, unique_key, trace_id, correlation_id, created_at, ready_at, started_at, completed_at, deadline_at,
			period_ns, cron, error, attempt, metadata
		FROM %s
		%s
		ORDER BY %s %s
		%s %s
	`, p.tableName, whereClause, orderBy, orderDir, limitClause, offsetClause)

	return p.queryInstances(ctx, sqlQuery, args...)
}

// Begin implements durex.TransactionalStorage.
func (p *Postgres) Begin(ctx context.Context) (durex.Transaction, error) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &postgresTx{
		tx:        tx,
		tableName: p.tableName,
	}, nil
}

func (p *Postgres) queryInstances(ctx context.Context, query string, args ...any) ([]*durex.Instance, error) {
	rows, err := p.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var instances []*durex.Instance
	for rows.Next() {
		instance, err := p.scanInstanceFromRows(rows)
		if err != nil {
			return nil, err
		}
		instances = append(instances, instance)
	}

	return instances, rows.Err()
}

func (p *Postgres) scanInstance(row *sql.Row) (*durex.Instance, error) {
	var (
		cmd           durex.Instance
		data          []byte
		sequence      []byte
		tags          []byte
		metadata      []byte
		periodNs      int64
		cronExpr      sql.NullString
		parentID      sql.NullString
		uniqueKey     sql.NullString
		traceID       sql.NullString
		correlationID sql.NullString
		startedAt     sql.NullTime
		completedAt   sql.NullTime
		deadlineAt    sql.NullTime
		errMsg        sql.NullString
	)

	err := row.Scan(
		&cmd.ID,
		&cmd.Name,
		&data,
		&cmd.Status,
		&cmd.Retries,
		&sequence,
		&parentID,
		&cmd.Priority,
		&tags,
		&uniqueKey,
		&traceID,
		&correlationID,
		&cmd.CreatedAt,
		&cmd.ReadyAt,
		&startedAt,
		&completedAt,
		&deadlineAt,
		&periodNs,
		&cronExpr,
		&errMsg,
		&cmd.Attempt,
		&metadata,
	)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, durex.ErrNotFound
		}
		return nil, err
	}

	if err := json.Unmarshal(data, &cmd.Data); err != nil && len(data) > 0 {
		return nil, fmt.Errorf("failed to unmarshal data: %w", err)
	}

	if err := json.Unmarshal(sequence, &cmd.Sequence); err != nil && len(sequence) > 0 {
		return nil, fmt.Errorf("failed to unmarshal sequence: %w", err)
	}

	if err := json.Unmarshal(tags, &cmd.Tags); err != nil && len(tags) > 0 {
		return nil, fmt.Errorf("failed to unmarshal tags: %w", err)
	}

	if err := json.Unmarshal(metadata, &cmd.Metadata); err != nil && len(metadata) > 0 {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	if parentID.Valid {
		cmd.ParentID = &parentID.String
	}
	if uniqueKey.Valid {
		cmd.UniqueKey = uniqueKey.String
	}
	if traceID.Valid {
		cmd.TraceID = traceID.String
	}
	if correlationID.Valid {
		cmd.CorrelationID = correlationID.String
	}
	if startedAt.Valid {
		cmd.StartedAt = &startedAt.Time
	}
	if completedAt.Valid {
		cmd.CompletedAt = &completedAt.Time
	}
	if deadlineAt.Valid {
		cmd.DeadlineAt = &deadlineAt.Time
	}
	if errMsg.Valid {
		cmd.Error = errMsg.String
	}
	if cronExpr.Valid {
		cmd.Cron = cronExpr.String
	}

	cmd.Period = time.Duration(periodNs)

	return &cmd, nil
}

func (p *Postgres) scanInstanceFromRows(rows *sql.Rows) (*durex.Instance, error) {
	var (
		cmd           durex.Instance
		data          []byte
		sequence      []byte
		tags          []byte
		metadata      []byte
		periodNs      int64
		cronExpr      sql.NullString
		parentID      sql.NullString
		uniqueKey     sql.NullString
		traceID       sql.NullString
		correlationID sql.NullString
		startedAt     sql.NullTime
		completedAt   sql.NullTime
		deadlineAt    sql.NullTime
		errMsg        sql.NullString
	)

	err := rows.Scan(
		&cmd.ID,
		&cmd.Name,
		&data,
		&cmd.Status,
		&cmd.Retries,
		&sequence,
		&parentID,
		&cmd.Priority,
		&tags,
		&uniqueKey,
		&traceID,
		&correlationID,
		&cmd.CreatedAt,
		&cmd.ReadyAt,
		&startedAt,
		&completedAt,
		&deadlineAt,
		&periodNs,
		&cronExpr,
		&errMsg,
		&cmd.Attempt,
		&metadata,
	)

	if err != nil {
		return nil, err
	}

	if len(data) > 0 {
		if err := json.Unmarshal(data, &cmd.Data); err != nil {
			return nil, fmt.Errorf("failed to unmarshal data: %w", err)
		}
	}

	if len(sequence) > 0 {
		if err := json.Unmarshal(sequence, &cmd.Sequence); err != nil {
			return nil, fmt.Errorf("failed to unmarshal sequence: %w", err)
		}
	}

	if len(tags) > 0 {
		if err := json.Unmarshal(tags, &cmd.Tags); err != nil {
			return nil, fmt.Errorf("failed to unmarshal tags: %w", err)
		}
	}

	if len(metadata) > 0 {
		if err := json.Unmarshal(metadata, &cmd.Metadata); err != nil {
			return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
		}
	}

	if parentID.Valid {
		cmd.ParentID = &parentID.String
	}
	if uniqueKey.Valid {
		cmd.UniqueKey = uniqueKey.String
	}
	if traceID.Valid {
		cmd.TraceID = traceID.String
	}
	if correlationID.Valid {
		cmd.CorrelationID = correlationID.String
	}
	if startedAt.Valid {
		cmd.StartedAt = &startedAt.Time
	}
	if completedAt.Valid {
		cmd.CompletedAt = &completedAt.Time
	}
	if deadlineAt.Valid {
		cmd.DeadlineAt = &deadlineAt.Time
	}
	if errMsg.Valid {
		cmd.Error = errMsg.String
	}
	if cronExpr.Valid {
		cmd.Cron = cronExpr.String
	}

	cmd.Period = time.Duration(periodNs)

	return &cmd, nil
}

// postgresTx wraps a sql.Tx to implement durex.Transaction.
type postgresTx struct {
	tx        *sql.Tx
	tableName string
}

// Commit implements durex.Transaction.
func (t *postgresTx) Commit() error {
	return t.tx.Commit()
}

// Rollback implements durex.Transaction.
func (t *postgresTx) Rollback() error {
	return t.tx.Rollback()
}

// Create implements durex.Storage.
func (t *postgresTx) Create(ctx context.Context, cmd *durex.Instance) error {
	data, err := json.Marshal(cmd.Data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}
	sequence, err := json.Marshal(cmd.Sequence)
	if err != nil {
		return fmt.Errorf("failed to marshal sequence: %w", err)
	}
	tags, err := json.Marshal(cmd.Tags)
	if err != nil {
		return fmt.Errorf("failed to marshal tags: %w", err)
	}
	metadata, err := json.Marshal(cmd.Metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	query := fmt.Sprintf(`
		INSERT INTO %s (
			id, name, data, status, retries, sequence, parent_id, priority,
			tags, unique_key, trace_id, correlation_id, created_at, ready_at, started_at, completed_at, deadline_at,
			period_ns, cron, error, attempt, metadata
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22
		)
	`, t.tableName)

	_, err = t.tx.ExecContext(ctx, query,
		cmd.ID, cmd.Name, data, cmd.Status, cmd.Retries, sequence,
		cmd.ParentID, cmd.Priority, tags, nullStr(cmd.UniqueKey),
		nullStr(cmd.TraceID), nullStr(cmd.CorrelationID),
		cmd.CreatedAt, cmd.ReadyAt,
		cmd.StartedAt, cmd.CompletedAt, cmd.DeadlineAt,
		int64(cmd.Period), nullStr(cmd.Cron), cmd.Error, cmd.Attempt, metadata,
	)
	return err
}

// Update implements durex.Storage.
func (t *postgresTx) Update(ctx context.Context, cmd *durex.Instance) error {
	data, err := json.Marshal(cmd.Data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}
	sequence, err := json.Marshal(cmd.Sequence)
	if err != nil {
		return fmt.Errorf("failed to marshal sequence: %w", err)
	}
	tags, err := json.Marshal(cmd.Tags)
	if err != nil {
		return fmt.Errorf("failed to marshal tags: %w", err)
	}
	metadata, err := json.Marshal(cmd.Metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	query := fmt.Sprintf(`
		UPDATE %s SET
			name = $2, data = $3, status = $4, retries = $5, sequence = $6,
			parent_id = $7, priority = $8, tags = $9, unique_key = $10,
			trace_id = $11, correlation_id = $12, ready_at = $13,
			started_at = $14, completed_at = $15, deadline_at = $16,
			period_ns = $17, cron = $18, error = $19, attempt = $20, metadata = $21
		WHERE id = $1
	`, t.tableName)

	_, err = t.tx.ExecContext(ctx, query,
		cmd.ID, cmd.Name, data, cmd.Status, cmd.Retries, sequence,
		cmd.ParentID, cmd.Priority, tags, nullStr(cmd.UniqueKey),
		nullStr(cmd.TraceID), nullStr(cmd.CorrelationID), cmd.ReadyAt,
		cmd.StartedAt, cmd.CompletedAt, cmd.DeadlineAt,
		int64(cmd.Period), nullStr(cmd.Cron), cmd.Error, cmd.Attempt, metadata,
	)
	return err
}

// Delete implements durex.Storage.
func (t *postgresTx) Delete(ctx context.Context, id string) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", t.tableName)
	_, err := t.tx.ExecContext(ctx, query, id)
	return err
}

// Get implements durex.Storage.
func (t *postgresTx) Get(ctx context.Context, id string) (*durex.Instance, error) {
	// Delegate to parent implementation logic - simplified for transaction
	return nil, errors.New("Get not supported in transaction context")
}

// FindPending implements durex.Storage.
func (t *postgresTx) FindPending(ctx context.Context) ([]*durex.Instance, error) {
	return nil, errors.New("FindPending not supported in transaction context")
}

// FindByStatus implements durex.Storage.
func (t *postgresTx) FindByStatus(ctx context.Context, status durex.Status) ([]*durex.Instance, error) {
	return nil, errors.New("FindByStatus not supported in transaction context")
}

// FindByParent implements durex.Storage.
func (t *postgresTx) FindByParent(ctx context.Context, parentID string) ([]*durex.Instance, error) {
	return nil, errors.New("FindByParent not supported in transaction context")
}

// FindByUniqueKey implements durex.Storage.
func (t *postgresTx) FindByUniqueKey(ctx context.Context, key string) (*durex.Instance, error) {
	return nil, errors.New("FindByUniqueKey not supported in transaction context")
}

// Cleanup implements durex.Storage.
func (t *postgresTx) Cleanup(ctx context.Context, olderThan time.Duration) (int64, error) {
	return 0, errors.New("Cleanup not supported in transaction context")
}

// Count implements durex.Storage.
func (t *postgresTx) Count(ctx context.Context, status *durex.Status) (int64, error) {
	return 0, errors.New("Count not supported in transaction context")
}

// Close implements durex.Storage.
func (t *postgresTx) Close() error {
	return nil
}

// nullStr returns nil for empty strings, otherwise the string.
func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// Ensure Postgres implements the interfaces.
var (
	_ durex.Storage              = (*Postgres)(nil)
	_ durex.QueryableStorage     = (*Postgres)(nil)
	_ durex.TransactionalStorage = (*Postgres)(nil)
	_ durex.LockingStorage       = (*Postgres)(nil)
)
