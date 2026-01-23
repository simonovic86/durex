package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/simonovic86/durex"
)

// SQLite is a SQLite storage implementation.
// Good for single-instance deployments and development.
type SQLite struct {
	db        *sql.DB
	tableName string
}

// SQLiteOption configures the SQLite storage.
type SQLiteOption func(*SQLite)

// WithSQLiteTableName sets the table name for command storage.
func WithSQLiteTableName(name string) SQLiteOption {
	return func(s *SQLite) {
		s.tableName = name
	}
}

// NewSQLite creates a new SQLite storage.
// The db connection should already be opened.
func NewSQLite(db *sql.DB, opts ...SQLiteOption) *SQLite {
	s := &SQLite{
		db:        db,
		tableName: "durex_commands",
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// OpenSQLite opens a SQLite database and returns a storage.
// Use ":memory:" for in-memory database.
func OpenSQLite(dsn string, opts ...SQLiteOption) (*SQLite, error) {
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite: %w", err)
	}

	// Enable WAL mode for better concurrency
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to enable WAL mode: %w", err)
	}

	return NewSQLite(db, opts...), nil
}

// Migrate creates the commands table if it doesn't exist.
func (s *SQLite) Migrate(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			data TEXT,
			status TEXT NOT NULL DEFAULT 'PENDING',
			retries INTEGER NOT NULL DEFAULT 0,
			sequence TEXT,
			parent_id TEXT,
			priority INTEGER NOT NULL DEFAULT 0,
			tags TEXT,
			unique_key TEXT,
			trace_id TEXT,
			correlation_id TEXT,
			created_at TEXT NOT NULL,
			ready_at TEXT NOT NULL,
			started_at TEXT,
			completed_at TEXT,
			deadline_at TEXT,
			period_ns INTEGER NOT NULL DEFAULT 0,
			error TEXT,
			attempt INTEGER NOT NULL DEFAULT 0,
			metadata TEXT,
			FOREIGN KEY (parent_id) REFERENCES %s(id) ON DELETE SET NULL
		);

		CREATE INDEX IF NOT EXISTS idx_%s_status ON %s(status);
		CREATE INDEX IF NOT EXISTS idx_%s_ready_at ON %s(ready_at);
		CREATE INDEX IF NOT EXISTS idx_%s_name ON %s(name);
		CREATE INDEX IF NOT EXISTS idx_%s_parent_id ON %s(parent_id);
		CREATE INDEX IF NOT EXISTS idx_%s_unique_key ON %s(unique_key);
		CREATE INDEX IF NOT EXISTS idx_%s_correlation_id ON %s(correlation_id);
	`, s.tableName, s.tableName,
		s.tableName, s.tableName,
		s.tableName, s.tableName,
		s.tableName, s.tableName,
		s.tableName, s.tableName,
		s.tableName, s.tableName,
		s.tableName, s.tableName)

	_, err := s.db.ExecContext(ctx, query)
	return err
}

// Create implements durex.Storage.
func (s *SQLite) Create(ctx context.Context, cmd *durex.Instance) error {
	data, _ := json.Marshal(cmd.Data)
	sequence, _ := json.Marshal(cmd.Sequence)
	tags, _ := json.Marshal(cmd.Tags)
	metadata, _ := json.Marshal(cmd.Metadata)

	query := fmt.Sprintf(`
		INSERT INTO %s (
			id, name, data, status, retries, sequence, parent_id, priority,
			tags, unique_key, trace_id, correlation_id, created_at, ready_at, started_at, completed_at, deadline_at,
			period_ns, error, attempt, metadata
		) VALUES (
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
		)
	`, s.tableName)

	_, err := s.db.ExecContext(ctx, query,
		cmd.ID,
		cmd.Name,
		string(data),
		cmd.Status,
		cmd.Retries,
		string(sequence),
		cmd.ParentID,
		cmd.Priority,
		string(tags),
		nullStr(cmd.UniqueKey),
		nullStr(cmd.TraceID),
		nullStr(cmd.CorrelationID),
		cmd.CreatedAt.Format(time.RFC3339Nano),
		cmd.ReadyAt.Format(time.RFC3339Nano),
		nullTimeStr(cmd.StartedAt),
		nullTimeStr(cmd.CompletedAt),
		nullTimeStr(cmd.DeadlineAt),
		int64(cmd.Period),
		nullStr(cmd.Error),
		cmd.Attempt,
		string(metadata),
	)

	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint") {
			return durex.ErrAlreadyExists
		}
		return err
	}

	return nil
}

// Update implements durex.Storage.
func (s *SQLite) Update(ctx context.Context, cmd *durex.Instance) error {
	data, _ := json.Marshal(cmd.Data)
	sequence, _ := json.Marshal(cmd.Sequence)
	tags, _ := json.Marshal(cmd.Tags)
	metadata, _ := json.Marshal(cmd.Metadata)

	query := fmt.Sprintf(`
		UPDATE %s SET
			name = ?,
			data = ?,
			status = ?,
			retries = ?,
			sequence = ?,
			parent_id = ?,
			priority = ?,
			tags = ?,
			unique_key = ?,
			trace_id = ?,
			correlation_id = ?,
			ready_at = ?,
			started_at = ?,
			completed_at = ?,
			deadline_at = ?,
			period_ns = ?,
			error = ?,
			attempt = ?,
			metadata = ?
		WHERE id = ?
	`, s.tableName)

	result, err := s.db.ExecContext(ctx, query,
		cmd.Name,
		string(data),
		cmd.Status,
		cmd.Retries,
		string(sequence),
		cmd.ParentID,
		cmd.Priority,
		string(tags),
		nullStr(cmd.UniqueKey),
		nullStr(cmd.TraceID),
		nullStr(cmd.CorrelationID),
		cmd.ReadyAt.Format(time.RFC3339Nano),
		nullTimeStr(cmd.StartedAt),
		nullTimeStr(cmd.CompletedAt),
		nullTimeStr(cmd.DeadlineAt),
		int64(cmd.Period),
		nullStr(cmd.Error),
		cmd.Attempt,
		string(metadata),
		cmd.ID,
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
func (s *SQLite) Delete(ctx context.Context, id string) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE id = ?", s.tableName)
	_, err := s.db.ExecContext(ctx, query, id)
	return err
}

// Get implements durex.Storage.
func (s *SQLite) Get(ctx context.Context, id string) (*durex.Instance, error) {
	query := fmt.Sprintf(`
		SELECT id, name, data, status, retries, sequence, parent_id, priority,
			tags, unique_key, trace_id, correlation_id, created_at, ready_at, started_at, completed_at, deadline_at,
			period_ns, error, attempt, metadata
		FROM %s WHERE id = ?
	`, s.tableName)

	row := s.db.QueryRowContext(ctx, query, id)
	return s.scanInstance(row)
}

// FindPending implements durex.Storage.
func (s *SQLite) FindPending(ctx context.Context) ([]*durex.Instance, error) {
	query := fmt.Sprintf(`
		SELECT id, name, data, status, retries, sequence, parent_id, priority,
			tags, unique_key, trace_id, correlation_id, created_at, ready_at, started_at, completed_at, deadline_at,
			period_ns, error, attempt, metadata
		FROM %s
		WHERE status IN ('PENDING', 'STARTED', 'REPEATING')
		ORDER BY priority DESC, ready_at ASC
	`, s.tableName)

	return s.queryInstances(ctx, query)
}

// FindByStatus implements durex.Storage.
func (s *SQLite) FindByStatus(ctx context.Context, status durex.Status) ([]*durex.Instance, error) {
	query := fmt.Sprintf(`
		SELECT id, name, data, status, retries, sequence, parent_id, priority,
			tags, unique_key, trace_id, correlation_id, created_at, ready_at, started_at, completed_at, deadline_at,
			period_ns, error, attempt, metadata
		FROM %s
		WHERE status = ?
		ORDER BY created_at DESC
	`, s.tableName)

	return s.queryInstances(ctx, query, status)
}

// FindByParent implements durex.Storage.
func (s *SQLite) FindByParent(ctx context.Context, parentID string) ([]*durex.Instance, error) {
	query := fmt.Sprintf(`
		SELECT id, name, data, status, retries, sequence, parent_id, priority,
			tags, unique_key, trace_id, correlation_id, created_at, ready_at, started_at, completed_at, deadline_at,
			period_ns, error, attempt, metadata
		FROM %s
		WHERE parent_id = ?
		ORDER BY created_at ASC
	`, s.tableName)

	return s.queryInstances(ctx, query, parentID)
}

// FindByUniqueKey implements durex.Storage.
func (s *SQLite) FindByUniqueKey(ctx context.Context, key string) (*durex.Instance, error) {
	query := fmt.Sprintf(`
		SELECT id, name, data, status, retries, sequence, parent_id, priority,
			tags, unique_key, trace_id, correlation_id, created_at, ready_at, started_at, completed_at, deadline_at,
			period_ns, error, attempt, metadata
		FROM %s
		WHERE unique_key = ? AND status IN ('PENDING', 'STARTED', 'REPEATING')
		LIMIT 1
	`, s.tableName)

	row := s.db.QueryRowContext(ctx, query, key)
	return s.scanInstance(row)
}

// FindByCorrelationID returns all commands with the given correlation ID.
func (s *SQLite) FindByCorrelationID(ctx context.Context, correlationID string) ([]*durex.Instance, error) {
	query := fmt.Sprintf(`
		SELECT id, name, data, status, retries, sequence, parent_id, priority,
			tags, unique_key, trace_id, correlation_id, created_at, ready_at, started_at, completed_at, deadline_at,
			period_ns, error, attempt, metadata
		FROM %s
		WHERE correlation_id = ?
		ORDER BY created_at ASC
	`, s.tableName)

	return s.queryInstances(ctx, query, correlationID)
}

// Cleanup implements durex.Storage.
func (s *SQLite) Cleanup(ctx context.Context, olderThan time.Duration) (int64, error) {
	cutoff := time.Now().Add(-olderThan).Format(time.RFC3339Nano)

	query := fmt.Sprintf(`
		DELETE FROM %s
		WHERE status IN ('COMPLETED', 'FAILED', 'EXPIRED', 'CANCELLED')
		AND completed_at < ?
	`, s.tableName)

	result, err := s.db.ExecContext(ctx, query, cutoff)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}

// Count implements durex.Storage.
func (s *SQLite) Count(ctx context.Context, status *durex.Status) (int64, error) {
	var query string
	var args []any

	if status != nil {
		query = fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE status = ?", s.tableName)
		args = []any{*status}
	} else {
		query = fmt.Sprintf("SELECT COUNT(*) FROM %s", s.tableName)
	}

	var count int64
	err := s.db.QueryRowContext(ctx, query, args...).Scan(&count)
	return count, err
}

// Close implements durex.Storage.
func (s *SQLite) Close() error {
	return s.db.Close()
}

func (s *SQLite) queryInstances(ctx context.Context, query string, args ...any) ([]*durex.Instance, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var instances []*durex.Instance
	for rows.Next() {
		instance, err := s.scanInstanceFromRows(rows)
		if err != nil {
			return nil, err
		}
		instances = append(instances, instance)
	}

	return instances, rows.Err()
}

func (s *SQLite) scanInstance(row *sql.Row) (*durex.Instance, error) {
	var (
		cmd           durex.Instance
		data          sql.NullString
		sequence      sql.NullString
		tags          sql.NullString
		metadata      sql.NullString
		periodNs      int64
		parentID      sql.NullString
		uniqueKey     sql.NullString
		traceID       sql.NullString
		correlationID sql.NullString
		createdAt     string
		readyAt       string
		startedAt     sql.NullString
		completedAt   sql.NullString
		deadlineAt    sql.NullString
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
		&createdAt,
		&readyAt,
		&startedAt,
		&completedAt,
		&deadlineAt,
		&periodNs,
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

	return s.populateInstance(&cmd, data, sequence, tags, metadata, periodNs,
		parentID, uniqueKey, traceID, correlationID, createdAt, readyAt, startedAt, completedAt, deadlineAt, errMsg)
}

func (s *SQLite) scanInstanceFromRows(rows *sql.Rows) (*durex.Instance, error) {
	var (
		cmd           durex.Instance
		data          sql.NullString
		sequence      sql.NullString
		tags          sql.NullString
		metadata      sql.NullString
		periodNs      int64
		parentID      sql.NullString
		uniqueKey     sql.NullString
		traceID       sql.NullString
		correlationID sql.NullString
		createdAt     string
		readyAt       string
		startedAt     sql.NullString
		completedAt   sql.NullString
		deadlineAt    sql.NullString
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
		&createdAt,
		&readyAt,
		&startedAt,
		&completedAt,
		&deadlineAt,
		&periodNs,
		&errMsg,
		&cmd.Attempt,
		&metadata,
	)

	if err != nil {
		return nil, err
	}

	return s.populateInstance(&cmd, data, sequence, tags, metadata, periodNs,
		parentID, uniqueKey, traceID, correlationID, createdAt, readyAt, startedAt, completedAt, deadlineAt, errMsg)
}

func (s *SQLite) populateInstance(
	cmd *durex.Instance,
	data, sequence, tags, metadata sql.NullString,
	periodNs int64,
	parentID, uniqueKey, traceID, correlationID sql.NullString,
	createdAt, readyAt string,
	startedAt, completedAt, deadlineAt, errMsg sql.NullString,
) (*durex.Instance, error) {
	if data.Valid && data.String != "" {
		if err := json.Unmarshal([]byte(data.String), &cmd.Data); err != nil {
			return nil, fmt.Errorf("failed to unmarshal data: %w", err)
		}
	}

	if sequence.Valid && sequence.String != "" {
		if err := json.Unmarshal([]byte(sequence.String), &cmd.Sequence); err != nil {
			return nil, fmt.Errorf("failed to unmarshal sequence: %w", err)
		}
	}

	if tags.Valid && tags.String != "" {
		if err := json.Unmarshal([]byte(tags.String), &cmd.Tags); err != nil {
			return nil, fmt.Errorf("failed to unmarshal tags: %w", err)
		}
	}

	if metadata.Valid && metadata.String != "" {
		if err := json.Unmarshal([]byte(metadata.String), &cmd.Metadata); err != nil {
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

	if t, err := time.Parse(time.RFC3339Nano, createdAt); err == nil {
		cmd.CreatedAt = t
	}

	if t, err := time.Parse(time.RFC3339Nano, readyAt); err == nil {
		cmd.ReadyAt = t
	}

	if startedAt.Valid {
		if t, err := time.Parse(time.RFC3339Nano, startedAt.String); err == nil {
			cmd.StartedAt = &t
		}
	}

	if completedAt.Valid {
		if t, err := time.Parse(time.RFC3339Nano, completedAt.String); err == nil {
			cmd.CompletedAt = &t
		}
	}

	if deadlineAt.Valid {
		if t, err := time.Parse(time.RFC3339Nano, deadlineAt.String); err == nil {
			cmd.DeadlineAt = &t
		}
	}

	if errMsg.Valid {
		cmd.Error = errMsg.String
	}

	cmd.Period = time.Duration(periodNs)

	return cmd, nil
}

func nullTimeStr(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.Format(time.RFC3339Nano)
}

// Ensure SQLite implements the interface.
var _ durex.Storage = (*SQLite)(nil)
