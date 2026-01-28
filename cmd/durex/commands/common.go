package commands

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/simonovic86/durex"
	"github.com/simonovic86/durex/storage"

	_ "github.com/mattn/go-sqlite3"
	_ "github.com/lib/pq"
)

// ConnectStorage creates a storage backend based on flags
func ConnectStorage(dbType, dbPath string) (durex.Storage, error) {
	if dbPath == "" {
		return nil, fmt.Errorf("--db flag is required")
	}

	switch dbType {
	case "sqlite", "sqlite3":
		// Check if file exists
		if _, err := os.Stat(dbPath); os.IsNotExist(err) {
			return nil, fmt.Errorf("database file does not exist: %s", dbPath)
		}
		
		db, err := sql.Open("sqlite3", dbPath)
		if err != nil {
			return nil, fmt.Errorf("failed to open sqlite database: %w", err)
		}
		
		return storage.NewSQLite(db), nil

	case "postgres", "postgresql":
		db, err := sql.Open("postgres", dbPath)
		if err != nil {
			return nil, fmt.Errorf("failed to open postgres database: %w", err)
		}
		
		if err := db.Ping(); err != nil {
			return nil, fmt.Errorf("failed to connect to postgres: %w", err)
		}
		
		return storage.NewPostgres(db), nil

	default:
		return nil, fmt.Errorf("unsupported database type: %s (use sqlite or postgres)", dbType)
	}
}

// FormatDuration formats a duration in a human-readable way
func FormatDuration(ms int64) string {
	if ms < 1000 {
		return fmt.Sprintf("%dms", ms)
	}
	seconds := ms / 1000
	if seconds < 60 {
		return fmt.Sprintf("%ds", seconds)
	}
	minutes := seconds / 60
	if minutes < 60 {
		return fmt.Sprintf("%dm%ds", minutes, seconds%60)
	}
	hours := minutes / 60
	return fmt.Sprintf("%dh%dm", hours, minutes%60)
}

// TruncateString truncates a string to maxLen characters
func TruncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}
