package commands_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"github.com/simonovic86/durex"
	"github.com/simonovic86/durex/storage"
	"github.com/simonovic86/durex/storagetest"
)

// TestSQLiteConformance runs conformance tests on SQLite storage.
func TestSQLiteConformance(t *testing.T) {
	storagetest.RunConformanceTests(t, func(t *testing.T) durex.Storage {
		s, err := storage.OpenSQLite(":memory:")
		if err != nil {
			t.Fatalf("Failed to open SQLite: %v", err)
		}
		if err := s.Migrate(context.Background()); err != nil {
			t.Fatalf("Failed to migrate SQLite: %v", err)
		}
		return s
	})
}

// TestPostgresConformance runs conformance tests on PostgreSQL storage.
// Requires a running PostgreSQL instance.
// Set DUREX_TEST_POSTGRES_DSN to the connection string, e.g.:
//
//	export DUREX_TEST_POSTGRES_DSN="postgres://localhost:5432/durex_test?sslmode=disable"
func TestPostgresConformance(t *testing.T) {
	dsn := os.Getenv("DUREX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("DUREX_TEST_POSTGRES_DSN not set; skipping PostgreSQL conformance tests")
	}

	storagetest.RunConformanceTests(t, func(t *testing.T) durex.Storage {
		db, err := sql.Open("postgres", dsn)
		if err != nil {
			t.Fatalf("Failed to open Postgres: %v", err)
		}
		if err := db.Ping(); err != nil {
			t.Fatalf("Failed to ping Postgres: %v", err)
		}

		// Use a unique table name per sub-test to avoid conflicts.
		// Sanitize test name to valid SQL identifier.
		sanitized := ""
		for _, c := range t.Name() {
			if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
				sanitized += string(c)
			} else {
				sanitized += "_"
			}
		}
		tableName := fmt.Sprintf("durex_test_%s", sanitized)
		if len(tableName) > 63 { // PostgreSQL identifier limit
			tableName = tableName[:63]
		}

		s := storage.NewPostgres(db, storage.WithTableName(tableName))
		if err := s.Migrate(context.Background()); err != nil {
			t.Fatalf("Failed to migrate Postgres: %v", err)
		}

		t.Cleanup(func() {
			_, _ = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
			db.Close()
		})

		return s
	})
}
