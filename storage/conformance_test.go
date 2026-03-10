// Package storage_test provides conformance tests for storage backends.
// All storage implementations should pass these tests to ensure consistent behavior.
package storage_test

import (
	"testing"

	"github.com/simonovic86/durex"
	"github.com/simonovic86/durex/storage"
	"github.com/simonovic86/durex/storagetest"
)

// TestMemoryConformance runs conformance tests on Memory storage.
func TestMemoryConformance(t *testing.T) {
	storagetest.RunConformanceTests(t, func(t *testing.T) durex.Storage {
		return storage.NewMemory()
	})
}
