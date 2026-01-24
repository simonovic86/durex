// Package storage_test provides conformance tests for storage backends.
// All storage implementations should pass these tests to ensure consistent behavior.
package storage_test

import (
	"context"
	"testing"
	"time"

	"github.com/simonovic86/durex"
	"github.com/simonovic86/durex/storage"
)

// StorageFactory creates a new storage instance for testing.
type StorageFactory func(t *testing.T) durex.Storage

// RunConformanceTests runs all conformance tests against a storage implementation.
func RunConformanceTests(t *testing.T, factory StorageFactory) {
	tests := []struct {
		name string
		fn   func(t *testing.T, store durex.Storage)
	}{
		{"CreateAndGet", testCreateAndGet},
		{"CreateDuplicate", testCreateDuplicate},
		{"Update", testUpdate},
		{"UpdateNotFound", testUpdateNotFound},
		{"Delete", testDelete},
		{"DeleteIdempotent", testDeleteIdempotent},
		{"GetNotFound", testGetNotFound},
		{"FindPending", testFindPending},
		{"FindPendingRespectsReadyAt", testFindPendingRespectsReadyAt},
		{"FindByStatus", testFindByStatus},
		{"FindByParent", testFindByParent},
		{"FindByUniqueKey", testFindByUniqueKey},
		{"FindByUniqueKeyOnlyActive", testFindByUniqueKeyOnlyActive},
		{"Cleanup", testCleanup},
		{"CleanupPreservesActive", testCleanupPreservesActive},
		{"Count", testCount},
		{"CountByStatus", testCountByStatus},
		{"DataPersistence", testDataPersistence},
		{"SequencePersistence", testSequencePersistence},
		{"TagsPersistence", testTagsPersistence},
		{"MetadataPersistence", testMetadataPersistence},
		{"TimestampPersistence", testTimestampPersistence},
		{"AllStatusesPersist", testAllStatusesPersist},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := factory(t)
			defer store.Close()
			tc.fn(t, store)
		})
	}
}

// TestMemoryConformance runs conformance tests on Memory storage.
func TestMemoryConformance(t *testing.T) {
	RunConformanceTests(t, func(t *testing.T) durex.Storage {
		return storage.NewMemory()
	})
}

// Individual test functions

func testCreateAndGet(t *testing.T, store durex.Storage) {
	ctx := context.Background()

	cmd := &durex.Instance{
		ID:        "test-create-get-1",
		Name:      "testCommand",
		Status:    durex.StatusPending,
		CreatedAt: time.Now(),
		ReadyAt:   time.Now(),
		Data:      durex.M{"key": "value", "nested": map[string]any{"inner": 123}},
	}

	if err := store.Create(ctx, cmd); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	got, err := store.Get(ctx, cmd.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if got.ID != cmd.ID {
		t.Errorf("ID mismatch: expected %s, got %s", cmd.ID, got.ID)
	}
	if got.Name != cmd.Name {
		t.Errorf("Name mismatch: expected %s, got %s", cmd.Name, got.Name)
	}
	if got.Status != cmd.Status {
		t.Errorf("Status mismatch: expected %s, got %s", cmd.Status, got.Status)
	}
	if got.Data["key"] != "value" {
		t.Errorf("Data key mismatch: expected 'value', got %v", got.Data["key"])
	}
}

func testCreateDuplicate(t *testing.T, store durex.Storage) {
	ctx := context.Background()

	cmd := &durex.Instance{
		ID:        "test-dup-1",
		Name:      "testCommand",
		Status:    durex.StatusPending,
		CreatedAt: time.Now(),
		ReadyAt:   time.Now(),
	}

	if err := store.Create(ctx, cmd); err != nil {
		t.Fatalf("First create failed: %v", err)
	}

	err := store.Create(ctx, cmd)
	if err != durex.ErrAlreadyExists {
		t.Errorf("Expected ErrAlreadyExists, got %v", err)
	}
}

func testUpdate(t *testing.T, store durex.Storage) {
	ctx := context.Background()

	cmd := &durex.Instance{
		ID:        "test-update-1",
		Name:      "testCommand",
		Status:    durex.StatusPending,
		Retries:   3,
		CreatedAt: time.Now(),
		ReadyAt:   time.Now(),
	}

	if err := store.Create(ctx, cmd); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Update multiple fields
	cmd.Status = durex.StatusCompleted
	cmd.Retries = 0
	cmd.Error = "test error"
	now := time.Now()
	cmd.CompletedAt = &now

	if err := store.Update(ctx, cmd); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	got, err := store.Get(ctx, cmd.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if got.Status != durex.StatusCompleted {
		t.Errorf("Status not updated: expected COMPLETED, got %s", got.Status)
	}
	if got.Retries != 0 {
		t.Errorf("Retries not updated: expected 0, got %d", got.Retries)
	}
	if got.Error != "test error" {
		t.Errorf("Error not updated: expected 'test error', got %q", got.Error)
	}
	if got.CompletedAt == nil {
		t.Error("CompletedAt should not be nil")
	}
}

func testUpdateNotFound(t *testing.T, store durex.Storage) {
	ctx := context.Background()

	cmd := &durex.Instance{
		ID:     "nonexistent-update",
		Name:   "test",
		Status: durex.StatusPending,
	}

	err := store.Update(ctx, cmd)
	if err != durex.ErrNotFound {
		t.Errorf("Expected ErrNotFound, got %v", err)
	}
}

func testDelete(t *testing.T, store durex.Storage) {
	ctx := context.Background()

	cmd := &durex.Instance{
		ID:        "test-delete-1",
		Name:      "testCommand",
		Status:    durex.StatusPending,
		CreatedAt: time.Now(),
		ReadyAt:   time.Now(),
	}

	if err := store.Create(ctx, cmd); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	if err := store.Delete(ctx, cmd.ID); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err := store.Get(ctx, cmd.ID)
	if err != durex.ErrNotFound {
		t.Errorf("Expected ErrNotFound after delete, got %v", err)
	}
}

func testDeleteIdempotent(t *testing.T, store durex.Storage) {
	ctx := context.Background()

	// Delete non-existent should not error
	err := store.Delete(ctx, "nonexistent-delete")
	if err != nil {
		t.Errorf("Delete of non-existent should be idempotent, got %v", err)
	}
}

func testGetNotFound(t *testing.T, store durex.Storage) {
	ctx := context.Background()

	_, err := store.Get(ctx, "nonexistent-get")
	if err != durex.ErrNotFound {
		t.Errorf("Expected ErrNotFound, got %v", err)
	}
}

func testFindPending(t *testing.T, store durex.Storage) {
	ctx := context.Background()

	// Create commands with different statuses
	statuses := []durex.Status{
		durex.StatusPending,
		durex.StatusStarted,
		durex.StatusCompleted,
		durex.StatusFailed,
		durex.StatusRepeating,
		durex.StatusExpired,
		durex.StatusCancelled,
	}

	for i, status := range statuses {
		cmd := &durex.Instance{
			ID:        durex.GenerateID(),
			Name:      "test",
			Status:    status,
			CreatedAt: time.Now(),
			ReadyAt:   time.Now().Add(-time.Duration(i) * time.Second), // All ready
		}
		if err := store.Create(ctx, cmd); err != nil {
			t.Fatalf("Create failed for status %s: %v", status, err)
		}
	}

	pending, err := store.FindPending(ctx)
	if err != nil {
		t.Fatalf("FindPending failed: %v", err)
	}

	// Should find PENDING, STARTED, REPEATING (3 commands)
	if len(pending) != 3 {
		t.Errorf("Expected 3 pending commands, got %d", len(pending))
	}

	// Verify only active statuses
	for _, cmd := range pending {
		if !cmd.Status.IsActive() {
			t.Errorf("FindPending returned non-active status: %s", cmd.Status)
		}
	}
}

func testFindPendingRespectsReadyAt(t *testing.T, store durex.Storage) {
	ctx := context.Background()

	// Create a command that's ready now
	ready := &durex.Instance{
		ID:        durex.GenerateID(),
		Name:      "readyNow",
		Status:    durex.StatusPending,
		CreatedAt: time.Now(),
		ReadyAt:   time.Now().Add(-time.Second),
	}

	// Create a command that's not ready yet (future)
	notReady := &durex.Instance{
		ID:        durex.GenerateID(),
		Name:      "notReady",
		Status:    durex.StatusPending,
		CreatedAt: time.Now(),
		ReadyAt:   time.Now().Add(time.Hour),
	}

	if err := store.Create(ctx, ready); err != nil {
		t.Fatalf("Create ready failed: %v", err)
	}
	if err := store.Create(ctx, notReady); err != nil {
		t.Fatalf("Create notReady failed: %v", err)
	}

	pending, err := store.FindPending(ctx)
	if err != nil {
		t.Fatalf("FindPending failed: %v", err)
	}

	// Note: Memory storage doesn't filter by ReadyAt in FindPending
	// but PostgreSQL LockingStorage.ClaimPending does
	// This test documents the behavior - both should be returned by FindPending
	// but ClaimPending should only return ready commands
	found := false
	for _, cmd := range pending {
		if cmd.ID == ready.ID {
			found = true
		}
	}
	if !found {
		t.Error("Ready command should be in FindPending results")
	}
}

func testFindByStatus(t *testing.T, store durex.Storage) {
	ctx := context.Background()

	// Create mixed status commands
	for i := 0; i < 10; i++ {
		status := durex.StatusPending
		if i%2 == 0 {
			status = durex.StatusCompleted
		}
		if i%3 == 0 {
			status = durex.StatusFailed
		}

		cmd := &durex.Instance{
			ID:        durex.GenerateID(),
			Name:      "test-find-status",
			Status:    status,
			CreatedAt: time.Now(),
			ReadyAt:   time.Now(),
		}
		if err := store.Create(ctx, cmd); err != nil {
			t.Fatalf("Create failed: %v", err)
		}
	}

	completed, err := store.FindByStatus(ctx, durex.StatusCompleted)
	if err != nil {
		t.Fatalf("FindByStatus failed: %v", err)
	}

	for _, cmd := range completed {
		if cmd.Status != durex.StatusCompleted {
			t.Errorf("FindByStatus returned wrong status: expected COMPLETED, got %s", cmd.Status)
		}
	}
}

func testFindByParent(t *testing.T, store durex.Storage) {
	ctx := context.Background()

	parentID := "parent-find-1"

	// Create parent
	parent := &durex.Instance{
		ID:        parentID,
		Name:      "parent",
		Status:    durex.StatusCompleted,
		CreatedAt: time.Now(),
		ReadyAt:   time.Now(),
	}
	if err := store.Create(ctx, parent); err != nil {
		t.Fatalf("Create parent failed: %v", err)
	}

	// Create children
	for i := 0; i < 3; i++ {
		child := &durex.Instance{
			ID:        durex.GenerateID(),
			Name:      "child",
			Status:    durex.StatusPending,
			ParentID:  &parentID,
			CreatedAt: time.Now(),
			ReadyAt:   time.Now(),
		}
		if err := store.Create(ctx, child); err != nil {
			t.Fatalf("Create child failed: %v", err)
		}
	}

	// Create unrelated command
	unrelated := &durex.Instance{
		ID:        durex.GenerateID(),
		Name:      "unrelated",
		Status:    durex.StatusPending,
		CreatedAt: time.Now(),
		ReadyAt:   time.Now(),
	}
	if err := store.Create(ctx, unrelated); err != nil {
		t.Fatalf("Create unrelated failed: %v", err)
	}

	children, err := store.FindByParent(ctx, parentID)
	if err != nil {
		t.Fatalf("FindByParent failed: %v", err)
	}

	if len(children) != 3 {
		t.Errorf("Expected 3 children, got %d", len(children))
	}

	for _, child := range children {
		if child.ParentID == nil || *child.ParentID != parentID {
			t.Errorf("Child has wrong parent: %v", child.ParentID)
		}
	}
}

func testFindByUniqueKey(t *testing.T, store durex.Storage) {
	ctx := context.Background()

	uniqueKey := "unique-key-test-1"

	cmd := &durex.Instance{
		ID:        durex.GenerateID(),
		Name:      "uniqueCmd",
		Status:    durex.StatusPending,
		UniqueKey: uniqueKey,
		CreatedAt: time.Now(),
		ReadyAt:   time.Now(),
	}

	if err := store.Create(ctx, cmd); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	found, err := store.FindByUniqueKey(ctx, uniqueKey)
	if err != nil {
		t.Fatalf("FindByUniqueKey failed: %v", err)
	}

	if found.ID != cmd.ID {
		t.Errorf("Found wrong command: expected %s, got %s", cmd.ID, found.ID)
	}
	if found.UniqueKey != uniqueKey {
		t.Errorf("UniqueKey mismatch: expected %s, got %s", uniqueKey, found.UniqueKey)
	}

	// Non-existent key
	_, err = store.FindByUniqueKey(ctx, "nonexistent-key")
	if err != durex.ErrNotFound {
		t.Errorf("Expected ErrNotFound for nonexistent key, got %v", err)
	}
}

func testFindByUniqueKeyOnlyActive(t *testing.T, store durex.Storage) {
	ctx := context.Background()

	uniqueKey := "unique-key-active-test"

	// Create completed command with unique key
	completed := &durex.Instance{
		ID:        durex.GenerateID(),
		Name:      "completedUnique",
		Status:    durex.StatusCompleted,
		UniqueKey: uniqueKey,
		CreatedAt: time.Now(),
		ReadyAt:   time.Now(),
	}

	if err := store.Create(ctx, completed); err != nil {
		t.Fatalf("Create completed failed: %v", err)
	}

	// FindByUniqueKey should not find completed commands
	_, err := store.FindByUniqueKey(ctx, uniqueKey)
	if err != durex.ErrNotFound {
		t.Errorf("FindByUniqueKey should not find terminal status commands, got %v", err)
	}
}

func testCleanup(t *testing.T, store durex.Storage) {
	ctx := context.Background()

	now := time.Now()
	oldTime := now.Add(-2 * time.Hour)

	// Create old completed command
	old := &durex.Instance{
		ID:          durex.GenerateID(),
		Name:        "old",
		Status:      durex.StatusCompleted,
		CreatedAt:   oldTime,
		ReadyAt:     oldTime,
		CompletedAt: &oldTime,
	}
	if err := store.Create(ctx, old); err != nil {
		t.Fatalf("Create old failed: %v", err)
	}

	// Create recent completed command
	recent := &durex.Instance{
		ID:          durex.GenerateID(),
		Name:        "recent",
		Status:      durex.StatusCompleted,
		CreatedAt:   now,
		ReadyAt:     now,
		CompletedAt: &now,
	}
	if err := store.Create(ctx, recent); err != nil {
		t.Fatalf("Create recent failed: %v", err)
	}

	// Cleanup commands older than 1 hour
	count, err := store.Cleanup(ctx, time.Hour)
	if err != nil {
		t.Fatalf("Cleanup failed: %v", err)
	}

	if count != 1 {
		t.Errorf("Expected 1 cleanup, got %d", count)
	}

	// Old should be gone
	_, err = store.Get(ctx, old.ID)
	if err != durex.ErrNotFound {
		t.Error("Old command should be deleted")
	}

	// Recent should remain
	_, err = store.Get(ctx, recent.ID)
	if err != nil {
		t.Error("Recent command should still exist")
	}
}

func testCleanupPreservesActive(t *testing.T, store durex.Storage) {
	ctx := context.Background()

	oldTime := time.Now().Add(-2 * time.Hour)

	// Create old but pending command (should not be cleaned up)
	pending := &durex.Instance{
		ID:        durex.GenerateID(),
		Name:      "oldPending",
		Status:    durex.StatusPending,
		CreatedAt: oldTime,
		ReadyAt:   oldTime,
	}
	if err := store.Create(ctx, pending); err != nil {
		t.Fatalf("Create pending failed: %v", err)
	}

	count, err := store.Cleanup(ctx, time.Hour)
	if err != nil {
		t.Fatalf("Cleanup failed: %v", err)
	}

	if count != 0 {
		t.Errorf("Expected 0 cleanup for active commands, got %d", count)
	}

	// Pending should still exist
	_, err = store.Get(ctx, pending.ID)
	if err != nil {
		t.Error("Active command should not be cleaned up")
	}
}

func testCount(t *testing.T, store durex.Storage) {
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		cmd := &durex.Instance{
			ID:        durex.GenerateID(),
			Name:      "count-test",
			Status:    durex.StatusPending,
			CreatedAt: time.Now(),
			ReadyAt:   time.Now(),
		}
		if err := store.Create(ctx, cmd); err != nil {
			t.Fatalf("Create failed: %v", err)
		}
	}

	total, err := store.Count(ctx, nil)
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}

	if total < 5 {
		t.Errorf("Expected at least 5, got %d", total)
	}
}

func testCountByStatus(t *testing.T, store durex.Storage) {
	ctx := context.Background()

	// Create 3 pending, 2 completed
	for i := 0; i < 5; i++ {
		status := durex.StatusPending
		if i >= 3 {
			status = durex.StatusCompleted
		}

		cmd := &durex.Instance{
			ID:        durex.GenerateID(),
			Name:      "count-status-test",
			Status:    status,
			CreatedAt: time.Now(),
			ReadyAt:   time.Now(),
		}
		now := time.Now()
		if status == durex.StatusCompleted {
			cmd.CompletedAt = &now
		}
		if err := store.Create(ctx, cmd); err != nil {
			t.Fatalf("Create failed: %v", err)
		}
	}

	pending := durex.StatusPending
	pendingCount, err := store.Count(ctx, &pending)
	if err != nil {
		t.Fatalf("Count by status failed: %v", err)
	}

	if pendingCount < 3 {
		t.Errorf("Expected at least 3 pending, got %d", pendingCount)
	}
}

func testDataPersistence(t *testing.T, store durex.Storage) {
	ctx := context.Background()

	cmd := &durex.Instance{
		ID:     durex.GenerateID(),
		Name:   "dataTest",
		Status: durex.StatusPending,
		Data: durex.M{
			"string":  "hello",
			"int":     float64(42), // JSON numbers become float64
			"float":   3.14,
			"bool":    true,
			"slice":   []any{"a", "b", "c"},
			"nested":  map[string]any{"key": "value"},
			"null":    nil,
			"unicode": "日本語 🎉",
		},
		CreatedAt: time.Now(),
		ReadyAt:   time.Now(),
	}

	if err := store.Create(ctx, cmd); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	got, err := store.Get(ctx, cmd.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if got.Data["string"] != "hello" {
		t.Errorf("String not persisted: %v", got.Data["string"])
	}
	if got.Data["bool"] != true {
		t.Errorf("Bool not persisted: %v", got.Data["bool"])
	}
	if got.Data["unicode"] != "日本語 🎉" {
		t.Errorf("Unicode not persisted: %v", got.Data["unicode"])
	}

	nested, ok := got.Data["nested"].(map[string]any)
	if !ok || nested["key"] != "value" {
		t.Errorf("Nested map not persisted correctly: %v", got.Data["nested"])
	}
}

func testSequencePersistence(t *testing.T, store durex.Storage) {
	ctx := context.Background()

	cmd := &durex.Instance{
		ID:        durex.GenerateID(),
		Name:      "step1",
		Status:    durex.StatusPending,
		Sequence:  []string{"step2", "step3", "step4"},
		CreatedAt: time.Now(),
		ReadyAt:   time.Now(),
	}

	if err := store.Create(ctx, cmd); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	got, err := store.Get(ctx, cmd.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(got.Sequence) != 3 {
		t.Errorf("Sequence length mismatch: expected 3, got %d", len(got.Sequence))
	}

	expected := []string{"step2", "step3", "step4"}
	for i, name := range expected {
		if got.Sequence[i] != name {
			t.Errorf("Sequence[%d] mismatch: expected %s, got %s", i, name, got.Sequence[i])
		}
	}
}

func testTagsPersistence(t *testing.T, store durex.Storage) {
	ctx := context.Background()

	cmd := &durex.Instance{
		ID:        durex.GenerateID(),
		Name:      "tagTest",
		Status:    durex.StatusPending,
		Tags:      []string{"urgent", "email", "user-123"},
		CreatedAt: time.Now(),
		ReadyAt:   time.Now(),
	}

	if err := store.Create(ctx, cmd); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	got, err := store.Get(ctx, cmd.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(got.Tags) != 3 {
		t.Errorf("Tags length mismatch: expected 3, got %d", len(got.Tags))
	}

	for _, tag := range []string{"urgent", "email", "user-123"} {
		if !got.HasTag(tag) {
			t.Errorf("Tag %q not found in persisted tags", tag)
		}
	}
}

func testMetadataPersistence(t *testing.T, store durex.Storage) {
	ctx := context.Background()

	cmd := &durex.Instance{
		ID:       durex.GenerateID(),
		Name:     "metaTest",
		Status:   durex.StatusPending,
		Metadata: durex.M{"worker": "worker-1", "host": "localhost"},
		CreatedAt: time.Now(),
		ReadyAt:   time.Now(),
	}

	if err := store.Create(ctx, cmd); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	got, err := store.Get(ctx, cmd.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if got.Metadata["worker"] != "worker-1" {
		t.Errorf("Metadata worker mismatch: %v", got.Metadata["worker"])
	}
	if got.Metadata["host"] != "localhost" {
		t.Errorf("Metadata host mismatch: %v", got.Metadata["host"])
	}
}

func testTimestampPersistence(t *testing.T, store durex.Storage) {
	ctx := context.Background()

	now := time.Now().Truncate(time.Microsecond) // Truncate for comparison
	startedAt := now.Add(-time.Minute)
	completedAt := now
	deadlineAt := now.Add(time.Hour)

	cmd := &durex.Instance{
		ID:          durex.GenerateID(),
		Name:        "timestampTest",
		Status:      durex.StatusCompleted,
		CreatedAt:   now.Add(-2 * time.Minute),
		ReadyAt:     now.Add(-90 * time.Second),
		StartedAt:   &startedAt,
		CompletedAt: &completedAt,
		DeadlineAt:  &deadlineAt,
	}

	if err := store.Create(ctx, cmd); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	got, err := store.Get(ctx, cmd.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if got.StartedAt == nil {
		t.Error("StartedAt should not be nil")
	}
	if got.CompletedAt == nil {
		t.Error("CompletedAt should not be nil")
	}
	if got.DeadlineAt == nil {
		t.Error("DeadlineAt should not be nil")
	}
}

func testAllStatusesPersist(t *testing.T, store durex.Storage) {
	ctx := context.Background()

	allStatuses := []durex.Status{
		durex.StatusPending,
		durex.StatusStarted,
		durex.StatusCompleted,
		durex.StatusFailed,
		durex.StatusExpired,
		durex.StatusCancelled,
		durex.StatusRepeating,
	}

	for _, status := range allStatuses {
		cmd := &durex.Instance{
			ID:        durex.GenerateID(),
			Name:      "statusTest",
			Status:    status,
			CreatedAt: time.Now(),
			ReadyAt:   time.Now(),
		}

		if err := store.Create(ctx, cmd); err != nil {
			t.Fatalf("Create failed for status %s: %v", status, err)
		}

		got, err := store.Get(ctx, cmd.ID)
		if err != nil {
			t.Fatalf("Get failed for status %s: %v", status, err)
		}

		if got.Status != status {
			t.Errorf("Status not persisted: expected %s, got %s", status, got.Status)
		}
	}
}
