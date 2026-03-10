package durex

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

const (
	// barrierCommandName is the internal command name for barrier coordination.
	barrierCommandName = "__durex_barrier"
)

// barrierCommand is an internal command that waits for all child commands
// to complete before spawning a continuation command.
type barrierCommand struct {
	executor *Executor
}

// barrierData holds the barrier command's configuration.
type barrierData struct {
	// CoordinatorID is the ID of the command that spawned the parallel tasks.
	CoordinatorID string `json:"coordinator_id"`

	// ExpectedCount is the number of child commands to wait for.
	ExpectedCount int `json:"expected_count"`

	// Continuation is the command to spawn after all children complete.
	Continuation Spec `json:"continuation"`

	// ChildIDs tracks the IDs of the child commands (for reference).
	ChildIDs []string `json:"child_ids,omitempty"`
}

// Execute implements Command.
func (b *barrierCommand) Execute(ctx context.Context, cmd *Instance) (Result, error) {
	// Parse barrier data
	var data barrierData
	dataBytes, err := json.Marshal(cmd.Data)
	if err != nil {
		return Empty(), fmt.Errorf("barrier: failed to marshal data: %w", err)
	}
	if err := json.Unmarshal(dataBytes, &data); err != nil {
		return Empty(), fmt.Errorf("barrier: failed to unmarshal data: %w", err)
	}

	if data.ExpectedCount == 0 || len(data.ChildIDs) == 0 {
		// No children to wait for, spawn continuation immediately
		return Next(data.Continuation), nil
	}

	// Fetch each child by ID to check their status
	var children []*Instance
	var missingChildren []string

	for _, childID := range data.ChildIDs {
		child, err := b.executor.storage.Get(ctx, childID)
		if err != nil {
			// Child not found yet - may still be persisting
			missingChildren = append(missingChildren, childID)
			continue
		}
		children = append(children, child)
	}

	// If some children are missing, wait and retry
	if len(missingChildren) > 0 {
		cmd.Set("_barrier_check_count", cmd.GetInt("_barrier_check_count")+1)

		// If we've checked too many times, something is wrong
		if cmd.GetInt("_barrier_check_count") > 30 {
			return Empty(), fmt.Errorf("barrier: timeout waiting for children (missing: %v)",
				missingChildren)
		}

		// Poll again after interval (uses command's Period)
		return Repeat(), nil
	}

	// Check if all children are in terminal state
	allComplete := true
	anyFailed := false
	failedChild := ""

	for _, child := range children {
		if !child.Status.IsTerminal() {
			allComplete = false
			break
		}
		if child.Status == StatusFailed || child.Status == StatusExpired || child.Status == StatusCancelled {
			anyFailed = true
			failedChild = child.ID
		}
	}

	if !allComplete {
		// Children still running, check again later
		return Repeat(), nil
	}

	if anyFailed {
		// At least one child failed, don't spawn continuation
		return Empty(), fmt.Errorf("barrier: child command failed (child_id: %s), continuation not spawned",
			failedChild)
	}

	// All children completed successfully, spawn continuation
	// Merge data from all children into the continuation
	continuationData := make(M)

	// Start with coordinator's original data if available
	for k, v := range data.Continuation.Data {
		continuationData[k] = v
	}

	// Add results from each child with a prefix.
	// Use child.ID (unique) to avoid collisions when multiple children share the same Name.
	for _, child := range children {
		idPrefix := fmt.Sprintf("_barrier_result_%s_", child.ID)
		namePrefix := fmt.Sprintf("_barrier_result_%s_", child.Name)
		for k, v := range child.Data {
			// Skip internal barrier metadata
			if k == "_barrier_check_count" || k == "_barrier_parent" {
				continue
			}
			// Write under both ID-based key (unique) and name-based key (convenient).
			// Name-based key may be overwritten if multiple children share a name;
			// ID-based key is always safe.
			continuationData[idPrefix+k] = v
			continuationData[namePrefix+k] = v
		}
	}

	// Create continuation with merged data
	continuation := data.Continuation
	continuation.Data = continuationData

	return Next(continuation), nil
}

// Name implements Command.
func (b *barrierCommand) Name() string {
	return barrierCommandName
}

// Default implements Defaulter.
func (b *barrierCommand) Default() Spec {
	return Spec{
		Name:   barrierCommandName,
		Period: time.Second, // Check every second by default
	}
}

// registerBarrierCommand registers the internal barrier command with the executor.
func (e *Executor) registerBarrierCommand() {
	e.Register(&barrierCommand{executor: e})
}
