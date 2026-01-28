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

	// PollInterval is how often to check for completion (default: 1s).
	PollInterval time.Duration `json:"poll_interval,omitempty"`
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

	if data.ExpectedCount == 0 {
		// No children to wait for, spawn continuation immediately
		return Next(data.Continuation), nil
	}

	// Find all children of the coordinator
	children, err := b.executor.storage.FindByParent(ctx, data.CoordinatorID)
	if err != nil {
		return Empty(), fmt.Errorf("barrier: failed to find children: %w", err)
	}

	// Filter only the relevant children (not the barrier itself)
	var relevantChildren []*Instance
	for _, child := range children {
		if child.Name != barrierCommandName {
			relevantChildren = append(relevantChildren, child)
		}
	}

	// Check if we have the expected number of children
	if len(relevantChildren) != data.ExpectedCount {
		// Still waiting for children to be created
		// This can happen if the barrier is scheduled before all children are persisted
		pollInterval := data.PollInterval
		if pollInterval == 0 {
			pollInterval = time.Second
		}
		cmd.Set("_barrier_check_count", cmd.GetInt("_barrier_check_count")+1)

		// If we've checked too many times, something is wrong
		if cmd.GetInt("_barrier_check_count") > 30 {
			return Empty(), fmt.Errorf("barrier: timeout waiting for %d children (found %d)",
				data.ExpectedCount, len(relevantChildren))
		}

		// Poll again after interval
		return Repeat(), nil
	}

	// Check if all children are in terminal state
	allComplete := true
	anyFailed := false
	failedChild := ""

	for _, child := range relevantChildren {
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

	// Add results from each child with a prefix
	for _, child := range relevantChildren {
		prefix := fmt.Sprintf("_barrier_result_%s_", child.Name)
		for k, v := range child.Data {
			// Skip internal barrier metadata
			if k == "_barrier_check_count" || k == "_barrier_parent" {
				continue
			}
			continuationData[prefix+k] = v
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
