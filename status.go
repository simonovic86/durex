package durex

// Status represents the execution state of a command instance.
type Status string

const (
	// StatusPending indicates the command is waiting to be executed.
	StatusPending Status = "PENDING"

	// StatusStarted indicates the command is currently executing.
	StatusStarted Status = "STARTED"

	// StatusCompleted indicates the command finished successfully.
	StatusCompleted Status = "COMPLETED"

	// StatusFailed indicates the command failed after exhausting retries.
	StatusFailed Status = "FAILED"

	// StatusExpired indicates the command's deadline passed before execution.
	StatusExpired Status = "EXPIRED"

	// StatusRepeating indicates the command is scheduled to repeat.
	StatusRepeating Status = "REPEATING"

	// StatusCancelled indicates the command was cancelled before completion.
	StatusCancelled Status = "CANCELLED"

	// StatusDeadLetter indicates the command failed permanently and was moved to DLQ.
	// Commands in DLQ can be inspected and replayed manually.
	StatusDeadLetter Status = "DEAD_LETTER"
)

// IsTerminal returns true if the status represents a final state.
func (s Status) IsTerminal() bool {
	switch s {
	case StatusCompleted, StatusFailed, StatusExpired, StatusCancelled, StatusDeadLetter:
		return true
	default:
		return false
	}
}

// IsActive returns true if the status represents an active/running state.
func (s Status) IsActive() bool {
	switch s {
	case StatusPending, StatusStarted, StatusRepeating:
		return true
	default:
		return false
	}
}

// String returns the string representation of the status.
func (s Status) String() string {
	return string(s)
}
