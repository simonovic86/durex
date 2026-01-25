package durex

import (
	"time"
)

// EventType represents the type of execution event.
type EventType string

// Event types for command lifecycle.
const (
	EventCreated   EventType = "created"
	EventStarted   EventType = "started"
	EventCompleted EventType = "completed"
	EventFailed    EventType = "failed"
	EventRetrying  EventType = "retrying"
	EventExpired   EventType = "expired"
	EventCancelled EventType = "cancelled"
	EventRepeating EventType = "repeating"
	EventRecovered EventType = "recovered" // Moved to DLQ or recovery handler called
)

// Event represents a single execution event in a command's history.
type Event struct {
	// Type is the event type (created, started, completed, etc.).
	Type EventType `json:"type"`

	// Timestamp is when the event occurred.
	Timestamp time.Time `json:"timestamp"`

	// Attempt is the attempt number (for started/failed/completed events).
	Attempt int `json:"attempt,omitempty"`

	// Error contains the error message (for failed events).
	Error string `json:"error,omitempty"`

	// DurationMs is how long the execution took in milliseconds.
	DurationMs int64 `json:"duration_ms,omitempty"`

	// Message contains additional context about the event.
	Message string `json:"message,omitempty"`
}
