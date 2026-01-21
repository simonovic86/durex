package durex

import (
	"encoding/json"
	"time"
)

// Instance represents a persisted command with runtime state.
// Instances are created from Specs and track execution progress.
type Instance struct {
	// ID is the unique identifier for this instance.
	ID string `json:"id"`

	// Name is the command type identifier.
	Name string `json:"name"`

	// Data contains the command payload.
	Data M `json:"data,omitempty"`

	// Status is the current execution state.
	Status Status `json:"status"`

	// Retries is the remaining retry count.
	Retries int `json:"retries"`

	// Sequence is the remaining command chain.
	Sequence []string `json:"sequence,omitempty"`

	// ParentID links to the parent command that spawned this instance.
	ParentID *string `json:"parent_id,omitempty"`

	// Priority determines execution order.
	Priority int `json:"priority"`

	// Tags for categorization.
	Tags []string `json:"tags,omitempty"`

	// CreatedAt is when the instance was created.
	CreatedAt time.Time `json:"created_at"`

	// ReadyAt is when the instance becomes eligible for execution.
	ReadyAt time.Time `json:"ready_at"`

	// StartedAt is when execution began (nil if not started).
	StartedAt *time.Time `json:"started_at,omitempty"`

	// CompletedAt is when execution finished (nil if not completed).
	CompletedAt *time.Time `json:"completed_at,omitempty"`

	// DeadlineAt is the execution deadline (nil if no deadline).
	DeadlineAt *time.Time `json:"deadline_at,omitempty"`

	// Period is the repeat interval for recurring commands.
	Period time.Duration `json:"period,omitempty"`

	// Error contains the error message if the command failed.
	Error string `json:"error,omitempty"`

	// Attempt tracks the current attempt number (starts at 1).
	Attempt int `json:"attempt"`

	// Metadata stores additional runtime information.
	Metadata M `json:"metadata,omitempty"`
}

// Get retrieves a value from the command data with type assertion.
// Returns the zero value if the key doesn't exist or type doesn't match.
func (i *Instance) Get(key string) any {
	if i.Data == nil {
		return nil
	}
	return i.Data[key]
}

// GetString retrieves a string value from command data.
func (i *Instance) GetString(key string) string {
	v, _ := i.Data[key].(string)
	return v
}

// GetInt retrieves an int value from command data.
// Handles both int and float64 (from JSON unmarshaling).
func (i *Instance) GetInt(key string) int {
	switch v := i.Data[key].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	default:
		return 0
	}
}

// GetBool retrieves a bool value from command data.
func (i *Instance) GetBool(key string) bool {
	v, _ := i.Data[key].(bool)
	return v
}

// GetSlice retrieves a slice value from command data.
func (i *Instance) GetSlice(key string) []any {
	v, _ := i.Data[key].([]any)
	return v
}

// GetMap retrieves a map value from command data.
func (i *Instance) GetMap(key string) M {
	v, _ := i.Data[key].(map[string]any)
	return v
}

// Set stores a value in the command data.
// Note: Changes are not automatically persisted. Call executor.Update() if needed.
func (i *Instance) Set(key string, value any) {
	if i.Data == nil {
		i.Data = make(M)
	}
	i.Data[key] = value
}

// ContinueSequence creates a Result that spawns the next command in the sequence.
// The accumulated data is passed to the next command.
// Returns Empty() if the sequence is empty.
//
// Example:
//
//	func (c *Step1Command) Execute(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
//		cmd.Set("step1_result", "done")
//		return cmd.ContinueSequence(nil), nil
//	}
func (i *Instance) ContinueSequence(additionalData M) Result {
	if len(i.Sequence) == 0 {
		return Empty()
	}

	nextName := i.Sequence[0]
	remaining := i.Sequence[1:]

	// Merge data
	data := make(M)
	for k, v := range i.Data {
		data[k] = v
	}
	for k, v := range additionalData {
		data[k] = v
	}

	return Result{
		Commands: []Spec{{
			Name:     nextName,
			Data:     data,
			Sequence: remaining,
		}},
	}
}

// Clone creates a deep copy of the instance.
func (i *Instance) Clone() *Instance {
	clone := *i

	if i.Data != nil {
		clone.Data = make(M)
		for k, v := range i.Data {
			clone.Data[k] = v
		}
	}

	if i.Sequence != nil {
		clone.Sequence = make([]string, len(i.Sequence))
		copy(clone.Sequence, i.Sequence)
	}

	if i.Tags != nil {
		clone.Tags = make([]string, len(i.Tags))
		copy(clone.Tags, i.Tags)
	}

	if i.Metadata != nil {
		clone.Metadata = make(M)
		for k, v := range i.Metadata {
			clone.Metadata[k] = v
		}
	}

	return &clone
}

// MarshalJSON implements json.Marshaler.
func (i *Instance) MarshalJSON() ([]byte, error) {
	type Alias Instance
	return json.Marshal(&struct {
		*Alias
		Period int64 `json:"period_ns,omitempty"`
	}{
		Alias:  (*Alias)(i),
		Period: int64(i.Period),
	})
}

// UnmarshalJSON implements json.Unmarshaler.
func (i *Instance) UnmarshalJSON(data []byte) error {
	type Alias Instance
	aux := &struct {
		*Alias
		Period int64 `json:"period_ns,omitempty"`
	}{
		Alias: (*Alias)(i),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	i.Period = time.Duration(aux.Period)
	return nil
}

// Duration returns how long the command took to execute.
// Returns 0 if the command hasn't completed.
func (i *Instance) Duration() time.Duration {
	if i.StartedAt == nil || i.CompletedAt == nil {
		return 0
	}
	return i.CompletedAt.Sub(*i.StartedAt)
}

// Age returns how long ago the command was created.
func (i *Instance) Age() time.Duration {
	return time.Since(i.CreatedAt)
}

// IsOverdue returns true if the command has passed its deadline.
func (i *Instance) IsOverdue() bool {
	if i.DeadlineAt == nil {
		return false
	}
	return time.Now().After(*i.DeadlineAt)
}

// HasTag returns true if the instance has the given tag.
func (i *Instance) HasTag(tag string) bool {
	for _, t := range i.Tags {
		if t == tag {
			return true
		}
	}
	return false
}
