package durex

// Result represents the outcome of command execution.
// It tells the executor what to do next.
type Result struct {
	// Commands to spawn after this command completes.
	// Child commands will have their ParentID set to this command's ID.
	Commands []Spec

	// Continuation is spawned after ALL Commands complete successfully.
	// This enables the fan-in pattern where parallel tasks must finish
	// before continuing the workflow. The executor creates a barrier
	// command that monitors the Commands and spawns Continuation when done.
	// Only valid when Commands is non-empty.
	Continuation *Spec

	// Repeat signals that this command should be rescheduled.
	// The command will run again after its Period duration.
	// Use this for recurring tasks like cleanup jobs or polling.
	Repeat bool

	// Retry signals that this command should retry immediately.
	// Only effective if the command has remaining retries.
	// Unlike returning an error, Retry doesn't trigger Recover.
	Retry bool
}

// Empty returns a Result that halts the execution chain.
// Use this when the command completes successfully with no follow-up.
func Empty() Result {
	return Result{}
}

// Repeat returns a Result that reschedules the command.
// The command will execute again after its Period duration.
//
// Example:
//
//	func (c *CleanupCommand) Execute(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
//		// cleanup logic...
//		return durex.Repeat(), nil  // run again after period
//	}
func Repeat() Result {
	return Result{Repeat: true}
}

// Retry returns a Result that retries the command.
// This decrements the retry counter without triggering Recover.
// If no retries remain, the command completes normally.
//
// Use Retry for expected transient failures where you want
// explicit control over retry behavior.
//
// Example:
//
//	func (c *SendCommand) Execute(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
//		err := c.send()
//		if isTransient(err) {
//			return durex.Retry(), nil  // retry without triggering Recover
//		}
//		if err != nil {
//			return durex.Empty(), err  // triggers Recover after retries exhausted
//		}
//		return durex.Empty(), nil
//	}
func Retry() Result {
	return Result{Retry: true}
}

// Next returns a Result that spawns a single follow-up command.
// This is a convenience wrapper around Result{Commands: []Spec{spec}}.
//
// Example:
//
//	func (c *Step1Command) Execute(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
//		// step 1 logic...
//		return durex.Next(durex.Spec{
//			Name: "step2Command",
//			Data: cmd.Data,
//		}), nil
//	}
func Next(spec Spec) Result {
	return Result{Commands: []Spec{spec}}
}

// Spawn returns a Result that creates multiple follow-up commands.
//
// Example:
//
//	func (c *FanOutCommand) Execute(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
//		items := cmd.Data["items"].([]string)
//		specs := make([]durex.Spec, len(items))
//		for i, item := range items {
//			specs[i] = durex.Spec{
//				Name: "processItemCommand",
//				Data: durex.M{"item": item},
//			}
//		}
//		return durex.Spawn(specs...), nil
//	}
func Spawn(specs ...Spec) Result {
	return Result{Commands: specs}
}

// SpawnThen returns a Result that spawns parallel commands and waits for all
// to complete before continuing with a follow-up command (fan-in pattern).
//
// The executor creates an internal barrier command that monitors the parallel
// tasks. When all parallel commands complete successfully, the continuation
// command is spawned automatically.
//
// Example:
//
//	func (c *ProcessOrderCommand) Execute(ctx context.Context, cmd *durex.Instance) (durex.Result, error) {
//		// Process payment, reserve inventory, and send email in parallel
//		// Then ship the order only after all three complete
//		return durex.SpawnThen(
//			[]durex.Spec{
//				{Name: "chargePayment", Data: cmd.Data},
//				{Name: "reserveInventory", Data: cmd.Data},
//				{Name: "sendEmail", Data: cmd.Data},
//			},
//			durex.Spec{Name: "shipOrder", Data: cmd.Data},
//		), nil
//	}
func SpawnThen(parallel []Spec, then Spec) Result {
	return Result{
		Commands:     parallel,
		Continuation: &then,
	}
}
