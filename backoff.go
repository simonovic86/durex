package durex

import (
	"math"
	"math/rand"
	"time"
)

// BackoffStrategy calculates the delay before retrying a failed command.
type BackoffStrategy interface {
	// NextDelay returns the delay before the next retry attempt.
	// attempt starts at 1 for the first retry.
	NextDelay(attempt int) time.Duration
}

// ConstantBackoff returns the same delay for every retry.
type ConstantBackoff struct {
	Delay time.Duration
}

// NextDelay implements BackoffStrategy.
func (b ConstantBackoff) NextDelay(attempt int) time.Duration {
	return b.Delay
}

// LinearBackoff increases delay linearly with each attempt.
// Delay = InitialDelay * attempt
type LinearBackoff struct {
	InitialDelay time.Duration
	MaxDelay     time.Duration
}

// NextDelay implements BackoffStrategy.
func (b LinearBackoff) NextDelay(attempt int) time.Duration {
	delay := b.InitialDelay * time.Duration(attempt)
	if b.MaxDelay > 0 && delay > b.MaxDelay {
		return b.MaxDelay
	}
	return delay
}

// ExponentialBackoff increases delay exponentially with each attempt.
// Delay = InitialDelay * (Multiplier ^ (attempt - 1))
type ExponentialBackoff struct {
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64
}

// NextDelay implements BackoffStrategy.
func (b ExponentialBackoff) NextDelay(attempt int) time.Duration {
	multiplier := b.Multiplier
	if multiplier == 0 {
		multiplier = 2.0
	}

	delay := float64(b.InitialDelay) * math.Pow(multiplier, float64(attempt-1))
	d := time.Duration(delay)

	if b.MaxDelay > 0 && d > b.MaxDelay {
		return b.MaxDelay
	}
	return d
}

// JitteredBackoff wraps another BackoffStrategy and adds random jitter.
// This helps prevent thundering herd problems.
type JitteredBackoff struct {
	Strategy   BackoffStrategy
	JitterRate float64 // 0.0 to 1.0, e.g., 0.1 = ±10% jitter
}

// NextDelay implements BackoffStrategy.
func (b JitteredBackoff) NextDelay(attempt int) time.Duration {
	base := b.Strategy.NextDelay(attempt)
	if b.JitterRate <= 0 {
		return base
	}

	jitterRange := float64(base) * b.JitterRate
	jitter := (rand.Float64()*2 - 1) * jitterRange // -jitterRange to +jitterRange
	return time.Duration(float64(base) + jitter)
}

// DefaultExponentialBackoff returns a sensible default exponential backoff.
// Starts at 1 second, doubles each time, max 5 minutes, with 10% jitter.
func DefaultExponentialBackoff() BackoffStrategy {
	return JitteredBackoff{
		Strategy: ExponentialBackoff{
			InitialDelay: 1 * time.Second,
			MaxDelay:     5 * time.Minute,
			Multiplier:   2.0,
		},
		JitterRate: 0.1,
	}
}

// NoBackoff returns a strategy with zero delay (immediate retry).
func NoBackoff() BackoffStrategy {
	return ConstantBackoff{Delay: 0}
}

// Ensure all types implement BackoffStrategy.
var (
	_ BackoffStrategy = ConstantBackoff{}
	_ BackoffStrategy = LinearBackoff{}
	_ BackoffStrategy = ExponentialBackoff{}
	_ BackoffStrategy = JitteredBackoff{}
)
