package durex_test

import (
	"testing"
	"time"

	"github.com/simonovic86/durex"
)

func TestConstantBackoff(t *testing.T) {
	backoff := durex.ConstantBackoff{Delay: 100 * time.Millisecond}

	// All attempts should have the same delay
	for attempt := 1; attempt <= 5; attempt++ {
		delay := backoff.NextDelay(attempt)
		if delay != 100*time.Millisecond {
			t.Errorf("Attempt %d: expected 100ms, got %v", attempt, delay)
		}
	}
}

func TestLinearBackoff(t *testing.T) {
	backoff := durex.LinearBackoff{
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     500 * time.Millisecond,
	}

	testCases := []struct {
		attempt  int
		expected time.Duration
	}{
		{1, 100 * time.Millisecond},  // 100 * 1
		{2, 200 * time.Millisecond},  // 100 * 2
		{3, 300 * time.Millisecond},  // 100 * 3
		{4, 400 * time.Millisecond},  // 100 * 4
		{5, 500 * time.Millisecond},  // 100 * 5 (at max)
		{6, 500 * time.Millisecond},  // 100 * 6 = 600, capped at 500
		{10, 500 * time.Millisecond}, // Capped at max
	}

	for _, tc := range testCases {
		delay := backoff.NextDelay(tc.attempt)
		if delay != tc.expected {
			t.Errorf("Attempt %d: expected %v, got %v", tc.attempt, tc.expected, delay)
		}
	}
}

func TestLinearBackoff_NoMax(t *testing.T) {
	backoff := durex.LinearBackoff{
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     0, // No max
	}

	delay := backoff.NextDelay(100)
	expected := 100 * time.Millisecond * 100 // 10 seconds

	if delay != expected {
		t.Errorf("Expected %v, got %v", expected, delay)
	}
}

func TestExponentialBackoff(t *testing.T) {
	backoff := durex.ExponentialBackoff{
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     5 * time.Second,
		Multiplier:   2.0,
	}

	testCases := []struct {
		attempt  int
		expected time.Duration
	}{
		{1, 100 * time.Millisecond},  // 100 * 2^0
		{2, 200 * time.Millisecond},  // 100 * 2^1
		{3, 400 * time.Millisecond},  // 100 * 2^2
		{4, 800 * time.Millisecond},  // 100 * 2^3
		{5, 1600 * time.Millisecond}, // 100 * 2^4
		{6, 3200 * time.Millisecond}, // 100 * 2^5
		{7, 5 * time.Second},         // 100 * 2^6 = 6.4s, capped at 5s
		{10, 5 * time.Second},        // Capped at max
	}

	for _, tc := range testCases {
		delay := backoff.NextDelay(tc.attempt)
		if delay != tc.expected {
			t.Errorf("Attempt %d: expected %v, got %v", tc.attempt, tc.expected, delay)
		}
	}
}

func TestExponentialBackoff_DefaultMultiplier(t *testing.T) {
	backoff := durex.ExponentialBackoff{
		InitialDelay: 100 * time.Millisecond,
		// Multiplier defaults to 2.0 when 0
	}

	// Attempt 2 should be 200ms with default multiplier of 2
	delay := backoff.NextDelay(2)
	expected := 200 * time.Millisecond

	if delay != expected {
		t.Errorf("Expected %v, got %v", expected, delay)
	}
}

func TestExponentialBackoff_CustomMultiplier(t *testing.T) {
	backoff := durex.ExponentialBackoff{
		InitialDelay: 100 * time.Millisecond,
		Multiplier:   3.0,
	}

	testCases := []struct {
		attempt  int
		expected time.Duration
	}{
		{1, 100 * time.Millisecond},  // 100 * 3^0
		{2, 300 * time.Millisecond},  // 100 * 3^1
		{3, 900 * time.Millisecond},  // 100 * 3^2
		{4, 2700 * time.Millisecond}, // 100 * 3^3
	}

	for _, tc := range testCases {
		delay := backoff.NextDelay(tc.attempt)
		if delay != tc.expected {
			t.Errorf("Attempt %d: expected %v, got %v", tc.attempt, tc.expected, delay)
		}
	}
}

func TestJitteredBackoff(t *testing.T) {
	base := durex.ConstantBackoff{Delay: 1000 * time.Millisecond}
	backoff := durex.JitteredBackoff{
		Strategy:   base,
		JitterRate: 0.1, // ±10%
	}

	minExpected := 900 * time.Millisecond  // 1000 - 10%
	maxExpected := 1100 * time.Millisecond // 1000 + 10%

	// Run multiple times to test randomness
	for i := 0; i < 100; i++ {
		delay := backoff.NextDelay(1)
		if delay < minExpected || delay > maxExpected {
			t.Errorf("Iteration %d: delay %v outside expected range [%v, %v]",
				i, delay, minExpected, maxExpected)
		}
	}
}

func TestJitteredBackoff_NilStrategy(t *testing.T) {
	backoff := durex.JitteredBackoff{JitterRate: 0.5}

	delay := backoff.NextDelay(1)
	if delay != 0 {
		t.Errorf("Expected 0 with nil strategy, got %v", delay)
	}
}

func TestJitteredBackoff_ClampsNegativeDelay(t *testing.T) {
	base := durex.ConstantBackoff{Delay: 100 * time.Millisecond}
	backoff := durex.JitteredBackoff{
		Strategy:   base,
		JitterRate: 10.0,
	}

	for i := 0; i < 100; i++ {
		delay := backoff.NextDelay(1)
		if delay < 0 {
			t.Fatalf("Expected non-negative delay, got %v", delay)
		}
	}
}

func TestJitteredBackoff_ZeroJitter(t *testing.T) {
	base := durex.ConstantBackoff{Delay: 100 * time.Millisecond}
	backoff := durex.JitteredBackoff{
		Strategy:   base,
		JitterRate: 0, // No jitter
	}

	delay := backoff.NextDelay(1)
	if delay != 100*time.Millisecond {
		t.Errorf("Expected 100ms with zero jitter, got %v", delay)
	}
}

func TestJitteredBackoff_WithExponential(t *testing.T) {
	exp := durex.ExponentialBackoff{
		InitialDelay: 100 * time.Millisecond,
		Multiplier:   2.0,
	}
	backoff := durex.JitteredBackoff{
		Strategy:   exp,
		JitterRate: 0.2, // ±20%
	}

	// Test attempt 3: base is 400ms (100 * 2^2)
	baseDelay := 400 * time.Millisecond
	minExpected := time.Duration(float64(baseDelay) * 0.8)
	maxExpected := time.Duration(float64(baseDelay) * 1.2)

	for i := 0; i < 50; i++ {
		delay := backoff.NextDelay(3)
		if delay < minExpected || delay > maxExpected {
			t.Errorf("Iteration %d: delay %v outside expected range [%v, %v]",
				i, delay, minExpected, maxExpected)
		}
	}
}

func TestNoBackoff(t *testing.T) {
	backoff := durex.NoBackoff()

	for attempt := 1; attempt <= 5; attempt++ {
		delay := backoff.NextDelay(attempt)
		if delay != 0 {
			t.Errorf("Attempt %d: expected 0, got %v", attempt, delay)
		}
	}
}

func TestDefaultExponentialBackoff(t *testing.T) {
	backoff := durex.DefaultExponentialBackoff()

	// First attempt should be around 1 second (±10% jitter)
	delay := backoff.NextDelay(1)
	minExpected := 900 * time.Millisecond
	maxExpected := 1100 * time.Millisecond

	if delay < minExpected || delay > maxExpected {
		t.Errorf("Attempt 1: delay %v outside expected range [%v, %v]",
			delay, minExpected, maxExpected)
	}

	// Later attempts should cap at 5 minutes
	delay = backoff.NextDelay(20)
	maxDelay := 5*time.Minute + 30*time.Second // 5 min + 10% jitter margin

	if delay > maxDelay {
		t.Errorf("Attempt 20: delay %v exceeds max delay with jitter %v", delay, maxDelay)
	}
}

func TestBackoffStrategy_Interface(t *testing.T) {
	// Verify all backoff types implement the interface
	var _ durex.BackoffStrategy = durex.ConstantBackoff{}
	var _ durex.BackoffStrategy = durex.LinearBackoff{}
	var _ durex.BackoffStrategy = durex.ExponentialBackoff{}
	var _ durex.BackoffStrategy = durex.JitteredBackoff{}
}
