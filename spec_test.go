package durex_test

import (
	"testing"
	"time"

	"github.com/simonovic86/durex"
)

func TestSpec_WithPeriod(t *testing.T) {
	spec := durex.Spec{Name: "test"}.WithPeriod(5 * time.Minute)
	if spec.Period != 5*time.Minute {
		t.Errorf("expected period 5m, got %v", spec.Period)
	}
}

func TestSpec_WithCron(t *testing.T) {
	spec := durex.Spec{Name: "test"}.WithCron("0 0 * * *")
	if spec.Cron != "0 0 * * *" {
		t.Errorf("expected cron '0 0 * * *', got %q", spec.Cron)
	}
}

func TestSpec_WithSequence(t *testing.T) {
	spec := durex.Spec{Name: "test"}.WithSequence("step2", "step3")
	if len(spec.Sequence) != 2 || spec.Sequence[0] != "step2" || spec.Sequence[1] != "step3" {
		t.Errorf("expected sequence [step2 step3], got %v", spec.Sequence)
	}
}

func TestSpec_WithDeadlineAt(t *testing.T) {
	deadline := time.Now().Add(time.Hour)
	spec := durex.Spec{Name: "test"}.WithDeadlineAt(deadline)
	if spec.DeadlineAt == nil || !spec.DeadlineAt.Equal(deadline) {
		t.Error("expected DeadlineAt to be set")
	}
}

func TestSpec_BuilderChaining(t *testing.T) {
	deadline := time.Now().Add(time.Hour)
	spec := durex.Spec{Name: "workflow"}.
		WithData(durex.M{"key": "value"}).
		WithRetries(3).
		WithPeriod(10*time.Minute).
		WithCron("0 9 * * 1-5").
		WithSequence("step1", "step2").
		WithDeadlineAt(deadline).
		WithPriority(5).
		WithTags("important", "workflow")

	if spec.Name != "workflow" {
		t.Errorf("name lost during chaining: got %q", spec.Name)
	}
	if spec.Retries != 3 {
		t.Errorf("retries: got %d", spec.Retries)
	}
	if spec.Period != 10*time.Minute {
		t.Errorf("period: got %v", spec.Period)
	}
	if spec.Cron != "0 9 * * 1-5" {
		t.Errorf("cron: got %q", spec.Cron)
	}
	if len(spec.Sequence) != 2 {
		t.Errorf("sequence: got %v", spec.Sequence)
	}
	if spec.DeadlineAt == nil {
		t.Error("deadline_at should be set")
	}
	if spec.Priority != 5 {
		t.Errorf("priority: got %d", spec.Priority)
	}
	if len(spec.Tags) != 2 {
		t.Errorf("tags: got %v", spec.Tags)
	}
}
