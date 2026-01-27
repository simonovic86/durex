package durex

import (
	"testing"
	"time"
)

func TestParseCron(t *testing.T) {
	tests := []struct {
		name    string
		expr    string
		wantErr bool
	}{
		{
			name:    "every minute",
			expr:    "* * * * *",
			wantErr: false,
		},
		{
			name:    "every 5 minutes",
			expr:    "*/5 * * * *",
			wantErr: false,
		},
		{
			name:    "daily at midnight",
			expr:    "0 0 * * *",
			wantErr: false,
		},
		{
			name:    "weekdays at 9am",
			expr:    "0 9 * * 1-5",
			wantErr: false,
		},
		{
			name:    "first of month",
			expr:    "0 0 1 * *",
			wantErr: false,
		},
		{
			name:    "invalid - empty",
			expr:    "",
			wantErr: true,
		},
		{
			name:    "invalid - wrong format",
			expr:    "invalid",
			wantErr: true,
		},
		{
			name:    "invalid - too few fields",
			expr:    "* * *",
			wantErr: true,
		},
		{
			name:    "invalid - out of range",
			expr:    "60 * * * *",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseCron(tt.expr)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseCron(%q) error = %v, wantErr %v", tt.expr, err, tt.wantErr)
			}
		})
	}
}

func TestValidateCron(t *testing.T) {
	// Valid expressions
	if err := ValidateCron("0 0 * * *"); err != nil {
		t.Errorf("ValidateCron() unexpected error for valid cron: %v", err)
	}

	// Invalid expressions
	if err := ValidateCron("invalid"); err == nil {
		t.Error("ValidateCron() expected error for invalid cron")
	}
}

func TestNextCronTime(t *testing.T) {
	// Test every minute
	now := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	next := NextCronTime("* * * * *", now)
	expected := time.Date(2024, 1, 15, 10, 31, 0, 0, time.UTC)
	if !next.Equal(expected) {
		t.Errorf("NextCronTime() = %v, want %v", next, expected)
	}

	// Test daily at midnight
	next = NextCronTime("0 0 * * *", now)
	expected = time.Date(2024, 1, 16, 0, 0, 0, 0, time.UTC)
	if !next.Equal(expected) {
		t.Errorf("NextCronTime() = %v, want %v", next, expected)
	}

	// Test invalid cron returns zero time
	next = NextCronTime("invalid", now)
	if !next.IsZero() {
		t.Errorf("NextCronTime() with invalid cron = %v, want zero time", next)
	}
}

func TestCronSchedule(t *testing.T) {
	sched, err := NewCronSchedule("*/5 * * * *")
	if err != nil {
		t.Fatalf("NewCronSchedule() error = %v", err)
	}

	if sched.String() != "*/5 * * * *" {
		t.Errorf("String() = %q, want %q", sched.String(), "*/5 * * * *")
	}

	now := time.Date(2024, 1, 15, 10, 32, 0, 0, time.UTC)
	next := sched.Next(now)
	expected := time.Date(2024, 1, 15, 10, 35, 0, 0, time.UTC)
	if !next.Equal(expected) {
		t.Errorf("Next() = %v, want %v", next, expected)
	}
}

func TestCronSchedule_Invalid(t *testing.T) {
	_, err := NewCronSchedule("invalid")
	if err == nil {
		t.Error("NewCronSchedule() expected error for invalid cron")
	}
	if err != ErrInvalidCron {
		t.Errorf("NewCronSchedule() error = %v, want %v", err, ErrInvalidCron)
	}
}

func TestPredefinedCronExpressions(t *testing.T) {
	expressions := []string{
		CronEveryMinute,
		CronEvery5Minutes,
		CronEvery15Minutes,
		CronEvery30Minutes,
		CronHourly,
		CronDaily,
		CronWeekly,
		CronMonthly,
		CronWeekdays,
		CronWeekends,
	}

	for _, expr := range expressions {
		t.Run(expr, func(t *testing.T) {
			if err := ValidateCron(expr); err != nil {
				t.Errorf("Predefined cron %q is invalid: %v", expr, err)
			}
		})
	}
}
