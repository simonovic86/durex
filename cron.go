package durex

import (
	"errors"
	"time"

	"github.com/robfig/cron/v3"
)

// Common cron errors.
var (
	ErrInvalidCron = errors.New("durex: invalid cron expression")
)

// cronParser is a standard cron parser (minute, hour, day, month, weekday).
// Uses the standard 5-field format without seconds.
var cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

// ParseCron validates a cron expression and returns the parsed schedule.
// Returns ErrInvalidCron if the expression is invalid.
//
// Supports standard 5-field cron format:
//
//	┌───────────── minute (0-59)
//	│ ┌───────────── hour (0-23)
//	│ │ ┌───────────── day of month (1-31)
//	│ │ │ ┌───────────── month (1-12)
//	│ │ │ │ ┌───────────── day of week (0-6, Sunday=0)
//	│ │ │ │ │
//	* * * * *
//
// Examples:
//   - "0 0 * * *"     - Daily at midnight
//   - "*/15 * * * *"  - Every 15 minutes
//   - "0 9 * * 1-5"   - Weekdays at 9 AM
//   - "0 0 1 * *"     - First day of every month at midnight
//   - "30 4 * * 0"    - Sundays at 4:30 AM
func ParseCron(expr string) (cron.Schedule, error) {
	schedule, err := cronParser.Parse(expr)
	if err != nil {
		return nil, ErrInvalidCron
	}
	return schedule, nil
}

// ValidateCron checks if a cron expression is valid.
func ValidateCron(expr string) error {
	_, err := ParseCron(expr)
	return err
}

// NextCronTime returns the next time the cron expression will trigger after the given time.
// Returns zero time if the expression is invalid.
func NextCronTime(expr string, after time.Time) time.Time {
	schedule, err := ParseCron(expr)
	if err != nil {
		return time.Time{}
	}
	return schedule.Next(after)
}

// CronSchedule wraps a parsed cron schedule for reuse.
type CronSchedule struct {
	expr     string
	schedule cron.Schedule
}

// NewCronSchedule creates a new CronSchedule from an expression.
// Returns an error if the expression is invalid.
func NewCronSchedule(expr string) (*CronSchedule, error) {
	schedule, err := ParseCron(expr)
	if err != nil {
		return nil, err
	}
	return &CronSchedule{
		expr:     expr,
		schedule: schedule,
	}, nil
}

// Next returns the next time the schedule will trigger after the given time.
func (c *CronSchedule) Next(after time.Time) time.Time {
	return c.schedule.Next(after)
}

// String returns the original cron expression.
func (c *CronSchedule) String() string {
	return c.expr
}

// Predefined cron expressions for common schedules.
const (
	// CronEveryMinute runs every minute.
	CronEveryMinute = "* * * * *"

	// CronEvery5Minutes runs every 5 minutes.
	CronEvery5Minutes = "*/5 * * * *"

	// CronEvery15Minutes runs every 15 minutes.
	CronEvery15Minutes = "*/15 * * * *"

	// CronEvery30Minutes runs every 30 minutes.
	CronEvery30Minutes = "*/30 * * * *"

	// CronHourly runs at the start of every hour.
	CronHourly = "0 * * * *"

	// CronDaily runs at midnight every day.
	CronDaily = "0 0 * * *"

	// CronWeekly runs at midnight on Sunday.
	CronWeekly = "0 0 * * 0"

	// CronMonthly runs at midnight on the first day of every month.
	CronMonthly = "0 0 1 * *"

	// CronWeekdays runs at midnight on weekdays (Monday-Friday).
	CronWeekdays = "0 0 * * 1-5"

	// CronWeekends runs at midnight on weekends (Saturday-Sunday).
	CronWeekends = "0 0 * * 0,6"
)
