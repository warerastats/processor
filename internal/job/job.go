// Package job defines the periodic processing primitive shared by every processor workload.
package job

import (
	"context"
	"log/slog"
	"time"
)

// Job is a single unit of periodic processing.
type Job interface {
	// Name identifies the job in logs.
	Name() string
	// Interval is how often a pass runs.
	Interval() time.Duration
	// Offset staggers the first pass so jobs don't all fire at start-up.
	Offset() time.Duration
	// Run performs exactly one pass.
	Run(ctx context.Context) error
}

// Loop drives a Job on its schedule until ctx is cancelled.
func Loop(ctx context.Context, j Job) error {
	if !sleep(ctx, j.Offset()) {
		return ctx.Err()
	}

	ticker := time.NewTicker(j.Interval())
	defer ticker.Stop()

	for {
		err := j.Run(ctx)
		if err != nil {
			slog.Error("Job pass failed", "job", j.Name(), "error", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// sleep waits for d or until ctx is cancelled.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
