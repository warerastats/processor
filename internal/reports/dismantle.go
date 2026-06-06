package reports

import (
	"context"
	"strconv"
	"time"

	"github.com/warerastats/models/models"
	"github.com/warerastats/models/models/stores/processed/reports"
	"github.com/warerastats/processor/internal/window"
)

const dismantleName = "dismantle_report"

// dismantleSpan bounds one fold chunk for resumable history catch-up.
const dismantleSpan = 24 * time.Hour

// Dismantle accumulates hourly destroyed-equipment state histograms.
type Dismantle struct {
	Colls    *models.Collections
	interval time.Duration
	offset   time.Duration
}

// NewDismantle builds the dismantle-report job.
func NewDismantle(colls *models.Collections, interval, offset time.Duration) *Dismantle {
	return &Dismantle{Colls: colls, interval: interval, offset: offset}
}

func (j *Dismantle) Name() string            { return dismantleName }
func (j *Dismantle) Interval() time.Duration { return j.interval }
func (j *Dismantle) Offset() time.Duration   { return j.offset }

// Run folds new dismantles into hourly histograms since the watermark.
func (j *Dismantle) Run(ctx context.Context) error {
	state, err := j.Colls.Processed.States.JobState.Get(ctx, j.Name())
	if err != nil {
		return err
	}

	since := state.Boundary
	if since.IsZero() {
		earliest, ok, err := j.Colls.Transactions.DismantleTransaction.EarliestTime(ctx)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		since = earliest.Add(-time.Second)
	}
	target := window.ClosedBoundary(time.Now(), time.Minute)

	for since.Before(target) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		until := since.Add(dismantleSpan)
		if until.After(target) {
			until = target
		}
		err = j.fold(ctx, since, until)
		if err != nil {
			return err
		}
		err = j.Colls.Processed.States.JobState.SetWatermark(ctx, j.Name(), until, nil)
		if err != nil {
			return err
		}
		since = until
	}
	return nil
}

// fold recomputes the histogram for every hour touched by dismantles in
// (since, until]. Each hour is rebuilt from its full hour of transactions and
// replace-upserted, so re-running an overlapping window is idempotent.
func (j *Dismantle) fold(ctx context.Context, since, until time.Time) error {
	rows, err := j.Colls.Transactions.DismantleTransaction.GetRange(ctx, since, until)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}

	touched := map[time.Time]struct{}{}
	for _, r := range rows {
		touched[r.At.UTC().Truncate(time.Hour)] = struct{}{}
	}

	for hour := range touched {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		hourRows, err := j.Colls.Transactions.DismantleTransaction.GetRange(ctx, hour.Add(-time.Nanosecond), hour.Add(time.Hour))
		if err != nil {
			return err
		}
		report := reports.DismantleReport{
			ID:           reports.DismantleReportID(hour),
			HourStart:    hour,
			StateBuckets: map[string]int{},
		}
		for _, r := range hourRows {
			if r.At.Before(hour) || !r.At.Before(hour.Add(time.Hour)) {
				continue
			}
			report.Count++
			report.StateBuckets[stateBucket(r.State)]++
		}
		report.UpdatedAt = time.Now().UTC()
		err = j.Colls.Processed.Reports.DismantleReport.Upsert(ctx, report)
		if err != nil {
			return err
		}
	}
	return nil
}

// stateBucket groups an item state (0-100) into a ten-wide bucket label.
func stateBucket(state int) string {
	if state < 0 {
		state = 0
	}
	if state > 100 {
		state = 100
	}
	return strconv.Itoa((state / 10) * 10)
}
