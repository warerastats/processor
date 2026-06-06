package reports

import (
	"context"
	"log/slog"
	"time"

	"github.com/warerastats/models/models"
	"github.com/warerastats/models/models/stores/processed/reports"
	"github.com/warerastats/models/models/stores/transactions"
	"github.com/warerastats/processor/internal/window"
)

const wageStateName = "wage_market_state"

// WageState writes a wage market snapshot (14d aggregates + 24h leaderboards).
type WageState struct {
	Colls    *models.Collections
	interval time.Duration
	offset   time.Duration
}

// NewWageState builds the wage-market-state job.
func NewWageState(colls *models.Collections, interval, offset time.Duration) *WageState {
	return &WageState{Colls: colls, interval: interval, offset: offset}
}

func (j *WageState) Name() string            { return wageStateName }
func (j *WageState) Interval() time.Duration { return j.interval }
func (j *WageState) Offset() time.Duration   { return j.offset }

// Run snapshots every aligned boundary since the watermark up to the latest closed one.
func (j *WageState) Run(ctx context.Context) error {
	state, err := j.Colls.Processed.States.JobState.Get(ctx, j.Name())
	if err != nil {
		return err
	}

	latest := window.FloorUTC(time.Now(), j.interval)
	from := state.Boundary
	if from.IsZero() {
		from = latest
	} else {
		from = from.Add(j.interval)
	}
	if earliest := latest.Add(-time.Duration(maxSnapshotCatchup) * j.interval); from.Before(earliest) {
		from = earliest
	}

	for b := from; !b.After(latest); b = b.Add(j.interval) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err = j.snapshot(ctx, b)
		if err != nil {
			return err
		}
		err = j.Colls.Processed.States.JobState.SetWatermark(ctx, j.Name(), b, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

// snapshot computes 14d wage aggregates and 24h pay leaderboards ending at b.
func (j *WageState) snapshot(ctx context.Context, b time.Time) error {
	stats, err := j.Colls.Transactions.WageTransaction.AggregateStats(ctx, b.Add(-14*24*time.Hour), b)
	if err != nil {
		return err
	}
	day := b.Add(-24 * time.Hour)
	most, err := j.Colls.Transactions.WageTransaction.TopPaidEmployees(ctx, day, b, 10, false)
	if err != nil {
		return err
	}
	least, err := j.Colls.Transactions.WageTransaction.TopPaidEmployees(ctx, day, b, 10, true)
	if err != nil {
		return err
	}

	st := reports.WageMarketState{
		ID:             reports.WageMarketStateID(b),
		At:             b,
		AvgWeighted14d: stats.WeightedAvg,
		Min14d:         stats.MinRate,
		Max14d:         stats.MaxRate,
		TotalPaid14d:   stats.Money,
		Top10Least24h:  toPaidUsers(least),
		Top10Most24h:   toPaidUsers(most),
	}
	err = j.Colls.Processed.Reports.WageMarketState.Upsert(ctx, st)
	if err != nil {
		return err
	}
	slog.Info("Wage market state snapshot", "at", b)
	return nil
}

// toPaidUsers maps grouped wage totals to leaderboard entries.
func toPaidUsers(rows []transactions.IDTotal) []reports.WagePaidUser {
	out := make([]reports.WagePaidUser, 0, len(rows))
	for _, r := range rows {
		out = append(out, reports.WagePaidUser{UserID: r.ID, TotalPaid: r.Total})
	}
	return out
}
