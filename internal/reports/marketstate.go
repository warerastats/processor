// Package reports holds the processor's periodic report jobs.
package reports

import (
	"context"
	"log/slog"
	"time"

	"github.com/warerastats/models/models"
	"github.com/warerastats/models/models/stores/processed/reports"
	"github.com/warerastats/processor/internal/window"
)

const marketStateName = "market_state"

// maxSnapshotCatchup caps how many missed aligned boundaries a snapshot job
// recomputes after a deploy gap, so a long outage can't stall startup.
const maxSnapshotCatchup = 288

// MarketState writes a 24h wage/market aggregate snapshot per aligned boundary.
type MarketState struct {
	Colls    *models.Collections
	interval time.Duration
	offset   time.Duration
}

// NewMarketState builds the 24h market-state job.
func NewMarketState(colls *models.Collections, interval, offset time.Duration) *MarketState {
	return &MarketState{Colls: colls, interval: interval, offset: offset}
}

func (j *MarketState) Name() string            { return marketStateName }
func (j *MarketState) Interval() time.Duration { return j.interval }
func (j *MarketState) Offset() time.Duration   { return j.offset }

// Run snapshots every aligned boundary since the watermark up to the latest closed one.
func (j *MarketState) Run(ctx context.Context) error {
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

// snapshot computes the 24h aggregates ending at boundary b and upserts them.
func (j *MarketState) snapshot(ctx context.Context, b time.Time) error {
	since := b.Add(-24 * time.Hour)

	wage, err := j.Colls.Transactions.WageTransaction.AggregateStats(ctx, since, b)
	if err != nil {
		return err
	}
	tradeVol, err := j.Colls.Transactions.TradeTransaction.SumMoney(ctx, since, b)
	if err != nil {
		return err
	}
	marketVol, err := j.Colls.Transactions.MarketTransaction.SumMoney(ctx, since, b)
	if err != nil {
		return err
	}

	st := reports.MarketState{
		ID:              reports.MarketStateID(b),
		At:              b,
		AvgWage24h:      wage.WeightedAvg,
		WageVolume24h:   wage.Volume,
		MarketVolume24h: tradeVol + marketVol,
		WageMin:         wage.MinRate,
		WageMax:         wage.MaxRate,
		WageAvgWeighted: wage.WeightedAvg,
	}
	err = j.Colls.Processed.Reports.MarketState.Upsert(ctx, st)
	if err != nil {
		return err
	}
	slog.Info("Market state snapshot", "at", b)
	return nil
}
