// Package estimators holds the processor's stateful estimator jobs.
package estimators

import (
	"context"
	"log/slog"
	"time"

	"github.com/warerastats/models/models"
	"github.com/warerastats/models/models/stores/processed/estimators"
	"github.com/warerastats/processor/internal/window"
)

const inflationName = "inflation"

// inflationWeights is the per-item economic weight used in the index.
var inflationWeights = map[string]float64{
	"concrete":   2.3,
	"steel":      2.2,
	"oil":        1.8,
	"petroleum":  1.1,
	"iron":       1.1,
	"limestone":  1.0,
	"grain":      0.8,
	"bread":      1.0,
	"livestock":  0.9,
	"steak":      1.0,
	"fish":       0.9,
	"cookedFish": 1.1,
	"lead":       1.0,
	"lightAmmo":  0.8,
	"ammo":       0.9,
	"heavyAmmo":  1.0,
	"case1":      0.4,
	"case2":      0.4,
	"wood":       0.6,
	"paper":      0.8,
}

// wageInflationWeight blends the day's weighted wage rate into the index to
// dampen volatility; tunable heuristic, no upstream-defined value.
const wageInflationWeight = 2.0

// Inflation computes a daily weighted price index from item candles.
type Inflation struct {
	Colls    *models.Collections
	interval time.Duration
	offset   time.Duration
}

// NewInflation builds the daily inflation job.
func NewInflation(colls *models.Collections, interval, offset time.Duration) *Inflation {
	return &Inflation{Colls: colls, interval: interval, offset: offset}
}

func (j *Inflation) Name() string            { return inflationName }
func (j *Inflation) Interval() time.Duration { return j.interval }
func (j *Inflation) Offset() time.Duration   { return j.offset }

// Run computes the index for every closed day since the watermark.
func (j *Inflation) Run(ctx context.Context) error {
	state, err := j.Colls.Processed.States.JobState.Get(ctx, j.Name())
	if err != nil {
		return err
	}

	day := time.Duration(24) * time.Hour
	lastClosed := window.FloorUTC(time.Now(), day).Add(-day)

	var from time.Time
	if state.Boundary.IsZero() {
		earliest, ok, err := j.Colls.Transactions.TradeTransaction.EarliestTime(ctx)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		from = window.FloorUTC(earliest, day)
	} else {
		from = state.Boundary.Add(day)
	}

	for d := from; !d.After(lastClosed); d = d.Add(day) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err = j.computeDay(ctx, d)
		if err != nil {
			return err
		}
		err = j.Colls.Processed.States.JobState.SetWatermark(ctx, j.Name(), d, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

// computeDay builds and stores the inflation point for the day starting at d.
func (j *Inflation) computeDay(ctx context.Context, d time.Time) error {
	avgs, err := j.Colls.Processed.Candles.ItemCandle.WeightedAvgByItem(ctx, d, d.Add(24*time.Hour))
	if err != nil {
		return err
	}
	priceByItem := make(map[string]float64, len(avgs))
	for _, a := range avgs {
		priceByItem[a.ItemCode] = a.WeightedAvg
	}

	var weighted, totalWeight float64
	perItem := make(map[string]float64)
	for code, w := range inflationWeights {
		price, ok := priceByItem[code]
		if !ok || price <= 0 {
			continue
		}
		perItem[code] = price
		weighted += w * price
		totalWeight += w
	}

	// Blend in the day's weighted wage rate so labour cost stabilises the index.
	wageAvg, hasWage, err := j.Colls.Processed.Candles.WageCandle.WeightedAvgRange(ctx, d, d.Add(24*time.Hour))
	if err != nil {
		return err
	}
	if hasWage && wageAvg > 0 {
		perItem["wage"] = wageAvg
		weighted += wageInflationWeight * wageAvg
		totalWeight += wageInflationWeight
	}

	index := 0.0
	if totalWeight > 0 {
		index = weighted / totalWeight
	}

	pct := 0.0
	prev, ok, err := j.Colls.Processed.Estimators.Inflation.Get(ctx, d.Add(-24*time.Hour))
	if err != nil {
		return err
	}
	if ok && prev.IndexValue > 0 {
		pct = (index - prev.IndexValue) / prev.IndexValue * 100
	}

	point := estimators.InflationPoint{
		ID:         estimators.InflationPointID(d),
		DayStart:   d,
		IndexValue: index,
		PctChange:  pct,
		PerItem:    perItem,
	}
	err = j.Colls.Processed.Estimators.Inflation.Upsert(ctx, point)
	if err != nil {
		return err
	}
	slog.Info("Inflation point", "day", d, "index", index, "pct", pct)
	return nil
}
