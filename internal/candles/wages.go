package candles

import (
	"context"
	"log/slog"
	"time"

	"github.com/warerastats/models/models"
	"github.com/warerastats/models/models/stores/processed/candles"
	"github.com/warerastats/processor/internal/window"
)

// wageCandleSize is the OHLC window width.
const wageCandleSize = 10 * time.Minute

// wageBackfillSpan bounds how much time one aggregation pass covers.
const wageBackfillSpan = 48 * time.Hour

// Wages aggregates wage_transactions into 10-minute OHLC candles.
type Wages struct {
	Colls    *models.Collections
	interval time.Duration
	offset   time.Duration
}

// NewWages builds the wage-candle job.
func NewWages(colls *models.Collections, interval, offset time.Duration) *Wages {
	return &Wages{Colls: colls, interval: interval, offset: offset}
}

func (j *Wages) Name() string            { return "wage_candles" }
func (j *Wages) Interval() time.Duration { return j.interval }
func (j *Wages) Offset() time.Duration   { return j.offset }

// Run folds every closed 10-minute window since the watermark into candles.
func (j *Wages) Run(ctx context.Context) error {
	state, err := j.Colls.Processed.States.JobState.Get(ctx, j.Name())
	if err != nil {
		return err
	}

	since := state.Boundary
	if since.IsZero() {
		earliest, ok, err := j.Colls.Transactions.WageTransaction.EarliestTime(ctx)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		since = window.FloorUTC(earliest, wageCandleSize).Add(-time.Second)
	}

	target := window.ClosedBoundary(time.Now(), wageCandleSize).Add(wageCandleSize)

	for since.Before(target) {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		until := since.Add(wageBackfillSpan)
		if until.After(target) {
			until = target
		}

		err = j.process(ctx, since, until)
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

// process aggregates one (since, until] chunk and upserts the resulting candles.
func (j *Wages) process(ctx context.Context, since, until time.Time) error {
	buckets, err := j.Colls.Transactions.WageTransaction.AggregateWageCandles(
		ctx, since, until, int(wageCandleSize/time.Minute),
	)
	if err != nil {
		return err
	}
	if len(buckets) == 0 {
		return nil
	}

	out := make([]candles.WageCandle, 0, len(buckets))
	for _, b := range buckets {
		avg := 0.0
		if b.Volume > 0 {
			avg = b.Money / float64(b.Volume)
		}
		out = append(out, candles.WageCandle{
			ID:          candles.WageCandleID(b.BucketStart),
			BucketStart: b.BucketStart.UTC(),
			Open:        b.Open,
			High:        b.High,
			Low:         b.Low,
			Close:       b.Close,
			Avg:         avg,
			Volume:      b.Volume,
			Money:       b.Money,
			Count:       b.Count,
		})
	}

	err = j.Colls.Processed.Candles.WageCandle.BulkUpsert(ctx, out)
	if err != nil {
		return err
	}
	slog.Info("Wage candles written", "count", len(out), "since", since, "until", until)
	return nil
}
