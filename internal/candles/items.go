package candles

import (
	"context"
	"log/slog"
	"time"

	"github.com/warerastats/models/models"
	"github.com/warerastats/models/models/stores/processed/candles"
	"github.com/warerastats/processor/internal/window"
)

// itemCandleSize is the OHLC window width.
const itemCandleSize = 10 * time.Minute

// itemBackfillSpan bounds how much time one aggregation pass covers so a
// full-history catch-up stays resumable and memory-bounded.
const itemBackfillSpan = 48 * time.Hour

// Items aggregates trade_transactions into 10-minute OHLC candles per item code.
type Items struct {
	Colls    *models.Collections
	interval time.Duration
	offset   time.Duration
}

// NewItems builds the item-candle job.
func NewItems(colls *models.Collections, interval, offset time.Duration) *Items {
	return &Items{Colls: colls, interval: interval, offset: offset}
}

func (j *Items) Name() string            { return "item_candles" }
func (j *Items) Interval() time.Duration { return j.interval }
func (j *Items) Offset() time.Duration   { return j.offset }

// Run folds every closed 10-minute window since the watermark into candles.
func (j *Items) Run(ctx context.Context) error {
	state, err := j.Colls.Processed.States.JobState.Get(ctx, j.Name())
	if err != nil {
		return err
	}

	since := state.Boundary
	if since.IsZero() {
		earliest, ok, err := j.Colls.Transactions.TradeTransaction.EarliestTime(ctx)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		since = window.FloorUTC(earliest, itemCandleSize).Add(-time.Second)
	}

	target := window.ClosedBoundary(time.Now(), itemCandleSize).Add(itemCandleSize)

	for since.Before(target) {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		until := since.Add(itemBackfillSpan)
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
func (j *Items) process(ctx context.Context, since, until time.Time) error {
	buckets, err := j.Colls.Transactions.TradeTransaction.AggregateItemCandles(
		ctx, since, until, int(itemCandleSize/time.Minute),
	)
	if err != nil {
		return err
	}
	if len(buckets) == 0 {
		return nil
	}

	out := make([]candles.ItemCandle, 0, len(buckets))
	for _, b := range buckets {
		avg := 0.0
		if b.Volume > 0 {
			avg = b.Money / float64(b.Volume)
		}
		out = append(out, candles.ItemCandle{
			ID:          candles.ItemCandleID(b.Key.ItemCode, b.Key.BucketStart),
			ItemCode:    b.Key.ItemCode,
			BucketStart: b.Key.BucketStart.UTC(),
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

	err = j.Colls.Processed.Candles.ItemCandle.BulkUpsert(ctx, out)
	if err != nil {
		return err
	}
	slog.Info("Item candles written", "count", len(out), "since", since, "until", until)
	return nil
}
