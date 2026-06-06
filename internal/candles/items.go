package candles

import (
	"context"
	"log/slog"
	"time"

	"github.com/warerastats/models/models"
)

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

// Run computes the candles for the most recent closed window.
func (j *Items) Run(ctx context.Context) error {
	// TODO: read the processor's candle state, aggregate trade_transactions
	// since the last closed 10-minute boundary into per-item OHLC candles,
	// and upsert them through a candle store in models/.
	_ = ctx
	slog.Info("Item candles pass (skeleton — not yet implemented)")
	return nil
}
