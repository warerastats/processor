package reports

import (
	"context"
	"log/slog"
	"time"

	"github.com/warerastats/models/models"
	"github.com/warerastats/models/models/stores/processed/reports"
	"github.com/warerastats/models/models/stores/trackers"
)

// orderbookDepth is the maximum number of flattened price levels per side.
const orderbookDepth = 20

// effectiveSizes are the fill sizes priced against the book.
var effectiveSizes = []int{10, 100, 250, 1000, 3000}

// ItemMarket recomputes per-fungible-item market reports including the orderbook.
type ItemMarket struct {
	Colls    *models.Collections
	interval time.Duration
	offset   time.Duration
}

// NewItemMarket builds the item-market-report job.
func NewItemMarket(colls *models.Collections, interval, offset time.Duration) *ItemMarket {
	return &ItemMarket{Colls: colls, interval: interval, offset: offset}
}

func (j *ItemMarket) Name() string            { return "item_market_reports" }
func (j *ItemMarket) Interval() time.Duration { return j.interval }
func (j *ItemMarket) Offset() time.Duration   { return j.offset }

// Run rebuilds the report for every fungible item code.
func (j *ItemMarket) Run(ctx context.Context) error {
	codes, err := j.Colls.Transactions.TradeTransaction.DistinctItemCodes(ctx)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	stats, err := j.Colls.Processed.Candles.ItemCandle.StatsByItem(ctx, now.Add(-24*time.Hour), now)
	if err != nil {
		return err
	}
	statByCode := make(map[string]struct {
		open, close, high, low, money float64
		volume                        int
	}, len(stats))
	for _, s := range stats {
		statByCode[s.ItemCode] = struct {
			open, close, high, low, money float64
			volume                        int
		}{s.Open, s.Close, s.High, s.Low, s.Money, s.Volume}
	}

	for _, code := range codes {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err = j.report(ctx, code, statByCode[code], now)
		if err != nil {
			slog.Error("Failed item market report", "itemCode", code, "error", err)
		}
	}
	return nil
}

// report builds and stores one item's market report.
func (j *ItemMarket) report(ctx context.Context, code string, st struct {
	open, close, high, low, money float64
	volume                        int
}, now time.Time) error {
	bids, err := j.Colls.Trackers.TradeOffer.GetOpenOffers(ctx, code, "BUY", true)
	if err != nil {
		return err
	}
	asks, err := j.Colls.Trackers.TradeOffer.GetOpenOffers(ctx, code, "SELL", false)
	if err != nil {
		return err
	}

	avg := 0.0
	if st.volume > 0 {
		avg = st.money / float64(st.volume)
	}
	pct := 0.0
	if st.open > 0 {
		pct = (st.close - st.open) / st.open * 100
	}

	r := reports.ItemMarketReport{
		ItemCode:       code,
		Volume24h:      st.volume,
		AvgWeighted24h: avg,
		PctChange24h:   pct,
		Low24h:         st.low,
		High24h:        st.high,
		Bids:           levels(bids),
		Asks:           levels(asks),
		Spread:         spread(bids, asks),
		EffectiveBuy:   effective(asks),
		EffectiveSell:  effective(bids),
		UpdatedAt:      now,
	}
	return j.Colls.Processed.Reports.ItemMarketReport.Upsert(ctx, r)
}

// levels flattens up to orderbookDepth price levels.
func levels(offers []trackers.OpenOffer) []reports.OrderbookLevel {
	n := len(offers)
	if n > orderbookDepth {
		n = orderbookDepth
	}
	out := make([]reports.OrderbookLevel, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, reports.OrderbookLevel{Price: offers[i].Price, Quantity: offers[i].Remaining})
	}
	return out
}

// spread is best ask minus best bid, or 0 when either side is empty.
func spread(bids, asks []trackers.OpenOffer) float64 {
	if len(bids) == 0 || len(asks) == 0 {
		return 0
	}
	return asks[0].Price - bids[0].Price
}

// effective computes average fill price for each size, skipping sizes deeper
// than the available book.
func effective(side []trackers.OpenOffer) []reports.EffectivePrice {
	var out []reports.EffectivePrice
	for _, size := range effectiveSizes {
		remaining := size
		var cost float64
		ok := false
		for _, lvl := range side {
			take := lvl.Remaining
			if take > remaining {
				take = remaining
			}
			cost += float64(take) * lvl.Price
			remaining -= take
			if remaining == 0 {
				ok = true
				break
			}
		}
		if !ok {
			continue
		}
		out = append(out, reports.EffectivePrice{Size: size, AvgPrice: cost / float64(size)})
	}
	return out
}
