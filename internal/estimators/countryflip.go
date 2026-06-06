package estimators

import (
	"context"
	"log/slog"
	"time"

	"github.com/warerastats/models/models"
	"github.com/warerastats/models/models/stores/processed/estimators"
	"github.com/warerastats/processor/internal/window"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// countryNonTracked items are consumed by countries, so trades in them are not
// treated as flips.
var countryNonTracked = map[string]bool{
	"petroleum": true,
	"steel":     true,
	"paper":     true,
}

const countryFlipName = "country_flip"

// flipBackfillSpan bounds one fold chunk so history catch-up stays resumable.
const flipBackfillSpan = 24 * time.Hour

// CountryFlip folds fungible trades into per-country FIFO inventory and realised flips.
type CountryFlip struct {
	Colls    *models.Collections
	interval time.Duration
	offset   time.Duration
}

// NewCountryFlip builds the country-flip estimator job.
func NewCountryFlip(colls *models.Collections, interval, offset time.Duration) *CountryFlip {
	return &CountryFlip{Colls: colls, interval: interval, offset: offset}
}

func (j *CountryFlip) Name() string            { return countryFlipName }
func (j *CountryFlip) Interval() time.Duration { return j.interval }
func (j *CountryFlip) Offset() time.Duration   { return j.offset }

// Run folds every trade since the watermark in resumable chunks.
func (j *CountryFlip) Run(ctx context.Context) error {
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
		since = earliest.Add(-time.Second)
	}
	target := window.ClosedBoundary(time.Now(), time.Minute)

	for since.Before(target) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		until := since.Add(flipBackfillSpan)
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

// fold applies all trades in (since, until] to per-country FIFO state.
func (j *CountryFlip) fold(ctx context.Context, since, until time.Time) error {
	trades, err := j.Colls.Transactions.TradeTransaction.GetRange(ctx, since, until)
	if err != nil {
		return err
	}
	if len(trades) == 0 {
		return nil
	}

	// realized holds sells already turned into flip events, so a reprocessed
	// chunk (crash before the watermark advanced) skips them idempotently.
	realized, err := j.Colls.Processed.Estimators.CountryFlipEvent.ExistingIDs(ctx, since, until)
	if err != nil {
		return err
	}

	invCache := map[bson.ObjectID]map[string][]estimators.InventoryLot{}
	stateCache := map[bson.ObjectID]*estimators.CountryFlipState{}

	loadInv := func(id bson.ObjectID) map[string][]estimators.InventoryLot {
		if m, ok := invCache[id]; ok {
			return m
		}
		inv, ok, err := j.Colls.Processed.Estimators.CountryInventory.Get(ctx, id)
		m := map[string][]estimators.InventoryLot{}
		if err == nil && ok && inv.Lots != nil {
			m = inv.Lots
		}
		invCache[id] = m
		return m
	}
	loadState := func(id bson.ObjectID) *estimators.CountryFlipState {
		if st, ok := stateCache[id]; ok {
			return st
		}
		st, ok, err := j.Colls.Processed.Estimators.CountryFlipState.Get(ctx, id)
		if err != nil || !ok {
			st = &estimators.CountryFlipState{CountryID: id}
		}
		stateCache[id] = st
		return st
	}

	for _, t := range trades {
		if t.Quantity <= 0 || countryNonTracked[t.ItemCode] {
			continue
		}
		unit := t.Money / float64(t.Quantity)
		at := t.ID.Timestamp()

		if t.BuyerCountryID != nil {
			inv := loadInv(*t.BuyerCountryID)
			if !hasLot(inv[t.ItemCode], t.ID) {
				inv[t.ItemCode] = append(inv[t.ItemCode], estimators.InventoryLot{
					TradeID: t.ID, Quantity: t.Quantity, UnitPrice: unit, BoughtAt: at,
				})
			}
		}
		if t.SellerCountryID != nil && !realized[t.ID] {
			inv := loadInv(*t.SellerCountryID)
			matchedQty, cost, lots := fifoMatch(inv[t.ItemCode], t.Quantity)
			inv[t.ItemCode] = lots
			if matchedQty > 0 {
				revenue := unit * float64(matchedQty)
				profit := revenue - cost
				ev := estimators.CountryFlipEvent{
					ID:          t.ID,
					CountryID:   *t.SellerCountryID,
					ItemCode:    t.ItemCode,
					Quantity:    matchedQty,
					BuyCost:     cost,
					SellRevenue: revenue,
					Profit:      profit,
					At:          at,
				}
				err = j.Colls.Processed.Estimators.CountryFlipEvent.Upsert(ctx, ev)
				if err != nil {
					slog.Error("Failed upserting country flip event", "tradeId", t.ID.Hex(), "error", err)
				}
				st := loadState(*t.SellerCountryID)
				st.TotalTrades++
				if profit > 0 {
					st.Profitable++
				}
				st.TotalProfit += profit
			}
		}
	}

	now := time.Now().UTC()
	for id, lots := range invCache {
		err = j.Colls.Processed.Estimators.CountryInventory.Upsert(ctx, estimators.CountryInventory{
			CountryID: id, Lots: lots, UpdatedAt: now,
		})
		if err != nil {
			slog.Error("Failed upserting country inventory", "countryId", id.Hex(), "error", err)
		}
	}
	for id, st := range stateCache {
		st.UpdatedAt = now
		err = j.Colls.Processed.Estimators.CountryFlipState.Upsert(ctx, *st)
		if err != nil {
			slog.Error("Failed upserting country flip state", "countryId", id.Hex(), "error", err)
		}
	}
	return nil
}

// hasLot reports whether a lot created by the given trade is already present.
func hasLot(lots []estimators.InventoryLot, tradeID bson.ObjectID) bool {
	for i := range lots {
		if lots[i].TradeID == tradeID {
			return true
		}
	}
	return false
}

// fifoMatch consumes up to qty from the front of lots, returning the matched
// quantity, its total buy cost, and the remaining lots.
func fifoMatch(lots []estimators.InventoryLot, qty int) (int, float64, []estimators.InventoryLot) {
	matched := 0
	var cost float64
	i := 0
	for i < len(lots) && qty > 0 {
		take := lots[i].Quantity
		if take > qty {
			take = qty
		}
		cost += float64(take) * lots[i].UnitPrice
		matched += take
		qty -= take
		lots[i].Quantity -= take
		if lots[i].Quantity == 0 {
			i++
		}
	}
	return matched, cost, lots[i:]
}
