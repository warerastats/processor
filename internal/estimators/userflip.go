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

const userFlipName = "user_flip"

// userFlipWindow is the max hold time for a buy to count toward a flip.
const userFlipWindow = 12 * time.Hour

// UserFlip folds fungible trades into per-user FIFO buys matched to sells within 12h.
type UserFlip struct {
	Colls    *models.Collections
	interval time.Duration
	offset   time.Duration
}

// NewUserFlip builds the user-flip estimator job.
func NewUserFlip(colls *models.Collections, interval, offset time.Duration) *UserFlip {
	return &UserFlip{Colls: colls, interval: interval, offset: offset}
}

func (j *UserFlip) Name() string            { return userFlipName }
func (j *UserFlip) Interval() time.Duration { return j.interval }
func (j *UserFlip) Offset() time.Duration   { return j.offset }

// Run folds every trade since the watermark in resumable chunks.
func (j *UserFlip) Run(ctx context.Context) error {
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

// fold applies all trades in (since, until] to per-user FIFO flip state.
func (j *UserFlip) fold(ctx context.Context, since, until time.Time) error {
	trades, err := j.Colls.Transactions.TradeTransaction.GetRange(ctx, since, until)
	if err != nil {
		return err
	}
	if len(trades) == 0 {
		return nil
	}

	// realized holds sells already turned into flip events, so a reprocessed
	// chunk (crash before the watermark advanced) skips them idempotently.
	realized, err := j.Colls.Processed.Estimators.UserFlipEvent.ExistingIDs(ctx, since, until)
	if err != nil {
		return err
	}

	stateCache := map[bson.ObjectID]*estimators.UserFlipState{}
	loadState := func(id bson.ObjectID) *estimators.UserFlipState {
		if st, ok := stateCache[id]; ok {
			return st
		}
		st, ok, err := j.Colls.Processed.Estimators.UserFlipState.Get(ctx, id)
		if err != nil || !ok {
			st = &estimators.UserFlipState{UserID: id}
		}
		if st.OpenLots == nil {
			st.OpenLots = map[string][]estimators.UserFlipLot{}
		}
		stateCache[id] = st
		return st
	}

	for _, t := range trades {
		if t.Quantity <= 0 {
			continue
		}
		unit := t.Money / float64(t.Quantity)
		at := t.ID.Timestamp()

		// Buyer acquires a lot (deduped by trade id for idempotent replay).
		buyer := loadState(t.BuyerID)
		if !hasUserLot(buyer.OpenLots[t.ItemCode], t.ID) {
			buyer.OpenLots[t.ItemCode] = append(buyer.OpenLots[t.ItemCode], estimators.UserFlipLot{
				TradeID: t.ID, Quantity: t.Quantity, UnitPrice: unit, BoughtAt: at,
			})
		}

		// Seller realises a flip against eligible (<=12h old) buy lots.
		if realized[t.ID] {
			continue
		}
		seller := loadState(t.SellerID)
		lots := pruneExpired(seller.OpenLots[t.ItemCode], at)
		matchedQty, cost, oldest, rem := userFifoMatch(lots, t.Quantity)
		seller.OpenLots[t.ItemCode] = rem
		if matchedQty > 0 {
			revenue := unit * float64(matchedQty)
			profit := revenue - cost
			ev := estimators.UserFlipEvent{
				ID:          t.ID,
				UserID:      t.SellerID,
				ItemCode:    t.ItemCode,
				Quantity:    matchedQty,
				BuyCost:     cost,
				SellRevenue: revenue,
				Profit:      profit,
				HeldMs:      at.Sub(oldest).Milliseconds(),
				At:          at,
			}
			err = j.Colls.Processed.Estimators.UserFlipEvent.Upsert(ctx, ev)
			if err != nil {
				slog.Error("Failed upserting user flip event", "tradeId", t.ID.Hex(), "error", err)
			}
			seller.TotalFlips++
			seller.TotalProfit += profit
		}
	}

	now := time.Now().UTC()
	for id, st := range stateCache {
		st.UserID = id
		st.UpdatedAt = now
		err = j.Colls.Processed.Estimators.UserFlipState.Upsert(ctx, *st)
		if err != nil {
			slog.Error("Failed upserting user flip state", "userId", id.Hex(), "error", err)
		}
	}
	return nil
}

// hasUserLot reports whether a buy lot created by the given trade already exists.
func hasUserLot(lots []estimators.UserFlipLot, tradeID bson.ObjectID) bool {
	for i := range lots {
		if lots[i].TradeID == tradeID {
			return true
		}
	}
	return false
}

// pruneExpired drops buy lots older than the flip window relative to now.
func pruneExpired(lots []estimators.UserFlipLot, now time.Time) []estimators.UserFlipLot {
	cutoff := now.Add(-userFlipWindow)
	out := lots[:0]
	for _, l := range lots {
		if !l.BoughtAt.Before(cutoff) {
			out = append(out, l)
		}
	}
	return out
}

// userFifoMatch consumes up to qty from the front of lots, returning matched
// quantity, total buy cost, the oldest matched lot's time, and remaining lots.
func userFifoMatch(lots []estimators.UserFlipLot, qty int) (int, float64, time.Time, []estimators.UserFlipLot) {
	matched := 0
	var cost float64
	var oldest time.Time
	i := 0
	for i < len(lots) && qty > 0 {
		if matched == 0 {
			oldest = lots[i].BoughtAt
		}
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
	return matched, cost, oldest, lots[i:]
}
