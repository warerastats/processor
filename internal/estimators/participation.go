package estimators

import (
	"context"
	"log/slog"
	"sort"
	"time"

	"github.com/warerastats/models/models"
	"github.com/warerastats/models/models/stores/trackers"
	"github.com/warerastats/processor/internal/pricing"
	"github.com/warerastats/processor/internal/window"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const participationName = "battle_participation"

// participationSpan bounds one damage-fold chunk for resumable history catch-up.
const participationSpan = 24 * time.Hour

// participationBattleBatch caps how many newly-finalized battles are counted per
// pass so a fresh-deploy backfill stays bounded; the counted-battle marker makes
// the remainder resumable on later passes.
const participationBattleBatch = 200

// Participation folds battle damage and dismantles into per-user counters.
type Participation struct {
	Colls    *models.Collections
	interval time.Duration
	offset   time.Duration
}

// NewParticipation builds the battle-participation estimator job.
func NewParticipation(colls *models.Collections, interval, offset time.Duration) *Participation {
	return &Participation{Colls: colls, interval: interval, offset: offset}
}

func (j *Participation) Name() string            { return participationName }
func (j *Participation) Interval() time.Duration { return j.interval }
func (j *Participation) Offset() time.Duration   { return j.offset }

// Run folds new damage/dismantle activity since the watermark in chunks.
func (j *Participation) Run(ctx context.Context) error {
	state, err := j.Colls.Processed.States.JobState.Get(ctx, j.Name())
	if err != nil {
		return err
	}

	since := state.Boundary
	if since.IsZero() {
		earliest, ok, err := j.Colls.Trackers.Damage.EarliestDamageTime(ctx)
		if err != nil {
			return err
		}
		if !ok {
			return j.countFinalized(ctx)
		}
		since = earliest.Add(-time.Second)
	}
	target := window.ClosedBoundary(time.Now(), time.Minute)

	for since.Before(target) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		until := since.Add(participationSpan)
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

	return j.countFinalized(ctx)
}

// fold adds damage and dismantle totals for one (since, until] window via $inc,
// so each row contributes exactly once and the count step can $inc independently.
func (j *Participation) fold(ctx context.Context, since, until time.Time) error {
	damages, err := j.Colls.Trackers.Damage.GetRange(ctx, since, until)
	if err != nil {
		return err
	}
	dismantles, err := j.Colls.Transactions.DismantleTransaction.GetRange(ctx, since, until)
	if err != nil {
		return err
	}
	if len(damages) == 0 && len(dismantles) == 0 {
		return nil
	}

	battles, err := j.loadBattles(ctx, damages)
	if err != nil {
		return err
	}
	prices, err := pricing.LoadItemAverages(ctx, j.Colls, until.Add(-14*24*time.Hour), until)
	if err != nil {
		return err
	}

	type dmgAcc struct {
		total    int64
		negative int64
	}
	dmg := map[bson.ObjectID]*dmgAcc{}
	for _, d := range damages {
		a := dmg[d.UserID]
		if a == nil {
			a = &dmgAcc{}
			dmg[d.UserID] = a
		}
		a.total += int64(d.Damages)
		if b, ok := battles[d.BattleID]; ok && isNegativeDamage(d, b) {
			a.negative += int64(d.Damages)
		}
	}

	type disAcc struct {
		value  float64
		counts map[string]float64
	}
	dis := map[bson.ObjectID]*disAcc{}
	for _, dm := range dismantles {
		a := dis[dm.UserID]
		if a == nil {
			a = &disAcc{counts: map[string]float64{}}
			dis[dm.UserID] = a
		}
		frac := float64(dm.State) / 100.0
		a.counts[dm.ItemCode] += frac
		a.value += frac * prices.Avg[dm.ItemCode]
	}

	for id, a := range dmg {
		err = j.Colls.Processed.Estimators.BattleParticipation.IncrementDamage(ctx, id, a.total, a.negative)
		if err != nil {
			slog.Error("Failed incrementing participation damage", "userId", id.Hex(), "error", err)
		}
	}
	for id, a := range dis {
		err = j.Colls.Processed.Estimators.BattleParticipation.IncrementDismantle(ctx, id, a.value, a.counts)
		if err != nil {
			slog.Error("Failed incrementing participation dismantle", "userId", id.Hex(), "error", err)
		}
	}
	return nil
}

// countFinalized counts distinct-battle participation once per finalized battle
// not yet counted, using the counted-battle marker for exactly-once semantics.
func (j *Participation) countFinalized(ctx context.Context) error {
	finalized, err := j.Colls.Trackers.Battle.GetFinalized(ctx)
	if err != nil {
		return err
	}
	if len(finalized) == 0 {
		return nil
	}
	counted, err := j.Colls.Processed.States.CountedBattle.ExistingAmong(ctx, finalized)
	if err != nil {
		return err
	}

	uncounted := make([]bson.ObjectID, 0, len(finalized))
	for _, id := range finalized {
		if !counted[id] {
			uncounted = append(uncounted, id)
		}
	}
	if len(uncounted) == 0 {
		return nil
	}
	// Process oldest-first, bounded per pass; markers make this resumable.
	sort.Slice(uncounted, func(a, b int) bool {
		return uncounted[a].Timestamp().Before(uncounted[b].Timestamp())
	})
	if len(uncounted) > participationBattleBatch {
		uncounted = uncounted[:participationBattleBatch]
	}

	countryDenom, muDenom, err := j.finalizedDenominators(ctx)
	if err != nil {
		return err
	}
	muOrders, err := j.muOrdersByBattle(ctx, uncounted)
	if err != nil {
		return err
	}
	battles, err := j.battlesByID(ctx, uncounted)
	if err != nil {
		return err
	}

	type cAcc struct {
		battles    int
		ownCountry int
		muOrder    int
		country    bson.ObjectID
		mu         *bson.ObjectID
	}
	acc := map[bson.ObjectID]*cAcc{}

	for _, battleID := range uncounted {
		damages, err := j.Colls.Trackers.Damage.GetByBattle(ctx, battleID)
		if err != nil {
			return err
		}
		b, hasBattle := battles[battleID]
		seen := map[bson.ObjectID]bool{}
		for _, d := range damages {
			if seen[d.UserID] {
				continue
			}
			seen[d.UserID] = true
			a := acc[d.UserID]
			if a == nil {
				a = &cAcc{}
				acc[d.UserID] = a
			}
			a.battles++
			a.country = d.CountryID
			a.mu = d.MuID
			if hasBattle && participatedOwnCountry(d.CountryID, b) {
				a.ownCountry++
			}
			if d.MuID != nil && muOrders[battleID][*d.MuID] {
				a.muOrder++
			}
		}
	}

	for id, a := range acc {
		muOrderBattles := 0
		if a.mu != nil {
			muOrderBattles = muDenom[*a.mu]
		}
		err = j.Colls.Processed.Estimators.BattleParticipation.IncrementBattleCounters(
			ctx, id, a.battles, a.ownCountry, a.muOrder, countryDenom[a.country], muOrderBattles,
		)
		if err != nil {
			slog.Error("Failed incrementing participation counters", "userId", id.Hex(), "error", err)
		}
	}

	err = j.Colls.Processed.States.CountedBattle.Mark(ctx, uncounted)
	if err != nil {
		return err
	}
	slog.Info("Participation counted finalized battles", "battles", len(uncounted), "users", len(acc))
	return nil
}

// finalizedDenominators returns per-country and per-MU finalized battle counts.
func (j *Participation) finalizedDenominators(ctx context.Context) (map[bson.ObjectID]int, map[bson.ObjectID]int, error) {
	countryRows, err := j.Colls.Trackers.Battle.CountByCountryFinalized(ctx)
	if err != nil {
		return nil, nil, err
	}
	countryDenom := make(map[bson.ObjectID]int, len(countryRows))
	for _, r := range countryRows {
		countryDenom[r.CountryID] = r.MemberCount
	}

	muRows, err := j.Colls.Events.BattleOrderChange.CountByMuFinalized(ctx)
	if err != nil {
		return nil, nil, err
	}
	muDenom := make(map[bson.ObjectID]int, len(muRows))
	for _, r := range muRows {
		muDenom[r.MuID] = r.Battles
	}
	return countryDenom, muDenom, nil
}

// muOrdersByBattle maps each given battle to the set of MUs that set an order.
func (j *Participation) muOrdersByBattle(ctx context.Context, battleIDs []bson.ObjectID) (map[bson.ObjectID]map[bson.ObjectID]bool, error) {
	pairs, err := j.Colls.Events.BattleOrderChange.MuOrdersByBattles(ctx, battleIDs)
	if err != nil {
		return nil, err
	}
	out := map[bson.ObjectID]map[bson.ObjectID]bool{}
	for _, p := range pairs {
		if out[p.BattleID] == nil {
			out[p.BattleID] = map[bson.ObjectID]bool{}
		}
		out[p.BattleID][p.MuID] = true
	}
	return out, nil
}

// battlesByID loads the given battles keyed by id.
func (j *Participation) battlesByID(ctx context.Context, ids []bson.ObjectID) (map[bson.ObjectID]trackers.Battle, error) {
	list, err := j.Colls.Trackers.Battle.GetMany(ctx, ids)
	if err != nil {
		return nil, err
	}
	out := make(map[bson.ObjectID]trackers.Battle, len(list))
	for _, b := range list {
		out[b.ID] = b
	}
	return out, nil
}

// loadBattles fetches the battles referenced by a window of damage rows.
func (j *Participation) loadBattles(ctx context.Context, damages []trackers.Damage) (map[bson.ObjectID]trackers.Battle, error) {
	idset := map[bson.ObjectID]struct{}{}
	for _, d := range damages {
		idset[d.BattleID] = struct{}{}
	}
	ids := make([]bson.ObjectID, 0, len(idset))
	for id := range idset {
		ids = append(ids, id)
	}
	return j.battlesByID(ctx, ids)
}

// isNegativeDamage reports whether a user dealt damage against their own country.
func isNegativeDamage(d trackers.Damage, b trackers.Battle) bool {
	var target bson.ObjectID
	if d.Side == "ATTACKER" {
		target = b.DefenderCountryID
	} else {
		target = b.AttackerCountryID
	}
	return target == d.CountryID
}

// participatedOwnCountry reports whether a user's own country fought in a battle.
func participatedOwnCountry(country bson.ObjectID, b trackers.Battle) bool {
	return b.AttackerCountryID == country || b.DefenderCountryID == country
}
