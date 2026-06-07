package reports

import (
	"context"
	"log/slog"
	"time"

	"github.com/warerastats/models/models"
	"github.com/warerastats/models/models/stores/processed/reports"
	"github.com/warerastats/models/models/stores/trackers"
	"github.com/warerastats/processor/internal/pricing"
	"go.mongodb.org/mongo-driver/v2/bson"
	"golang.org/x/sync/errgroup"
)

// battleInterval is the reporting bucket width within a battle.
const battleInterval = 2 * time.Minute

// BattleDamage recomputes per-battle, per-2-minute, per-entity damage reports.
type BattleDamage struct {
	Colls    *models.Collections
	interval time.Duration
	offset   time.Duration
	workers  int
}

// NewBattleDamage builds the per-battle damage report job.
func NewBattleDamage(colls *models.Collections, interval, offset time.Duration, workers int) *BattleDamage {
	return &BattleDamage{Colls: colls, interval: interval, offset: offset, workers: workers}
}

func (j *BattleDamage) Name() string            { return "battle_damage_reports" }
func (j *BattleDamage) Interval() time.Duration { return j.interval }
func (j *BattleDamage) Offset() time.Duration   { return j.offset }

// Run recomputes reports for every active or recently-ended battle.
func (j *BattleDamage) Run(ctx context.Context) error {
	battles, err := j.Colls.Trackers.Battle.GetReportable(ctx, time.Now().Add(-2*j.interval))
	if err != nil {
		return err
	}
	for i := range battles {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err = j.processBattle(ctx, battles[i].ID)
		if err != nil {
			slog.Error("Failed battle damage report", "battleId", battles[i].ID.Hex(), "error", err)
		}
	}
	return nil
}

// Backfill generates reports for ended battles that have no report rows yet,
// recovering battles that aged out of the live reportable window.
func (j *BattleDamage) Backfill(ctx context.Context) error {
	inactive, err := j.Colls.Trackers.Battle.GetInactiveIDs(ctx)
	if err != nil {
		return err
	}
	reported, err := j.Colls.Processed.Reports.BattleDamageReport.BattleIDsWithReports(ctx)
	if err != nil {
		return err
	}
	have := make(map[bson.ObjectID]struct{}, len(reported))
	for _, id := range reported {
		have[id] = struct{}{}
	}
	missing := make([]bson.ObjectID, 0)
	for _, id := range inactive {
		if _, ok := have[id]; !ok {
			missing = append(missing, id)
		}
	}
	if len(missing) == 0 {
		slog.Info("Battle damage backfill: nothing to do")
		return nil
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(j.workers)
	for i := range missing {
		battleID := missing[i]
		g.Go(func() error {
			if gctx.Err() != nil {
				return gctx.Err()
			}
			err := j.processBattle(gctx, battleID)
			if err != nil {
				slog.Error("Failed battle damage backfill", "battleId", battleID.Hex(), "error", err)
			}
			return nil
		})
	}
	err = g.Wait()
	if err != nil {
		return err
	}
	slog.Info("Battle damage backfill complete", "battles", len(missing))
	return nil
}

// entityKey identifies one reporting entity within an interval and side.
type entityKey struct {
	interval time.Time
	side     string
	kind     string
	id       bson.ObjectID
}

// sideKey identifies one (interval, side) damage total bucket.
type sideKey struct {
	interval time.Time
	side     string
}

// processBattle buckets a battle's damage by 2-minute interval, side, and entity.
func (j *BattleDamage) processBattle(ctx context.Context, battleID bson.ObjectID) error {
	damages, err := j.Colls.Trackers.Damage.GetByBattle(ctx, battleID)
	if err != nil {
		return err
	}
	if len(damages) == 0 {
		return nil
	}

	codeByItem, err := j.resolveEquipment(ctx, damages)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	prices, err := pricing.LoadItemAverages(ctx, j.Colls, now.Add(-14*24*time.Hour), now)
	if err != nil {
		return err
	}

	sideTotals := map[sideKey]int64{}
	entityDamage := map[entityKey]int64{}
	entityEquip := map[entityKey]map[string]float64{}

	for i := range damages {
		d := damages[i]
		start := d.At.UTC().Truncate(battleInterval)
		side := string(d.Side)
		sideTotals[sideKey{start, side}] += int64(d.Damages)

		add := func(kind string, id bson.ObjectID) {
			ek := entityKey{start, side, kind, id}
			entityDamage[ek] += int64(d.Damages)
			equip := entityEquip[ek]
			if equip == nil {
				equip = map[string]float64{}
				entityEquip[ek] = equip
			}
			for _, itemID := range equipmentIDs(d) {
				if code, ok := codeByItem[itemID]; ok {
					equip[code]++
				}
			}
		}
		add("user", d.UserID)
		add("country", d.CountryID)
		if d.MuID != nil {
			add("mu", *d.MuID)
		}
		if d.PartyID != nil {
			add("party", *d.PartyID)
		}
	}

	rows := make([]reports.BattleDamageReport, 0, len(entityDamage))
	for ek, dmg := range entityDamage {
		total := sideTotals[sideKey{ek.interval, ek.side}]
		pct := 0.0
		if total > 0 {
			pct = float64(dmg) / float64(total) * 100
		}
		rows = append(rows, reports.BattleDamageReport{
			ID:            reports.BattleDamageReportID(battleID, ek.interval, ek.side, ek.kind, ek.id),
			BattleID:      battleID,
			IntervalStart: ek.interval,
			Side:          ek.side,
			EntityType:    ek.kind,
			EntityID:      ek.id,
			Damage:        dmg,
			DamagePct:     pct,
			Equipment:     equipmentUsage(entityEquip[ek], prices),
		})
	}

	err = j.Colls.Processed.Reports.BattleDamageReport.Upsert(ctx, rows)
	if err != nil {
		return err
	}
	slog.Info("Battle damage report", "battleId", battleID.Hex(), "rows", len(rows))
	return nil
}

// equipmentIDs returns the equipped item ids referenced by a damage row.
func equipmentIDs(d trackers.Damage) []bson.ObjectID {
	out := make([]bson.ObjectID, 0, 6)
	for _, p := range []*bson.ObjectID{d.WeaponID, d.HelmetID, d.ChestID, d.PantsID, d.BootsID, d.GlovesID} {
		if p != nil {
			out = append(out, *p)
		}
	}
	return out
}

// resolveEquipment batch-resolves every equipped item id in the battle to its code.
func (j *BattleDamage) resolveEquipment(ctx context.Context, damages []trackers.Damage) (map[bson.ObjectID]string, error) {
	idset := map[bson.ObjectID]struct{}{}
	for i := range damages {
		for _, id := range equipmentIDs(damages[i]) {
			idset[id] = struct{}{}
		}
	}
	ids := make([]bson.ObjectID, 0, len(idset))
	for id := range idset {
		ids = append(ids, id)
	}
	items, err := j.Colls.Trackers.Item.GetMany(ctx, ids)
	if err != nil {
		return nil, err
	}
	out := make(map[bson.ObjectID]string, len(items))
	for i := range items {
		out[items[i].ID] = items[i].ItemCode
	}
	return out, nil
}

// equipmentUsage converts a code->count map into priced usage rows.
func equipmentUsage(counts map[string]float64, prices pricing.ItemAverages) []reports.EquipmentUsage {
	if len(counts) == 0 {
		return nil
	}
	out := make([]reports.EquipmentUsage, 0, len(counts))
	for code, count := range counts {
		out = append(out, reports.EquipmentUsage{
			ItemCode: code,
			Count:    count,
			Value:    count * prices.Avg[code],
		})
	}
	return out
}
