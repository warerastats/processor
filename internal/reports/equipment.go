package reports

import (
	"context"
	"log/slog"
	"time"

	"github.com/warerastats/models/models"
	"github.com/warerastats/models/models/stores/processed/reports"
	"github.com/warerastats/models/models/stores/transactions"
	"github.com/warerastats/processor/internal/pricing"
)

// equipmentWindows are the per-item summary windows in days.
var equipmentWindows = []int{3, 7, 14, 30}

// equipmentSkillWindows are the windows that additionally get per-skill rows.
var equipmentSkillWindows = []int{14, 30}

// Equipment recomputes per-item and per-skill equipment price summaries.
type Equipment struct {
	Colls    *models.Collections
	interval time.Duration
	offset   time.Duration
}

// NewEquipment builds the equipment-pricing job.
func NewEquipment(colls *models.Collections, interval, offset time.Duration) *Equipment {
	return &Equipment{Colls: colls, interval: interval, offset: offset}
}

func (j *Equipment) Name() string            { return "equipment_pricing" }
func (j *Equipment) Interval() time.Duration { return j.interval }
func (j *Equipment) Offset() time.Duration   { return j.offset }

// Run loads the widest window once and derives every window/skill summary from it.
func (j *Equipment) Run(ctx context.Context) error {
	now := time.Now().UTC()
	maxDays := equipmentWindows[len(equipmentWindows)-1]
	trades, err := j.Colls.Transactions.MarketTransaction.GetEquipmentTradesRange(
		ctx, now.Add(-time.Duration(maxDays)*24*time.Hour), now,
	)
	if err != nil {
		return err
	}
	if len(trades) == 0 {
		return nil
	}

	windowRows := j.windowRows(trades, now)
	err = j.Colls.Processed.Reports.EquipmentPricing.UpsertWindows(ctx, windowRows)
	if err != nil {
		return err
	}

	skillRows := j.skillRows(trades, now)
	err = j.Colls.Processed.Reports.EquipmentPricing.UpsertSkills(ctx, skillRows)
	if err != nil {
		return err
	}
	slog.Info("Equipment pricing updated", "windows", len(windowRows), "skills", len(skillRows))
	return nil
}

// windowRows builds per-item-code weighted prices for each window.
func (j *Equipment) windowRows(trades []transactions.EquipmentTrade, now time.Time) []reports.EquipmentWindowPrice {
	var out []reports.EquipmentWindowPrice
	for _, days := range equipmentWindows {
		cutoff := now.Add(-time.Duration(days) * 24 * time.Hour)
		sum := map[string]float64{}
		cnt := map[string]int{}
		for _, t := range trades {
			if t.At.Before(cutoff) {
				continue
			}
			sum[t.ItemCode] += t.Money
			cnt[t.ItemCode]++
		}
		for code, total := range sum {
			c := cnt[code]
			avg := 0.0
			if c > 0 {
				avg = total / float64(c)
			}
			out = append(out, reports.EquipmentWindowPrice{
				ID:          reports.EquipmentWindowPriceID(code, days),
				ItemCode:    code,
				WindowDays:  days,
				WeightedAvg: avg,
				Volume:      c,
				Count:       c,
				UpdatedAt:   now,
			})
		}
	}
	return out
}

// skillAccum accumulates per skill-combo price stats within a window.
type skillAccum struct {
	itemCode string
	skills   map[string]float64
	min      float64
	max      float64
	sum      float64
	count    int
}

// skillRows builds per-item, per-skill-combo price stats for the skill windows.
func (j *Equipment) skillRows(trades []transactions.EquipmentTrade, now time.Time) []reports.EquipmentSkillPrice {
	var out []reports.EquipmentSkillPrice
	for _, days := range equipmentSkillWindows {
		cutoff := now.Add(-time.Duration(days) * 24 * time.Hour)
		acc := map[string]*skillAccum{}
		for _, t := range trades {
			if t.At.Before(cutoff) {
				continue
			}
			sk := pricing.SkillKey(t.Skills)
			key := t.ItemCode + "@" + sk
			a := acc[key]
			if a == nil {
				a = &skillAccum{itemCode: t.ItemCode, skills: t.Skills, min: t.Money, max: t.Money}
				acc[key] = a
			}
			if t.Money < a.min {
				a.min = t.Money
			}
			if t.Money > a.max {
				a.max = t.Money
			}
			a.sum += t.Money
			a.count++
		}
		for _, a := range acc {
			avg := 0.0
			if a.count > 0 {
				avg = a.sum / float64(a.count)
			}
			sk := pricing.SkillKey(a.skills)
			out = append(out, reports.EquipmentSkillPrice{
				ID:         reports.EquipmentSkillPriceID(a.itemCode, days, sk),
				ItemCode:   a.itemCode,
				WindowDays: days,
				SkillKey:   sk,
				Skills:     a.skills,
				Min:        a.min,
				Max:        a.max,
				Avg:        avg,
				Volume:     a.count,
				UpdatedAt:  now,
			})
		}
	}
	return out
}
