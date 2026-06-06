package reports

import (
	"context"
	"log/slog"
	"sort"
	"time"

	"github.com/warerastats/models/models"
	"github.com/warerastats/models/models/stores/processed/reports"
	"github.com/warerastats/processor/internal/pricing"
	"github.com/warerastats/processor/internal/window"
)

const casesName = "cases_report"

// casesSpan bounds one fold chunk for resumable history catch-up.
const casesSpan = 24 * time.Hour

// Cases accumulates all-time case drop stats and rolling 14d valuations.
type Cases struct {
	Colls    *models.Collections
	interval time.Duration
	offset   time.Duration
}

// NewCases builds the cases-report job.
func NewCases(colls *models.Collections, interval, offset time.Duration) *Cases {
	return &Cases{Colls: colls, interval: interval, offset: offset}
}

func (j *Cases) Name() string            { return casesName }
func (j *Cases) Interval() time.Duration { return j.interval }
func (j *Cases) Offset() time.Duration   { return j.offset }

// Run folds new drops into all-time counts then refreshes 14d valuations.
func (j *Cases) Run(ctx context.Context) error {
	state, err := j.Colls.Processed.States.JobState.Get(ctx, j.Name())
	if err != nil {
		return err
	}

	since := state.Boundary
	if since.IsZero() {
		earliest, ok, err := j.Colls.Transactions.CaseTransaction.EarliestTime(ctx)
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
		until := since.Add(casesSpan)
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

	return j.refresh14d(ctx)
}

// caseAccum is the mutable in-memory form of a CasesReport during a fold.
type caseAccum struct {
	totalOpened int
	perItem     map[string]*itemAccum
}

type itemAccum struct {
	totalDrops int
	perSkill   map[string]int
}

// fold accumulates drops in (since, until] into the per-case stored reports.
func (j *Cases) fold(ctx context.Context, since, until time.Time) error {
	drops, err := j.Colls.Transactions.CaseTransaction.GetDropsRange(ctx, since, until)
	if err != nil {
		return err
	}
	if len(drops) == 0 {
		return nil
	}

	byCase := map[string]*caseAccum{}
	for _, d := range drops {
		ca := byCase[d.Case]
		if ca == nil {
			ca = &caseAccum{perItem: map[string]*itemAccum{}}
			byCase[d.Case] = ca
		}
		ca.totalOpened++
		ia := ca.perItem[d.ItemCode]
		if ia == nil {
			ia = &itemAccum{perSkill: map[string]int{}}
			ca.perItem[d.ItemCode] = ia
		}
		ia.totalDrops++
		ia.perSkill[pricing.SkillKey(d.Skills)]++
	}

	for caseCode, ca := range byCase {
		err = j.mergeCase(ctx, caseCode, ca)
		if err != nil {
			return err
		}
	}
	return nil
}

// mergeCase merges a fold accumulator into the stored all-time report.
func (j *Cases) mergeCase(ctx context.Context, caseCode string, ca *caseAccum) error {
	existing, ok, err := j.Colls.Processed.Reports.CasesReport.Get(ctx, caseCode)
	if err != nil {
		return err
	}

	stat := map[string]*reports.CaseItemStat{}
	report := reports.CasesReport{Case: caseCode}
	if ok {
		report = *existing
		for i := range existing.PerItem {
			s := existing.PerItem[i]
			stat[s.ItemCode] = &reports.CaseItemStat{
				ItemCode:     s.ItemCode,
				TotalDrops:   s.TotalDrops,
				PerSkillRoll: cloneIntMap(s.PerSkillRoll),
			}
		}
	}

	report.TotalOpened += ca.totalOpened
	for code, ia := range ca.perItem {
		s := stat[code]
		if s == nil {
			s = &reports.CaseItemStat{ItemCode: code, PerSkillRoll: map[string]int{}}
			stat[code] = s
		}
		s.TotalDrops += ia.totalDrops
		for sk, c := range ia.perSkill {
			s.PerSkillRoll[sk] += c
		}
	}

	report.PerItem = report.PerItem[:0]
	report.UniqueItemCodes = report.UniqueItemCodes[:0]
	for _, s := range stat {
		report.PerItem = append(report.PerItem, *s)
		report.UniqueItemCodes = append(report.UniqueItemCodes, s.ItemCode)
	}
	sort.Strings(report.UniqueItemCodes)
	report.UpdatedAt = time.Now().UTC()
	return j.Colls.Processed.Reports.CasesReport.Upsert(ctx, report)
}

// refresh14d recomputes rolling 14d valuations for each stored case report.
func (j *Cases) refresh14d(ctx context.Context) error {
	now := time.Now().UTC()
	since := now.Add(-14 * 24 * time.Hour)
	drops, err := j.Colls.Transactions.CaseTransaction.GetDropsRange(ctx, since, now)
	if err != nil {
		return err
	}
	prices, err := pricing.LoadItemAverages(ctx, j.Colls, since, now)
	if err != nil {
		return err
	}

	type dist struct {
		total   int
		perItem map[string]int
	}
	byCase := map[string]*dist{}
	for _, d := range drops {
		c := byCase[d.Case]
		if c == nil {
			c = &dist{perItem: map[string]int{}}
			byCase[d.Case] = c
		}
		c.total++
		c.perItem[d.ItemCode]++
	}

	for caseCode, dst := range byCase {
		existing, ok, err := j.Colls.Processed.Reports.CasesReport.Get(ctx, caseCode)
		if err != nil || !ok {
			continue
		}
		var expected float64
		for code, n := range dst.perItem {
			price := prices.Avg[code]
			if dst.total > 0 {
				expected += float64(n) / float64(dst.total) * price
			}
		}
		existing.ExpectedValue14d = expected
		for i := range existing.PerItem {
			existing.PerItem[i].AvgWeighted14d = prices.Avg[existing.PerItem[i].ItemCode]
		}
		err = j.Colls.Processed.Reports.CasesReport.Upsert(ctx, *existing)
		if err != nil {
			slog.Error("Failed refreshing case 14d", "case", caseCode, "error", err)
		}
	}
	return nil
}

// cloneIntMap returns a shallow copy of a string->int map.
func cloneIntMap(m map[string]int) map[string]int {
	out := make(map[string]int, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
