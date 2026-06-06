package reports

import (
	"context"
	"log/slog"
	"time"

	"github.com/warerastats/models/models"
	"github.com/warerastats/models/models/stores/processed/reports"
	"github.com/warerastats/models/models/stores/transactions"
	"github.com/warerastats/processor/internal/pricing"
	"github.com/warerastats/processor/internal/window"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const financeName = "user_finance_report"

// Finance computes per-user daily income, spending, and equipment activity.
type Finance struct {
	Colls    *models.Collections
	interval time.Duration
	offset   time.Duration
}

// NewFinance builds the user-finance-report job.
func NewFinance(colls *models.Collections, interval, offset time.Duration) *Finance {
	return &Finance{Colls: colls, interval: interval, offset: offset}
}

func (j *Finance) Name() string            { return financeName }
func (j *Finance) Interval() time.Duration { return j.interval }
func (j *Finance) Offset() time.Duration   { return j.offset }

// Run computes every closed day since the watermark.
func (j *Finance) Run(ctx context.Context) error {
	state, err := j.Colls.Processed.States.JobState.Get(ctx, j.Name())
	if err != nil {
		return err
	}

	day := 24 * time.Hour
	lastClosed := window.FloorUTC(time.Now(), day).Add(-day)

	var from time.Time
	if state.Boundary.IsZero() {
		earliest, ok, err := j.Colls.Transactions.WageTransaction.EarliestTime(ctx)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		from = window.FloorUTC(earliest, day)
	} else {
		from = state.Boundary.Add(day)
	}

	for d := from; !d.After(lastClosed); d = d.Add(day) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err = j.computeDay(ctx, d)
		if err != nil {
			return err
		}
		err = j.Colls.Processed.States.JobState.SetWatermark(ctx, j.Name(), d, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

// computeDay builds and stores all user finance reports for the day at d.
func (j *Finance) computeDay(ctx context.Context, d time.Time) error {
	end := d.Add(24 * time.Hour)
	rows := map[bson.ObjectID]*reports.UserFinanceReport{}
	get := func(id bson.ObjectID) *reports.UserFinanceReport {
		r := rows[id]
		if r == nil {
			r = &reports.UserFinanceReport{
				ID:       reports.UserFinanceReportID(id, d),
				UserID:   id,
				DayStart: d,
			}
			rows[id] = r
		}
		return r
	}

	apply := func(list []transactions.IDTotal, f func(*reports.UserFinanceReport, transactions.IDTotal)) {
		for _, t := range list {
			f(get(t.ID), t)
		}
	}

	paid, err := j.Colls.Transactions.WageTransaction.PaidByEmployer(ctx, d, end)
	if err != nil {
		return err
	}
	apply(paid, func(r *reports.UserFinanceReport, t transactions.IDTotal) { r.WagesPaid = t.Total })

	earned, err := j.Colls.Transactions.WageTransaction.EarnedByEmployee(ctx, d, end)
	if err != nil {
		return err
	}
	apply(earned, func(r *reports.UserFinanceReport, t transactions.IDTotal) { r.WagesEarned = t.Total })

	bought, err := j.Colls.Transactions.TradeTransaction.MoneyByField(ctx, "buyerId", d, end)
	if err != nil {
		return err
	}
	apply(bought, func(r *reports.UserFinanceReport, t transactions.IDTotal) { r.ItemsBought = t.Total })

	sold, err := j.Colls.Transactions.TradeTransaction.MoneyByField(ctx, "sellerId", d, end)
	if err != nil {
		return err
	}
	apply(sold, func(r *reports.UserFinanceReport, t transactions.IDTotal) { r.ItemsSold = t.Total })

	eqBought, err := j.Colls.Transactions.MarketTransaction.MoneyByField(ctx, "buyerId", d, end)
	if err != nil {
		return err
	}
	apply(eqBought, func(r *reports.UserFinanceReport, t transactions.IDTotal) { r.EquipBought = t.Total })

	eqSold, err := j.Colls.Transactions.MarketTransaction.MoneyByField(ctx, "sellerId", d, end)
	if err != nil {
		return err
	}
	apply(eqSold, func(r *reports.UserFinanceReport, t transactions.IDTotal) { r.EquipSold = t.Total })

	cases, err := j.Colls.Transactions.CaseTransaction.CountByUser(ctx, d, end)
	if err != nil {
		return err
	}
	apply(cases, func(r *reports.UserFinanceReport, t transactions.IDTotal) { r.CasesOpened = t.Count })

	prices, err := pricing.LoadItemAverages(ctx, j.Colls, end.Add(-14*24*time.Hour), end)
	if err != nil {
		return err
	}

	err = j.applyDismantles(ctx, d, end, prices, get)
	if err != nil {
		return err
	}

	err = j.applyCasesNet(ctx, d, end, prices, get)
	if err != nil {
		return err
	}

	out := make([]reports.UserFinanceReport, 0, len(rows))
	for _, r := range rows {
		out = append(out, *r)
	}
	err = j.Colls.Processed.Reports.UserFinanceReport.Upsert(ctx, out)
	if err != nil {
		return err
	}
	slog.Info("User finance reports", "day", d, "users", len(out))
	return nil
}

// applyDismantles adds the day's dismantled equipment value per user.
func (j *Finance) applyDismantles(ctx context.Context, d, end time.Time, prices pricing.ItemAverages, get func(bson.ObjectID) *reports.UserFinanceReport) error {
	rows, err := j.Colls.Transactions.DismantleTransaction.GetRange(ctx, d, end)
	if err != nil {
		return err
	}
	for _, dm := range rows {
		frac := float64(dm.State) / 100.0
		get(dm.UserID).ValueDismantled += frac * prices.Avg[dm.ItemCode]
	}
	return nil
}

// applyCasesNet sets each user's signed case profit/loss: dropped-equipment
// value (14d avg) minus the case's own 14d traded price, per opening.
func (j *Finance) applyCasesNet(ctx context.Context, d, end time.Time, prices pricing.ItemAverages, get func(bson.ObjectID) *reports.UserFinanceReport) error {
	drops, err := j.Colls.Transactions.CaseTransaction.GetDropsRange(ctx, d, end)
	if err != nil {
		return err
	}
	if len(drops) == 0 {
		return nil
	}
	caseAvgs, err := j.Colls.Processed.Candles.ItemCandle.WeightedAvgByItem(ctx, end.Add(-14*24*time.Hour), end)
	if err != nil {
		return err
	}
	casePrice := make(map[string]float64, len(caseAvgs))
	for _, a := range caseAvgs {
		casePrice[a.ItemCode] = a.WeightedAvg
	}
	for _, dr := range drops {
		get(dr.UserID).CasesNet += prices.Avg[dr.ItemCode] - casePrice[dr.Case]
	}
	return nil
}
