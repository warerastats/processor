package reports

import (
	"context"
	"log/slog"
	"time"

	"github.com/warerastats/models/models"
	"github.com/warerastats/models/models/stores/processed/reports"
	"github.com/warerastats/processor/internal/window"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const wealthName = "entity_wealth_report"

// Wealth computes per-24h country/MU/party damage and wealth roll-ups.
type Wealth struct {
	Colls    *models.Collections
	interval time.Duration
	offset   time.Duration
}

// NewWealth builds the entity-wealth-report job.
func NewWealth(colls *models.Collections, interval, offset time.Duration) *Wealth {
	return &Wealth{Colls: colls, interval: interval, offset: offset}
}

func (j *Wealth) Name() string            { return wealthName }
func (j *Wealth) Interval() time.Duration { return j.interval }
func (j *Wealth) Offset() time.Duration   { return j.offset }

// Run computes every closed day since the watermark.
func (j *Wealth) Run(ctx context.Context) error {
	state, err := j.Colls.Processed.States.JobState.Get(ctx, j.Name())
	if err != nil {
		return err
	}

	day := 24 * time.Hour
	lastClosed := window.FloorUTC(time.Now(), day).Add(-day)

	// Wealth folds in each user's finance report, so never compute a day the
	// finance job hasn't finished yet; clamp the ceiling to its watermark.
	finance, err := j.Colls.Processed.States.JobState.Get(ctx, financeName)
	if err != nil {
		return err
	}
	if finance.Boundary.IsZero() {
		return nil
	}
	if finance.Boundary.Before(lastClosed) {
		lastClosed = finance.Boundary
	}

	var from time.Time
	if state.Boundary.IsZero() {
		from = lastClosed
	} else {
		from = state.Boundary.Add(day)
	}
	if earliest := lastClosed.Add(-30 * day); from.Before(earliest) {
		from = earliest
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

// wagePair holds a user's wages paid and earned for a day.
type wagePair struct {
	paid   float64
	earned float64
}

// computeDay builds wealth reports for every country, MU, and party.
func (j *Wealth) computeDay(ctx context.Context, d time.Time) error {
	finance, err := j.Colls.Processed.Reports.UserFinanceReport.GetByDay(ctx, d)
	if err != nil {
		return err
	}
	wages := make(map[bson.ObjectID]wagePair, len(finance))
	for _, f := range finance {
		wages[f.UserID] = wagePair{paid: f.WagesPaid, earned: f.WagesEarned}
	}

	var rows []reports.EntityWealthReport

	countries, err := j.Colls.Trackers.Country.GetAll(ctx)
	if err != nil {
		return err
	}
	for i := range countries {
		members, err := j.Colls.Trackers.User.GetByCountry(ctx, countries[i].ID)
		if err != nil {
			return err
		}
		row, err := j.buildRow(ctx, "country", countries[i].ID, d, members, wages)
		if err != nil {
			return err
		}
		rows = append(rows, row)
	}

	mus, err := j.Colls.Trackers.Mu.GetActive(ctx)
	if err != nil {
		return err
	}
	for i := range mus {
		row, err := j.buildRow(ctx, "mu", mus[i].ID, d, mus[i].MemberUserIDs, wages)
		if err != nil {
			return err
		}
		rows = append(rows, row)
	}

	parties, err := j.Colls.Trackers.Party.GetActive(ctx)
	if err != nil {
		return err
	}
	for i := range parties {
		row, err := j.buildRow(ctx, "party", parties[i].ID, d, parties[i].MemberUserIDs, wages)
		if err != nil {
			return err
		}
		rows = append(rows, row)
	}

	err = j.Colls.Processed.Reports.EntityWealthReport.Upsert(ctx, rows)
	if err != nil {
		return err
	}
	slog.Info("Entity wealth reports", "day", d, "entities", len(rows))
	return nil
}

// buildRow rolls up member wealth, damage, and wages for one entity.
func (j *Wealth) buildRow(ctx context.Context, kind string, id bson.ObjectID, d time.Time, members []bson.ObjectID, wages map[bson.ObjectID]wagePair) (reports.EntityWealthReport, error) {
	row := reports.EntityWealthReport{
		ID:          reports.EntityWealthReportID(kind, id, d),
		EntityType:  kind,
		EntityID:    id,
		DayStart:    d,
		MemberCount: len(members),
	}
	if len(members) == 0 {
		return row, nil
	}

	users, err := j.Colls.Trackers.User.GetMany(ctx, members)
	if err != nil {
		return row, err
	}
	for i := range users {
		for _, v := range users[i].Wealth {
			row.TotalWealth += v
		}
	}
	for _, m := range members {
		row.WagesPaid += wages[m].paid
		row.WagesEarned += wages[m].earned
	}

	damage, err := j.Colls.Processed.Estimators.BattleParticipation.SumDamageForUsers(ctx, members)
	if err != nil {
		return row, err
	}
	row.TotalDamage = damage
	return row, nil
}
