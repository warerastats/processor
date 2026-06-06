package reports

import (
	"context"
	"log/slog"
	"time"

	"github.com/warerastats/models/models"
	"github.com/warerastats/models/models/stores/processed/reports"
	"github.com/warerastats/models/models/stores/trackers"
	"github.com/warerastats/processor/internal/window"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const taxFlowName = "country_tax_flow"

// hijackMaxFraction is the share of income tax the initial country seizes at
// full resistance on a non-core region.
const hijackMaxFraction = 0.4

// TaxFlow computes hourly per-country income-tax flow reports.
type TaxFlow struct {
	Colls    *models.Collections
	interval time.Duration
	offset   time.Duration
}

// NewTaxFlow builds the country-tax-flow job.
func NewTaxFlow(colls *models.Collections, interval, offset time.Duration) *TaxFlow {
	return &TaxFlow{Colls: colls, interval: interval, offset: offset}
}

func (j *TaxFlow) Name() string            { return taxFlowName }
func (j *TaxFlow) Interval() time.Duration { return j.interval }
func (j *TaxFlow) Offset() time.Duration   { return j.offset }

// Run computes every closed hour since the watermark.
func (j *TaxFlow) Run(ctx context.Context) error {
	state, err := j.Colls.Processed.States.JobState.Get(ctx, j.Name())
	if err != nil {
		return err
	}

	hour := time.Hour
	lastClosed := window.FloorUTC(time.Now(), hour).Add(-hour)

	var from time.Time
	if state.Boundary.IsZero() {
		earliest, ok, err := j.Colls.Transactions.WageTransaction.EarliestTime(ctx)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		from = window.FloorUTC(earliest, hour)
	} else {
		from = state.Boundary.Add(hour)
	}

	refs, err := j.loadRefs(ctx)
	if err != nil {
		return err
	}

	for h := from; !h.After(lastClosed); h = h.Add(hour) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err = j.computeHour(ctx, h, refs)
		if err != nil {
			return err
		}
		err = j.Colls.Processed.States.JobState.SetWatermark(ctx, j.Name(), h, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

// taxRefs holds the slowly-changing lookups used across hours in one pass.
type taxRefs struct {
	regions   map[bson.ObjectID]trackers.Region
	countries map[bson.ObjectID]trackers.Country
	companies map[bson.ObjectID]trackers.Company
	employee  map[bson.ObjectID]*trackers.Employee
	ownerCtry map[bson.ObjectID]bson.ObjectID
}

// loadRefs batch-loads regions, countries, and companies for the pass.
func (j *TaxFlow) loadRefs(ctx context.Context) (*taxRefs, error) {
	regions, err := j.Colls.Trackers.Region.GetAll(ctx)
	if err != nil {
		return nil, err
	}
	countries, err := j.Colls.Trackers.Country.GetAll(ctx)
	if err != nil {
		return nil, err
	}
	companies, err := j.Colls.Trackers.Company.GetAll(ctx)
	if err != nil {
		return nil, err
	}

	refs := &taxRefs{
		regions:   make(map[bson.ObjectID]trackers.Region, len(regions)),
		countries: make(map[bson.ObjectID]trackers.Country, len(countries)),
		companies: make(map[bson.ObjectID]trackers.Company, len(companies)),
		employee:  map[bson.ObjectID]*trackers.Employee{},
		ownerCtry: map[bson.ObjectID]bson.ObjectID{},
	}
	for i := range regions {
		refs.regions[regions[i].ID] = regions[i]
	}
	for i := range countries {
		refs.countries[countries[i].ID] = countries[i]
	}
	for i := range companies {
		refs.companies[companies[i].ID] = companies[i]
	}
	return refs, nil
}

// taxAccum accumulates one country's hourly tax flow during a pass.
type taxAccum struct {
	core      float64
	nonCore   float64
	hijackOut float64
	hijackIn  float64
	hijackers map[bson.ObjectID]float64
	sources   map[bson.ObjectID]*sourceAccum
}

type sourceAccum struct {
	total    float64
	hijacked float64
	core     float64
}

// computeHour builds and stores tax-flow reports for the hour starting at h.
func (j *TaxFlow) computeHour(ctx context.Context, h time.Time, refs *taxRefs) error {
	wages, err := j.Colls.Transactions.WageTransaction.GetRange(ctx, h, h.Add(time.Hour))
	if err != nil {
		return err
	}
	if len(wages) == 0 {
		return nil
	}

	acc := map[bson.ObjectID]*taxAccum{}
	get := func(id bson.ObjectID) *taxAccum {
		a := acc[id]
		if a == nil {
			a = &taxAccum{hijackers: map[bson.ObjectID]float64{}, sources: map[bson.ObjectID]*sourceAccum{}}
			acc[id] = a
		}
		return a
	}

	for _, w := range wages {
		emp := j.employee(ctx, refs, w.EmployeeID)
		if emp == nil {
			continue
		}
		company, ok := refs.companies[emp.CompanyID]
		if !ok {
			continue
		}
		region, ok := refs.regions[company.RegionID]
		if !ok {
			continue
		}
		taxing, ok := refs.countries[region.CountryID]
		if !ok {
			continue
		}

		// Income tax is stored as a percentage where 1 == 1%, so scale to a fraction.
		gross := taxing.Taxes.Income / 100 * w.Money
		if gross <= 0 {
			continue
		}
		isCore := region.CountryID == region.InitialCountryID
		hijacked := 0.0
		if !isCore && region.MaxResistance > 0 {
			hijacked = gross * hijackMaxFraction * (region.Resistance / region.MaxResistance)
		}
		kept := gross - hijacked

		c := get(region.CountryID)
		if isCore {
			c.core += kept
		} else {
			c.nonCore += kept
		}
		if hijacked > 0 {
			c.hijackOut += hijacked
			c.hijackers[region.InitialCountryID] += hijacked
			get(region.InitialCountryID).hijackIn += hijacked
		}

		home := j.ownerCountry(ctx, refs, company.UserID)
		src := c.sources[home]
		if src == nil {
			src = &sourceAccum{}
			c.sources[home] = src
		}
		src.total += gross
		src.hijacked += hijacked
		if isCore {
			src.core += gross
		}
	}

	rows := make([]reports.CountryTaxFlow, 0, len(acc))
	for id, a := range acc {
		rows = append(rows, buildTaxRow(id, h, a))
	}
	err = j.Colls.Processed.Reports.CountryTaxFlow.Upsert(ctx, rows)
	if err != nil {
		return err
	}
	slog.Info("Country tax flow", "hour", h, "countries", len(rows))
	return nil
}

// buildTaxRow converts an accumulator into a stored tax-flow document.
func buildTaxRow(id bson.ObjectID, h time.Time, a *taxAccum) reports.CountryTaxFlow {
	hijackers := make([]reports.TaxHijack, 0, len(a.hijackers))
	for cid, amt := range a.hijackers {
		hijackers = append(hijackers, reports.TaxHijack{CountryID: cid, Amount: amt})
	}
	sources := make([]reports.TaxSource, 0, len(a.sources))
	for cid, s := range a.sources {
		corePct := 0.0
		if s.total > 0 {
			corePct = s.core / s.total * 100
		}
		sources = append(sources, reports.TaxSource{
			CountryID: cid, Total: s.total, Hijacked: s.hijacked, CorePct: corePct,
		})
	}
	return reports.CountryTaxFlow{
		ID:            reports.CountryTaxFlowID(id, h),
		CountryID:     id,
		HourStart:     h,
		TotalTax:      a.core + a.nonCore + a.hijackIn,
		HijackedIn:    a.hijackIn,
		CoreEarned:    a.core,
		NonCoreEarned: a.nonCore,
		HijackedOut:   a.hijackOut,
		Hijackers:     hijackers,
		Sources:       sources,
	}
}

// employee resolves and caches an employee by user id.
func (j *TaxFlow) employee(ctx context.Context, refs *taxRefs, userID bson.ObjectID) *trackers.Employee {
	if e, ok := refs.employee[userID]; ok {
		return e
	}
	e, ok, err := j.Colls.Trackers.Employee.GetByUserID(ctx, userID)
	if err != nil || !ok {
		refs.employee[userID] = nil
		return nil
	}
	refs.employee[userID] = e
	return e
}

// ownerCountry resolves and caches a company owner's country id.
func (j *TaxFlow) ownerCountry(ctx context.Context, refs *taxRefs, ownerID bson.ObjectID) bson.ObjectID {
	if c, ok := refs.ownerCtry[ownerID]; ok {
		return c
	}
	users, err := j.Colls.Trackers.User.GetMany(ctx, []bson.ObjectID{ownerID})
	var c bson.ObjectID
	if err == nil && len(users) > 0 {
		c = users[0].CountryID
	}
	refs.ownerCtry[ownerID] = c
	return c
}
