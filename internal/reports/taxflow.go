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

// foreignTaxFraction is the share of income tax redirected to a foreign
// worker's citizenship country (30% of gross income tax).
const foreignTaxFraction = 0.30

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
	regions    map[bson.ObjectID]trackers.Region
	countries  map[bson.ObjectID]trackers.Country
	companies  map[bson.ObjectID]trackers.Company
	employee   map[bson.ObjectID]*trackers.Employee
	ownerCtry  map[bson.ObjectID]bson.ObjectID
	workerCtry map[bson.ObjectID]bson.ObjectID // employee userID → citizenship countryID
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
		regions:    make(map[bson.ObjectID]trackers.Region, len(regions)),
		countries:  make(map[bson.ObjectID]trackers.Country, len(countries)),
		companies:  make(map[bson.ObjectID]trackers.Company, len(companies)),
		employee:   map[bson.ObjectID]*trackers.Employee{},
		ownerCtry:  map[bson.ObjectID]bson.ObjectID{},
		workerCtry: map[bson.ObjectID]bson.ObjectID{},
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
	core                 float64
	nonCore              float64
	foreignTaxOut        float64
	foreignTaxIn         float64
	foreignTaxRecipients map[bson.ObjectID]float64 // citizenship countryID → amount redirected
	sources              map[bson.ObjectID]*sourceAccum
}

type sourceAccum struct {
	total                float64
	core                 float64
	foreignTaxRedirected float64
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
			a = &taxAccum{foreignTaxRecipients: map[bson.ObjectID]float64{}, sources: map[bson.ObjectID]*sourceAccum{}}
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
		workCountryID := region.CountryID
		taxing, ok := refs.countries[workCountryID]
		if !ok {
			continue
		}

		// Income tax is stored as a percentage where 1 == 1%, so scale to a fraction.
		gross := taxing.Taxes.Income / 100 * w.Money
		if gross <= 0 {
			continue
		}

		// Foreign-citizen income tax: 30% of gross goes to the worker's
		// citizenship country when it differs from the work country.
		citizenshipID := j.workerCountry(ctx, refs, emp.UserID)
		redirected := 0.0
		if !citizenshipID.IsZero() && citizenshipID != workCountryID {
			redirected = gross * foreignTaxFraction
		}
		kept := gross - redirected

		isCore := workCountryID == region.InitialCountryID
		c := get(workCountryID)
		if isCore {
			c.core += kept
		} else {
			c.nonCore += kept
		}
		if redirected > 0 {
			c.foreignTaxOut += redirected
			c.foreignTaxRecipients[citizenshipID] += redirected
			get(citizenshipID).foreignTaxIn += redirected
		}

		home := j.ownerCountry(ctx, refs, company.UserID)
		src := c.sources[home]
		if src == nil {
			src = &sourceAccum{}
			c.sources[home] = src
		}
		src.total += gross
		src.foreignTaxRedirected += redirected
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
	foreignRecipients := make([]reports.ForeignTaxRecipient, 0, len(a.foreignTaxRecipients))
	for cid, amt := range a.foreignTaxRecipients {
		foreignRecipients = append(foreignRecipients, reports.ForeignTaxRecipient{CountryID: cid, Amount: amt})
	}
	sources := make([]reports.TaxSource, 0, len(a.sources))
	for cid, s := range a.sources {
		corePct := 0.0
		if s.total > 0 {
			corePct = s.core / s.total * 100
		}
		sources = append(sources, reports.TaxSource{
			CountryID: cid, Total: s.total, CorePct: corePct,
			ForeignTaxRedirected: s.foreignTaxRedirected,
		})
	}
	return reports.CountryTaxFlow{
		ID:                   reports.CountryTaxFlowID(id, h),
		CountryID:            id,
		HourStart:            h,
		TotalTax:             a.core + a.nonCore + a.foreignTaxIn,
		CoreEarned:           a.core,
		NonCoreEarned:        a.nonCore,
		ForeignTaxIn:         a.foreignTaxIn,
		ForeignTaxOut:        a.foreignTaxOut,
		ForeignTaxRecipients: foreignRecipients,
		Sources:              sources,
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

// workerCountry resolves and caches an employee's citizenship country id.
func (j *TaxFlow) workerCountry(ctx context.Context, refs *taxRefs, userID bson.ObjectID) bson.ObjectID {
	if c, ok := refs.workerCtry[userID]; ok {
		return c
	}
	users, err := j.Colls.Trackers.User.GetMany(ctx, []bson.ObjectID{userID})
	var c bson.ObjectID
	if err == nil && len(users) > 0 {
		c = users[0].CountryID
	}
	refs.workerCtry[userID] = c
	return c
}
