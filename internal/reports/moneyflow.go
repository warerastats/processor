package reports

import (
	"context"
	"log/slog"
	"time"

	"github.com/warerastats/models/models"
	processedreports "github.com/warerastats/models/models/stores/processed/reports"
	"github.com/warerastats/models/models/stores/trackers"
	"github.com/warerastats/models/models/stores/transactions"
	"github.com/warerastats/processor/internal/window"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const moneyFlowName = "money_flow_report"

const (
	flowEquipment = "equipment"
	flowItems     = "items"
	flowWages     = "wages"
)

// MoneyFlow computes daily country and MU money-flow reports.
type MoneyFlow struct {
	Colls    *models.Collections
	interval time.Duration
	offset   time.Duration
}

// NewMoneyFlow builds the money-flow-report job.
func NewMoneyFlow(colls *models.Collections, interval, offset time.Duration) *MoneyFlow {
	return &MoneyFlow{Colls: colls, interval: interval, offset: offset}
}

func (j *MoneyFlow) Name() string            { return moneyFlowName }
func (j *MoneyFlow) Interval() time.Duration { return j.interval }
func (j *MoneyFlow) Offset() time.Duration   { return j.offset }

// Run computes every closed day since the watermark.
func (j *MoneyFlow) Run(ctx context.Context) error {
	state, err := j.Colls.Processed.States.JobState.Get(ctx, j.Name())
	if err != nil {
		return err
	}

	day := 24 * time.Hour
	lastClosed := window.FloorUTC(time.Now(), day).Add(-day)

	var from time.Time
	if state.Boundary.IsZero() {
		earliest, ok, err := j.earliestTime(ctx)
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

func (j *MoneyFlow) earliestTime(ctx context.Context) (time.Time, bool, error) {
	var earliest time.Time
	hasAny := false

	setIfEarlier := func(ts time.Time, has bool) {
		if !has {
			return
		}
		if !hasAny || ts.Before(earliest) {
			earliest = ts
			hasAny = true
		}
	}

	ts, has, err := j.Colls.Transactions.MarketTransaction.EarliestTime(ctx)
	if err != nil {
		return time.Time{}, false, err
	}
	setIfEarlier(ts, has)

	ts, has, err = j.Colls.Transactions.TradeTransaction.EarliestTime(ctx)
	if err != nil {
		return time.Time{}, false, err
	}
	setIfEarlier(ts, has)

	ts, has, err = j.Colls.Transactions.WageTransaction.EarliestTime(ctx)
	if err != nil {
		return time.Time{}, false, err
	}
	setIfEarlier(ts, has)

	return earliest, hasAny, nil
}

type flowParty struct {
	countryID *bson.ObjectID
	muID      *bson.ObjectID
}

func (j *MoneyFlow) party(userID bson.ObjectID, txCountry, txMu *bson.ObjectID, users map[bson.ObjectID]trackers.UserAttrs) flowParty {
	out := flowParty{countryID: txCountry, muID: txMu}
	u, ok := users[userID]
	if !ok {
		return out
	}
	if out.countryID == nil && !u.CountryID.IsZero() {
		countryID := u.CountryID
		out.countryID = &countryID
	}
	if out.muID == nil && u.MuID != nil && !u.MuID.IsZero() {
		muID := *u.MuID
		out.muID = &muID
	}
	return out
}

func collectUserIDs(markets []transactions.MarketTransaction, trades []transactions.TradeTransaction, wages []transactions.WageRow) []bson.ObjectID {
	set := map[bson.ObjectID]struct{}{}
	add := func(id bson.ObjectID) {
		if id.IsZero() {
			return
		}
		set[id] = struct{}{}
	}

	for _, tx := range markets {
		add(tx.SellerID)
		add(tx.BuyerID)
	}
	for _, tx := range trades {
		add(tx.SellerID)
		add(tx.BuyerID)
	}
	for _, tx := range wages {
		add(tx.EmployeeID)
		add(tx.EmployerID)
	}

	out := make([]bson.ObjectID, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	return out
}

type countryAccum struct {
	row          *processedreports.CountryMoneyFlowReport
	counterparts map[bson.ObjectID]*processedreports.CountryMoneyFlowCounterpart
}

func newCountryAccum(id bson.ObjectID, dayStart time.Time) *countryAccum {
	return &countryAccum{
		row: &processedreports.CountryMoneyFlowReport{
			ID:        processedreports.CountryMoneyFlowReportID(id, dayStart),
			CountryID: id,
			DayStart:  dayStart,
		},
		counterparts: map[bson.ObjectID]*processedreports.CountryMoneyFlowCounterpart{},
	}
}

func (a *countryAccum) counterpart(id bson.ObjectID) *processedreports.CountryMoneyFlowCounterpart {
	r := a.counterparts[id]
	if r == nil {
		r = &processedreports.CountryMoneyFlowCounterpart{CountryID: id}
		a.counterparts[id] = r
	}
	return r
}

func applyCountryCategoryIn(r *processedreports.CountryMoneyFlowReport, category string, amount float64, domestic bool) {
	switch category {
	case flowEquipment:
		r.InEquipment += amount
		if domestic {
			r.InEquipmentDomestic += amount
		} else {
			r.InEquipmentCrossBorder += amount
		}
	case flowItems:
		r.InItems += amount
		if domestic {
			r.InItemsDomestic += amount
		} else {
			r.InItemsCrossBorder += amount
		}
	case flowWages:
		r.InWages += amount
		if domestic {
			r.InWagesDomestic += amount
		} else {
			r.InWagesCrossBorder += amount
		}
	}
}

func applyCountryCategoryOut(r *processedreports.CountryMoneyFlowReport, category string, amount float64, domestic bool) {
	switch category {
	case flowEquipment:
		r.OutEquipment += amount
		if domestic {
			r.OutEquipmentDomestic += amount
		} else {
			r.OutEquipmentCrossBorder += amount
		}
	case flowItems:
		r.OutItems += amount
		if domestic {
			r.OutItemsDomestic += amount
		} else {
			r.OutItemsCrossBorder += amount
		}
	case flowWages:
		r.OutWages += amount
		if domestic {
			r.OutWagesDomestic += amount
		} else {
			r.OutWagesCrossBorder += amount
		}
	}
}

func applyCountryCounterpartIn(r *processedreports.CountryMoneyFlowCounterpart, category string, amount float64) {
	switch category {
	case flowEquipment:
		r.InEquipment += amount
	case flowItems:
		r.InItems += amount
	case flowWages:
		r.InWages += amount
	}
}

func applyCountryCounterpartOut(r *processedreports.CountryMoneyFlowCounterpart, category string, amount float64) {
	switch category {
	case flowEquipment:
		r.OutEquipment += amount
	case flowItems:
		r.OutItems += amount
	case flowWages:
		r.OutWages += amount
	}
}

func applyCountryFlow(
	acc map[bson.ObjectID]*countryAccum,
	dayStart time.Time,
	category string,
	amount float64,
	source flowParty,
	target flowParty,
) {
	if amount <= 0 || source.countryID == nil || target.countryID == nil {
		return
	}

	src := *source.countryID
	dst := *target.countryID
	domestic := src == dst

	srcAcc := acc[src]
	if srcAcc == nil {
		srcAcc = newCountryAccum(src, dayStart)
		acc[src] = srcAcc
	}
	applyCountryCategoryOut(srcAcc.row, category, amount, domestic)
	applyCountryCounterpartOut(srcAcc.counterpart(dst), category, amount)

	dstAcc := acc[dst]
	if dstAcc == nil {
		dstAcc = newCountryAccum(dst, dayStart)
		acc[dst] = dstAcc
	}
	applyCountryCategoryIn(dstAcc.row, category, amount, domestic)
	applyCountryCounterpartIn(dstAcc.counterpart(src), category, amount)
}

type muAccum struct {
	row          *processedreports.MuCountryMoneyFlowReport
	counterparts map[bson.ObjectID]*processedreports.MuCountryMoneyFlowCounterpart
}

func newMuAccum(id bson.ObjectID, dayStart time.Time) *muAccum {
	return &muAccum{
		row: &processedreports.MuCountryMoneyFlowReport{
			ID:       processedreports.MuCountryMoneyFlowReportID(id, dayStart),
			MuID:     id,
			DayStart: dayStart,
		},
		counterparts: map[bson.ObjectID]*processedreports.MuCountryMoneyFlowCounterpart{},
	}
}

func (a *muAccum) counterpart(id bson.ObjectID) *processedreports.MuCountryMoneyFlowCounterpart {
	r := a.counterparts[id]
	if r == nil {
		r = &processedreports.MuCountryMoneyFlowCounterpart{CountryID: id}
		a.counterparts[id] = r
	}
	return r
}

func applyMuTotalIn(r *processedreports.MuCountryMoneyFlowReport, category string, amount float64) {
	switch category {
	case flowEquipment:
		r.InEquipment += amount
	case flowItems:
		r.InItems += amount
	case flowWages:
		r.InWages += amount
	}
}

func applyMuTotalOut(r *processedreports.MuCountryMoneyFlowReport, category string, amount float64) {
	switch category {
	case flowEquipment:
		r.OutEquipment += amount
	case flowItems:
		r.OutItems += amount
	case flowWages:
		r.OutWages += amount
	}
}

func applyMuCounterpartIn(r *processedreports.MuCountryMoneyFlowCounterpart, category string, amount float64) {
	switch category {
	case flowEquipment:
		r.InEquipment += amount
	case flowItems:
		r.InItems += amount
	case flowWages:
		r.InWages += amount
	}
}

func applyMuCounterpartOut(r *processedreports.MuCountryMoneyFlowCounterpart, category string, amount float64) {
	switch category {
	case flowEquipment:
		r.OutEquipment += amount
	case flowItems:
		r.OutItems += amount
	case flowWages:
		r.OutWages += amount
	}
}

func applyMuInsideIn(r *processedreports.MuCountryMoneyFlowReport, category string, amount float64) {
	switch category {
	case flowEquipment:
		r.InEquipmentInsideMu += amount
	case flowItems:
		r.InItemsInsideMu += amount
	case flowWages:
		r.InWagesInsideMu += amount
	}
}

func applyMuInsideOut(r *processedreports.MuCountryMoneyFlowReport, category string, amount float64) {
	switch category {
	case flowEquipment:
		r.OutEquipmentInsideMu += amount
	case flowItems:
		r.OutItemsInsideMu += amount
	case flowWages:
		r.OutWagesInsideMu += amount
	}
}

func applyMuSameCountryOutsideIn(r *processedreports.MuCountryMoneyFlowReport, category string, amount float64) {
	switch category {
	case flowEquipment:
		r.InEquipmentSameCountryOutsideMu += amount
	case flowItems:
		r.InItemsSameCountryOutsideMu += amount
	case flowWages:
		r.InWagesSameCountryOutsideMu += amount
	}
}

func applyMuSameCountryOutsideOut(r *processedreports.MuCountryMoneyFlowReport, category string, amount float64) {
	switch category {
	case flowEquipment:
		r.OutEquipmentSameCountryOutsideMu += amount
	case flowItems:
		r.OutItemsSameCountryOutsideMu += amount
	case flowWages:
		r.OutWagesSameCountryOutsideMu += amount
	}
}

func applyMuCrossBorderIn(r *processedreports.MuCountryMoneyFlowReport, category string, amount float64) {
	switch category {
	case flowEquipment:
		r.InEquipmentCrossBorderOutsideMuCountry += amount
	case flowItems:
		r.InItemsCrossBorderOutsideMuCountry += amount
	case flowWages:
		r.InWagesCrossBorderOutsideMuCountry += amount
	}
}

func applyMuCrossBorderOut(r *processedreports.MuCountryMoneyFlowReport, category string, amount float64) {
	switch category {
	case flowEquipment:
		r.OutEquipmentCrossBorderOutsideMuCountry += amount
	case flowItems:
		r.OutItemsCrossBorderOutsideMuCountry += amount
	case flowWages:
		r.OutWagesCrossBorderOutsideMuCountry += amount
	}
}

func applyMuIn(
	acc map[bson.ObjectID]*muAccum,
	dayStart time.Time,
	category string,
	amount float64,
	muSide flowParty,
	counter flowParty,
) {
	if amount <= 0 || muSide.muID == nil {
		return
	}

	muID := *muSide.muID
	row := acc[muID]
	if row == nil {
		row = newMuAccum(muID, dayStart)
		acc[muID] = row
	}
	applyMuTotalIn(row.row, category, amount)

	inside := counter.muID != nil && *counter.muID == muID
	if inside {
		applyMuInsideIn(row.row, category, amount)
	} else if muSide.countryID != nil && counter.countryID != nil && *muSide.countryID == *counter.countryID {
		applyMuSameCountryOutsideIn(row.row, category, amount)
	} else {
		applyMuCrossBorderIn(row.row, category, amount)
	}

	if counter.countryID != nil {
		applyMuCounterpartIn(row.counterpart(*counter.countryID), category, amount)
	}
}

func applyMuOut(
	acc map[bson.ObjectID]*muAccum,
	dayStart time.Time,
	category string,
	amount float64,
	muSide flowParty,
	counter flowParty,
) {
	if amount <= 0 || muSide.muID == nil {
		return
	}

	muID := *muSide.muID
	row := acc[muID]
	if row == nil {
		row = newMuAccum(muID, dayStart)
		acc[muID] = row
	}
	applyMuTotalOut(row.row, category, amount)

	inside := counter.muID != nil && *counter.muID == muID
	if inside {
		applyMuInsideOut(row.row, category, amount)
	} else if muSide.countryID != nil && counter.countryID != nil && *muSide.countryID == *counter.countryID {
		applyMuSameCountryOutsideOut(row.row, category, amount)
	} else {
		applyMuCrossBorderOut(row.row, category, amount)
	}

	if counter.countryID != nil {
		applyMuCounterpartOut(row.counterpart(*counter.countryID), category, amount)
	}
}

func (j *MoneyFlow) computeDay(ctx context.Context, d time.Time) error {
	end := d.Add(24 * time.Hour)

	markets, err := j.Colls.Transactions.MarketTransaction.GetRange(ctx, d, end)
	if err != nil {
		return err
	}
	trades, err := j.Colls.Transactions.TradeTransaction.GetRange(ctx, d, end)
	if err != nil {
		return err
	}
	wages, err := j.Colls.Transactions.WageTransaction.GetRange(ctx, d, end)
	if err != nil {
		return err
	}

	userIDs := collectUserIDs(markets, trades, wages)
	usersByID := map[bson.ObjectID]trackers.UserAttrs{}
	if len(userIDs) > 0 {
		users, err := j.Colls.Trackers.User.GetManyAttrs(ctx, userIDs)
		if err != nil {
			return err
		}
		for i := range users {
			usersByID[users[i].ID] = users[i]
		}
	}

	countryAcc := map[bson.ObjectID]*countryAccum{}
	muAcc := map[bson.ObjectID]*muAccum{}

	// Build country → alliance map for alliance money-flow attribution.
	countries, err := j.Colls.Trackers.Country.GetAll(ctx)
	if err != nil {
		return err
	}
	allianceByCountry := map[bson.ObjectID]*bson.ObjectID{}
	for i := range countries {
		if countries[i].AllianceID != nil {
			aid := *countries[i].AllianceID
			allianceByCountry[countries[i].ID] = &aid
		}
	}

	countryAllianceAcc := map[bson.ObjectID]*countryAllianceAccum{}
	allianceAcc := map[bson.ObjectID]*allianceAccum{}
	muAllianceAcc := map[bson.ObjectID]*muAllianceAccum{}

	allianceOf := func(fp flowParty) *bson.ObjectID {
		if fp.countryID == nil {
			return nil
		}
		return allianceByCountry[*fp.countryID]
	}

	for _, tx := range markets {
		source := j.party(tx.SellerID, nil, nil, usersByID)
		target := j.party(tx.BuyerID, nil, nil, usersByID)
		applyCountryFlow(countryAcc, d, flowEquipment, tx.Money, source, target)
		applyMuOut(muAcc, d, flowEquipment, tx.Money, source, target)
		applyMuIn(muAcc, d, flowEquipment, tx.Money, target, source)
		srcA, dstA := allianceOf(source), allianceOf(target)
		applyCountryAllianceFlow(countryAllianceAcc, d, flowEquipment, tx.Money, source, target, srcA, dstA)
		applyAllianceFlow(allianceAcc, d, flowEquipment, tx.Money, srcA, dstA)
		applyMuAllianceOut(muAllianceAcc, d, flowEquipment, tx.Money, source, srcA, dstA)
		applyMuAllianceIn(muAllianceAcc, d, flowEquipment, tx.Money, target, srcA, dstA)
	}

	for _, tx := range trades {
		source := j.party(tx.SellerID, tx.SellerCountryID, tx.SellerMuID, usersByID)
		target := j.party(tx.BuyerID, tx.BuyerCountryID, tx.BuyerMuID, usersByID)
		applyCountryFlow(countryAcc, d, flowItems, tx.Money, source, target)
		applyMuOut(muAcc, d, flowItems, tx.Money, source, target)
		applyMuIn(muAcc, d, flowItems, tx.Money, target, source)
		srcA, dstA := allianceOf(source), allianceOf(target)
		applyCountryAllianceFlow(countryAllianceAcc, d, flowItems, tx.Money, source, target, srcA, dstA)
		applyAllianceFlow(allianceAcc, d, flowItems, tx.Money, srcA, dstA)
		applyMuAllianceOut(muAllianceAcc, d, flowItems, tx.Money, source, srcA, dstA)
		applyMuAllianceIn(muAllianceAcc, d, flowItems, tx.Money, target, srcA, dstA)
	}

	for _, tx := range wages {
		source := j.party(tx.EmployerID, nil, nil, usersByID)
		target := j.party(tx.EmployeeID, nil, nil, usersByID)
		applyCountryFlow(countryAcc, d, flowWages, tx.Money, source, target)
		applyMuOut(muAcc, d, flowWages, tx.Money, source, target)
		applyMuIn(muAcc, d, flowWages, tx.Money, target, source)
		srcA, dstA := allianceOf(source), allianceOf(target)
		applyCountryAllianceFlow(countryAllianceAcc, d, flowWages, tx.Money, source, target, srcA, dstA)
		applyAllianceFlow(allianceAcc, d, flowWages, tx.Money, srcA, dstA)
		applyMuAllianceOut(muAllianceAcc, d, flowWages, tx.Money, source, srcA, dstA)
		applyMuAllianceIn(muAllianceAcc, d, flowWages, tx.Money, target, srcA, dstA)
	}

	countryRows := make([]processedreports.CountryMoneyFlowReport, 0, len(countryAcc))
	for _, a := range countryAcc {
		a.row.Counterparts = make([]processedreports.CountryMoneyFlowCounterpart, 0, len(a.counterparts))
		for _, cp := range a.counterparts {
			a.row.Counterparts = append(a.row.Counterparts, *cp)
		}
		countryRows = append(countryRows, *a.row)
	}
	err = j.Colls.Processed.Reports.CountryMoneyFlow.Upsert(ctx, countryRows)
	if err != nil {
		return err
	}

	muRows := make([]processedreports.MuCountryMoneyFlowReport, 0, len(muAcc))
	for _, a := range muAcc {
		a.row.Counterparts = make([]processedreports.MuCountryMoneyFlowCounterpart, 0, len(a.counterparts))
		for _, cp := range a.counterparts {
			a.row.Counterparts = append(a.row.Counterparts, *cp)
		}
		muRows = append(muRows, *a.row)
	}
	err = j.Colls.Processed.Reports.MuCountryMoneyFlow.Upsert(ctx, muRows)
	if err != nil {
		return err
	}

	// Country-alliance money-flow reports.
	caRows := make([]processedreports.CountryAllianceMoneyFlowReport, 0, len(countryAllianceAcc))
	for _, a := range countryAllianceAcc {
		a.row.Counterparts = make([]processedreports.CountryAllianceMoneyFlowCounterpart, 0, len(a.counterparts))
		for _, cp := range a.counterparts {
			a.row.Counterparts = append(a.row.Counterparts, *cp)
		}
		caRows = append(caRows, *a.row)
	}
	err = j.Colls.Processed.Reports.CountryAllianceMoneyFlow.Upsert(ctx, caRows)
	if err != nil {
		return err
	}

	// Alliance money-flow reports.
	aRows := make([]processedreports.AllianceMoneyFlowReport, 0, len(allianceAcc))
	for _, a := range allianceAcc {
		a.row.Counterparts = make([]processedreports.AllianceMoneyFlowCounterpart, 0, len(a.counterparts))
		for _, cp := range a.counterparts {
			a.row.Counterparts = append(a.row.Counterparts, *cp)
		}
		aRows = append(aRows, *a.row)
	}
	err = j.Colls.Processed.Reports.AllianceMoneyFlow.Upsert(ctx, aRows)
	if err != nil {
		return err
	}

	// MU-alliance money-flow reports.
	maRows := make([]processedreports.MuAllianceMoneyFlowReport, 0, len(muAllianceAcc))
	for _, a := range muAllianceAcc {
		a.row.Counterparts = make([]processedreports.MuAllianceMoneyFlowCounterpart, 0, len(a.counterparts))
		for _, cp := range a.counterparts {
			a.row.Counterparts = append(a.row.Counterparts, *cp)
		}
		maRows = append(maRows, *a.row)
	}
	err = j.Colls.Processed.Reports.MuAllianceMoneyFlow.Upsert(ctx, maRows)
	if err != nil {
		return err
	}

	slog.Info("Money flow reports", "day", d, "countries", len(countryRows), "mus", len(muRows),
		"countryAlliance", len(caRows), "alliances", len(aRows), "muAlliance", len(maRows))
	return nil
}
