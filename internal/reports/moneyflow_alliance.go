package reports

import (
	"time"

	processedreports "github.com/warerastats/models/models/stores/processed/reports"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// --- country-alliance: per-country daily flow grouped by counterpart alliance ---

type countryAllianceAccum struct {
	row          *processedreports.CountryAllianceMoneyFlowReport
	counterparts map[bson.ObjectID]*processedreports.CountryAllianceMoneyFlowCounterpart
}

func newCountryAllianceAccum(id bson.ObjectID, dayStart time.Time) *countryAllianceAccum {
	return &countryAllianceAccum{
		row: &processedreports.CountryAllianceMoneyFlowReport{
			ID:        processedreports.CountryAllianceMoneyFlowReportID(id, dayStart),
			CountryID: id,
			DayStart:  dayStart,
		},
		counterparts: map[bson.ObjectID]*processedreports.CountryAllianceMoneyFlowCounterpart{},
	}
}

func (a *countryAllianceAccum) counterpart(id bson.ObjectID) *processedreports.CountryAllianceMoneyFlowCounterpart {
	r := a.counterparts[id]
	if r == nil {
		r = &processedreports.CountryAllianceMoneyFlowCounterpart{AllianceID: id}
		a.counterparts[id] = r
	}
	return r
}

// --- alliance: per-alliance daily flow grouped by counterpart alliance ---

type allianceAccum struct {
	row          *processedreports.AllianceMoneyFlowReport
	counterparts map[bson.ObjectID]*processedreports.AllianceMoneyFlowCounterpart
}

func newAllianceAccum(id bson.ObjectID, dayStart time.Time) *allianceAccum {
	return &allianceAccum{
		row: &processedreports.AllianceMoneyFlowReport{
			ID:         processedreports.AllianceMoneyFlowReportID(id, dayStart),
			AllianceID: id,
			DayStart:   dayStart,
		},
		counterparts: map[bson.ObjectID]*processedreports.AllianceMoneyFlowCounterpart{},
	}
}

func (a *allianceAccum) counterpart(id bson.ObjectID) *processedreports.AllianceMoneyFlowCounterpart {
	r := a.counterparts[id]
	if r == nil {
		r = &processedreports.AllianceMoneyFlowCounterpart{AllianceID: id}
		a.counterparts[id] = r
	}
	return r
}

// --- mu-alliance: per-MU daily flow grouped by counterpart alliance ---

type muAllianceAccum struct {
	row          *processedreports.MuAllianceMoneyFlowReport
	counterparts map[bson.ObjectID]*processedreports.MuAllianceMoneyFlowCounterpart
}

func newMuAllianceAccum(id bson.ObjectID, dayStart time.Time) *muAllianceAccum {
	return &muAllianceAccum{
		row: &processedreports.MuAllianceMoneyFlowReport{
			ID:       processedreports.MuAllianceMoneyFlowReportID(id, dayStart),
			MuID:     id,
			DayStart: dayStart,
		},
		counterparts: map[bson.ObjectID]*processedreports.MuAllianceMoneyFlowCounterpart{},
	}
}

func (a *muAllianceAccum) counterpart(id bson.ObjectID) *processedreports.MuAllianceMoneyFlowCounterpart {
	r := a.counterparts[id]
	if r == nil {
		r = &processedreports.MuAllianceMoneyFlowCounterpart{AllianceID: id}
		a.counterparts[id] = r
	}
	return r
}

// applyCountryAllianceFlow records the alliance-dimension flow for a country pair.
func applyCountryAllianceFlow(
	acc map[bson.ObjectID]*countryAllianceAccum,
	dayStart time.Time,
	category string,
	amount float64,
	source, target flowParty,
	srcAlliance, dstAlliance *bson.ObjectID,
) {
	if amount <= 0 || source.countryID == nil {
		return
	}
	src := *source.countryID
	a := acc[src]
	if a == nil {
		a = newCountryAllianceAccum(src, dayStart)
		acc[src] = a
	}

	inAlliance := srcAlliance != nil && dstAlliance != nil && *srcAlliance == *dstAlliance
	applyFlowAmountOut(a.row, category, amount, inAlliance)
	if dstAlliance != nil {
		applyCounterpartAmountOut(a.counterpart(*dstAlliance), category, amount)
	}

	if target.countryID == nil {
		return
	}
	dst := *target.countryID
	b := acc[dst]
	if b == nil {
		b = newCountryAllianceAccum(dst, dayStart)
		acc[dst] = b
	}
	applyFlowAmountIn(b.row, category, amount, inAlliance)
	if srcAlliance != nil {
		applyCounterpartAmountIn(b.counterpart(*srcAlliance), category, amount)
	}
}

// applyAllianceFlow records the alliance-level flow.
func applyAllianceFlow(
	acc map[bson.ObjectID]*allianceAccum,
	dayStart time.Time,
	category string,
	amount float64,
	srcAlliance, dstAlliance *bson.ObjectID,
) {
	if amount <= 0 {
		return
	}
	inAlliance := srcAlliance != nil && dstAlliance != nil && *srcAlliance == *dstAlliance

	if srcAlliance != nil {
		a := acc[*srcAlliance]
		if a == nil {
			a = newAllianceAccum(*srcAlliance, dayStart)
			acc[*srcAlliance] = a
		}
		applyAllianceAmountOut(a.row, category, amount, inAlliance)
		if dstAlliance != nil {
			applyAllianceCounterpartOut(a.counterpart(*dstAlliance), category, amount)
		}
	}

	if dstAlliance != nil {
		b := acc[*dstAlliance]
		if b == nil {
			b = newAllianceAccum(*dstAlliance, dayStart)
			acc[*dstAlliance] = b
		}
		applyAllianceAmountIn(b.row, category, amount, inAlliance)
		if srcAlliance != nil {
			applyAllianceCounterpartIn(b.counterpart(*srcAlliance), category, amount)
		}
	}
}

// applyMuAllianceIn records the MU-alliance flow for the receiving side.
func applyMuAllianceIn(
	acc map[bson.ObjectID]*muAllianceAccum,
	dayStart time.Time,
	category string,
	amount float64,
	muSide flowParty,
	srcAlliance, dstAlliance *bson.ObjectID,
) {
	if amount <= 0 || muSide.muID == nil {
		return
	}
	muID := *muSide.muID
	a := acc[muID]
	if a == nil {
		a = newMuAllianceAccum(muID, dayStart)
		acc[muID] = a
	}
	inside := dstAlliance != nil && srcAlliance != nil && *dstAlliance == *srcAlliance
	applyMuAllianceTotalIn(a.row, category, amount)
	if inside {
		applyMuAllianceInsideIn(a.row, category, amount)
	} else {
		applyMuAllianceOutsideIn(a.row, category, amount)
	}
	if srcAlliance != nil {
		applyMuAllianceCounterpartIn(a.counterpart(*srcAlliance), category, amount)
	}
}

// applyMuAllianceOut records the MU-alliance flow for the sending side.
func applyMuAllianceOut(
	acc map[bson.ObjectID]*muAllianceAccum,
	dayStart time.Time,
	category string,
	amount float64,
	muSide flowParty,
	srcAlliance, dstAlliance *bson.ObjectID,
) {
	if amount <= 0 || muSide.muID == nil {
		return
	}
	muID := *muSide.muID
	a := acc[muID]
	if a == nil {
		a = newMuAllianceAccum(muID, dayStart)
		acc[muID] = a
	}
	inside := srcAlliance != nil && dstAlliance != nil && *srcAlliance == *dstAlliance
	applyMuAllianceTotalOut(a.row, category, amount)
	if inside {
		applyMuAllianceInsideOut(a.row, category, amount)
	} else {
		applyMuAllianceOutsideOut(a.row, category, amount)
	}
	if dstAlliance != nil {
		applyMuAllianceCounterpartOut(a.counterpart(*dstAlliance), category, amount)
	}
}

// --- helpers for country-alliance report fields ---

func applyFlowAmountIn(r *processedreports.CountryAllianceMoneyFlowReport, category string, amount float64, inAlliance bool) {
	switch category {
	case flowEquipment:
		r.InEquipment += amount
		if inAlliance {
			r.InEquipmentInAlliance += amount
		} else {
			r.InEquipmentOutsideAlliance += amount
		}
	case flowItems:
		r.InItems += amount
		if inAlliance {
			r.InItemsInAlliance += amount
		} else {
			r.InItemsOutsideAlliance += amount
		}
	case flowWages:
		r.InWages += amount
		if inAlliance {
			r.InWagesInAlliance += amount
		} else {
			r.InWagesOutsideAlliance += amount
		}
	}
}

func applyFlowAmountOut(r *processedreports.CountryAllianceMoneyFlowReport, category string, amount float64, inAlliance bool) {
	switch category {
	case flowEquipment:
		r.OutEquipment += amount
		if inAlliance {
			r.OutEquipmentInAlliance += amount
		} else {
			r.OutEquipmentOutsideAlliance += amount
		}
	case flowItems:
		r.OutItems += amount
		if inAlliance {
			r.OutItemsInAlliance += amount
		} else {
			r.OutItemsOutsideAlliance += amount
		}
	case flowWages:
		r.OutWages += amount
		if inAlliance {
			r.OutWagesInAlliance += amount
		} else {
			r.OutWagesOutsideAlliance += amount
		}
	}
}

func applyCounterpartAmountIn(r *processedreports.CountryAllianceMoneyFlowCounterpart, category string, amount float64) {
	switch category {
	case flowEquipment:
		r.InEquipment += amount
	case flowItems:
		r.InItems += amount
	case flowWages:
		r.InWages += amount
	}
}

func applyCounterpartAmountOut(r *processedreports.CountryAllianceMoneyFlowCounterpart, category string, amount float64) {
	switch category {
	case flowEquipment:
		r.OutEquipment += amount
	case flowItems:
		r.OutItems += amount
	case flowWages:
		r.OutWages += amount
	}
}

// --- helpers for alliance report fields ---

func applyAllianceAmountIn(r *processedreports.AllianceMoneyFlowReport, category string, amount float64, inAlliance bool) {
	switch category {
	case flowEquipment:
		r.InEquipment += amount
		if inAlliance {
			r.InEquipmentInAlliance += amount
		} else {
			r.InEquipmentOutsideAlliance += amount
		}
	case flowItems:
		r.InItems += amount
		if inAlliance {
			r.InItemsInAlliance += amount
		} else {
			r.InItemsOutsideAlliance += amount
		}
	case flowWages:
		r.InWages += amount
		if inAlliance {
			r.InWagesInAlliance += amount
		} else {
			r.InWagesOutsideAlliance += amount
		}
	}
}

func applyAllianceAmountOut(r *processedreports.AllianceMoneyFlowReport, category string, amount float64, inAlliance bool) {
	switch category {
	case flowEquipment:
		r.OutEquipment += amount
		if inAlliance {
			r.OutEquipmentInAlliance += amount
		} else {
			r.OutEquipmentOutsideAlliance += amount
		}
	case flowItems:
		r.OutItems += amount
		if inAlliance {
			r.OutItemsInAlliance += amount
		} else {
			r.OutItemsOutsideAlliance += amount
		}
	case flowWages:
		r.OutWages += amount
		if inAlliance {
			r.OutWagesInAlliance += amount
		} else {
			r.OutWagesOutsideAlliance += amount
		}
	}
}

func applyAllianceCounterpartIn(r *processedreports.AllianceMoneyFlowCounterpart, category string, amount float64) {
	switch category {
	case flowEquipment:
		r.InEquipment += amount
	case flowItems:
		r.InItems += amount
	case flowWages:
		r.InWages += amount
	}
}

func applyAllianceCounterpartOut(r *processedreports.AllianceMoneyFlowCounterpart, category string, amount float64) {
	switch category {
	case flowEquipment:
		r.OutEquipment += amount
	case flowItems:
		r.OutItems += amount
	case flowWages:
		r.OutWages += amount
	}
}

// --- helpers for MU-alliance report fields ---

func applyMuAllianceTotalIn(r *processedreports.MuAllianceMoneyFlowReport, category string, amount float64) {
	switch category {
	case flowEquipment:
		r.InEquipment += amount
	case flowItems:
		r.InItems += amount
	case flowWages:
		r.InWages += amount
	}
}

func applyMuAllianceTotalOut(r *processedreports.MuAllianceMoneyFlowReport, category string, amount float64) {
	switch category {
	case flowEquipment:
		r.OutEquipment += amount
	case flowItems:
		r.OutItems += amount
	case flowWages:
		r.OutWages += amount
	}
}

func applyMuAllianceInsideIn(r *processedreports.MuAllianceMoneyFlowReport, category string, amount float64) {
	switch category {
	case flowEquipment:
		r.InEquipmentInsideMuAlliance += amount
	case flowItems:
		r.InItemsInsideMuAlliance += amount
	case flowWages:
		r.InWagesInsideMuAlliance += amount
	}
}

func applyMuAllianceInsideOut(r *processedreports.MuAllianceMoneyFlowReport, category string, amount float64) {
	switch category {
	case flowEquipment:
		r.OutEquipmentInsideMuAlliance += amount
	case flowItems:
		r.OutItemsInsideMuAlliance += amount
	case flowWages:
		r.OutWagesInsideMuAlliance += amount
	}
}

func applyMuAllianceOutsideIn(r *processedreports.MuAllianceMoneyFlowReport, category string, amount float64) {
	switch category {
	case flowEquipment:
		r.InEquipmentOutsideMuAlliance += amount
	case flowItems:
		r.InItemsOutsideMuAlliance += amount
	case flowWages:
		r.InWagesOutsideMuAlliance += amount
	}
}

func applyMuAllianceOutsideOut(r *processedreports.MuAllianceMoneyFlowReport, category string, amount float64) {
	switch category {
	case flowEquipment:
		r.OutEquipmentOutsideMuAlliance += amount
	case flowItems:
		r.OutItemsOutsideMuAlliance += amount
	case flowWages:
		r.OutWagesOutsideMuAlliance += amount
	}
}

func applyMuAllianceCounterpartIn(r *processedreports.MuAllianceMoneyFlowCounterpart, category string, amount float64) {
	switch category {
	case flowEquipment:
		r.InEquipment += amount
	case flowItems:
		r.InItems += amount
	case flowWages:
		r.InWages += amount
	}
}

func applyMuAllianceCounterpartOut(r *processedreports.MuAllianceMoneyFlowCounterpart, category string, amount float64) {
	switch category {
	case flowEquipment:
		r.OutEquipment += amount
	case flowItems:
		r.OutItems += amount
	case flowWages:
		r.OutWages += amount
	}
}
