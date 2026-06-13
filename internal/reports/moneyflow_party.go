package reports

import (
	"time"

	processedreports "github.com/warerastats/models/models/stores/processed/reports"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type partyAccum struct {
	row          *processedreports.PartyMoneyFlowReport
	counterparts map[bson.ObjectID]*processedreports.PartyMoneyFlowCounterpart
}

func newPartyAccum(id bson.ObjectID, dayStart time.Time) *partyAccum {
	return &partyAccum{
		row: &processedreports.PartyMoneyFlowReport{
			ID:       processedreports.PartyMoneyFlowReportID(id, dayStart),
			PartyID:  id,
			DayStart: dayStart,
		},
		counterparts: map[bson.ObjectID]*processedreports.PartyMoneyFlowCounterpart{},
	}
}

func (a *partyAccum) counterpart(id bson.ObjectID) *processedreports.PartyMoneyFlowCounterpart {
	r := a.counterparts[id]
	if r == nil {
		r = &processedreports.PartyMoneyFlowCounterpart{CountryID: id}
		a.counterparts[id] = r
	}
	return r
}

func applyPartyTotalIn(r *processedreports.PartyMoneyFlowReport, category string, amount float64) {
	switch category {
	case flowEquipment:
		r.InEquipment += amount
	case flowItems:
		r.InItems += amount
	case flowWages:
		r.InWages += amount
	}
}

func applyPartyTotalOut(r *processedreports.PartyMoneyFlowReport, category string, amount float64) {
	switch category {
	case flowEquipment:
		r.OutEquipment += amount
	case flowItems:
		r.OutItems += amount
	case flowWages:
		r.OutWages += amount
	}
}

func applyPartyInsideIn(r *processedreports.PartyMoneyFlowReport, category string, amount float64) {
	switch category {
	case flowEquipment:
		r.InEquipmentInsideParty += amount
	case flowItems:
		r.InItemsInsideParty += amount
	case flowWages:
		r.InWagesInsideParty += amount
	}
}

func applyPartyInsideOut(r *processedreports.PartyMoneyFlowReport, category string, amount float64) {
	switch category {
	case flowEquipment:
		r.OutEquipmentInsideParty += amount
	case flowItems:
		r.OutItemsInsideParty += amount
	case flowWages:
		r.OutWagesInsideParty += amount
	}
}

func applyPartySameCountryIn(r *processedreports.PartyMoneyFlowReport, category string, amount float64) {
	switch category {
	case flowEquipment:
		r.InEquipmentSameCountryOutsideParty += amount
	case flowItems:
		r.InItemsSameCountryOutsideParty += amount
	case flowWages:
		r.InWagesSameCountryOutsideParty += amount
	}
}

func applyPartySameCountryOut(r *processedreports.PartyMoneyFlowReport, category string, amount float64) {
	switch category {
	case flowEquipment:
		r.OutEquipmentSameCountryOutsideParty += amount
	case flowItems:
		r.OutItemsSameCountryOutsideParty += amount
	case flowWages:
		r.OutWagesSameCountryOutsideParty += amount
	}
}

func applyPartySameAllianceIn(r *processedreports.PartyMoneyFlowReport, category string, amount float64) {
	switch category {
	case flowEquipment:
		r.InEquipmentSameAllianceCrossBorder += amount
	case flowItems:
		r.InItemsSameAllianceCrossBorder += amount
	case flowWages:
		r.InWagesSameAllianceCrossBorder += amount
	}
}

func applyPartySameAllianceOut(r *processedreports.PartyMoneyFlowReport, category string, amount float64) {
	switch category {
	case flowEquipment:
		r.OutEquipmentSameAllianceCrossBorder += amount
	case flowItems:
		r.OutItemsSameAllianceCrossBorder += amount
	case flowWages:
		r.OutWagesSameAllianceCrossBorder += amount
	}
}

func applyPartyOutsideAllianceIn(r *processedreports.PartyMoneyFlowReport, category string, amount float64) {
	switch category {
	case flowEquipment:
		r.InEquipmentOutsideAlliance += amount
	case flowItems:
		r.InItemsOutsideAlliance += amount
	case flowWages:
		r.InWagesOutsideAlliance += amount
	}
}

func applyPartyOutsideAllianceOut(r *processedreports.PartyMoneyFlowReport, category string, amount float64) {
	switch category {
	case flowEquipment:
		r.OutEquipmentOutsideAlliance += amount
	case flowItems:
		r.OutItemsOutsideAlliance += amount
	case flowWages:
		r.OutWagesOutsideAlliance += amount
	}
}

func applyPartyCounterpartIn(r *processedreports.PartyMoneyFlowCounterpart, category string, amount float64) {
	switch category {
	case flowEquipment:
		r.InEquipment += amount
	case flowItems:
		r.InItems += amount
	case flowWages:
		r.InWages += amount
	}
}

func applyPartyCounterpartOut(r *processedreports.PartyMoneyFlowCounterpart, category string, amount float64) {
	switch category {
	case flowEquipment:
		r.OutEquipment += amount
	case flowItems:
		r.OutItems += amount
	case flowWages:
		r.OutWages += amount
	}
}

// applyPartyIn records the party-level flow for the receiving side.
func applyPartyIn(
	acc map[bson.ObjectID]*partyAccum,
	dayStart time.Time,
	category string,
	amount float64,
	partySide flowParty,
	counter flowParty,
	allianceByCountry map[bson.ObjectID]*bson.ObjectID,
) {
	if amount <= 0 || partySide.partyID == nil {
		return
	}

	partyID := *partySide.partyID
	a := acc[partyID]
	if a == nil {
		a = newPartyAccum(partyID, dayStart)
		acc[partyID] = a
	}
	applyPartyTotalIn(a.row, category, amount)

	inside := counter.partyID != nil && *counter.partyID == partyID
	sameCountry := !inside && partySide.countryID != nil && counter.countryID != nil && *partySide.countryID == *counter.countryID

	if inside {
		applyPartyInsideIn(a.row, category, amount)
	} else if sameCountry {
		applyPartySameCountryIn(a.row, category, amount)
	} else {
		srcA := allianceFor(partySide.countryID, allianceByCountry)
		dstA := allianceFor(counter.countryID, allianceByCountry)
		if srcA != nil && dstA != nil && *srcA == *dstA {
			applyPartySameAllianceIn(a.row, category, amount)
		} else {
			applyPartyOutsideAllianceIn(a.row, category, amount)
		}
	}

	if counter.countryID != nil {
		applyPartyCounterpartIn(a.counterpart(*counter.countryID), category, amount)
	}
}

// applyPartyOut records the party-level flow for the sending side.
func applyPartyOut(
	acc map[bson.ObjectID]*partyAccum,
	dayStart time.Time,
	category string,
	amount float64,
	partySide flowParty,
	counter flowParty,
	allianceByCountry map[bson.ObjectID]*bson.ObjectID,
) {
	if amount <= 0 || partySide.partyID == nil {
		return
	}

	partyID := *partySide.partyID
	a := acc[partyID]
	if a == nil {
		a = newPartyAccum(partyID, dayStart)
		acc[partyID] = a
	}
	applyPartyTotalOut(a.row, category, amount)

	inside := counter.partyID != nil && *counter.partyID == partyID
	sameCountry := !inside && partySide.countryID != nil && counter.countryID != nil && *partySide.countryID == *counter.countryID

	if inside {
		applyPartyInsideOut(a.row, category, amount)
	} else if sameCountry {
		applyPartySameCountryOut(a.row, category, amount)
	} else {
		srcA := allianceFor(partySide.countryID, allianceByCountry)
		dstA := allianceFor(counter.countryID, allianceByCountry)
		if srcA != nil && dstA != nil && *srcA == *dstA {
			applyPartySameAllianceOut(a.row, category, amount)
		} else {
			applyPartyOutsideAllianceOut(a.row, category, amount)
		}
	}

	if counter.countryID != nil {
		applyPartyCounterpartOut(a.counterpart(*counter.countryID), category, amount)
	}
}

func allianceFor(countryID *bson.ObjectID, m map[bson.ObjectID]*bson.ObjectID) *bson.ObjectID {
	if countryID == nil {
		return nil
	}
	return m[*countryID]
}
