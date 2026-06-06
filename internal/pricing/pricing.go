// Package pricing derives equipment valuations from market trades for reuse
// across battle, participation, and finance jobs.
package pricing

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/warerastats/models/models"
)

// SkillKey is a deterministic canonical key for an equipment skill combination.
func SkillKey(skills map[string]float64) string {
	if len(skills) == 0 {
		return "-"
	}
	keys := make([]string, 0, len(skills))
	for k := range skills {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte('|')
		}
		b.WriteString(k)
		b.WriteByte(':')
		b.WriteString(strconv.FormatFloat(skills[k], 'f', -1, 64))
	}
	return b.String()
}

// ItemAverages maps each equipment item code to its volume-weighted average
// price (and trade count) over a window.
type ItemAverages struct {
	Avg   map[string]float64
	Count map[string]int
}

// LoadItemAverages computes per-item-code average equipment prices over (since, until].
func LoadItemAverages(ctx context.Context, colls *models.Collections, since, until time.Time) (ItemAverages, error) {
	trades, err := colls.Transactions.MarketTransaction.GetEquipmentTradesRange(ctx, since, until)
	if err != nil {
		return ItemAverages{}, err
	}
	sum := make(map[string]float64)
	cnt := make(map[string]int)
	for _, t := range trades {
		sum[t.ItemCode] += t.Money
		cnt[t.ItemCode]++
	}
	avg := make(map[string]float64, len(sum))
	for code, total := range sum {
		if cnt[code] > 0 {
			avg[code] = total / float64(cnt[code])
		}
	}
	return ItemAverages{Avg: avg, Count: cnt}, nil
}
