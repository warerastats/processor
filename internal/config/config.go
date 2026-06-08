package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds the processor's tunables.
type Config struct {
	ItemCandleInterval    time.Duration
	WageCandleInterval    time.Duration
	MarketStateInterval   time.Duration
	WageStateInterval     time.Duration
	EquipmentInterval     time.Duration
	InflationInterval     time.Duration
	UserInventoryInterval time.Duration
	CountryFlipInterval   time.Duration
	UserFlipInterval      time.Duration
	ParticipationInterval time.Duration
	BattleDamageInterval  time.Duration
	ItemMarketInterval    time.Duration
	CasesInterval         time.Duration
	DismantleInterval     time.Duration
	TaxFlowInterval       time.Duration
	FinanceInterval       time.Duration
	MoneyFlowInterval     time.Duration
	WealthInterval        time.Duration
	WorkerPoolSize        int
}

// Load reads configuration from the environment.
func Load() (Config, error) {
	cfg := Config{
		ItemCandleInterval:    getDuration("ITEM_CANDLE_INTERVAL", 10*time.Minute),
		WageCandleInterval:    getDuration("WAGE_CANDLE_INTERVAL", 10*time.Minute),
		MarketStateInterval:   getDuration("MARKET_STATE_INTERVAL", 10*time.Minute),
		WageStateInterval:     getDuration("WAGE_STATE_INTERVAL", 10*time.Minute),
		EquipmentInterval:     getDuration("EQUIPMENT_PRICING_INTERVAL", 30*time.Minute),
		InflationInterval:     getDuration("INFLATION_INTERVAL", time.Hour),
		UserInventoryInterval: getDuration("USER_INVENTORY_INTERVAL", time.Hour),
		CountryFlipInterval:   getDuration("COUNTRY_FLIP_INTERVAL", 15*time.Minute),
		UserFlipInterval:      getDuration("USER_FLIP_INTERVAL", 15*time.Minute),
		ParticipationInterval: getDuration("PARTICIPATION_INTERVAL", 30*time.Minute),
		BattleDamageInterval:  getDuration("BATTLE_DAMAGE_INTERVAL", 10*time.Minute),
		ItemMarketInterval:    getDuration("ITEM_MARKET_INTERVAL", 10*time.Minute),
		CasesInterval:         getDuration("CASES_INTERVAL", 30*time.Minute),
		DismantleInterval:     getDuration("DISMANTLE_INTERVAL", 30*time.Minute),
		TaxFlowInterval:       getDuration("TAX_FLOW_INTERVAL", time.Hour),
		FinanceInterval:       getDuration("FINANCE_INTERVAL", time.Hour),
		MoneyFlowInterval:     getDuration("MONEY_FLOW_INTERVAL", time.Hour),
		WealthInterval:        getDuration("WEALTH_INTERVAL", time.Hour),
		WorkerPoolSize:        getInt("WORKER_POOL_SIZE", 32),
	}
	return cfg, nil
}

func getDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v != "" {
		d, err := time.ParseDuration(v)
		if err == nil {
			return d
		}
	}
	return def
}

func getInt(key string, def int) int {
	v := os.Getenv(key)
	if v != "" {
		i, err := strconv.Atoi(v)
		if err == nil {
			return i
		}
	}
	return def
}
