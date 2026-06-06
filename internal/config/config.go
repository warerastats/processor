package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds the processor's tunables.
type Config struct {
	ItemCandleInterval  time.Duration
	WageCandleInterval  time.Duration
	MarketStateInterval time.Duration
	WorkerPoolSize      int
}

// Load reads configuration from the environment.
func Load() (Config, error) {
	cfg := Config{
		ItemCandleInterval:  getDuration("ITEM_CANDLE_INTERVAL", 10*time.Minute),
		WageCandleInterval:  getDuration("WAGE_CANDLE_INTERVAL", 10*time.Minute),
		MarketStateInterval: getDuration("MARKET_STATE_INTERVAL", 10*time.Minute),
		WorkerPoolSize:      getInt("WORKER_POOL_SIZE", 32),
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
