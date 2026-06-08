package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/warerastats/models/models"
	"github.com/warerastats/processor/internal/candles"
	"github.com/warerastats/processor/internal/config"
	"github.com/warerastats/processor/internal/estimators"
	"github.com/warerastats/processor/internal/job"
	"github.com/warerastats/processor/internal/reports"
	"golang.org/x/sync/errgroup"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	slog.Info("Processor starting")

	cfg, err := config.Load()
	if err != nil {
		slog.Error("Failed loading config", "error", err)
		os.Exit(1)
	}

	colls, err := models.Init(ctx)
	if err != nil {
		slog.Error("Failed connecting to the database!", "error", err)
		os.Exit(1)
	}
	defer colls.Close(ctx)

	// Each job is a periodic, database-only processing loop. Offsets stagger
	// first passes so cadence-aligned jobs don't all fire at once.
	jobs := []job.Job{
		// Candles (foundational).
		candles.NewItems(colls, cfg.ItemCandleInterval, 0),
		candles.NewWages(colls, cfg.WageCandleInterval, 30*time.Second),
		// Market/wage state snapshots.
		reports.NewMarketState(colls, cfg.MarketStateInterval, 1*time.Minute),
		reports.NewWageState(colls, cfg.WageStateInterval, 90*time.Second),
		reports.NewItemMarket(colls, cfg.ItemMarketInterval, 2*time.Minute),
		// Equipment pricing.
		reports.NewEquipment(colls, cfg.EquipmentInterval, 3*time.Minute),
		// Estimators.
		estimators.NewInflation(colls, cfg.InflationInterval, 4*time.Minute),
		estimators.NewUserInventory(colls, cfg.UserInventoryInterval, 5*time.Minute),
		estimators.NewCountryFlip(colls, cfg.CountryFlipInterval, 90*time.Second),
		estimators.NewUserFlip(colls, cfg.UserFlipInterval, 2*time.Minute),
		estimators.NewParticipation(colls, cfg.ParticipationInterval, 6*time.Minute),
		// Battle, cases, dismantle reports.
		reports.NewBattleDamage(colls, cfg.BattleDamageInterval, 30*time.Second, cfg.WorkerPoolSize),
		reports.NewCases(colls, cfg.CasesInterval, 7*time.Minute),
		reports.NewDismantle(colls, cfg.DismantleInterval, 8*time.Minute),
		// Hourly/daily heavy reports.
		reports.NewTaxFlow(colls, cfg.TaxFlowInterval, 5*time.Minute),
		reports.NewFinance(colls, cfg.FinanceInterval, 10*time.Minute),
		reports.NewMoneyFlow(colls, cfg.MoneyFlowInterval, 12*time.Minute),
		reports.NewWealth(colls, cfg.WealthInterval, 15*time.Minute),
	}

	g, gctx := errgroup.WithContext(ctx)
	for _, j := range jobs {
		g.Go(func() error { return job.Loop(gctx, j) })
	}

	// One-shot recovery sweep: fill in reports for ended battles that aged out
	// of the live reportable window. A backfill failure must not stop the jobs.
	backfill := reports.NewBattleDamage(colls, cfg.BattleDamageInterval, 0, cfg.WorkerPoolSize)
	g.Go(func() error {
		err := backfill.Backfill(gctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("Battle damage backfill failed", "error", err)
		}
		return nil
	})

	err = g.Wait()
	if err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("Processor exited with error", "error", err)
		os.Exit(1)
	}
	slog.Info("Processor stopped")
}
