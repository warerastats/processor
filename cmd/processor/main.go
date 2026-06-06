package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/warerastats/models/models"
	"github.com/warerastats/processor/internal/candles"
	"github.com/warerastats/processor/internal/config"
	"github.com/warerastats/processor/internal/job"
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

	// Each job is a periodic, database-only processing loop.
	jobs := []job.Job{
		candles.NewItems(colls, cfg.ItemCandleInterval, 0),
	}

	g, gctx := errgroup.WithContext(ctx)
	for _, j := range jobs {
		g.Go(func() error { return job.Loop(gctx, j) })
	}

	err = g.Wait()
	if err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("Processor exited with error", "error", err)
		os.Exit(1)
	}
	slog.Info("Processor stopped")
}
