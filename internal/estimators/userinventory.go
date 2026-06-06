package estimators

import (
	"context"
	"log/slog"
	"time"

	"github.com/warerastats/models/models"
	"github.com/warerastats/models/models/stores/processed/estimators"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// UserInventory snapshots each user's current equipment holdings from the items tracker.
type UserInventory struct {
	Colls    *models.Collections
	interval time.Duration
	offset   time.Duration
}

// NewUserInventory builds the user-inventory estimator job.
func NewUserInventory(colls *models.Collections, interval, offset time.Duration) *UserInventory {
	return &UserInventory{Colls: colls, interval: interval, offset: offset}
}

func (j *UserInventory) Name() string            { return "user_inventory" }
func (j *UserInventory) Interval() time.Duration { return j.interval }
func (j *UserInventory) Offset() time.Duration   { return j.offset }

// Run rebuilds each user's current inventory from non-destroyed items.
func (j *UserInventory) Run(ctx context.Context) error {
	counts, err := j.Colls.Trackers.Item.AggregateActiveInventory(ctx)
	if err != nil {
		return err
	}

	byUser := make(map[bson.ObjectID]map[string]int)
	for _, c := range counts {
		m := byUser[c.Key.OwnerUserID]
		if m == nil {
			m = make(map[string]int)
			byUser[c.Key.OwnerUserID] = m
		}
		m[c.Key.ItemCode] += c.Count
	}

	now := time.Now().UTC()
	for userID, items := range byUser {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err = j.Colls.Processed.Estimators.UserInventory.Upsert(ctx, estimators.UserInventory{
			UserID:    userID,
			Items:     items,
			UpdatedAt: now,
		})
		if err != nil {
			slog.Error("Failed upserting user inventory", "userId", userID.Hex(), "error", err)
		}
	}
	slog.Info("User inventory rebuilt", "users", len(byUser))
	return nil
}
