package estimators

import (
	"context"
	"log/slog"
	"time"

	"github.com/warerastats/models/models"
	"github.com/warerastats/models/models/stores/processed/estimators"
	"github.com/warerastats/models/models/stores/trackers"
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
	now := time.Now().UTC()
	var (
		curUser  bson.ObjectID
		curItems map[string]int
		users    int
	)

	flush := func() error {
		if curItems == nil {
			return nil
		}
		err := j.Colls.Processed.Estimators.UserInventory.Upsert(ctx, estimators.UserInventory{
			UserID:    curUser,
			Items:     curItems,
			UpdatedAt: now,
		})
		if err != nil {
			slog.Error("Failed upserting user inventory", "userId", curUser.Hex(), "error", err)
		}
		return nil
	}

	// Rows arrive sorted by owner, so a new owner id closes the prior user's
	// inventory; only one user's counts are held in memory at a time.
	err := j.Colls.Trackers.Item.StreamActiveInventory(ctx, func(c trackers.InventoryCount) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if curItems == nil || c.Key.OwnerUserID != curUser {
			err := flush()
			if err != nil {
				return err
			}
			curUser = c.Key.OwnerUserID
			curItems = make(map[string]int)
			users++
		}
		curItems[c.Key.ItemCode] += c.Count
		return nil
	})
	if err != nil {
		return err
	}

	err = flush()
	if err != nil {
		return err
	}
	slog.Info("User inventory rebuilt", "users", users)
	return nil
}
