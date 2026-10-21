package scheduler

import (
	"context"
	"time"

	"github.com/opensvc/oc3/availability"
)

// TaskServicesAvailability stores the availability rate of the last 30 days of
// every service, which the Services view sorts and filters by: the rate moves
// as the statuses are received, hence the short period. The dashboard alerts on
// the services under their SLA follow the rates just stored.
var TaskServicesAvailability = Task{
	name:    "services_availability",
	desc:    "store the 30-day availability rate of the services",
	period:  10 * time.Minute,
	fn:      taskServicesAvailability,
	timeout: 5 * time.Minute,
}

func taskServicesAvailability(ctx context.Context, task *Task) error {
	odb := task.DB()
	ids, err := odb.ServiceIDs(ctx)
	if err != nil {
		return err
	}
	if err := availability.Refresh(ctx, odb, ids, time.Now()); err != nil {
		return err
	}
	return alertServicesBelowSLA(ctx, task)
}

func alertServicesBelowSLA(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()
	if err := odb.DashboardUpdateServicesBelowSLA(ctx); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}
