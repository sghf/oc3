package cdb

import (
	"context"
	"fmt"
	"slices"
)

// alertsUpsert describes alerts to write in the dashboard, each kept in place
// when already there, see rowsUpsert: the feeders and the scheduler confirm
// most alerts again and again.
type alertsUpsert struct {
	// source is a SELECT giving one row per alert, its columns named after
	// those of the dashboard. It gives dash_type, and svc_id, node_id and
	// dash_instance when not empty: with dash_type, the unique key of an alert.
	source string
	args   []any

	// columns are the columns of source, written in an alert missing.
	columns []string

	// update are the assignments to an alert already there, as
	// "column = expression", the expression reading the row of source as s.
	update []string
}

// upsertAlerts writes the alerts u describes and returns the count of alerts
// changed or inserted.
func (oDb *DB) upsertAlerts(ctx context.Context, u alertsUpsert) (int64, error) {
	if !slices.Contains(u.columns, "dash_type") {
		return 0, fmt.Errorf("upsertAlerts: the source gives no dash_type")
	}
	column := func(name string) string {
		if slices.Contains(u.columns, name) {
			return "s." + name
		}
		// The default of the column, the value a new alert takes.
		return "''"
	}
	changed, inserted, err := oDb.upsertRows(ctx, rowsUpsert{
		table:   "dashboard",
		source:  u.source,
		args:    u.args,
		columns: u.columns,
		// The unique key idx1, dash_md5 being generated from the type and the
		// instance.
		match: fmt.Sprintf("t.svc_id = %s AND t.node_id = %s AND t.dash_md5 = MD5(CONCAT(s.dash_type, IFNULL(%s, '')))",
			column("svc_id"), column("node_id"), column("dash_instance")),
		update: u.update,
	})
	return changed + inserted, err
}

// positionalAlertColumns are the columns of the dashboard in the order of the
// table, but the id and the generated dash_md5: those the sources converted from
// positional INSERT ... SELECT statements give.
var positionalAlertColumns = []string{
	"dash_type", "svc_id", "dash_severity", "dash_fmt", "dash_dict", "dash_created",
	"dash_dict_md5", "dash_env", "dash_updated", "node_id", "dash_instance",
}
