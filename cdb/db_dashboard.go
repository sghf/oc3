package cdb

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

type (
	Dashboard struct {
		ID       int64
		ObjectID string
		NodeID   string
		Type     string
		Fmt      string
		Dict     string
		Severity int
		Env      string
		Instance string
		Created  time.Time
		Updated  time.Time
	}

	DashboardObjectType struct {
		ObjectID string
		DashType string
	}
)

// dashboardVisibleCond restricts the rows of a dashboard table (dashboard,
// dashboard_events) to those the caller may see, as the historical dashboard
// does (init/controllers/dashboard.py, q_filter on the node, or on the service,
// of a row): a row whose node or whose service belongs to an app published to
// one of the caller's groups. A manager sees every row.
func dashboardVisibleCond(table string, groups []string, isManager bool) (string, []any) {
	if isManager {
		return "1=1", nil
	}
	clean := cleanGroups(groups)
	if len(clean) == 0 {
		return "1=0", nil
	}
	published := "SELECT a.app FROM apps a" +
		" JOIN apps_publications ap ON ap.app_id = a.id" +
		" JOIN auth_group ag ON ag.id = ap.group_id" +
		" WHERE ag.role IN (" + Placeholders(len(clean)) + ")"
	cond := "(" + table + ".node_id IN (SELECT n.node_id FROM nodes n WHERE n.app IN (" + published + "))" +
		" OR " + table + ".svc_id IN (SELECT s.svc_id FROM services s WHERE s.svc_app IN (" + published + ")))"
	args := make([]any, 0, 2*len(clean))
	for range 2 {
		for _, g := range clean {
			args = append(args, g)
		}
	}
	return cond, args
}

func (oDb *DB) GetAlerts(ctx context.Context, p ListParams) ([]map[string]any, error) {
	defer logDuration("getAlerts", time.Now())

	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getAlerts: no select expressions")
	}

	// Joined so that "nodes.nodename" and "services.svcname" can be selected:
	// a dashboard row only carries the ids.
	cond, args := dashboardVisibleCond("dashboard", p.Groups, p.IsManager)
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") + " FROM dashboard LEFT JOIN nodes ON nodes.node_id = dashboard.node_id LEFT JOIN services ON services.svc_id = dashboard.svc_id WHERE " + cond
	filterConds, filterArgs := p.FilterConditions()
	for _, fc := range filterConds {
		query += " AND " + fc
	}
	args = append(args, filterArgs...)
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("dashboard.id DESC")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getAlerts: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

func (oDb *DB) GetAlert(ctx context.Context, id string, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getAlert: no select expressions")
	}

	cond, condArgs := dashboardVisibleCond("dashboard", p.Groups, p.IsManager)
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") + " FROM dashboard LEFT JOIN nodes ON nodes.node_id = dashboard.node_id LEFT JOIN services ON services.svc_id = dashboard.svc_id WHERE dashboard.id = ? AND " + cond
	args := append([]any{id}, condArgs...)
	query += " " + p.OrderByClause("dashboard.id DESC")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getAlert: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

func (oDb *DB) GetAlertOwner(ctx context.Context, id int64) (svcID, nodeID string, found bool, err error) {
	const query = "SELECT COALESCE(svc_id, ''), COALESCE(node_id, '') FROM dashboard WHERE id = ?"
	err = oDb.DB.QueryRowContext(ctx, query, id).Scan(&svcID, &nodeID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", "", false, nil
	case err != nil:
		return "", "", false, fmt.Errorf("getAlertOwner: %w", err)
	}
	return svcID, nodeID, true, nil
}

func (oDb *DB) DeleteAlert(ctx context.Context, id int64) (int64, error) {
	const query = "DELETE FROM dashboard WHERE id = ?"
	res, err := oDb.DB.ExecContext(ctx, query, id)
	if err != nil {
		return 0, fmt.Errorf("deleteAlert: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("deleteAlert rowsAffected: %w", err)
	}
	if n > 0 {
		oDb.SetChange("dashboard")
	}
	return n, nil
}

func (oDb *DB) FindAlertIDByCriteria(ctx context.Context, cols []string, vals []any) (int64, bool, error) {
	query := "SELECT id FROM dashboard WHERE 1=1"
	for _, c := range cols {
		query += " AND dashboard." + c + " = ?"
	}
	query += " ORDER BY id LIMIT 1"

	var id int64
	err := oDb.DB.QueryRowContext(ctx, query, vals...).Scan(&id)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, nil
	case err != nil:
		return 0, false, fmt.Errorf("findAlertIDByCriteria: %w", err)
	}
	return id, true, nil
}

func (oDb *DB) GetAlertEvents(ctx context.Context, p ListParams) ([]map[string]any, error) {
	defer logDuration("getAlertEvents", time.Now())

	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getAlertEvents: no select expressions")
	}

	cond, args := dashboardVisibleCond("dashboard_events", p.Groups, p.IsManager)
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") + " FROM dashboard_events WHERE " + cond
	filterConds, filterArgs := p.FilterConditions()
	for _, fc := range filterConds {
		query += " AND " + fc
	}
	args = append(args, filterArgs...)

	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("dashboard_events.id DESC")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getAlertEvents: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

var alertWritableColumns = map[string]bool{
	"dash_type": true, "dash_instance": true, "svc_id": true, "node_id": true,
	"dash_severity": true, "dash_fmt": true, "dash_dict": true,
	"dash_env": true, "dash_md5": true,
}

// AlertSvcEnv returns the environment of a service.
func (oDb *DB) AlertSvcEnv(ctx context.Context, svcID string) (string, bool, error) {
	const query = "SELECT COALESCE(svc_env, '') FROM services WHERE svc_id = ?"
	var env string
	err := oDb.DB.QueryRowContext(ctx, query, svcID).Scan(&env)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", false, nil
	case err != nil:
		return "", false, fmt.Errorf("alertSvcEnv: %w", err)
	}
	return env, true, nil
}

// AlertNodeEnv returns the environment of a node.
func (oDb *DB) AlertNodeEnv(ctx context.Context, nodeID string) (string, bool, error) {
	const query = "SELECT COALESCE(node_env, '') FROM nodes WHERE node_id = ?"
	var env string
	err := oDb.DB.QueryRowContext(ctx, query, nodeID).Scan(&env)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", false, nil
	case err != nil:
		return "", false, fmt.Errorf("alertNodeEnv: %w", err)
	}
	return env, true, nil
}

func (oDb *DB) GetAlertForUpdate(ctx context.Context, id int64) (env, dashFmt, dashDict string, found bool, err error) {
	const query = "SELECT COALESCE(dash_env, ''), COALESCE(dash_fmt, ''), COALESCE(dash_dict, '') FROM dashboard WHERE id = ?"
	err = oDb.DB.QueryRowContext(ctx, query, id).Scan(&env, &dashFmt, &dashDict)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", "", "", false, nil
	case err != nil:
		return "", "", "", false, fmt.Errorf("getAlertForUpdate: %w", err)
	}
	return env, dashFmt, dashDict, true, nil
}

func (oDb *DB) UpdateAlertFields(ctx context.Context, id int64, fields map[string]any) error {
	setClauses := []string{"dash_updated = NOW()"}
	args := []any{}
	for col, val := range fields {
		if !alertWritableColumns[col] {
			continue
		}
		setClauses = append(setClauses, col+" = ?")
		args = append(args, val)
	}
	query := "UPDATE dashboard SET " + strings.Join(setClauses, ", ") + " WHERE id = ?"
	args = append(args, id)
	if _, err := oDb.DB.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("updateAlertFields: %w", err)
	}
	oDb.SetChange("dashboard")
	return nil
}

func (oDb *DB) FindAlertIDByKey(ctx context.Context, dashType, nodeID, svcID, dashInstance string) (int64, bool, error) {
	const query = `SELECT id FROM dashboard
		WHERE dash_type = ?
		  AND COALESCE(node_id, '') = ?
		  AND COALESCE(svc_id, '') = ?
		  AND COALESCE(dash_instance, '') = ?
		ORDER BY id LIMIT 1`
	var id int64
	err := oDb.DB.QueryRowContext(ctx, query, dashType, nodeID, svcID, dashInstance).Scan(&id)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, nil
	case err != nil:
		return 0, false, fmt.Errorf("findAlertIDByKey: %w", err)
	}
	return id, true, nil
}

func (oDb *DB) UpsertUpdateAlert(ctx context.Context, id int64, dashFmt, dashDict, dashEnv string, dashSeverity int) error {
	const query = `UPDATE dashboard
		SET dash_updated = NOW(), dash_fmt = ?, dash_dict = ?, dash_env = ?, dash_severity = ?
		WHERE id = ?`
	if _, err := oDb.DB.ExecContext(ctx, query, dashFmt, dashDict, dashEnv, dashSeverity, id); err != nil {
		return fmt.Errorf("upsertUpdateAlert: %w", err)
	}
	oDb.SetChange("dashboard")
	return nil
}

func (oDb *DB) InsertAlert(ctx context.Context, fields map[string]any) (int64, error) {
	cols := []string{"dash_created", "dash_updated"}
	placeholders := []string{"NOW()", "NOW()"}
	var args []any
	for col, val := range fields {
		if !alertWritableColumns[col] {
			continue
		}
		cols = append(cols, col)
		placeholders = append(placeholders, "?")
		args = append(args, val)
	}
	query := "INSERT INTO dashboard (" + strings.Join(cols, ", ") + ") VALUES (" + strings.Join(placeholders, ", ") + ")"
	res, err := oDb.DB.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("insertAlert: %w", err)
	}
	oDb.SetChange("dashboard")
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("insertAlert lastInsertId: %w", err)
	}
	return id, nil
}

func (oDb *DB) GetNodeAlerts(ctx context.Context, nodeID string, p ListParams) ([]map[string]any, error) {
	defer logDuration("getNodeAlerts", time.Now())

	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getNodeAlerts: no select expressions")
	}

	query := "SELECT " + strings.Join(p.SelectExprs, ", ") + " FROM dashboard WHERE dashboard.node_id = ?"
	args := []any{nodeID}

	if !p.IsManager {
		clean := cleanGroups(p.Groups)

		if len(clean) == 0 {
			query += ` AND (
				dashboard.node_id IN (SELECT n.node_id FROM nodes n WHERE n.team_responsible = 'Everybody')
			)`
		} else {
			placeholders := Placeholders(len(clean))
			query += ` AND (
				dashboard.svc_id IN (
					SELECT s.svc_id FROM services s
					JOIN apps a ON s.svc_app = a.app
					JOIN apps_responsibles ar ON ar.app_id = a.id
					JOIN auth_group ag ON ag.id = ar.group_id
					WHERE ag.role IN (` + placeholders + `)
				)
				OR
				dashboard.node_id IN (
					SELECT n.node_id FROM nodes n
					WHERE n.team_responsible = 'Everybody'
					   OR n.team_responsible IN (` + placeholders + `)
				)
			)`
			for _, g := range clean {
				args = append(args, g)
			}
			for _, g := range clean {
				args = append(args, g)
			}
		}
	}

	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("dashboard.id DESC")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getNodeAlerts: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

func (oDb *DB) GetServiceAlerts(ctx context.Context, svcID string, p ListParams) ([]map[string]any, error) {
	defer logDuration("getServiceAlerts", time.Now())

	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getServiceAlerts: no select expressions")
	}

	query := "SELECT " + strings.Join(p.SelectExprs, ", ") + " FROM dashboard WHERE dashboard.svc_id = ?"
	args := []any{svcID}

	if !p.IsManager {
		clean := cleanGroups(p.Groups)
		if len(clean) == 0 {
			query += " AND 1=0"
		} else {
			placeholders := Placeholders(len(clean))
			query += ` AND dashboard.svc_id IN (
				SELECT s.svc_id FROM services s
				JOIN apps a ON s.svc_app = a.app
				JOIN apps_responsibles ar ON ar.app_id = a.id
				JOIN auth_group ag ON ag.id = ar.group_id
				WHERE ag.role IN (` + placeholders + `)
			)`
			for _, g := range clean {
				args = append(args, g)
			}
		}
	}

	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("dashboard.id DESC")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getServiceAlerts: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// DashboardInstanceFrozenUpdate update or remove the "service frozen" alerts for instance
func (oDb *DB) DashboardInstanceFrozenUpdate(ctx context.Context, objectID, nodeID string, objectEnv string, frozen bool) error {
	defer logDuration("dashboardInstanceFrozenUpdate", time.Now())
	const (
		queryThawed = `
			DELETE FROM dashboard
			WHERE 
			    dash_type = 'service frozen' 
			  AND svc_id = ?
			  AND node_id = ?
			`
		sourceFrozen = `SELECT 'service frozen' AS dash_type, ? AS svc_id, ? AS node_id,
			1 AS dash_severity, '' AS dash_fmt, '' AS dash_dict,
			NOW() AS dash_created, NOW() AS dash_updated, ? AS dash_env`
	)
	var (
		err   error
		count int64
	)
	switch frozen {
	case true:
		count, err = oDb.upsertAlerts(ctx, alertsUpsert{
			source:  sourceFrozen,
			args:    []any{objectID, nodeID, objectEnv},
			columns: []string{"dash_type", "svc_id", "node_id", "dash_severity", "dash_fmt", "dash_dict", "dash_created", "dash_updated", "dash_env"},
			update:  []string{"dash_severity = s.dash_severity", "dash_fmt = s.dash_fmt", "dash_dict = s.dash_dict", "dash_updated = s.dash_updated", "dash_env = s.dash_env"},
		})
		if err != nil {
			return fmt.Errorf("update dashboard 'service frozen' for %s@%s: %w", objectID, nodeID, err)
		}
	case false:
		count, err = oDb.execCountContext(ctx, queryThawed, objectID, nodeID)
		if err != nil {
			return fmt.Errorf("delete dashboard 'service frozen' for %s@%s: %w", objectID, nodeID, err)
		}
	}
	if count > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

// dashboardDeleteInstanceNotUpdated delete "instance status not updated" alerts.
func (oDb *DB) DashboardDeleteInstanceNotUpdated(ctx context.Context, objectID, nodeID string) error {
	defer logDuration("dashboardDeleteInstanceNotUpdated", time.Now())
	const (
		query = `DELETE FROM dashboard WHERE svc_id = ? AND node_id = ? AND dash_type = 'instance status not updated'`
	)
	if count, err := oDb.execCountContext(ctx, query, objectID, nodeID); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

func (oDb *DB) DashboardObjectWithTypeDelete(ctx context.Context, l ...*DashboardObjectType) error {
	defer logDuration("dashboardDeleteObject", time.Now())
	if len(l) == 0 {
		return nil
	}
	placeholders := strings.Repeat("(?,?),", len(l)-1) + "(?,?)"

	query := fmt.Sprintf("DELETE FROM `dashboard` WHERE (`svc_id`, `dash_type`) IN (%s)", placeholders)
	args := make([]any, 0, 2*len(l))
	for _, v := range l {
		args = append(args, v.ObjectID, v.DashType)
	}

	_, err := oDb.ExecContext(ctx, query, args...)
	return err
}

// DashboardDeleteObjectWithType delete from dashboard where svc_id and dash_type match
func (oDb *DB) DashboardDeleteObjectWithType(ctx context.Context, objectID, dashType string) error {
	defer logDuration("dashboardDeleteObjectWithType: "+dashType, time.Now())
	const (
		query = `DELETE FROM dashboard WHERE svc_id = ? AND dash_type = ?`
	)
	if count, err := oDb.execCountContext(ctx, query, objectID, dashType); err != nil {
		return fmt.Errorf("dashboardDeleteObjectWithType %s: %w", dashType, err)
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

// ObjectInAckUnavailabilityPeriod returns list of objectIDs is in acknowledge unavailability period.
func (oDb *DB) ObjectInAckUnavailabilityPeriod(ctx context.Context, l ...string) (ids []string, err error) {
	defer logDuration("ObjectInAckUnavailabilityPeriod", time.Now())
	if len(l) == 0 {
		return nil, nil
	}
	placeholders := strings.Repeat("?,", len(l)-1) + "?"
	query := fmt.Sprintf("SELECT `svc_id` FROM svcmon_log_ack WHERE `svc_id` IN (%s) AND `mon_begin` <= NOW() AND `mon_end` >= NOW()", placeholders)
	args := make([]any, len(l))
	for i, s := range l {
		args[i] = s
	}
	var (
		rows *sql.Rows
	)

	rows, err = oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	ids = make([]string, 0, len(l))
	for rows.Next() {
		var s string
		if err = rows.Scan(&s); err != nil {
			return
		}
		ids = append(ids, s)
	}
	return ids, rows.Err()
}

// dashboardUpdateObject delete "service unavailable" alerts.
func (oDb *DB) DashboardUpdateObject(ctx context.Context, l ...*Dashboard) error {
	defer logDuration("DashboardUpdateObject", time.Now())
	if len(l) == 0 {
		return nil
	}
	rows := make([][]any, len(l))
	for i, v := range l {
		rows[i] = []any{v.ObjectID, v.Type, v.Fmt, v.Severity, v.Dict, v.Env}
	}
	given, args := givenRows([]string{"svc_id", "dash_type", "dash_fmt", "dash_severity", "dash_dict", "dash_env"}, rows)
	count, err := oDb.upsertAlerts(ctx, alertsUpsert{
		source:  "SELECT r.*, NOW() AS dash_created, NOW() AS dash_updated FROM (" + given + ") r",
		args:    args,
		columns: []string{"svc_id", "dash_type", "dash_fmt", "dash_severity", "dash_dict", "dash_env", "dash_created", "dash_updated"},
		update:  []string{"dash_fmt = s.dash_fmt", "dash_severity = s.dash_severity", "dash_dict = s.dash_dict", "dash_updated = s.dash_updated", "dash_env = s.dash_env"},
	})
	if err != nil {
		return err
	}
	if count > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

func (oDb *DB) DashboardDeleteNetworkWrongMaskNotUpdated(ctx context.Context) error {
	defer logDuration("DashboardDeleteNetworkWrongMaskNotUpdated", time.Now())
	const (
		query = `DELETE FROM dashboard
                  WHERE
                    dash_type="netmask misconfigured" AND
                    dash_updated < DATE_SUB(NOW(), INTERVAL 1 MINUTE)`
	)
	if count, err := oDb.execCountContext(ctx, query); err != nil {
		return fmt.Errorf("DashboardDeleteNetworkWrongMaskNotUpdated: %w", err)
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

func (oDb *DB) DashboardDeleteActionErrors(ctx context.Context) error {
	defer logDuration("DashboardDeleteActionErrors", time.Now())
	const (
		query = `DELETE FROM dashboard
                  WHERE
                    dash_type LIKE "%action err%" AND
                    (svc_id, node_id) NOT IN (
                      SELECT svc_id, node_id
                      FROM b_action_errors
                    )`
	)
	if count, err := oDb.execCountContext(ctx, query); err != nil {
		return fmt.Errorf("DashboardDeleteActionErrors: %w", err)
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

func (oDb *DB) PurgeAlertsOnDeletedNodes(ctx context.Context) error {
	const (
		query = `DELETE d
			FROM dashboard d
			LEFT JOIN nodes n ON d.node_id = n.node_id
			WHERE
			  n.node_id IS NULL AND
			  d.node_id != ""`
	)
	if count, err := oDb.execCountContext(ctx, query); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

func (oDb *DB) PurgeAlertsOnDeletedInstances(ctx context.Context) error {
	const (
		query = `DELETE d
			FROM dashboard d
			LEFT JOIN svcmon n ON d.node_id = n.node_id AND d.svc_id = n.svc_id
			WHERE
			  n.node_id IS NULL AND
			  n.svc_id IS NULL AND
			  d.node_id != "" AND
			  d.svc_id != ""`
	)
	if count, err := oDb.execCountContext(ctx, query); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

func (oDb *DB) PurgeAlertsOnDeletedServices(ctx context.Context) error {
	const (
		query = `DELETE d
			FROM dashboard d
			LEFT JOIN services n ON d.svc_id = n.svc_id
			WHERE
			  n.svc_id IS NULL AND
			  d.svc_id != ""`
	)
	if count, err := oDb.execCountContext(ctx, query); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

func (oDb *DB) DashboardUpdateNodesNotUpdated(ctx context.Context, maxAge time.Duration) error {
	u := alertsUpsert{
		source: `SELECT
                 "node information not updated" AS dash_type,
                 "" AS svc_id,
                 0 AS dash_severity,
                 "" AS dash_fmt,
                 "" AS dash_dict,
                 updated AS dash_created,
                 "" AS dash_dict_md5,
                 node_env AS dash_env,
                 NOW() AS dash_updated,
                 node_id,
                 NULL AS dash_instance
               FROM nodes
               WHERE updated < date_sub(NOW(), INTERVAL ? SECOND)`,
		args:    []any{maxAgeSeconds(maxAge)},
		columns: positionalAlertColumns,
		update:  []string{"dash_updated = s.dash_updated"},
	}
	if count, err := oDb.upsertAlerts(ctx, u); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

func (oDb *DB) DashboardUpdateChecksNotUpdated(ctx context.Context, maxAge time.Duration) error {
	request := `
		DELETE FROM dashboard
		WHERE
		  dash_type = "check value not updated" AND
		  node_id NOT IN (SELECT DISTINCT node_id FROM checks_live)
	`
	if count, err := oDb.execCountContext(ctx, request); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}

	request = `
		DELETE FROM dashboard
		WHERE id IN (
		    -- Suppression des "check out of bounds" non correspondants
		    SELECT d.id FROM dashboard d
		    LEFT JOIN checks_live c ON
			d.dash_dict_md5 = MD5(` + checkOutOfBoundsDictSQL("c") + `)
			AND d.node_id = c.node_id
		    WHERE
			d.dash_type = "check out of bounds"
			AND c.id IS NULL

		    UNION ALL

		    -- Suppression des "check value not updated" non correspondents
		    SELECT d.id FROM dashboard d
		    LEFT JOIN checks_live c ON
			d.dash_dict_md5 = MD5(CONCAT('{"i":"', c.chk_instance, '", "t":"', c.chk_type, '"}'))
			AND d.node_id = c.node_id
		    WHERE
			d.dash_type = "check value not updated"
			AND c.id IS NULL
		)
	`
	if count, err := oDb.execCountContext(ctx, request); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}
	u := alertsUpsert{
		source: `
	SELECT
	    "check value not updated" AS dash_type,
	    "" AS svc_id,
	    IF(n.node_env = "PRD", 1, 0) AS dash_severity,
	    "%(t)s:%(i)s" AS dash_fmt,
	    CONCAT('{"i":"', chk_instance, '", "t":"', chk_type, '"}') AS dash_dict,
	    chk_updated AS dash_created,
	    MD5(CONCAT('{"i":"', chk_instance, '", "t":"', chk_type, '"}')) AS dash_dict_md5,
	    n.node_env AS dash_env,
	    NOW() AS dash_updated,
	    c.node_id AS node_id,
	    CONCAT(chk_type, ":", chk_instance) AS dash_instance
	FROM checks_live c
	JOIN nodes n ON c.node_id = n.node_id
	WHERE chk_updated < DATE_SUB(NOW(), INTERVAL ? SECOND)`,
		args:    []any{maxAgeSeconds(maxAge)},
		columns: positionalAlertColumns,
		update:  []string{"dash_updated = s.dash_updated"},
	}
	if count, err := oDb.upsertAlerts(ctx, u); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

func (oDb *DB) AlertActionErrors(ctx context.Context, line BActionErrorCount) error {
	var (
		env      string
		severity int
	)
	if line.SvcEnv != nil && *line.SvcEnv == "PRD" {
		env = *line.SvcEnv
		severity = 4
	} else {
		env = "TST"
		severity = 3
	}
	u := alertsUpsert{
		source: `SELECT
                   "action errors" AS dash_type,
                   ? AS svc_id,
                   ? AS node_id,
                   ? AS dash_severity,
                   "%(err)s action errors" AS dash_fmt,
                   CONCAT('{"err": "', ?, '"}') AS dash_dict,
                   NOW() AS dash_created,
                   ? AS dash_env,
                   NOW() AS dash_updated`,
		args:    []any{line.SvcID, line.NodeID, severity, line.ErrCount, env},
		columns: []string{"dash_type", "svc_id", "node_id", "dash_severity", "dash_fmt", "dash_dict", "dash_created", "dash_env", "dash_updated"},
		update:  []string{"dash_severity = s.dash_severity", "dash_fmt = s.dash_fmt", "dash_dict = s.dash_dict", "dash_updated = s.dash_updated"},
	}
	if count, err := oDb.upsertAlerts(ctx, u); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

func (oDb *DB) DashboardDeleteActionErrorsWithNoError(ctx context.Context) error {
	request := `
             DELETE FROM dashboard
             WHERE
               dash_dict='{"err": "0"}' and
               dash_type='action errors'
	`
	if count, err := oDb.execCountContext(ctx, request); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

func (oDb *DB) DashboardUpdateServiceConfigNotUpdated(ctx context.Context, maxAge time.Duration) error {
	u := alertsUpsert{
		source: `SELECT
               "service configuration not updated" AS dash_type,
               svc_id,
               IF(svc_env="PRD", 1, 0) AS dash_severity,
               "" AS dash_fmt,
               "" AS dash_dict,
               updated AS dash_created,
               "" AS dash_dict_md5,
               svc_env AS dash_env,
               NOW() AS dash_updated,
               "" AS node_id,
               NULL AS dash_instance
             FROM services
             WHERE updated < DATE_SUB(NOW(), INTERVAL ? SECOND)`,
		args:    []any{maxAgeSeconds(maxAge)},
		columns: positionalAlertColumns,
		update:  []string{"dash_updated = s.dash_updated"},
	}
	if count, err := oDb.upsertAlerts(ctx, u); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

func (oDb *DB) DashboardUpdateInstancesNotUpdated(ctx context.Context, maxAge time.Duration) error {
	u := alertsUpsert{
		source: `SELECT
		  "service status not updated" AS dash_type,
		  svc_id,
		  IF(mon_svctype="PRD", 1, 0) AS dash_severity,
		  "" AS dash_fmt,
		  "" AS dash_dict,
		  mon_updated AS dash_created,
		  "" AS dash_dict_md5,
		  mon_svctype AS dash_env,
		  NOW() AS dash_updated,
		  node_id,
		  NULL AS dash_instance
		FROM svcmon
		WHERE mon_updated < DATE_SUB(NOW(), INTERVAL ? SECOND)`,
		args:    []any{maxAgeSeconds(maxAge)},
		columns: positionalAlertColumns,
		update:  []string{"dash_updated = s.dash_updated"},
	}
	if count, err := oDb.upsertAlerts(ctx, u); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}

	request := `
		DELETE FROM dashboard
		WHERE id IN (
		    SELECT dashboard.id
		    FROM dashboard
		    LEFT JOIN svcmon ON
			dashboard.svc_id = svcmon.svc_id AND
			dashboard.node_id = svcmon.node_id
		    WHERE
			dashboard.dash_type = "service status not updated" AND
			dashboard.svc_id != "" AND
			dashboard.node_id != "" AND
			(svcmon.id IS NULL OR svcmon.mon_updated >= DATE_SUB(NOW(), INTERVAL ? SECOND))
		)
	`
	if count, err := oDb.execCountContext(ctx, request, maxAgeSeconds(maxAge)); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

func (oDb *DB) DashboardUpdateNodeMaintenanceExpired(ctx context.Context) error {
	request := `SET @now = NOW()`
	if _, err := oDb.ExecContext(ctx, request); err != nil {
		return err
	}

	u := alertsUpsert{
		source: `SELECT
		  "node maintenance expired" AS dash_type,
		  "" AS svc_id,
		  1 AS dash_severity,
		  "" AS dash_fmt,
		  "" AS dash_dict,
		  @now AS dash_created,
		  "" AS dash_dict_md5,
		  node_env AS dash_env,
		  @now AS dash_updated,
		  node_id,
		  NULL AS dash_instance
		FROM nodes
		WHERE
		  maintenance_end IS NOT NULL AND
		  maintenance_end != "0000-00-00 00:00:00" AND
		  maintenance_end < @now`,
		columns: positionalAlertColumns,
		update:  []string{"dash_updated = s.dash_updated"},
	}
	if count, err := oDb.upsertAlerts(ctx, u); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}

	request = `
		DELETE FROM dashboard
		WHERE
		  dash_type="node maintenance expired" AND
		  (
		    dash_updated < @now or
		    dash_updated IS NULL
		  )
	`
	if count, err := oDb.execCountContext(ctx, request); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

func (oDb *DB) DashboardUpdateNodeCloseToMaintenanceEnd(ctx context.Context) error {
	request := `SET @now = NOW()`
	if _, err := oDb.ExecContext(ctx, request); err != nil {
		return err
	}

	u := alertsUpsert{
		source: `SELECT
		  "node close to maintenance end" AS dash_type,
		  "" AS svc_id,
		  0 AS dash_severity,
		  "" AS dash_fmt,
		  "" AS dash_dict,
		  @now AS dash_created,
		  "" AS dash_dict_md5,
		  node_env AS dash_env,
		  @now AS dash_updated,
		  node_id,
		  NULL AS dash_instance
		FROM nodes
		WHERE
		  maintenance_end IS NOT NULL AND
		  maintenance_end != "0000-00-00 00:00:00" AND
		  maintenance_end > DATE_SUB(@now, INTERVAL 30 DAY) AND
		  maintenance_end > @now`,
		columns: positionalAlertColumns,
		update:  []string{"dash_updated = s.dash_updated"},
	}
	if count, err := oDb.upsertAlerts(ctx, u); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}

	request = `
		DELETE FROM dashboard
		WHERE
		  dash_type="node close to maintenance end" AND
		  (
		    dash_updated < @now or
		    dash_updated IS NULL
		  )
	`
	if count, err := oDb.execCountContext(ctx, request); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

func (oDb *DB) DashboardUpdateNodeWithoutMaintenanceEnd(ctx context.Context) error {
	request := `SET @now = NOW()`
	if _, err := oDb.ExecContext(ctx, request); err != nil {
		return err
	}

	u := alertsUpsert{
		source: `SELECT
		  "node without maintenance end date" AS dash_type,
		  "" AS svc_id,
		  0 AS dash_severity,
		  "" AS dash_fmt,
		  "" AS dash_dict,
		  @now AS dash_created,
		  "" AS dash_dict_md5,
		  node_env AS dash_env,
		  @now AS dash_updated,
		  node_id,
		  NULL AS dash_instance
		FROM nodes
		WHERE
                 (warranty_end IS NULL OR
                  warranty_end = "0000-00-00 00:00:00" OR
                  warranty_end < @now) AND
                 (maintenance_end IS NULL OR
                  maintenance_end = "0000-00-00 00:00:00") AND
                 model not like "%virt%" AND
                 model not like "%Not Specified%" AND
                 model not like "%KVM%"`,
		columns: positionalAlertColumns,
		update:  []string{"dash_updated = s.dash_updated"},
	}
	if count, err := oDb.upsertAlerts(ctx, u); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}

	request = `
		DELETE FROM dashboard
		WHERE
		  dash_type="node without maintenance end date" AND
		  (
		    dash_updated < @now or
		    dash_updated IS NULL
		  )
	`
	if count, err := oDb.execCountContext(ctx, request); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

func (oDb *DB) DashboardUpdateAppWithoutResponsible(ctx context.Context) error {
	request := `
		DELETE FROM dashboard
		WHERE
		  dash_type="application code without responsible" and
		  (
		    dash_dict IN (
		      SELECT
		        JSON_OBJECT("a", a.app)
		      FROM apps a JOIN apps_responsibles ar ON a.id=ar.app_id
		    ) OR
		    dash_dict = "" OR
		    dash_dict IS NULL
		  )
	`
	if _, err := oDb.ExecContext(ctx, request); err != nil {
		return err
	}

	request = `
		DELETE FROM dashboard
		WHERE
                  dash_type="application code without responsible" AND
                  dash_dict NOT IN (
                    SELECT
                      JSON_OBJECT("a", a.app)
                    FROM apps a
                )
	`
	if count, err := oDb.execCountContext(ctx, request); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}

	u := alertsUpsert{
		source: `SELECT
		  "application code without responsible" AS dash_type,
		  "" AS svc_id,
		  2 AS dash_severity,
		  "%(a)s" AS dash_fmt,
		  JSON_OBJECT("a", a.app) AS dash_dict,
		  NOW() AS dash_created,
		  MD5(JSON_OBJECT("a", a.app)) AS dash_dict_md5,
		  "" AS dash_env,
		  NOW() AS dash_updated,
		  "" AS node_id,
		  a.app AS dash_instance
		FROM apps a LEFT JOIN apps_responsibles ar ON a.id=ar.app_id
		WHERE
		  ar.group_id IS NULL`,
		columns: positionalAlertColumns,
		update:  []string{"dash_updated = s.dash_updated"},
	}
	if count, err := oDb.upsertAlerts(ctx, u); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

func (oDb *DB) DashboardUpdatePkgDiffForNode(ctx context.Context, nodeID string) error {
	request := `SET @now = NOW()`
	_, err := oDb.ExecContext(ctx, request)
	if err != nil {
		return err
	}

	processSvcID := func(svcID, monSvctype, monVmtype string) error {
		var query string
		if monVmtype != "" {
			// encap peers
			query = `
				SELECT DISTINCT nodes.node_id, nodes.nodename
				FROM svcmon
				JOIN nodes ON svcmon.node_id = nodes.node_id
				WHERE svcmon.svc_id = ?
				AND svcmon.mon_updated > DATE_SUB(NOW(), INTERVAL 20 MINUTE)
				AND svcmon.mon_vmtype != ""
				ORDER BY nodes.nodename
			`
		} else {
			// non-encap peers
			query = `
				SELECT DISTINCT nodes.node_id, nodes.nodename
				FROM svcmon
				JOIN nodes ON svcmon.node_id = nodes.node_id
				WHERE svcmon.svc_id = ?
				AND svcmon.mon_updated > DATE_SUB(NOW(), INTERVAL 20 MINUTE)
				AND svcmon.mon_vmtype = ""
				ORDER BY nodes.nodename
			`
		}

		rows, err := oDb.DB.QueryContext(ctx, query, svcID)
		if err != nil {
			return fmt.Errorf("failed to query nodes: %v", err)
		}
		defer rows.Close()

		var nodeIDs []string
		var nodenames []string
		for rows.Next() {
			var nodeID string
			var nodename string
			if err := rows.Scan(&nodeID, &nodename); err != nil {
				return fmt.Errorf("failed to scan node row: %v", err)
			}
			nodeIDs = append(nodeIDs, nodeID)
			nodenames = append(nodenames, nodename)
		}

		if len(nodeIDs) < 2 {
			return nil
		}

		// Count pkg diffs
		var pkgDiffCount int
		placeholders := make([]string, len(nodeIDs))
		args := make([]any, len(nodeIDs))
		for i, id := range nodeIDs {
			placeholders[i] = "?"
			args[i] = id
		}
		query = fmt.Sprintf(`
			SELECT COUNT(pkg_name)
			FROM (
				SELECT
					pkg_name,
					pkg_version,
					pkg_arch,
					pkg_type,
					COUNT(DISTINCT node_id) AS c
				FROM packages
				WHERE
					node_id IN (%s)
					AND pkg_name NOT LIKE "gpg-pubkey%%"
				GROUP BY
					pkg_name,
					pkg_version,
					pkg_arch,
					pkg_type
			) AS t
			WHERE t.c != ?
		`, strings.Join(placeholders, ","))
		args = append(args, len(nodeIDs))

		err = oDb.DB.QueryRowContext(ctx, query, args...).Scan(&pkgDiffCount)
		if err != nil {
			return fmt.Errorf("failed to count package differences: %v", err)
		}

		if pkgDiffCount == 0 {
			return nil
		}

		sev := 0
		if monSvctype == "PRD" {
			sev = 1
		}

		// truncate too long node names list
		skip := 0
		trail := ""
		nodesStr := strings.Join(nodenames, ",")
		for len(nodesStr)+len(trail) > 50 {
			skip++
			nodenames = nodenames[:len(nodenames)-1]
			nodesStr = strings.Join(nodenames, ",")
			trail = fmt.Sprintf(", ... (+%d)", skip)
		}
		nodesStr += trail

		// Format dash_dict JSON content
		dashDict := map[string]any{
			"n":     pkgDiffCount,
			"nodes": nodesStr,
		}
		dashDictJSON, err := json.Marshal(dashDict)
		if err != nil {
			return fmt.Errorf("failed to marshal dash_dict: %v", err)
		}

		dashDictMD5 := fmt.Sprintf("%x", md5.Sum(dashDictJSON))

		_, err = oDb.upsertAlerts(ctx, alertsUpsert{
			source: `SELECT
				"package differences in cluster" AS dash_type,
				? AS svc_id,
				"" AS node_id,
				? AS dash_severity,
				"%(n)s package differences in cluster %(nodes)s" AS dash_fmt,
				? AS dash_dict,
				? AS dash_dict_md5,
				@now AS dash_created,
				@now AS dash_updated,
				? AS dash_env`,
			args:    []any{svcID, sev, dashDictJSON, dashDictMD5, monSvctype},
			columns: []string{"dash_type", "svc_id", "node_id", "dash_severity", "dash_fmt", "dash_dict", "dash_dict_md5", "dash_created", "dash_updated", "dash_env"},
			update: []string{"dash_severity = s.dash_severity", "dash_fmt = s.dash_fmt", "dash_dict = s.dash_dict",
				"dash_dict_md5 = s.dash_dict_md5", "dash_updated = s.dash_updated", "dash_env = s.dash_env"},
		})
		if err != nil {
			return fmt.Errorf("failed to insert/update dashboard: %v", err)
		}

		return nil
	}

	// Get the list of svc_id for instances having recently updated status
	rows, err := oDb.DB.QueryContext(ctx, `
		SELECT svcmon.svc_id, svcmon.mon_svctype, svcmon.mon_vmtype
		FROM svcmon
		JOIN nodes ON svcmon.node_id = nodes.node_id
		WHERE svcmon.node_id = ? AND svcmon.mon_updated > DATE_SUB(NOW(), INTERVAL 19 MINUTE)
	`, nodeID)
	if err != nil {
		return fmt.Errorf("failed to query svcmon: %v", err)
	}
	defer rows.Close()

	var svcIDs []any
	todo := make(map[string]func() error)
	for rows.Next() {
		var svcID string
		var monSvctype, monVmtype sql.NullString
		if err := rows.Scan(&svcID, &monSvctype, &monVmtype); err != nil {
			return fmt.Errorf("failed to scan svcmon row: %v", err)
		}

		// Remember which svc_id needs non-updated alert clean up
		svcIDs = append(svcIDs, svcID)

		// Defer after rows.Close() to avoid busy db conn errors
		todo[svcID] = func() error {
			return processSvcID(svcID, monSvctype.String, monVmtype.String)
		}
	}
	rows.Close()
	for svcID, fn := range todo {
		if err := fn(); err != nil {
			return fmt.Errorf("failed to process svc_id %s: %v", svcID, err)
		}
	}

	// Clean up non updated alerts
	if len(svcIDs) > 0 {
		query := fmt.Sprintf(`
			DELETE FROM dashboard
			WHERE svc_id IN (%s)
			AND dash_type = "package differences in cluster"
			AND dash_updated < @now
		`, Placeholders(len(svcIDs)))

		_, err := oDb.ExecContext(ctx, query, svcIDs...)
		if err != nil {
			return fmt.Errorf("failed to delete old dashboard entries: %v", err)
		}
	}

	return nil
}

func (oDb *DB) DashboardUpdateCompModDiff(ctx context.Context) error {
	svcIDs, err := oDb.ObjectIDsUpdatedLast(ctx, 2)
	if err != nil {
		return err
	}
	for _, svcID := range svcIDs {
		if err := oDb.DashboardUpdateCompModDiffForSvc(ctx, svcID); err != nil {
			return err
		}
	}
	return nil
}

func (oDb *DB) DashboardUpdateCompModDiffForSvc(ctx context.Context, svcID string) error {
	_, err := oDb.ExecContext(ctx, "SET @now = NOW()")
	if err != nil {
		return fmt.Errorf("failed to set @now: %v", err)
	}

	defer func() {
		_, err := oDb.ExecContext(ctx, `
			DELETE FROM dashboard
			WHERE
				dash_type = "compliance moduleset attachment differences in cluster" AND
				svc_id = ? AND
				dash_updated < @now
		`, svcID)
		if err != nil {
			slog.Warn(fmt.Sprintf("failed to clean up old dashboard entries: %v", err))
		}
	}()

	// Récupérer les nœuds associés au svc_id
	rows, err := oDb.DB.QueryContext(ctx, `
		SELECT node_id, mon_svctype
		FROM svcmon
		WHERE svc_id = ?
		ORDER BY node_id
	`, svcID)
	if err != nil {
		return fmt.Errorf("failed to query svcmon: %v", err)
	}
	defer rows.Close()

	var nodes []string
	var monSvctype sql.NullString
	for rows.Next() {
		var nodeID string
		if err := rows.Scan(&nodeID, &monSvctype); err != nil {
			return fmt.Errorf("failed to scan svcmon row: %v", err)
		}
		nodes = append(nodes, nodeID)
	}

	n := len(nodes)
	if n < 2 {
		return nil
	}

	// Déterminer la sévérité
	sev := 0
	if monSvctype.String == "PRD" {
		sev = 1
	}

	// Tronquer la liste des nodes si trop longue
	skip := 0
	trail := ""
	nodesStr := strings.Join(nodes, ",")
	for len(nodesStr)+len(trail) > 50 {
		skip++
		nodes = nodes[:len(nodes)-1]
		nodesStr = strings.Join(nodes, ",")
		trail = fmt.Sprintf(", ... (+%d)", skip)
	}
	nodesStr += trail

	// Compter les différences de moduleset
	var ndiff int64
	err = oDb.DB.QueryRowContext(ctx, `
		SELECT COUNT(t.n)
		FROM (
			SELECT
				COUNT(nm.node_id) AS n,
				GROUP_CONCAT(nm.node_id) AS nodes,
				ms.modset_name AS modset
			FROM
				comp_node_moduleset nm,
				svcmon m,
				comp_moduleset ms
			WHERE
				m.svc_id = ? AND
				m.node_id = nm.node_id AND
				nm.modset_id = ms.id
			GROUP BY
				modset_name
		) AS t
		WHERE t.n != ?
	`, svcID, n).Scan(&ndiff)
	if err != nil {
		return fmt.Errorf("failed to count moduleset differences: %v", err)
	}

	// Si aucune différence, nettoyer et retourner
	if ndiff == 0 {
		return nil
	}

	// Créer le JSON pour dash_dict
	dashDict := map[string]interface{}{
		"n":     ndiff,
		"nodes": nodesStr,
	}
	dashDictJSON, err := json.Marshal(dashDict)
	if err != nil {
		return fmt.Errorf("failed to marshal dash_dict: %v", err)
	}

	// Calculer le MD5 de dash_dict
	dashDictMD5 := fmt.Sprintf("%x", md5.Sum(dashDictJSON))

	// Insérer ou mettre à jour l'alerte
	_, err = oDb.upsertAlerts(ctx, alertsUpsert{
		source: `SELECT
			"compliance moduleset attachment differences in cluster" AS dash_type,
			? AS svc_id,
			"" AS node_id,
			? AS dash_severity,
			'%(n)d differences in cluster %(nodes)s' AS dash_fmt,
			? AS dash_dict,
			? AS dash_dict_md5,
			@now AS dash_created,
			@now AS dash_updated,
			? AS dash_env`,
		args:    []any{svcID, sev, dashDictJSON, dashDictMD5, monSvctype.String},
		columns: []string{"dash_type", "svc_id", "node_id", "dash_severity", "dash_fmt", "dash_dict", "dash_dict_md5", "dash_created", "dash_updated", "dash_env"},
		update:  []string{"dash_updated = s.dash_updated"},
	})
	if err != nil {
		return fmt.Errorf("failed to insert/update dashboard: %v", err)
	}

	return nil
}

func (oDb *DB) ObjectIDsUpdatedLast(ctx context.Context, days int) ([]string, error) {
	rows, err := oDb.DB.QueryContext(ctx, `
		SELECT svc_id
		FROM services
		WHERE updated > DATE_SUB(NOW(), INTERVAL ? DAY)
	`, days)
	if err != nil {
		return nil, fmt.Errorf("failed to query svcmon: %v", err)
	}
	defer rows.Close()

	var svcIDs []string
	for rows.Next() {
		var svcID string
		if err := rows.Scan(&svcID); err != nil {
			return nil, fmt.Errorf("failed to scan services row: %v", err)
		}
		svcIDs = append(svcIDs, svcID)
	}
	return svcIDs, nil
}

func (oDb *DB) DashboardUpdateCompRsetDiff(ctx context.Context) error {
	svcIDs, err := oDb.ObjectIDsUpdatedLast(ctx, 2)
	if err != nil {
		return err
	}
	for _, svcID := range svcIDs {
		if err := oDb.DashboardUpdateCompRsetDiffForSvc(ctx, svcID); err != nil {
			return err
		}
	}
	return nil
}

func (oDb *DB) DashboardUpdateCompRsetDiffForSvc(ctx context.Context, svcID string) error {
	_, err := oDb.ExecContext(ctx, "SET @now = NOW()")
	if err != nil {
		return fmt.Errorf("failed to set @now: %v", err)
	}

	defer func() {
		_, err := oDb.ExecContext(ctx, `
			DELETE FROM dashboard
			WHERE
				dash_type = "compliance ruleset attachment differences in cluster" AND
				svc_id = ? AND
				dash_updated < @now
		`, svcID)
		if err != nil {
			slog.Warn(fmt.Sprintf("failed to clean up old dashboard entries: %v", err))
		}
	}()

	// Récupérer les nœuds associés au svc_id
	rows, err := oDb.DB.QueryContext(ctx, `
		SELECT node_id, mon_svctype
		FROM svcmon
		WHERE svc_id = ?
		ORDER BY node_id
	`, svcID)
	if err != nil {
		return fmt.Errorf("failed to query svcmon: %v", err)
	}
	defer rows.Close()

	var nodes []string
	var monSvctype sql.NullString
	for rows.Next() {
		var nodeID string
		if err := rows.Scan(&nodeID, &monSvctype); err != nil {
			return fmt.Errorf("failed to scan svcmon row: %v", err)
		}
		nodes = append(nodes, nodeID)
	}

	n := len(nodes)
	if n < 2 {
		return nil
	}

	// Déterminer la sévérité
	sev := 0
	if monSvctype.String == "PRD" {
		sev = 1
	}

	// Tronquer la liste des nodes si trop longue
	skip := 0
	trail := ""
	nodesStr := strings.Join(nodes, ",")
	for len(nodesStr)+len(trail) > 50 {
		skip++
		nodes = nodes[:len(nodes)-1]
		nodesStr = strings.Join(nodes, ",")
		trail = fmt.Sprintf(", ... (+%d)", skip)
	}
	nodesStr += trail

	// Compter les différences de ruleset
	var ndiff int64
	err = oDb.DB.QueryRowContext(ctx, `
		SELECT COUNT(t.n)
		FROM (
			SELECT
				COUNT(rn.node_id) AS n,
				GROUP_CONCAT(rn.node_id) AS nodes,
				rs.ruleset_name AS ruleset
			FROM
				comp_rulesets_nodes rn,
				svcmon m,
				comp_ruleset rs
			WHERE
				m.svc_id = ? AND
				m.node_id = rn.node_id AND
				rn.ruleset_id = rs.id
			GROUP BY
				ruleset_name
			ORDER BY
				ruleset_name
		) AS t
		WHERE t.n != ?
	`, svcID, n).Scan(&ndiff)
	if err != nil {
		return fmt.Errorf("failed to count ruleset differences: %v", err)
	}

	// Si aucune différence, nettoyer et retourner
	if ndiff == 0 {
		return nil
	}

	// Créer le JSON pour dash_dict
	dashDict := map[string]interface{}{
		"n":     ndiff,
		"nodes": nodesStr,
	}
	dashDictJSON, err := json.Marshal(dashDict)
	if err != nil {
		return fmt.Errorf("failed to marshal dash_dict: %v", err)
	}

	// Calculer le MD5 de dash_dict
	dashDictMD5 := fmt.Sprintf("%x", md5.Sum(dashDictJSON))

	// Insérer ou mettre à jour l'alerte
	_, err = oDb.upsertAlerts(ctx, alertsUpsert{
		source: `SELECT
			"compliance ruleset attachment differences in cluster" AS dash_type,
			? AS svc_id,
			"" AS node_id,
			? AS dash_severity,
			'%(n)d differences in cluster %(nodes)s' AS dash_fmt,
			? AS dash_dict,
			? AS dash_dict_md5,
			@now AS dash_created,
			@now AS dash_updated,
			? AS dash_env`,
		args:    []any{svcID, sev, dashDictJSON, dashDictMD5, monSvctype.String},
		columns: []string{"dash_type", "svc_id", "node_id", "dash_severity", "dash_fmt", "dash_dict", "dash_dict_md5", "dash_created", "dash_updated", "dash_env"},
		update:  []string{"dash_updated = s.dash_updated"},
	})
	if err != nil {
		return fmt.Errorf("failed to insert/update dashboard: %v", err)
	}

	return nil
}

// AlertEvent is an occurrence of an alert: when it began, and when it ended,
// none while it is open.
type AlertEvent struct {
	ID    int64
	Begin string
	End   *string
}

// GetAlertEventsOf returns the occurrences of the alert visible to the caller:
// the dashboard_events of its dash_md5, node and service, as alert_timeline()
// of the historical collector selects them, the last limit of them, oldest
// first. found is false when the alert does not exist or is not visible;
// truncated tells that older ones were left out.
func (oDb *DB) GetAlertEventsOf(ctx context.Context, id string, groups []string, isManager bool, limit int) (events []AlertEvent, found, truncated bool, err error) {
	cond, args := dashboardVisibleCond("dashboard", groups, isManager)
	var n int
	if err := oDb.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM dashboard WHERE dashboard.id = ? AND "+cond,
		append([]any{id}, args...)...).Scan(&n); err != nil {
		return nil, false, false, fmt.Errorf("getAlertEventsOf: %w", err)
	}
	if n == 0 {
		return nil, false, false, nil
	}
	// An open event is written without its end: the NOT NULL datetime then holds
	// the zero date, which IS NULL matches, as the closing trigger relies on.
	const query = `SELECT e.id, DATE_FORMAT(e.dash_begin, '%Y-%m-%d %H:%i:%s'),
			IF(e.dash_end IS NULL, NULL, DATE_FORMAT(e.dash_end, '%Y-%m-%d %H:%i:%s'))
		FROM dashboard d
		JOIN dashboard_events e ON e.dash_md5 = d.dash_md5
			AND COALESCE(e.node_id, '') = COALESCE(d.node_id, '')
			AND COALESCE(e.svc_id, '') = COALESCE(d.svc_id, '')
		WHERE d.id = ?
		ORDER BY e.dash_begin DESC, e.id DESC
		LIMIT ?`
	rows, err := oDb.DB.QueryContext(ctx, query, id, limit+1)
	if err != nil {
		return nil, true, false, fmt.Errorf("getAlertEventsOf: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var e AlertEvent
		var end sql.NullString
		if err := rows.Scan(&e.ID, &e.Begin, &end); err != nil {
			return nil, true, false, fmt.Errorf("getAlertEventsOf: %w", err)
		}
		if end.Valid {
			e.End = &end.String
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, true, false, fmt.Errorf("getAlertEventsOf: %w", err)
	}
	if len(events) > limit {
		events, truncated = events[:limit], true
	}
	// Oldest first, as a timeline reads.
	for i, j := 0, len(events)-1; i < j; i, j = i+1, j-1 {
		events[i], events[j] = events[j], events[i]
	}
	return events, true, truncated, nil
}

// DashboardUpdateServicesBelowSLA raises the "service availability below SLA"
// alert of each service whose availability rate of the last 30 days is under
// its target, critical in production, a warning elsewhere, its message naming
// both, and clears it once the rate is back over the target, or the target or
// the rate gone. The rate is that the task services_availability stores.
func (oDb *DB) DashboardUpdateServicesBelowSLA(ctx context.Context) error {
	const below = "svc_sla IS NOT NULL AND svc_availability IS NOT NULL AND svc_availability < svc_sla"
	// "99.500" reads "99.5", "97.210000" "97.21": the percents as written.
	const percent = "CONCAT(TRIM(TRAILING '.' FROM TRIM(TRAILING '0' FROM CAST(%s AS CHAR))), '%%')"
	availability := fmt.Sprintf(percent, "ROUND(svc_availability, 2)")
	sla := fmt.Sprintf(percent, "svc_sla")
	u := alertsUpsert{
		source: `SELECT
		  svc_id, '' AS node_id, 'service availability below SLA' AS dash_type, IF(svc_env = 'PRD', 3, 2) AS dash_severity,
		  'availability %(availability)s below the SLA of %(sla)s' AS dash_fmt,
		  JSON_OBJECT('availability', ` + availability + `, 'sla', ` + sla + `) AS dash_dict,
		  NOW() AS dash_created, NOW() AS dash_updated, svc_env AS dash_env, '' AS dash_instance
		FROM services
		WHERE ` + below,
		columns: []string{"svc_id", "node_id", "dash_type", "dash_severity", "dash_fmt", "dash_dict", "dash_created", "dash_updated", "dash_env", "dash_instance"},
		update:  []string{"dash_severity = s.dash_severity", "dash_fmt = s.dash_fmt", "dash_dict = s.dash_dict", "dash_env = s.dash_env", "dash_updated = s.dash_updated"},
	}
	if count, err := oDb.upsertAlerts(ctx, u); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}
	request := `
		DELETE FROM dashboard
		WHERE dash_type = 'service availability below SLA'
		  AND svc_id NOT IN (SELECT svc_id FROM services WHERE ` + below + `)`
	if count, err := oDb.execCountContext(ctx, request); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

// AlertTypesByIDs returns the type of each alert of ids that exists, by id.
func (oDb *DB) AlertTypesByIDs(ctx context.Context, ids []int64) (map[int64]string, error) {
	m := map[int64]string{}
	if len(ids) == 0 {
		return m, nil
	}
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	query := "SELECT id, dash_type FROM dashboard WHERE id IN (" + strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",") + ")"
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("alertTypesByIDs: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var (
			id  int64
			typ string
		)
		if err := rows.Scan(&id, &typ); err != nil {
			return nil, fmt.Errorf("alertTypesByIDs: %w", err)
		}
		m[id] = typ
	}
	return m, rows.Err()
}

// DeleteAlertsByIDs deletes the alerts of ids in one statement, and returns how
// many there were. Each one still closes its event (trigger dash_del_evt).
func (oDb *DB) DeleteAlertsByIDs(ctx context.Context, ids []int64) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	query := "DELETE FROM dashboard WHERE id IN (" + strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",") + ")"
	n, err := oDb.execCountContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("deleteAlertsByIDs: %w", err)
	}
	if n > 0 {
		oDb.SetChange("dashboard")
	}
	return n, nil
}
