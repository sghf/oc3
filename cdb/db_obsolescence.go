package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

func (oDb *DB) StatObsolescenceHW(ctx context.Context) error {
	query := `INSERT IGNORE INTO obsolescence (
			obs_type,
			obs_name,
			obs_warn_date_updated_by,
			obs_warn_date_updated,
			obs_alert_date_updated_by,
			obs_alert_date_updated
		     )
		     SELECT "hw", model, "collector", NOW(), "collector", NOW()
		     FROM nodes
		     WHERE model != ''
		     GROUP BY model
            `
	if _, err := oDb.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("update obsolescence failed: %w", err)
	}
	query = `
		DELETE FROM dashboard
		WHERE node_id IN (
		    SELECT n.node_id
		    FROM obsolescence o
		    JOIN nodes n ON o.obs_name = n.model
		    WHERE o.obs_type = "hw" AND (
			o.obs_alert_date IS NULL
			OR o.obs_name LIKE "%virtual%"
			OR o.obs_name LIKE "%virtuel%"
			OR o.obs_name LIKE "%cluster%"
			OR o.obs_alert_date = "0000-00-00 00:00:00"
			OR o.obs_warn_date >= NOW()
			OR o.obs_alert_date <= NOW()
		    )
		) AND dash_type = "hardware obsolescence warning"
           `
	if _, err := oDb.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("update obsolescence failed: %w", err)
	}
	query = `
		SELECT
		    "hardware obsolescence warning" AS dash_type,
		    "" AS svc_id,
		    0 AS dash_severity,
		    "%(o)s warning since %(a)s" AS dash_fmt,
		    JSON_OBJECT("a", o.obs_warn_date, "o", o.obs_name) AS dash_dict,
		    NOW() AS dash_created,
		    "" AS dash_dict_md5,
		    n.node_env AS dash_env,
		    NOW() AS dash_updated,
		    n.node_id AS node_id
		FROM obsolescence o
		JOIN nodes n ON o.obs_name = n.model
		WHERE
		    o.obs_type = "hw"
		    AND o.obs_alert_date IS NOT NULL
		    AND o.obs_alert_date != "0000-00-00 00:00:00"
		    AND o.obs_name NOT LIKE "%virtual%"
		    AND o.obs_name NOT LIKE "%virtuel%"
		    AND o.obs_name NOT LIKE "%cluster%"
		    AND o.obs_warn_date < NOW()
		    AND o.obs_alert_date > NOW()
	`
	if _, err := oDb.upsertAlerts(ctx, alertsUpsert{
		source:  query,
		columns: []string{"dash_type", "svc_id", "dash_severity", "dash_fmt", "dash_dict", "dash_created", "dash_dict_md5", "dash_env", "dash_updated", "node_id"},
		update:  []string{"dash_updated = NOW()"},
	}); err != nil {
		return fmt.Errorf("update obsolescence failed: %w", err)
	}
	query = `
		DELETE FROM dashboard
		WHERE node_id IN (
		    SELECT n.node_id
		    FROM obsolescence o
		    JOIN nodes n ON o.obs_name = n.model
		    WHERE o.obs_type = "hw" AND (
			o.obs_alert_date IS NULL
			OR o.obs_name LIKE "%virtual%"
			OR o.obs_name LIKE "%virtuel%"
			OR o.obs_name LIKE "%cluster%"
			OR o.obs_alert_date = "0000-00-00 00:00:00"
			OR o.obs_alert_date >= NOW()
		    )
		) AND dash_type = "hardware obsolescence alert"
           `
	if _, err := oDb.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("update obsolescence failed: %w", err)
	}
	query = `
		SELECT
		    "hardware obsolescence alert" AS dash_type,
		    "" AS svc_id,
		    0 AS dash_severity,
		    "%(o)s alert since %(a)s" AS dash_fmt,
		    JSON_OBJECT("a", o.obs_alert_date, "o", o.obs_name) AS dash_dict,
		    NOW() AS dash_created,
		    "" AS dash_dict_md5,
		    n.node_env AS dash_env,
		    NOW() AS dash_updated,
		    n.node_id AS node_id
		FROM obsolescence o
		JOIN nodes n ON o.obs_name = n.model
		WHERE
		    o.obs_type = "hw"
		    AND o.obs_alert_date IS NOT NULL
		    AND o.obs_alert_date != "0000-00-00 00:00:00"
		    AND o.obs_name NOT LIKE "%virtual%"
		    AND o.obs_name NOT LIKE "%virtuel%"
		    AND o.obs_name NOT LIKE "%cluster%"
		    AND o.obs_alert_date < NOW()
	`
	if _, err := oDb.upsertAlerts(ctx, alertsUpsert{
		source:  query,
		columns: []string{"dash_type", "svc_id", "dash_severity", "dash_fmt", "dash_dict", "dash_created", "dash_dict_md5", "dash_env", "dash_updated", "node_id"},
		update:  []string{"dash_updated = NOW()"},
	}); err != nil {
		return fmt.Errorf("update obsolescence failed: %w", err)
	}
	return nil
}

func (oDb *DB) StatObsolescenceOS(ctx context.Context) error {
	query := `INSERT IGNORE INTO obsolescence (
			obs_type,
			obs_name,
			obs_warn_date_updated_by,
			obs_warn_date_updated,
			obs_alert_date_updated_by,
			obs_alert_date_updated
		     )
		     SELECT "os", os_concat, "collector", NOW(), "collector", NOW()
		     FROM nodes
		     WHERE os_concat != ''
		     GROUP BY os_concat
                    `
	if _, err := oDb.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("update obsolescence failed: %w", err)
	}
	query = `
		DELETE FROM dashboard
		WHERE node_id IN (
		    SELECT n.node_id
		    FROM obsolescence o
		    JOIN nodes n ON o.obs_name = n.os_concat
		    WHERE o.obs_type = "os" AND (
			o.obs_alert_date IS NULL
			OR o.obs_alert_date = "0000-00-00 00:00:00"
			OR o.obs_warn_date >= NOW()
			OR o.obs_alert_date <= NOW()
		    )
		) AND dash_type = "os obsolescence warning"
           `
	if _, err := oDb.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("update obsolescence failed: %w", err)
	}
	query = `
		SELECT
		    "os obsolescence warning" AS dash_type,
		    "" AS svc_id,
		    0 AS dash_severity,
		    "%(o)s warning since %(a)s" AS dash_fmt,
		    JSON_OBJECT("a", o.obs_warn_date, "o", o.obs_name) AS dash_dict,
		    NOW() AS dash_created,
		    "" AS dash_dict_md5,
		    n.node_env AS dash_env,
		    NOW() AS dash_updated,
		    n.node_id AS node_id
		FROM obsolescence o
		JOIN nodes n ON o.obs_name = n.os_concat
		WHERE
		    o.obs_type = "os"
		    AND o.obs_alert_date IS NOT NULL
		    AND o.obs_alert_date != "0000-00-00 00:00:00"
		    AND o.obs_warn_date < NOW()
		    AND o.obs_alert_date > NOW()
	`
	if _, err := oDb.upsertAlerts(ctx, alertsUpsert{
		source:  query,
		columns: []string{"dash_type", "svc_id", "dash_severity", "dash_fmt", "dash_dict", "dash_created", "dash_dict_md5", "dash_env", "dash_updated", "node_id"},
		update:  []string{"dash_updated = NOW()"},
	}); err != nil {
		return fmt.Errorf("update obsolescence failed: %w", err)
	}
	query = `
		DELETE FROM dashboard
		WHERE node_id IN (
		    SELECT n.node_id
		    FROM obsolescence o
		    JOIN nodes n ON o.obs_name = n.os_concat
		    WHERE o.obs_type = "os" AND (
			o.obs_alert_date IS NULL
			OR o.obs_alert_date = "0000-00-00 00:00:00"
			OR o.obs_alert_date >= NOW()
		    )
		) AND dash_type = "os obsolescence alert"
           `
	if _, err := oDb.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("update obsolescence failed: %w", err)
	}
	query = `
		SELECT
		    "os obsolescence alert" AS dash_type,
		    "" AS svc_id,
		    0 AS dash_severity,
		    "%(o)s alert since %(a)s" AS dash_fmt,
		    JSON_OBJECT("a", o.obs_alert_date, "o", o.obs_name) AS dash_dict,
		    NOW() AS dash_created,
		    "" AS dash_dict_md5,
		    n.node_env AS dash_env,
		    NOW() AS dash_updated,
		    n.node_id AS node_id
		FROM obsolescence o
		JOIN nodes n ON o.obs_name = n.os_concat
		WHERE
		    o.obs_type = "os"
		    AND o.obs_alert_date IS NOT NULL
		    AND o.obs_alert_date != "0000-00-00 00:00:00"
		    AND o.obs_alert_date < NOW()
	`
	if _, err := oDb.upsertAlerts(ctx, alertsUpsert{
		source:  query,
		columns: []string{"dash_type", "svc_id", "dash_severity", "dash_fmt", "dash_dict", "dash_created", "dash_dict_md5", "dash_env", "dash_updated", "node_id"},
		update:  []string{"dash_updated = NOW()"},
	}); err != nil {
		return fmt.Errorf("update obsolescence failed: %w", err)
	}
	return nil
}

// PurgeAlertsObsWithout deletes dashboard "obsolescence date not set" alerts if
// the node model or operating system is no longer the one referenced in the alert.
func (oDb *DB) PurgeAlertsObsWithout(ctx context.Context) error {
	query := `DELETE d FROM dashboard d
		  JOIN nodes n ON d.node_id = n.node_id
		  WHERE
		    d.dash_type IN ("hardware obsolescence alert date not set", "hardware obsolescence warning date not set") AND
		    d.dash_dict != JSON_OBJECT('o', n.model)
	         `
	if _, err := oDb.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("update obsolescence alerts failed: %w", err)
	}
	query = `DELETE d FROM dashboard d
                 JOIN nodes n ON d.node_id=n.node_id
                 WHERE
		   d.dash_type IN ("os obsolescence alert date not set", "os obsolescence warning date not set") AND
                   d.dash_dict != JSON_OBJECT('o', CONCAT(n.os_name, " ", n.os_vendor, " ", n.os_release))
	        `
	if _, err := oDb.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("update obsolescence alerts failed: %w", err)
	}
	query = `DELETE FROM dashboard
                 WHERE
		   dash_type IN ("hardware obsolescence alert date not set", "hardware obsolescence warning date not set") AND
                   dash_dict IN (
                     SELECT JSON_OBJECT('o', obs_name)
                     FROM obsolescence
                     WHERE
                       obs_warn_date IS NOT NULL AND
                       obs_type = "hw"
                     )
	        `
	if _, err := oDb.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("update obsolescence alerts failed: %w", err)
	}
	query = `DELETE from dashboard
                 WHERE
		   dash_type IN ("hardware os alert date not set", "os obsolescence warning date not set") AND
                   dash_dict IN (
                     SELECT JSON_OBJECT('o', obs_name)
                     FROM obsolescence
                     WHERE
                       obs_warn_date IS NOT NULL AND
                       obs_type = "os"
                     )
	       `
	if _, err := oDb.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("update obsolescence alerts failed: %w", err)
	}
	return nil
}

type ObsolescenceSettingRow struct {
	ID           int
	ObsType      string
	ObsName      string
	ObsWarnDate  sql.NullString
	ObsAlertDate sql.NullString
}

func (oDb *DB) GetObsolescenceSettingRow(ctx context.Context, id string) (*ObsolescenceSettingRow, error) {
	const query = "SELECT id, obs_type, obs_name, obs_warn_date, obs_alert_date FROM obsolescence WHERE id = ? LIMIT 1"
	var row ObsolescenceSettingRow
	err := oDb.DB.QueryRowContext(ctx, query, id).Scan(&row.ID, &row.ObsType, &row.ObsName, &row.ObsWarnDate, &row.ObsAlertDate)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("GetObsolescenceSettingRow: %w", err)
	}
	return &row, nil
}

func (oDb *DB) GetObsolescenceSetting(ctx context.Context, id string, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("GetObsolescenceSetting: no select expressions")
	}
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") +
		" FROM v_obsolescence WHERE v_obsolescence.id = ?"
	args := []any{id}
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("GetObsolescenceSetting: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

func (oDb *DB) GetObsolescenceSettings(ctx context.Context, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("GetObsolescenceSettings: no select expressions")
	}
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") +
		" FROM v_obsolescence WHERE v_obsolescence.id > 0"
	// Column filters of the request.
	filterConds, args := p.FilterConditions()
	for _, cond := range filterConds {
		query += " AND " + cond
	}
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("v_obsolescence.obs_type, v_obsolescence.obs_name")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("GetObsolescenceSettings: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

type UpdateObsolescenceSettingFields struct {
	ObsWarnDate  *string
	ObsAlertDate *string
}

func (oDb *DB) UpdateObsolescenceSetting(ctx context.Context, id int, fields UpdateObsolescenceSettingFields, author string) error {
	setClauses := []string{}
	args := []any{}
	if fields.ObsWarnDate != nil {
		setClauses = append(setClauses, "obs_warn_date = ?", "obs_warn_date_updated_by = ?", "obs_warn_date_updated = NOW()")
		args = append(args, nullIfEmpty(*fields.ObsWarnDate), author)
	}
	if fields.ObsAlertDate != nil {
		setClauses = append(setClauses, "obs_alert_date = ?", "obs_alert_date_updated_by = ?", "obs_alert_date_updated = NOW()")
		args = append(args, nullIfEmpty(*fields.ObsAlertDate), author)
	}
	if len(setClauses) == 0 {
		return nil
	}
	query := "UPDATE obsolescence SET " + strings.Join(setClauses, ", ") + " WHERE id = ?"
	args = append(args, id)
	if _, err := oDb.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("UpdateObsolescenceSetting: %w", err)
	}
	oDb.SetChange("obsolescence")
	return nil
}

func (oDb *DB) DeleteObsolescenceSetting(ctx context.Context, id int) error {
	if _, err := oDb.ExecContext(ctx, "DELETE FROM obsolescence WHERE id = ?", id); err != nil {
		return fmt.Errorf("DeleteObsolescenceSetting: %w", err)
	}
	oDb.SetChange("obsolescence")
	return nil
}

// SyncNodeObsolescenceDates copies the stored dates of an obsolescence setting to
// the nodes it applies to. Copying inside the database keeps datetime values
// intact: passing back a date read through the driver would send it in RFC 3339
// form, which a datetime column rejects in strict mode.
func (oDb *DB) SyncNodeObsolescenceDates(ctx context.Context, id int, obsType string) error {
	var query string
	switch obsType {
	case "hw":
		query = `UPDATE nodes JOIN obsolescence o ON o.id = ? AND nodes.model = o.obs_name
			SET nodes.hw_obs_warn_date = o.obs_warn_date, nodes.hw_obs_alert_date = o.obs_alert_date`
	case "os":
		query = `UPDATE nodes JOIN obsolescence o ON o.id = ? AND nodes.os_concat = o.obs_name
			SET nodes.os_obs_warn_date = o.obs_warn_date, nodes.os_obs_alert_date = o.obs_alert_date`
	default:
		return nil
	}
	if _, err := oDb.ExecContext(ctx, query, id); err != nil {
		return fmt.Errorf("SyncNodeObsolescenceDates: %w", err)
	}
	return nil
}

func (oDb *DB) DeleteDashObsWithout(ctx context.Context, obsName, obsType, kind string) error {
	typeLabel := obsType
	if obsType == "hw" {
		typeLabel = "hardware"
	}
	kindLabel := kind
	if kind == "warn" {
		kindLabel = "warning"
	}
	dashType := fmt.Sprintf("%s obsolescence %s date not set", typeLabel, kindLabel)
	query := `DELETE FROM dashboard WHERE dash_dict = JSON_OBJECT('o', ?) AND dash_type = ?`
	if _, err := oDb.ExecContext(ctx, query, obsName, dashType); err != nil {
		return fmt.Errorf("DeleteDashObsWithout: %w", err)
	}
	return nil
}

func (oDb *DB) UpdateDashObsHWWarnForName(ctx context.Context, obsName string) error {
	query := `DELETE FROM dashboard
		WHERE node_id IN (
		    SELECT n.node_id
		    FROM obsolescence o
		    JOIN nodes n ON o.obs_name = n.model
		    WHERE o.obs_name = ? AND o.obs_type = "hw" AND (
			o.obs_alert_date IS NULL
			OR o.obs_name LIKE "%virtual%"
			OR o.obs_name LIKE "%virtuel%"
			OR o.obs_name LIKE "%cluster%"
			OR o.obs_alert_date = "0000-00-00 00:00:00"
			OR o.obs_warn_date >= NOW()
			OR o.obs_alert_date <= NOW()
		    )
		) AND dash_type = "hardware obsolescence warning"`
	if _, err := oDb.ExecContext(ctx, query, obsName); err != nil {
		return fmt.Errorf("UpdateDashObsHWWarnForName: %w", err)
	}
	query = `SELECT
		    "hardware obsolescence warning" AS dash_type, "" AS svc_id, 0 AS dash_severity,
		    "%(o)s warning since %(a)s" AS dash_fmt,
		    JSON_OBJECT("a", o.obs_warn_date, "o", o.obs_name) AS dash_dict,
		    NOW() AS dash_created, "" AS dash_dict_md5, n.node_env AS dash_env, NOW() AS dash_updated, n.node_id AS node_id
		FROM obsolescence o
		JOIN nodes n ON o.obs_name = n.model
		WHERE
		    o.obs_name = ?
		    AND o.obs_alert_date IS NOT NULL
		    AND o.obs_alert_date != "0000-00-00 00:00:00"
		    AND o.obs_name NOT LIKE "%virtual%"
		    AND o.obs_name NOT LIKE "%virtuel%"
		    AND o.obs_name NOT LIKE "%cluster%"
		    AND o.obs_warn_date < NOW()
		    AND o.obs_alert_date > NOW()
		    AND o.obs_type = "hw"
	`
	if _, err := oDb.upsertAlerts(ctx, alertsUpsert{
		source:  query,
		args:    []any{obsName},
		columns: []string{"dash_type", "svc_id", "dash_severity", "dash_fmt", "dash_dict", "dash_created", "dash_dict_md5", "dash_env", "dash_updated", "node_id"},
		update:  []string{"dash_updated = NOW()"},
	}); err != nil {
		return fmt.Errorf("UpdateDashObsHWWarnForName: %w", err)
	}
	return nil
}

func (oDb *DB) UpdateDashObsHWAlertForName(ctx context.Context, obsName string) error {
	query := `DELETE FROM dashboard
		WHERE node_id IN (
		    SELECT n.node_id
		    FROM obsolescence o
		    JOIN nodes n ON o.obs_name = n.model
		    WHERE o.obs_name = ? AND o.obs_type = "hw" AND (
			o.obs_alert_date IS NULL
			OR o.obs_name LIKE "%virtual%"
			OR o.obs_name LIKE "%virtuel%"
			OR o.obs_name LIKE "%cluster%"
			OR o.obs_alert_date = "0000-00-00 00:00:00"
			OR o.obs_alert_date >= NOW()
		    )
		) AND dash_type = "hardware obsolescence alert"`
	if _, err := oDb.ExecContext(ctx, query, obsName); err != nil {
		return fmt.Errorf("UpdateDashObsHWAlertForName: %w", err)
	}
	query = `SELECT
		    "hardware obsolescence alert" AS dash_type, "" AS svc_id, 1 AS dash_severity,
		    "%(o)s obsolete since %(a)s" AS dash_fmt,
		    JSON_OBJECT("a", o.obs_alert_date, "o", o.obs_name) AS dash_dict,
		    NOW() AS dash_created, "" AS dash_dict_md5, n.node_env AS dash_env, NOW() AS dash_updated, n.node_id AS node_id
		FROM obsolescence o
		JOIN nodes n ON o.obs_name = n.model
		WHERE
		    o.obs_name = ?
		    AND o.obs_alert_date IS NOT NULL
		    AND o.obs_name NOT LIKE "%virtual%"
		    AND o.obs_name NOT LIKE "%virtuel%"
		    AND o.obs_name NOT LIKE "%cluster%"
		    AND o.obs_alert_date != "0000-00-00 00:00:00"
		    AND o.obs_alert_date < NOW()
		    AND o.obs_type = "hw"
	`
	if _, err := oDb.upsertAlerts(ctx, alertsUpsert{
		source:  query,
		args:    []any{obsName},
		columns: []string{"dash_type", "svc_id", "dash_severity", "dash_fmt", "dash_dict", "dash_created", "dash_dict_md5", "dash_env", "dash_updated", "node_id"},
		update:  []string{"dash_updated = NOW()"},
	}); err != nil {
		return fmt.Errorf("UpdateDashObsHWAlertForName: %w", err)
	}
	return nil
}

func (oDb *DB) UpdateDashObsOSWarnForName(ctx context.Context, obsName string) error {
	query := `DELETE FROM dashboard
		WHERE node_id IN (
		    SELECT n.node_id
		    FROM obsolescence o
		    JOIN nodes n ON o.obs_name = n.os_concat
		    WHERE o.obs_name = ? AND o.obs_type = "os" AND (
			o.obs_alert_date IS NULL
			OR o.obs_alert_date = "0000-00-00 00:00:00"
			OR o.obs_warn_date >= NOW()
			OR o.obs_alert_date <= NOW()
		    )
		) AND dash_type = "os obsolescence warning"`
	if _, err := oDb.ExecContext(ctx, query, obsName); err != nil {
		return fmt.Errorf("UpdateDashObsOSWarnForName: %w", err)
	}
	query = `SELECT
		    "os obsolescence warning" AS dash_type, "" AS svc_id, 0 AS dash_severity,
		    "%(o)s warning since %(a)s" AS dash_fmt,
		    JSON_OBJECT("a", o.obs_warn_date, "o", o.obs_name) AS dash_dict,
		    NOW() AS dash_created, "" AS dash_dict_md5, n.node_env AS dash_env, NOW() AS dash_updated, n.node_id AS node_id
		FROM obsolescence o
		JOIN nodes n ON o.obs_name = concat_ws(' ', n.os_name, n.os_vendor, n.os_release, n.os_update)
		WHERE
		    o.obs_name = ?
		    AND o.obs_alert_date IS NOT NULL
		    AND o.obs_alert_date != "0000-00-00 00:00:00"
		    AND o.obs_warn_date < NOW()
		    AND o.obs_alert_date > NOW()
		    AND o.obs_type = "os"
	`
	if _, err := oDb.upsertAlerts(ctx, alertsUpsert{
		source:  query,
		args:    []any{obsName},
		columns: []string{"dash_type", "svc_id", "dash_severity", "dash_fmt", "dash_dict", "dash_created", "dash_dict_md5", "dash_env", "dash_updated", "node_id"},
		update:  []string{"dash_updated = NOW()"},
	}); err != nil {
		return fmt.Errorf("UpdateDashObsOSWarnForName: %w", err)
	}
	return nil
}

func (oDb *DB) UpdateDashObsOSAlertForName(ctx context.Context, obsName string) error {
	query := `DELETE FROM dashboard
		WHERE node_id IN (
		    SELECT n.node_id
		    FROM obsolescence o
		    JOIN nodes n ON o.obs_name = n.os_concat
		    WHERE o.obs_name = ? AND o.obs_type = "os" AND (
			o.obs_alert_date IS NULL
			OR o.obs_alert_date = "0000-00-00 00:00:00"
			OR o.obs_alert_date >= NOW()
		    )
		) AND dash_type = "os obsolescence alert"`
	if _, err := oDb.ExecContext(ctx, query, obsName); err != nil {
		return fmt.Errorf("UpdateDashObsOSAlertForName: %w", err)
	}

	query = `SELECT
		    "os obsolescence alert" AS dash_type, "" AS svc_id, 1 AS dash_severity,
		    "%(o)s obsolete since %(a)s" AS dash_fmt,
		    JSON_OBJECT("a", o.obs_alert_date, "o", o.obs_name) AS dash_dict,
		    NOW() AS dash_created, "" AS dash_dict_md5, n.node_env AS dash_env, NOW() AS dash_updated, n.nodename AS node_id
		FROM obsolescence o
		JOIN nodes n ON o.obs_name = concat_ws(' ', n.os_name, n.os_vendor, n.os_release, n.os_update)
		WHERE
		    o.obs_name = ?
		    AND o.obs_alert_date IS NOT NULL
		    AND o.obs_alert_date != "0000-00-00 00:00:00"
		    AND o.obs_alert_date < NOW()
		    AND o.obs_type = "os"
	`
	if _, err := oDb.upsertAlerts(ctx, alertsUpsert{
		source:  query,
		args:    []any{obsName},
		columns: []string{"dash_type", "svc_id", "dash_severity", "dash_fmt", "dash_dict", "dash_created", "dash_dict_md5", "dash_env", "dash_updated", "node_id"},
		update:  []string{"dash_updated = NOW()"},
	}); err != nil {
		return fmt.Errorf("UpdateDashObsOSAlertForName: %w", err)
	}
	return nil
}

func (oDb *DB) UpdateNodesObsolescence(ctx context.Context) error {
	query := `
		UPDATE nodes n
		JOIN obsolescence o ON
		    (o.obs_type = 'hw' AND n.model = o.obs_name) OR
		    (o.obs_type = 'os' AND n.os_concat = o.obs_name)
		SET
		    n.hw_obs_warn_date = CASE
					    WHEN o.obs_type = 'hw' THEN o.obs_warn_date
					    ELSE n.hw_obs_warn_date
					 END,
		    n.hw_obs_alert_date = CASE
					     WHEN o.obs_type = 'hw' THEN o.obs_alert_date
					     ELSE n.hw_obs_alert_date
					  END,
		    n.os_obs_warn_date = CASE
					    WHEN o.obs_type = 'os' THEN o.obs_warn_date
					    ELSE n.os_obs_warn_date
					 END,
		    n.os_obs_alert_date = CASE
					     WHEN o.obs_type = 'os' THEN o.obs_alert_date
					     ELSE n.os_obs_alert_date
					  END
		WHERE o.id > 0`
	if _, err := oDb.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("update node obsolescence data failed: %w", err)
	}
	return nil
}

// nullIfEmpty maps an empty date to NULL: a datetime column rejects ”.
func nullIfEmpty(value string) any {
	if value == "" {
		return nil
	}
	return value
}
