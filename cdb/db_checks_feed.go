package cdb

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"
)

type (
	// CheckFeedRow is a check a node reports, its object resolved.
	CheckFeedRow struct {
		SvcID    string
		Type     string
		Instance string
		Value    int64
	}

	// CheckLiveRow is a check of a node, as stored.
	CheckLiveRow struct {
		ID       int64
		NodeID   string
		SvcID    string
		Type     string
		Instance string
		Value    int64
	}

	checkThresholdFilter struct {
		fsetID   int
		fsetName string
		chkType  string
		instance *regexp.Regexp
		low      int64
		high     int64
	}
)

const (
	// checksNotPurged are the check types a node feed does not replace,
	// as the old collector left them.
	checksNotPurged = `"netdev_err", "save"`

	checksInsertBatch = 100
)

// checkOutOfBoundsDictSQL is the dash_dict of the "check out of bounds"
// alert of the checks_live row aliased alias, as the old collector wrote it:
// its md5 identifies the alert, so every query rebuilding it uses this one.
func checkOutOfBoundsDictSQL(alias string) string {
	return fmt.Sprintf(`CONCAT('{"ctype": "', %[1]s.chk_type,
		'", "inst": "', IFNULL(%[1]s.chk_instance, ""),
		'", "ttype": "', %[1]s.chk_threshold_provider,
		'", "val": ', %[1]s.chk_value,
		', "min": ', IFNULL(%[1]s.chk_low, "null"),
		', "max": ', IFNULL(%[1]s.chk_high, "null"),
		'}')`, alias)
}

// ChecksLiveUpsert stores the checks a node reports, updated at now, keeping
// the creation date and the thresholds of the checks it reported before.
func (oDb *DB) ChecksLiveUpsert(ctx context.Context, nodeID string, rows []CheckFeedRow, now time.Time) error {
	defer logDuration("ChecksLiveUpsert", time.Now())
	for i := 0; i < len(rows); i += checksInsertBatch {
		batch := rows[i:min(i+checksInsertBatch, len(rows))]
		placeholders := make([]string, len(batch))
		args := make([]any, 0, 6*len(batch))
		for j, r := range batch {
			placeholders[j] = "(?, ?, ?, ?, ?, ?)"
			args = append(args, nodeID, r.SvcID, r.Type, r.Instance, r.Value, now)
		}
		query := "INSERT INTO checks_live (node_id, svc_id, chk_type, chk_instance, chk_value, chk_updated) VALUES " +
			strings.Join(placeholders, ", ") +
			" ON DUPLICATE KEY UPDATE chk_value = VALUES(chk_value), chk_updated = VALUES(chk_updated)"
		if _, err := oDb.DB.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("ChecksLiveUpsert: %w", err)
		}
	}
	oDb.SetChange("checks_live")
	return nil
}

// ChecksLivePurgeBefore removes the checks of the node not updated since
// before, the ones it no longer reports.
func (oDb *DB) ChecksLivePurgeBefore(ctx context.Context, nodeID string, before time.Time) error {
	defer logDuration("ChecksLivePurgeBefore", time.Now())
	query := `DELETE FROM checks_live WHERE node_id = ? AND chk_type NOT IN (` + checksNotPurged + `) AND chk_updated < ?`
	if count, err := oDb.execCountContext(ctx, query, nodeID, before); err != nil {
		return fmt.Errorf("ChecksLivePurgeBefore: %w", err)
	} else if count > 0 {
		oDb.SetChange("checks_live")
	}
	return nil
}

// ChecksLiveForNode returns the checks of the node.
func (oDb *DB) ChecksLiveForNode(ctx context.Context, nodeID string) ([]CheckLiveRow, error) {
	query := `SELECT id, node_id, IFNULL(svc_id, ""), chk_type, IFNULL(chk_instance, ""), IFNULL(chk_value, 0)
		FROM checks_live WHERE node_id = ?`
	rows, err := oDb.DB.QueryContext(ctx, query, nodeID)
	if err != nil {
		return nil, fmt.Errorf("ChecksLiveForNode: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var l []CheckLiveRow
	for rows.Next() {
		var r CheckLiveRow
		if err := rows.Scan(&r.ID, &r.NodeID, &r.SvcID, &r.Type, &r.Instance, &r.Value); err != nil {
			return nil, fmt.Errorf("ChecksLiveForNode: %w", err)
		}
		l = append(l, r)
	}
	return l, rows.Err()
}

// ObjectIDOfVM returns the object whose instance runs the node as a virtual
// machine, and empty when none does: the checks of such a node are the
// object's.
func (oDb *DB) ObjectIDOfVM(ctx context.Context, nodename string) (string, error) {
	var svcID sql.NullString
	err := oDb.DB.QueryRowContext(ctx, "SELECT svc_id FROM svcmon WHERE mon_vmname = ? AND svc_id != '' LIMIT 1", nodename).Scan(&svcID)
	switch {
	case err == sql.ErrNoRows:
		return "", nil
	case err != nil:
		return "", fmt.Errorf("ObjectIDOfVM: %w", err)
	}
	return svcID.String, nil
}

// ChecksLiveUpdateThresholds sets the low and high thresholds of the checks
// of the node, from the first source having some, as the old collector did:
//
//   - the settings of the check on the node
//   - the check thresholds of a filterset matching the node, and the object
//     of the check, the last matching one
//   - the defaults of the check type, the one of the highest priority,
//     then of the longest instance pattern, matching the instance
func (oDb *DB) ChecksLiveUpdateThresholds(ctx context.Context, nodeID string) error {
	defer logDuration("ChecksLiveUpdateThresholds", time.Now())
	rest, err := oDb.checksLiveThresholdsFromSettings(ctx, nodeID)
	if err != nil {
		return err
	}
	rest, err = oDb.checksLiveThresholdsFromFiltersets(ctx, rest)
	if err != nil {
		return err
	}
	if err := oDb.checksLiveThresholdsFromDefaults(ctx, rest); err != nil {
		return err
	}
	oDb.SetChange("checks_live")
	return nil
}

// checksLiveThresholdsFromSettings sets the thresholds of the checks of the
// node having settings, and returns the others.
func (oDb *DB) checksLiveThresholdsFromSettings(ctx context.Context, nodeID string) ([]CheckLiveRow, error) {
	query := `UPDATE checks_live cl
		JOIN checks_settings cs ON cs.node_id = cl.node_id AND cs.chk_type = cl.chk_type AND cs.chk_instance = cl.chk_instance
		SET cl.chk_low = cs.chk_low, cl.chk_high = cs.chk_high, cl.chk_threshold_provider = "settings"
		WHERE cl.node_id = ? AND cs.chk_low IS NOT NULL AND cl.chk_type NOT IN (` + checksNotPurged + `)`
	if _, err := oDb.DB.ExecContext(ctx, query, nodeID); err != nil {
		return nil, fmt.Errorf("thresholds from settings: %w", err)
	}
	query = `SELECT cl.id, cl.node_id, IFNULL(cl.svc_id, ""), cl.chk_type, IFNULL(cl.chk_instance, ""), IFNULL(cl.chk_value, 0)
		FROM checks_live cl
		WHERE cl.node_id = ? AND cl.chk_type NOT IN (` + checksNotPurged + `)
		AND NOT EXISTS (
			SELECT 1 FROM checks_settings cs
			WHERE cs.node_id = cl.node_id AND cs.chk_type = cl.chk_type AND cs.chk_instance = cl.chk_instance AND cs.chk_low IS NOT NULL
		)`
	rows, err := oDb.DB.QueryContext(ctx, query, nodeID)
	if err != nil {
		return nil, fmt.Errorf("checks without settings: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var l []CheckLiveRow
	for rows.Next() {
		var r CheckLiveRow
		if err := rows.Scan(&r.ID, &r.NodeID, &r.SvcID, &r.Type, &r.Instance, &r.Value); err != nil {
			return nil, fmt.Errorf("checks without settings: %w", err)
		}
		l = append(l, r)
	}
	return l, rows.Err()
}

// checksLiveThresholdsFromFiltersets sets the thresholds of the checks a
// filterset of check thresholds matches, and returns the others.
func (oDb *DB) checksLiveThresholdsFromFiltersets(ctx context.Context, rows []CheckLiveRow) ([]CheckLiveRow, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	filters, err := oDb.checkThresholdFilters(ctx)
	if err != nil {
		return nil, err
	}
	if len(filters) == 0 {
		return rows, nil
	}
	// The ids each filterset resolves to, by filterset and reference field.
	resolved := make(map[string][]string)
	matches := func(fsetID int, field, id string) (bool, error) {
		k := fmt.Sprintf("%d:%s", fsetID, field)
		ids, ok := resolved[k]
		if !ok {
			var err error
			if ids, err = oDb.ResolveFilterset(ctx, fsetID, field); err != nil {
				return false, err
			}
			resolved[k] = ids
		}
		return slices.Contains(ids, id), nil
	}
	var rest []CheckLiveRow
	for _, r := range rows {
		var match *checkThresholdFilter
		for i := range filters {
			f := &filters[i]
			if f.chkType != r.Type || !f.instance.MatchString(r.Instance) {
				continue
			}
			if ok, err := matches(f.fsetID, "node_id", r.NodeID); err != nil {
				return nil, err
			} else if !ok {
				continue
			}
			if r.SvcID != "" {
				if ok, err := matches(f.fsetID, "svc_id", r.SvcID); err != nil {
					return nil, err
				} else if !ok {
					continue
				}
			}
			match = f
		}
		if match == nil {
			rest = append(rest, r)
			continue
		}
		query := `UPDATE checks_live SET chk_low = ?, chk_high = ?, chk_threshold_provider = ? WHERE id = ?`
		if _, err := oDb.DB.ExecContext(ctx, query, match.low, match.high, "fset:"+match.fsetName, r.ID); err != nil {
			return nil, fmt.Errorf("thresholds from filtersets: %w", err)
		}
	}
	return rest, nil
}

// checkThresholdFilters returns the check thresholds of the filtersets, in
// the order they were defined, an instance pattern not compiling left out.
func (oDb *DB) checkThresholdFilters(ctx context.Context) ([]checkThresholdFilter, error) {
	query := `SELECT cf.fset_id, f.fset_name, cf.chk_type, cf.chk_instance, cf.chk_low, cf.chk_high
		FROM gen_filterset_check_threshold cf JOIN gen_filtersets f ON f.id = cf.fset_id
		ORDER BY cf.id`
	rows, err := oDb.DB.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("check thresholds of filtersets: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var l []checkThresholdFilter
	for rows.Next() {
		var (
			f       checkThresholdFilter
			pattern string
		)
		if err := rows.Scan(&f.fsetID, &f.fsetName, &f.chkType, &pattern, &f.low, &f.high); err != nil {
			return nil, fmt.Errorf("check thresholds of filtersets: %w", err)
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			continue
		}
		f.instance = re
		l = append(l, f)
	}
	return l, rows.Err()
}

// checksLiveThresholdsFromDefaults sets the thresholds of the checks from
// the defaults of their type, null when none matches.
func (oDb *DB) checksLiveThresholdsFromDefaults(ctx context.Context, rows []CheckLiveRow) error {
	if len(rows) == 0 {
		return nil
	}
	pick := func(col string) string {
		return `(SELECT cd.` + col + ` FROM checks_defaults cd
			WHERE cd.chk_type = cl.chk_type AND (
				cl.chk_instance RLIKE CONCAT("^", cd.chk_inst, "$") OR
				cl.chk_instance = cd.chk_inst OR
				cd.chk_inst = "" OR
				cd.chk_inst IS NULL)
			ORDER BY cd.chk_prio DESC, LENGTH(cd.chk_inst) DESC, cd.id
			LIMIT 1)`
	}
	for i := 0; i < len(rows); i += checksInsertBatch {
		batch := rows[i:min(i+checksInsertBatch, len(rows))]
		placeholders := make([]string, len(batch))
		args := make([]any, len(batch))
		for j, r := range batch {
			placeholders[j] = "?"
			args[j] = r.ID
		}
		query := `UPDATE checks_live cl
			SET cl.chk_low = ` + pick("chk_low") + `, cl.chk_high = ` + pick("chk_high") + `, cl.chk_threshold_provider = "defaults"
			WHERE cl.id IN (` + strings.Join(placeholders, ", ") + `)`
		if _, err := oDb.DB.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("thresholds from defaults: %w", err)
		}
	}
	return nil
}

// DashboardUpdateChecksOutOfBounds alerts the checks of the node updated
// within a day whose value is out of their thresholds, and removes the
// alerts of the checks of the node no longer out of them, and the "check
// value not updated" alerts of the node, which just updated its checks.
func (oDb *DB) DashboardUpdateChecksOutOfBounds(ctx context.Context, nodeID string, now time.Time) error {
	return oDb.dashboardUpdateChecksOutOfBounds(ctx, nodeID, now, true)
}

// dashboardUpdateChecksOutOfBounds alerts the checks of the node out of their
// thresholds, an alert already there taking the value and the thresholds of
// now, and removes the alerts of the checks back within them. fed tells the
// node just sent its checks: its "check value not updated" alerts go then.
func (oDb *DB) dashboardUpdateChecksOutOfBounds(ctx context.Context, nodeID string, now time.Time, fed bool) error {
	defer logDuration("DashboardUpdateChecksOutOfBounds", time.Now())
	env := "TST"
	var nodeEnv sql.NullString
	if err := oDb.DB.QueryRowContext(ctx, "SELECT node_env FROM nodes WHERE node_id = ?", nodeID).Scan(&nodeEnv); err == nil && nodeEnv.String != "" {
		env = nodeEnv.String
	}
	severity := 2
	if env == "PRD" {
		severity = 3
	}
	dict := checkOutOfBoundsDictSQL("t")
	u := alertsUpsert{
		source: `SELECT
			"check out of bounds" AS dash_type,
			IFNULL(t.svc_id, "") AS svc_id,
			? AS dash_severity,
			"%(ctype)s:%(inst)s check value %(val)d. %(ttype)s thresholds: %(min)d - %(max)d" AS dash_fmt,
			` + dict + ` AS dash_dict,
			? AS dash_created,
			MD5(` + dict + `) AS dash_dict_md5,
			? AS dash_env,
			? AS dash_updated,
			t.node_id AS node_id,
			CONCAT(t.chk_type, ":", IFNULL(t.chk_instance, "")) AS dash_instance
		FROM checks_live t
		WHERE
			t.node_id = ? AND
			t.chk_updated >= DATE_SUB(?, INTERVAL 1 DAY) AND
			(t.chk_value < t.chk_low OR t.chk_value > t.chk_high)`,
		args:    []any{severity, now, env, now, nodeID, now},
		columns: positionalAlertColumns,
		update: []string{"dash_updated = s.dash_updated", "dash_dict = s.dash_dict",
			"dash_dict_md5 = s.dash_dict_md5", "dash_severity = s.dash_severity"},
	}
	if count, err := oDb.upsertAlerts(ctx, u); err != nil {
		return fmt.Errorf("DashboardUpdateChecksOutOfBounds: %w", err)
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}
	query := `DELETE FROM dashboard
		WHERE node_id = ? AND (
			(dash_type = "check out of bounds" AND (dash_updated < ? OR dash_updated IS NULL))`
	if fed {
		query += ` OR dash_type = "check value not updated"`
	}
	query += `)`
	if count, err := oDb.execCountContext(ctx, query, nodeID, now); err != nil {
		return fmt.Errorf("DashboardUpdateChecksOutOfBounds: %w", err)
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}
