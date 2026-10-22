package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// The checks of the collector as the users manage them: the checks the agents
// report (checks_live), the thresholds set on one of them (checks_settings),
// the defaults of a check type (checks_defaults) and the thresholds of the
// checks of the nodes a filterset selects (gen_filterset_check_threshold). The
// thresholds a check gets from them are set by ChecksLiveUpdateThresholds.

// CheckLive is a check reported by an agent, as the actions on it need it.
type CheckLive struct {
	ID       int64
	NodeID   string
	SvcID    string
	Type     string
	Instance string
	Low      sql.NullInt64
	High     sql.NullInt64
}

// CheckKey names a check: its node, its object (empty for the node), its type
// and its instance, the unique key of checks_live and checks_settings.
type CheckKey struct {
	NodeID   string
	SvcID    string
	Type     string
	Instance string
}

// checksVisibility restricts a query on checks_live to the checks of the nodes
// whose application the user is responsible for, as the other node lists.
func checksVisibility(p ListParams, table string) (string, []any) {
	if p.IsManager {
		return "", nil
	}
	groups := cleanGroups(p.Groups)
	if len(groups) == 0 {
		return "1=0", nil
	}
	return table + ".node_id IN (" +
		"SELECT n.node_id FROM nodes n" +
		" JOIN apps a ON n.app = a.app" +
		" JOIN apps_responsibles ar ON ar.app_id = a.id" +
		" JOIN auth_group ag ON ag.id = ar.group_id" +
		" WHERE ag.role IN (" + Placeholders(len(groups)) + "))", stringsToAny(groups)
}

// GetChecks lists the checks of every node the user may see, with their node
// and their object.
func (oDb *DB) GetChecks(ctx context.Context, p ListParams) ([]map[string]any, error) {
	defer logDuration("getChecks", time.Now())
	var conds []string
	var args []any
	if cond, condArgs := checksVisibility(p, "checks_live"); cond != "" {
		conds = append(conds, cond)
		args = append(args, condArgs...)
	}
	return oDb.listQuery(ctx, "getChecks",
		"checks_live"+
			" LEFT JOIN nodes ON nodes.node_id = checks_live.node_id"+
			" LEFT JOIN services ON services.svc_id = checks_live.svc_id",
		conds, args, "nodes.nodename, checks_live.chk_type, checks_live.chk_instance", p)
}

// CheckLiveByID returns a check, nil when there is none of that id.
func (oDb *DB) CheckLiveByID(ctx context.Context, id int64) (*CheckLive, error) {
	var c CheckLive
	err := oDb.DB.QueryRowContext(ctx, `SELECT id, IFNULL(node_id, ""), IFNULL(svc_id, ""), chk_type,
		IFNULL(chk_instance, ""), chk_low, chk_high FROM checks_live WHERE id = ?`, id).
		Scan(&c.ID, &c.NodeID, &c.SvcID, &c.Type, &c.Instance, &c.Low, &c.High)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("checkLiveByID: %w", err)
	}
	return &c, nil
}

// CheckLiveByKey returns the check of that key, nil when there is none.
func (oDb *DB) CheckLiveByKey(ctx context.Context, k CheckKey) (*CheckLive, error) {
	var c CheckLive
	// A check reported before oc3 may hold NULL for no service or no instance,
	// where oc3 stores "": both match an empty key. Should both rows be there,
	// the one oc3 updates comes first.
	err := oDb.DB.QueryRowContext(ctx, `SELECT id, IFNULL(node_id, ""), IFNULL(svc_id, ""), chk_type,
		IFNULL(chk_instance, ""), chk_low, chk_high FROM checks_live
		WHERE node_id = ? AND IFNULL(svc_id, "") = ? AND chk_type = ? AND IFNULL(chk_instance, "") = ?
		ORDER BY svc_id IS NULL, chk_instance IS NULL, chk_updated DESC LIMIT 1`,
		k.NodeID, k.SvcID, k.Type, k.Instance).
		Scan(&c.ID, &c.NodeID, &c.SvcID, &c.Type, &c.Instance, &c.Low, &c.High)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("checkLiveByKey: %w", err)
	}
	return &c, nil
}

// DeleteCheckLive deletes a check and its alerts. The agent reports it again
// at its next check run if the check still exists. A check reported before oc3
// with NULL for no service or no instance goes too: it is the same check.
func (oDb *DB) DeleteCheckLive(ctx context.Context, c *CheckLive) error {
	if _, err := oDb.DB.ExecContext(ctx, `DELETE FROM checks_live
		WHERE id = ? OR (node_id = ? AND IFNULL(svc_id, "") = ? AND chk_type = ? AND IFNULL(chk_instance, "") = ?)`,
		c.ID, c.NodeID, c.SvcID, c.Type, c.Instance); err != nil {
		return fmt.Errorf("deleteCheckLive: %w", err)
	}
	oDb.SetChange("checks_live")
	count, err := oDb.execCountContext(ctx, `DELETE FROM dashboard
		WHERE node_id = ? AND IFNULL(svc_id, "") = ? AND dash_instance = ?
		AND dash_type IN ("check out of bounds", "check value not updated")`,
		c.NodeID, c.SvcID, c.Type+":"+c.Instance)
	if err != nil {
		return fmt.Errorf("deleteCheckLive alerts: %w", err)
	}
	if count > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

// GetChecksSettings lists the thresholds set on checks of the nodes the user
// may see.
func (oDb *DB) GetChecksSettings(ctx context.Context, p ListParams) ([]map[string]any, error) {
	var conds []string
	var args []any
	if cond, condArgs := checksVisibility(p, "checks_settings"); cond != "" {
		conds = append(conds, cond)
		args = append(args, condArgs...)
	}
	return oDb.listQuery(ctx, "getChecksSettings",
		"checks_settings"+
			" LEFT JOIN nodes ON nodes.node_id = checks_settings.node_id"+
			" LEFT JOIN services ON services.svc_id = checks_settings.svc_id",
		conds, args, "nodes.nodename, checks_settings.chk_type, checks_settings.chk_instance", p)
}

// SetCheckSettings sets the thresholds of a check, by whom and when.
func (oDb *DB) SetCheckSettings(ctx context.Context, k CheckKey, low, high int64, by string) error {
	_, err := oDb.DB.ExecContext(ctx, `INSERT INTO checks_settings
		(node_id, svc_id, chk_type, chk_instance, chk_low, chk_high, chk_changed, chk_changed_by)
		VALUES (?, ?, ?, ?, ?, ?, NOW(), ?)
		ON DUPLICATE KEY UPDATE chk_low = VALUES(chk_low), chk_high = VALUES(chk_high),
		chk_changed = VALUES(chk_changed), chk_changed_by = VALUES(chk_changed_by)`,
		k.NodeID, k.SvcID, k.Type, k.Instance, low, high, by)
	if err != nil {
		return fmt.Errorf("setCheckSettings: %w", err)
	}
	oDb.SetChange("checks_settings")
	return nil
}

// DeleteCheckSettings removes the thresholds set on a check; it reports
// whether there were some.
func (oDb *DB) DeleteCheckSettings(ctx context.Context, k CheckKey) (bool, error) {
	count, err := oDb.execCountContext(ctx, `DELETE FROM checks_settings
		WHERE node_id = ? AND IFNULL(svc_id, "") = ? AND chk_type = ? AND IFNULL(chk_instance, "") = ?`,
		k.NodeID, k.SvcID, k.Type, k.Instance)
	if err != nil {
		return false, fmt.Errorf("deleteCheckSettings: %w", err)
	}
	if count > 0 {
		oDb.SetChange("checks_settings")
	}
	return count > 0, nil
}

// CheckDefault is the default thresholds of a check type, for the instances
// its pattern matches.
type CheckDefault struct {
	ID   int64
	Type string
	Inst string
	Low  sql.NullInt64
	High sql.NullInt64
	Prio int64
}

// GetChecksDefaults lists the default thresholds of the check types.
func (oDb *DB) GetChecksDefaults(ctx context.Context, p ListParams) ([]map[string]any, error) {
	return oDb.listQuery(ctx, "getChecksDefaults", "checks_defaults", nil, nil,
		"checks_defaults.chk_type, checks_defaults.chk_inst, checks_defaults.chk_prio", p)
}

// CheckDefaultByID returns default thresholds, nil when there are none of that id.
func (oDb *DB) CheckDefaultByID(ctx context.Context, id int64) (*CheckDefault, error) {
	var d CheckDefault
	err := oDb.DB.QueryRowContext(ctx, `SELECT id, chk_type, IFNULL(chk_inst, ""), chk_low, chk_high,
		IFNULL(chk_prio, 0) FROM checks_defaults WHERE id = ?`, id).
		Scan(&d.ID, &d.Type, &d.Inst, &d.Low, &d.High, &d.Prio)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("checkDefaultByID: %w", err)
	}
	return &d, nil
}

// ErrDuplicate tells a record another one already has the unique key of.
var ErrDuplicate = errors.New("duplicate")

func isDuplicate(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Error 1062")
}

// InsertCheckDefault adds default thresholds; it returns their id.
func (oDb *DB) InsertCheckDefault(ctx context.Context, d CheckDefault) (int64, error) {
	result, err := oDb.DB.ExecContext(ctx, `INSERT INTO checks_defaults
		(chk_type, chk_inst, chk_low, chk_high, chk_prio) VALUES (?, ?, ?, ?, ?)`,
		d.Type, d.Inst, d.Low, d.High, d.Prio)
	if isDuplicate(err) {
		return 0, ErrDuplicate
	}
	if err != nil {
		return 0, fmt.Errorf("insertCheckDefault: %w", err)
	}
	oDb.SetChange("checks_defaults")
	return result.LastInsertId()
}

// UpdateCheckDefault writes default thresholds back.
func (oDb *DB) UpdateCheckDefault(ctx context.Context, d CheckDefault) error {
	_, err := oDb.DB.ExecContext(ctx, `UPDATE checks_defaults
		SET chk_type = ?, chk_inst = ?, chk_low = ?, chk_high = ?, chk_prio = ? WHERE id = ?`,
		d.Type, d.Inst, d.Low, d.High, d.Prio, d.ID)
	if isDuplicate(err) {
		return ErrDuplicate
	}
	if err != nil {
		return fmt.Errorf("updateCheckDefault: %w", err)
	}
	oDb.SetChange("checks_defaults")
	return nil
}

// DeleteCheckDefault removes default thresholds.
func (oDb *DB) DeleteCheckDefault(ctx context.Context, id int64) error {
	if _, err := oDb.DB.ExecContext(ctx, "DELETE FROM checks_defaults WHERE id = ?", id); err != nil {
		return fmt.Errorf("deleteCheckDefault: %w", err)
	}
	oDb.SetChange("checks_defaults")
	return nil
}

// CheckContextual is the thresholds of the checks of a type and of the
// instances its pattern matches, on the nodes and objects a filterset selects.
type CheckContextual struct {
	ID       int64
	FsetID   int64
	FsetName string
	Type     string
	Instance string
	Low      int64
	High     int64
}

// GetChecksContextual lists the thresholds set through filtersets.
func (oDb *DB) GetChecksContextual(ctx context.Context, p ListParams) ([]map[string]any, error) {
	return oDb.listQuery(ctx, "getChecksContextual",
		"gen_filterset_check_threshold"+
			" LEFT JOIN gen_filtersets ON gen_filtersets.id = gen_filterset_check_threshold.fset_id",
		nil, nil,
		"gen_filtersets.fset_name, gen_filterset_check_threshold.chk_type, gen_filterset_check_threshold.chk_instance", p)
}

// CheckContextualByID returns contextual thresholds, nil when there are none
// of that id.
func (oDb *DB) CheckContextualByID(ctx context.Context, id int64) (*CheckContextual, error) {
	var c CheckContextual
	err := oDb.DB.QueryRowContext(ctx, `SELECT t.id, t.fset_id, IFNULL(f.fset_name, ""), t.chk_type,
		t.chk_instance, t.chk_low, t.chk_high
		FROM gen_filterset_check_threshold t LEFT JOIN gen_filtersets f ON f.id = t.fset_id
		WHERE t.id = ?`, id).
		Scan(&c.ID, &c.FsetID, &c.FsetName, &c.Type, &c.Instance, &c.Low, &c.High)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("checkContextualByID: %w", err)
	}
	return &c, nil
}

// InsertCheckContextual adds contextual thresholds; it returns their id.
func (oDb *DB) InsertCheckContextual(ctx context.Context, c CheckContextual) (int64, error) {
	result, err := oDb.DB.ExecContext(ctx, `INSERT INTO gen_filterset_check_threshold
		(fset_id, chk_type, chk_instance, chk_low, chk_high) VALUES (?, ?, ?, ?, ?)`,
		c.FsetID, c.Type, c.Instance, c.Low, c.High)
	if isDuplicate(err) {
		return 0, ErrDuplicate
	}
	if err != nil {
		return 0, fmt.Errorf("insertCheckContextual: %w", err)
	}
	oDb.SetChange("gen_filterset_check_threshold")
	return result.LastInsertId()
}

// UpdateCheckContextual writes contextual thresholds back.
func (oDb *DB) UpdateCheckContextual(ctx context.Context, c CheckContextual) error {
	_, err := oDb.DB.ExecContext(ctx, `UPDATE gen_filterset_check_threshold
		SET fset_id = ?, chk_type = ?, chk_instance = ?, chk_low = ?, chk_high = ? WHERE id = ?`,
		c.FsetID, c.Type, c.Instance, c.Low, c.High, c.ID)
	if isDuplicate(err) {
		return ErrDuplicate
	}
	if err != nil {
		return fmt.Errorf("updateCheckContextual: %w", err)
	}
	oDb.SetChange("gen_filterset_check_threshold")
	return nil
}

// DeleteCheckContextual removes contextual thresholds.
func (oDb *DB) DeleteCheckContextual(ctx context.Context, id int64) error {
	if _, err := oDb.DB.ExecContext(ctx, "DELETE FROM gen_filterset_check_threshold WHERE id = ?", id); err != nil {
		return fmt.Errorf("deleteCheckContextual: %w", err)
	}
	oDb.SetChange("gen_filterset_check_threshold")
	return nil
}

// NodesWithChecksOfType returns the nodes having checks of a type, all
// checks when the type is empty.
func (oDb *DB) NodesWithChecksOfType(ctx context.Context, chkType string) ([]string, error) {
	query := "SELECT DISTINCT node_id FROM checks_live WHERE node_id != ''"
	var args []any
	if chkType != "" {
		query += " AND chk_type = ?"
		args = append(args, chkType)
	}
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("nodesWithChecksOfType: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var l []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("nodesWithChecksOfType: %w", err)
		}
		l = append(l, id)
	}
	return l, rows.Err()
}

// ChecksRefreshNode sets the thresholds of the checks of a node again, then
// its "check out of bounds" alerts, after a change of the thresholds. Unlike
// a feed of the node's checks, it leaves its "check value not updated" alerts.
func (oDb *DB) ChecksRefreshNode(ctx context.Context, nodeID string) error {
	if err := oDb.ChecksLiveUpdateThresholds(ctx, nodeID); err != nil {
		return err
	}
	// The time of the database, as the feed jobs take it: the alerts kept are
	// those updated from then on, at the precision of the dates stored.
	var now time.Time
	if err := oDb.DB.QueryRowContext(ctx, "SELECT NOW()").Scan(&now); err != nil {
		return fmt.Errorf("checksRefreshNode: %w", err)
	}
	return oDb.dashboardUpdateChecksOutOfBounds(ctx, nodeID, now, false)
}
