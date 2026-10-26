package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/opensvc/oc3/schema"
)

type (
	/*
		CREATE TABLE `svcactions` (
		  `action` varchar(128) DEFAULT NULL,
		  `status` enum('err','ok','warn','') DEFAULT '',
		  `begin` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
		  `end` datetime DEFAULT NULL,
		  `hostid` varchar(30) DEFAULT NULL,
		  `status_log` mediumtext CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
		  `pid` varchar(32) DEFAULT NULL,
		  `id` int(11) NOT NULL AUTO_INCREMENT,
		  `ack` tinyint(4) DEFAULT NULL,
		  `alert` tinyint(1) DEFAULT NULL,
		  `acked_by` varchar(50) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
		  `acked_comment` text CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
		  `acked_date` datetime DEFAULT NULL,
		  `version` varchar(20) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
		  `cron` tinyint(1) DEFAULT 0,
		  `time` int(11) DEFAULT NULL,
		  `node_id` char(36) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT '',
		  `svc_id` char(36) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT '',
		  `sid` char(36) DEFAULT NULL,
		  `rid` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
		  `subset` varchar(255) DEFAULT NULL,
		  `command` varchar(256) DEFAULT '',
		  `origin` varchar(128) DEFAULT '',
		  `log_type` varchar(30) DEFAULT '',
		  PRIMARY KEY (`id`),
		  KEY `action` (`action`),
		  KEY `end` (`end`),
		  KEY `status` (`status`),
		  KEY `k_node_id` (`node_id`),
		  KEY `k_svc_id` (`svc_id`),
		  KEY `begin` (`begin`),
		  KEY `errcount` (`svc_id`,`node_id`,`begin`),
		  KEY `k_instance` (`node_id`,`svc_id`),
		  KEY `idx_count_err` (`svc_id`,`node_id`,`status`,`begin`),
		  KEY `k_sid` (`sid`)
		) ENGINE=InnoDB AUTO_INCREMENT=398516 DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci
	*/
	SvcAction struct {
		ID        int64
		SvcID     uuid.UUID
		NodeID    uuid.UUID
		RID       string
		Subset    string
		Action    string
		Session   string
		Pid       string
		Cron      bool
		BeginAt   time.Time
		EndAt     time.Time
		Status    string
		StatusLog string
		Command   string
		Origin    string
		Version   string
		LogType   string
	}

	// SvcActionLine contains the SvcAction log line for a rid or subset
	SvcActionLine struct {
		Begin     string
		RID       string
		Subset    string
		Pid       string
		Status    string
		StatusLog string
	}

	BActionErrorCount struct {
		SvcID    string
		NodeID   string
		ErrCount int
		SvcEnv   *string
	}
)

func (oDb *DB) BActionErrorsRefresh(ctx context.Context) error {
	_, err := oDb.ExecContext(ctx, "TRUNCATE b_action_errors")
	if err != nil {
		return err
	}

	sql := `INSERT INTO b_action_errors (
             SELECT NULL, a.svc_id, a.node_id, count(a.id)
             FROM svcactions a
             WHERE
               a.end>DATE_SUB(NOW(), INTERVAL 1 DAY) AND
               a.status='err' AND
               isnull(a.ack) AND
               a.end IS NOT NULL
             GROUP BY a.svc_id, a.node_id
        )`

	_, err = oDb.ExecContext(ctx, sql)
	if err != nil {
		return err
	}
	return nil
}

func argsFromIDs(ids []int64) []any {
	l := make([]any, len(ids))
	for i, id := range ids {
		l[i] = any(id)
	}
	return l
}

func (oDb *DB) AutoAckActionErrors(ctx context.Context, ids []int64) error {
	request := fmt.Sprintf(`UPDATE svcactions
             SET
               ack=1,
               acked_date=NOW(),
               acked_comment="Automatically acknowledged",
               acked_by="admin@opensvc.com"
	     WHERE
                 id IN (%s)`, Placeholders(len(ids)))
	args := argsFromIDs(ids)
	if count, err := oDb.execCountContext(ctx, request, args...); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("svcactions")
	}
	return nil
}

func (oDb *DB) LogActionErrorsNotAcked(ctx context.Context, ids []int64) error {
	request := fmt.Sprintf(`INSERT IGNORE INTO log
               SELECT NULL,
                      "service.action.notacked",
                      "scheduler",
                      "unacknowledged failed action '%%(action)s' at '%%(begin)s'",
                      concat('{"action": "', action, '", "begin": "', begin, '"}'),
                      NOW(),
                      svc_id,
                      0,
                      0,
                      MD5(CONCAT("service.action.notacked",node_id,svc_id,begin)),
                      "warning",
                      node_id
               FROM svcactions
               WHERE
                 id IN (%s)`, Placeholders(len(ids)))
	args := argsFromIDs(ids)
	if count, err := oDb.execCountContext(ctx, request, args...); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("log")
	}
	return nil
}

func (oDb *DB) GetActionErrorsNotAcked(ctx context.Context) (ids []int64, err error) {
	var age = 1
	var query = fmt.Sprintf(`SELECT
                 id
               FROM svcactions
	       WHERE
                 status="err" AND
                 ack IS NULL AND
                 begin<DATE_SUB(NOW(), INTERVAL %d DAY)`, age)

	var rows *sql.Rows

	rows, err = oDb.DB.QueryContext(ctx, query)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			return
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	return
}

func (oDb *DB) GetBActionErrors(ctx context.Context) (lines []BActionErrorCount, err error) {
	const query = `SELECT
               e.svc_id,
               e.node_id,
               s.svc_env,
               e.err
             FROM
               b_action_errors e
             JOIN services s ON e.svc_id=s.svc_id`

	var rows *sql.Rows

	rows, err = oDb.DB.QueryContext(ctx, query)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var line BActionErrorCount
		if err = rows.Scan(&line.SvcID, &line.NodeID, &line.SvcEnv, &line.ErrCount); err != nil {
			return
		}
		lines = append(lines, line)
	}
	err = rows.Err()
	return
}

func (oDb *DB) UpdateUnfinishedActions(ctx context.Context, maxAge time.Duration) error {
	request := `UPDATE svcactions
		SET
		    status = "err",
		    end = "1000-01-01 00:00:00"
		WHERE
		    begin < DATE_SUB(NOW(), INTERVAL ? SECOND)
		    AND end IS NULL
		    AND status IS NULL
		    AND action NOT LIKE "%#%"`
	if count, err := oDb.execCountContext(ctx, request, maxAgeSeconds(maxAge)); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("svcactions")
	}
	return nil
}

func (oDb *DB) GetUnfinishedActions(ctx context.Context, maxAge time.Duration) (lines []SvcAction, err error) {
	query := `SELECT id, node_id, svc_id FROM svcactions
		WHERE
		    begin < DATE_SUB(NOW(), INTERVAL ? SECOND)
		    AND end IS NULL
		    AND status IS NULL
		    AND action NOT LIKE "%#%"`
	var rows *sql.Rows
	rows, err = oDb.DB.QueryContext(ctx, query, maxAgeSeconds(maxAge))
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var line SvcAction
		if err = rows.Scan(&line.ID, &line.NodeID, &line.SvcID); err != nil {
			return
		}
		lines = append(lines, line)
	}
	err = rows.Err()
	return

}

func (oDb *DB) InsertSvcAction(ctx context.Context, a SvcAction, lines ...SvcActionLine) (int64, error) {
	maxCommandLen := 255
	query := "INSERT INTO svcactions (svc_id, node_id, rid, subset, action, begin, status_log, sid, pid, cron, command, origin, log_type"
	placeholders := "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?"

	statusLogLen := len(a.StatusLog)
	if statusLogLen > MediumTextMax {
		// Fix end svc action failed: Error 1406 (22001): Data too long for column 'status_log' at row 1
		// TODO: add metrics svcactions status_log truncated with len / TextMax
		a.StatusLog = a.StatusLog[:MediumTextMax]
	}

	if runes := []rune(a.Command); len(runes) > maxCommandLen {
		a.Command = string(runes[:maxCommandLen])
	}

	args := []any{a.SvcID, a.NodeID, a.RID, a.Subset, a.Action, a.BeginAt, a.StatusLog, a.Session, a.Pid, a.Cron, a.Command, a.Origin, "status"}

	if !a.EndAt.IsZero() {
		query += ", end, time"
		placeholders += ", ?, ?"
		args = append(args, a.EndAt, a.EndAt.Sub(a.BeginAt).Seconds())

	}
	if a.Status != "" {
		query += ", status"
		placeholders += ", ?"
		args = append(args, a.Status)
	}
	query += fmt.Sprintf(") VALUES (%s)", placeholders)

	result, err := oDb.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("insert svcactions failed, status_log length %d: %w", statusLogLen, err)
	} else if result == nil {
		return 0, errors.New("insert svcactions returns unexpected nil result")
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}

	if rowsAffected, err := result.RowsAffected(); err != nil {
		return id, err
	} else if rowsAffected > 0 {
		oDb.SetChange("svcactions")
		oDb.Metrics.InstanceActionInsert.Inc()
	}

	if len(lines) > 0 {
		if err := oDb.insertSvcActionLines(ctx, a, lines); err != nil {
			return id, err
		}
	}

	return id, nil
}

func (oDb *DB) UpdateSvcAction(ctx context.Context, a SvcAction, lines ...SvcActionLine) error {
	const (
		queryEnd = `UPDATE svcactions SET end = ?, status = ?, time = TIMESTAMPDIFF(SECOND, begin, ?), status_log = ? WHERE id = ?`
	)
	statusLogLen := len(a.StatusLog)
	if statusLogLen > MediumTextMax {
		// Fix end svc action failed: Error 1406 (22001): Data too long for column 'status_log' at row 1
		// TODO: add metrics svcactions status_log truncated with len / TextMax
		a.StatusLog = a.StatusLog[:MediumTextMax]
	}
	result, err := oDb.ExecContext(ctx, queryEnd, a.EndAt, a.Status, a.EndAt, a.StatusLog, a.ID)
	if err != nil {
		return fmt.Errorf("update svcactions failed status_log length %d: %w", statusLogLen, err)
	} else if result == nil {
		return errors.New("update svcactions returns unexpected nil result")
	}
	if rowsAffected, err := result.RowsAffected(); err != nil {
		return err
	} else if rowsAffected > 0 {
		oDb.SetChange("svcactions")
		oDb.Metrics.InstanceActionUpdate.Inc()
	}

	if len(lines) == 0 {
		return nil
	}
	return oDb.insertSvcActionLines(ctx, a, lines)
}

func (oDb *DB) insertSvcActionLines(ctx context.Context, a SvcAction, lines []SvcActionLine) error {
	if len(lines) == 0 {
		return nil
	}

	placeholders := strings.Repeat("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),", len(lines)-1) + "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	query := fmt.Sprintf("INSERT INTO svcactions (svc_id,node_id,sid,action,command,origin,version,cron,begin,pid,rid,subset,status,status_log) VALUES %s", placeholders)
	args := make([]any, 0, 14*len(lines))
	for _, line := range lines {
		args = append(args, a.SvcID, a.NodeID, a.Session, a.Action, a.Command, a.Origin, a.Version, a.Cron,
			line.Begin, line.Pid, line.RID, line.Subset, line.Status, line.StatusLog)
	}

	if _, err := oDb.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("insert svcactions loglines failed: %w", err)
	}
	return nil
}

// FindInstanceActionIDFromPidAndSID finds the instance action ID from the sid.
func (oDb *DB) FindInstanceActionIDFromPidAndSID(ctx context.Context, nodeID string, svcID string, pid, sid string) (id int64, found bool, err error) {
	const query = "SELECT id FROM svcactions WHERE node_id = ? AND svc_id = ? AND pid = ? AND sid = ?"
	err = oDb.DB.QueryRowContext(ctx, query, nodeID, svcID, pid, sid).Scan(&id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			err = nil
		}
	} else {
		found = true
	}
	return
}

// UpdateActionErrors updates the action errors in the database.
func (oDb *DB) UpdateActionErrors(ctx context.Context, svcID string, nodeID string) error {
	const queryCount = `SELECT count(id) FROM svcactions a
             WHERE
               a.svc_id = ? AND
               a.node_id = ? AND
               a.status = "err" AND
               ((a.ack <> 1) OR (a.ack IS NULL)) AND
               a.begin > DATE_SUB(NOW(), INTERVAL 2 DAY)`

	var errCount int
	if err := oDb.DB.QueryRowContext(ctx, queryCount, svcID, nodeID).Scan(&errCount); err != nil {
		return fmt.Errorf("UpdateActionErrors: count failed: %w", err)
	}

	if errCount == 0 {
		const queryDelete = `DELETE FROM b_action_errors
                  WHERE
                    svc_id = ? AND
                    node_id = ?`
		if _, err := oDb.ExecContext(ctx, queryDelete, svcID, nodeID); err != nil {
			return fmt.Errorf("UpdateActionErrors: delete failed: %w", err)
		}
	} else {
		const queryInsert = `INSERT INTO b_action_errors
                 SET
                   svc_id= ?,
                   node_id= ?,
                   err= ?
                 ON DUPLICATE KEY UPDATE
                   err= ?`
		if _, err := oDb.ExecContext(ctx, queryInsert, svcID, nodeID, errCount, errCount); err != nil {
			return fmt.Errorf("UpdateActionErrors: insert/update failed: %w", err)
		}
	}
	return nil
}

// UpdateDashActionErrors updates the dashboard with action errors.
func (oDb *DB) UpdateDashActionErrors(ctx context.Context, svcID string, nodeID string) error {
	const query = `SELECT e.err, s.svc_env FROM b_action_errors e
             JOIN services s ON e.svc_id=s.svc_id
             WHERE
               e.svc_id = ? AND
               e.node_id = ?`

	var errCount int
	var svcEnv sql.NullString
	var err error

	err = oDb.DB.QueryRowContext(ctx, query, svcID, nodeID).Scan(&errCount, &svcEnv)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("UpdateDashActionErrors: select failed: %w", err)
	}

	if err == nil {
		sev := 3
		if svcEnv.Valid && svcEnv.String == "PRD" {
			sev = 4
		}
		const sourceAlert = `SELECT
                   "action errors" AS dash_type,
                   ? AS svc_id,
                   ? AS node_id,
                   ? AS dash_severity,
                   "%(err)s action errors" AS dash_fmt,
                   ? AS dash_dict,
                   NOW() AS dash_created,
                   NOW() AS dash_updated,
                   ? AS dash_env`

		dashDict := fmt.Sprintf(`{"err": "%d"}`, errCount)

		if _, err := oDb.upsertAlerts(ctx, alertsUpsert{
			source:  sourceAlert,
			args:    []any{svcID, nodeID, sev, dashDict, svcEnv},
			columns: []string{"dash_type", "svc_id", "node_id", "dash_severity", "dash_fmt", "dash_dict", "dash_created", "dash_updated", "dash_env"},
			update:  []string{"dash_severity = s.dash_severity", "dash_fmt = s.dash_fmt", "dash_dict = s.dash_dict", "dash_updated = s.dash_updated", "dash_env = s.dash_env"},
		}); err != nil {
			return fmt.Errorf("UpdateDashActionErrors: insert/update failed: %w", err)
		}
		// TODO: WebSocket notification
	} else {
		// TODO: WebSocket notification
		const queryDelete = `DELETE FROM dashboard
                 WHERE
                   dash_type="action errors" AND
                   svc_id = ? AND
                   node_id = ?`
		if _, err := oDb.ExecContext(ctx, queryDelete, svcID, nodeID); err != nil {
			return fmt.Errorf("UpdateDashActionErrors: delete failed: %w", err)
		}
	}
	return nil
}

// queryServiceActions lists the actions the agents ran (svcactions), each with its
// service and its node when the node row exists, the latest first as the
// historical actions view; svcID and id, when not empty, narrow it to a service or
// to one row. A non-manager sees only the actions of the services of an app their
// groups are responsible for.
func (oDb *DB) queryServiceActions(ctx context.Context, svcID, id string, p ListParams) ([]map[string]any, error) {
	defer logDuration("getServiceActions", time.Now())
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getServiceActions: no select expressions")
	}
	// services is an inner join: the access check filters on services.svc_app.
	q := From(schema.TSvcactions).
		Via(schema.TServices).
		LeftJoin(schema.TNodes).
		RawSelect(p.SelectExprs...)
	if svcID != "" {
		q = q.Where(schema.SvcactionsSvcID, "=", svcID)
	}
	if id != "" {
		q = q.Where(schema.SvcactionsID, "=", id)
	}
	if !p.IsManager {
		clean := cleanGroups(p.Groups)
		if len(clean) == 0 {
			q = q.WhereRaw("1=0")
		} else {
			q = q.WhereRaw(
				"services.svc_app IN ("+
					"SELECT a.app FROM apps a"+
					" JOIN apps_responsibles ar ON ar.app_id = a.id"+
					" JOIN auth_group ag ON ag.id = ar.group_id"+
					" WHERE ag.role IN ("+Placeholders(len(clean))+"))",
				stringsToAny(clean)...,
			)
		}
	} else if svcID == "" && id == "" {
		// The query builder wants a condition to build its WHERE on.
		q = q.Where(schema.SvcactionsID, ">", 0)
	}
	// Column filters of the request, ANDed with the access control above.
	q = q.WhereFilters(p.Filters)
	query, args, err := q.Build()
	if err != nil {
		return nil, fmt.Errorf("getServiceActions: %w", err)
	}
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("svcactions.begin DESC, svcactions.id DESC")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getServiceActions: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// GetServiceActions returns the actions the agents ran on a service.
func (oDb *DB) GetServiceActions(ctx context.Context, svcID string, p ListParams) ([]map[string]any, error) {
	return oDb.queryServiceActions(ctx, svcID, "", p)
}

// GetServicesActions returns the actions the agents ran on the services the
// caller may see.
func (oDb *DB) GetServicesActions(ctx context.Context, p ListParams) ([]map[string]any, error) {
	return oDb.queryServiceActions(ctx, "", "", p)
}

// GetServicesAction returns one action, by its record id, if the caller may see it.
func (oDb *DB) GetServicesAction(ctx context.Context, id string, p ListParams) ([]map[string]any, error) {
	return oDb.queryServiceActions(ctx, "", id, p)
}

// ServiceActionAck is an action the agents ran, as its acknowledgement needs it.
type ServiceActionAck struct {
	ID     int64
	SvcID  string
	NodeID string
	// SID ties an action to the lines it logged, rows of their own.
	SID    string
	Status string
	Acked  bool
}

// ServiceActionForAck returns an action, nil when there is none of that id.
func (oDb *DB) ServiceActionForAck(ctx context.Context, id int64) (*ServiceActionAck, error) {
	var a ServiceActionAck
	err := oDb.DB.QueryRowContext(ctx, `SELECT id, IFNULL(svc_id, ""), IFNULL(node_id, ""),
		IFNULL(sid, ""), IFNULL(status, ""), IFNULL(ack, 0) = 1 FROM svcactions WHERE id = ?`, id).
		Scan(&a.ID, &a.SvcID, &a.NodeID, &a.SID, &a.Status, &a.Acked)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("serviceActionForAck: %w", err)
	}
	return &a, nil
}

// AckServiceAction acknowledges a failed action, by whom, when and why, and the
// failed lines it logged, which the count of the action errors of the instance
// counts too: without them, it would not come back to zero.
func (oDb *DB) AckServiceAction(ctx context.Context, a *ServiceActionAck, by, comment string) error {
	query := `UPDATE svcactions
		SET ack = 1, acked_by = ?, acked_date = NOW(), acked_comment = ?
		WHERE id = ?`
	args := []any{by, comment, a.ID}
	if a.SID != "" {
		query += ` OR (sid = ? AND svc_id = ? AND node_id = ? AND IFNULL(log_type, "") = ""
			AND status = "err" AND IFNULL(ack, 0) <> 1)`
		args = append(args, a.SID, a.SvcID, a.NodeID)
	}
	if _, err := oDb.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("ackServiceAction: %w", err)
	}
	oDb.SetChange("svcactions")
	return nil
}
