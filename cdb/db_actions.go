package cdb

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/google/uuid"
)

type (
	SvcAction struct {
		ID     int64
		SvcID  uuid.UUID
		NodeID uuid.UUID
	}

	BActionErrorCount struct {
		SvcID    string
		NodeID   string
		ErrCount int
		SvcEnv   *string
	}
)

func (oDb *DB) BActionErrorsRefresh(ctx context.Context) error {
	_, err := oDb.DB.ExecContext(ctx, "TRUNCATE b_action_errors")
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

	_, err = oDb.DB.ExecContext(ctx, sql)
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
	result, err := oDb.DB.ExecContext(ctx, request, args...)
	if err != nil {
		return err
	}
	if rowAffected, err := result.RowsAffected(); err != nil {
		return err
	} else if rowAffected > 0 {
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
	result, err := oDb.DB.ExecContext(ctx, request, args...)
	if err != nil {
		return err
	}
	if rowAffected, err := result.RowsAffected(); err != nil {
		return err
	} else if rowAffected > 0 {
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
	request := `
                 INSERT INTO dashboard
                 SET
                   dash_type="action errors",
                   svc_id=?,
                   node_id=?,
                   dash_severity=?,
                   dash_fmt="%(err)s action errors",
                   dash_dict=CONCAT('{"err": "', ?, '"}'),
                   dash_created=NOW(),
                   dash_env=?,
                   dash_updated=NOW()
                 ON DUPLICATE KEY UPDATE
                   dash_severity=?,
                   dash_fmt="%(err)s action errors",
                   dash_dict=CONCAT('{"err": "', ?, '"}'),
                   dash_updated=NOW()`
	result, err := oDb.DB.ExecContext(ctx, request, line.SvcID, line.NodeID, severity, line.ErrCount, env, severity, line.ErrCount)
	if err != nil {
		return err
	}
	if rowAffected, err := result.RowsAffected(); err != nil {
		return err
	} else if rowAffected > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

func (oDb *DB) UpdateUnfinishedActions(ctx context.Context) error {
	request := `UPDATE svcactions
		SET
		    status = "err",
		    end = "1000-01-01 00:00:00"
		WHERE
		    begin < DATE_SUB(NOW(), INTERVAL 120 MINUTE)
		    AND end IS NULL
		    AND status IS NULL
		    AND action NOT LIKE "%#%"`
	result, err := oDb.DB.ExecContext(ctx, request)
	if err != nil {
		return err
	}
	if rowAffected, err := result.RowsAffected(); err != nil {
		return err
	} else if rowAffected > 0 {
		oDb.SetChange("svcactions")
	}
	return nil
}

func (oDb *DB) GetUnfinishedActions(ctx context.Context) (lines []SvcAction, err error) {
	query := `SELECT id, node_id, svc_id FROM svcactions
		WHERE
		    begin < DATE_SUB(NOW(), INTERVAL 120 MINUTE)
		    AND end IS NULL
		    AND status IS NULL
		    AND action NOT LIKE "%#%"`
	var rows *sql.Rows
	rows, err = oDb.DB.QueryContext(ctx, query)
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
