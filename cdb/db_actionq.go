package cdb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type (
	ActionQueueEntry struct {
		ID           int
		Status       string
		Command      string
		DateQueued   time.Time
		DateDequeued time.Time
		Ret          int
		Stdout       string
		Stderr       string
		ActionType   string
		UserId       *int
		FormId       *int
		ConnectTo    *string
		NodeId       string
		SvcId        string
		Fqdn         *string
		ListenerPort int
	}

	/*
		ActionQueue represents a row in the action_queue table

			CREATE TABLE `action_queue` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `status` varchar(1) DEFAULT 'W',
			  `command` text NOT NULL,
			  `date_queued` timestamp NOT NULL DEFAULT current_timestamp(),
			  `date_dequeued` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
			  `ret` int(11) DEFAULT NULL,
			  `stdout` text DEFAULT NULL,
			  `stderr` text DEFAULT NULL,
			  `action_type` varchar(8) DEFAULT NULL,
			  `user_id` int(11) DEFAULT NULL,
			  `form_id` int(11) DEFAULT NULL,
			  `connect_to` varchar(128) DEFAULT NULL,
			  `node_id` char(36) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT '',
			  `svc_id` char(36) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT '',
			  PRIMARY KEY (`id`),
			  KEY `idx1` (`status`),
			  KEY `idx_ret` (`ret`),
			  KEY `idx2` (`status`,`ret`),
			  KEY `k_node_id` (`node_id`),
			  KEY `k_svc_id` (`svc_id`)
			) ENGINE=InnoDB AUTO_INCREMENT=640 DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci
	*/
	ActionQueue struct {
		ID           int
		Status       string
		Command      string
		DateQueued   time.Time
		DateDequeued time.Time
		Ret          int
		Stdout       string
		Stderr       string
		ActionType   string
		UserId       *int
		FormId       *int
		ConnectTo    *string
		NodeId       string
		SvcId        string
	}

	// ActionQueueNamed is a db ActionQueue with explicit Nodename and Svcname fields
	ActionQueueNamed struct {
		ActionQueue

		Nodename string
		Svcname  string
	}
)

const (
	ActionQTypePull = "pull"
	ActionQTypePush = "push"
	ActionQTypeFeed = "feed"
)

// ActionQSetNow sets @now, which ActionQPushPullSetDequeuedToNow and
// ActionQPushPullGetQueued read: a variable of the database session, so the
// three are to be called on one transaction, the runner's poll.
func (oDb *DB) ActionQSetNow(ctx context.Context) error {
	const query = `SET @now := NOW();`
	_, err := oDb.ExecContext(ctx, query)
	return err
}

func (oDb *DB) ActionQPushPullSetDequeuedToNow(ctx context.Context) error {
	const query = `UPDATE action_queue SET date_dequeued = @now WHERE status = 'W' AND date_dequeued=0 AND action_type IN (?, ?);`
	_, err := oDb.ExecContext(ctx, query, ActionQTypePush, ActionQTypePull)
	return err
}

// ActionQueueNamedByClusterID retrieves action queue named entries of type feed for a specific cluster ID meeting given conditions from the database.
func (oDb *DB) ActionQueueNamedByClusterID(ctx context.Context, clusterID string, actionType string) (lines []ActionQueueNamed, err error) {
	const query = `SELECT
    	a.id, a.command, a.action_type, a.status, a.form_id, a.svc_id, s.svcname, a.date_queued, a.date_dequeued, n.nodename
		FROM action_queue a
		LEFT JOIN services s
		    ON a.svc_id = s.svc_id
		JOIN nodes n
		    ON a.node_id = n.node_id
		WHERE
		    n.cluster_id = ?
		    AND a.action_type = ?
		    AND (
                a.status IN ('W', 'N')
                OR (
                    a.status = 'S'
            		AND a.date_queued < NOW() - INTERVAL 20 SECOND
            	)
            )
		`

	rows, err := oDb.DB.QueryContext(ctx, query, clusterID, actionType)
	if err != nil {
		return nil, fmt.Errorf("instancesFromObjectIDs query: %w", err)
	}
	if rows == nil {
		return nil, fmt.Errorf("instancesFromObjectIDs query returns nil rows")
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var a ActionQueueNamed
		var formID sql.NullInt64
		var svcname sql.NullString
		if err := rows.Scan(&a.ID, &a.Command, &a.ActionType, &a.Status, &formID, &a.SvcId, &svcname, &a.DateQueued, &a.DateDequeued, &a.Nodename); err != nil {
			return nil, fmt.Errorf("instancesFromObjectIDs scan: %w", err)
		}
		if formID.Valid {
			i := int(formID.Int64)
			a.FormId = &i
		}
		if svcname.Valid {
			a.Svcname = svcname.String
		}
		lines = append(lines, a)
	}
	return lines, rows.Err()
}

// ActionQueueNamedByNodeID retrieves action queue named entries associated with the specified node ID, filtered by specific conditions.
func (oDb *DB) ActionQueueNamedByNodeID(ctx context.Context, nodeID string, actionType string) (lines []ActionQueueNamed, err error) {
	const query = `SELECT
    	a.id, a.command, a.action_type, a.status, a.form_id, a.svc_id, s.svcname, a.date_queued, a.date_dequeued, n.nodename
		FROM action_queue a
		LEFT JOIN services s
		    ON a.svc_id = s.svc_id
		JOIN nodes n
			ON a.node_id = n.node_id
		WHERE
		    a.node_id = ?
		    AND a.action_type = ?
		    AND (
                a.status IN ('W', 'N')
                OR (
                    a.status = 'S'
            		AND a.date_queued < DATE_SUB(NOW(), INTERVAL 20 SECOND)
            	)
            )
		`

	rows, err := oDb.DB.QueryContext(ctx, query, nodeID, actionType)
	if err != nil {
		return nil, fmt.Errorf("instancesFromObjectIDs query: %w", err)
	}
	if rows == nil {
		return nil, fmt.Errorf("instancesFromObjectIDs query returns nil rows")
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var a ActionQueueNamed
		var formID sql.NullInt64
		var svcname sql.NullString
		if err := rows.Scan(&a.ID, &a.Command, &a.ActionType, &a.Status, &formID, &a.SvcId, &svcname, &a.DateQueued, &a.DateDequeued, &a.Nodename); err != nil {
			return nil, fmt.Errorf("instancesFromObjectIDs scan: %w", err)
		}
		if formID.Valid {
			i := int(formID.Int64)
			a.FormId = &i
		}
		if svcname.Valid {
			a.Svcname = svcname.String
		}
		lines = append(lines, a)
	}
	return lines, rows.Err()
}

// ActionQSetSent updates the status of action queue entries to 'S' for the provided IDs in the database.
// It receives a context for operation control and a variable number of integer IDs.
// Returns an error if the database operation fails or no IDs are provided.
func (oDb *DB) ActionQSetSent(ctx context.Context, ids ...int) error {
	if len(ids) == 0 {
		return nil
	}
	placeholders := strings.Repeat("?,", len(ids)-1) + "?"
	args := make([]any, len(ids))
	for i, v := range ids {
		args[i] = v
	}

	query := fmt.Sprintf("UPDATE action_queue SET status='S' WHERE id in (%s)", placeholders)
	_, err := oDb.ExecContext(ctx, query, args...)
	if err == nil {
		oDb.SetChange("action_queue")
	}
	return err
}

// ActionQSetRunningForClusterID updates the status of action queue entries to 'R' for the given cluster ID and entry IDs.
// It requires a non-empty list of entry IDs and uses the context for database operations.
// It challenges the action id node vs the cluster id of the requester to prevent the compromised node
// from corrupting action_queue.
func (oDb *DB) ActionQSetRunningForClusterID(ctx context.Context, clusterID string, ids ...int) error {
	if len(ids) == 0 {
		return nil
	}
	placeholders := strings.Repeat("?,", len(ids)-1) + "?"
	args := make([]any, len(ids)+1)
	args[0] = clusterID
	for i, v := range ids {
		args[i+1] = v
	}

	query := fmt.Sprintf("UPDATE action_queue SET status='R' WHERE node_id IN (SELECT node_id FROM nodes n WHERE n.cluster_id = ?) AND id in (%s)", placeholders)
	_, err := oDb.ExecContext(ctx, query, args...)
	if err == nil {
		oDb.SetChange("action_queue")
	}
	return err
}

// ActionQSetDoneForNodeID updates the status and results of an action queue entry for a given node ID in the database.
func (oDb *DB) ActionQSetDoneForNodeID(ctx context.Context, nodeID string, a ActionQueue) error {
	query := `UPDATE action_queue SET
			status='T',
			stdout = ?,
			stderr = ?, 
			ret = ?,
			date_dequeued = ?
        WHERE id = ? AND node_id = ?`
	_, err := oDb.ExecContext(ctx, query, a.Stdout, a.Stderr, a.Ret, a.DateDequeued, a.ID, nodeID)
	if err == nil {
		oDb.SetChange("action_queue")
	}
	return err
}

func (oDb *DB) ActionQPushPullGetQueued(ctx context.Context) (lines []ActionQueueEntry, err error) {
	const query = `SELECT
    	a.id, a.command, a.action_type, a.connect_to, n.fqdn, n.listener_port, a.form_id 
		FROM action_queue a JOIN nodes n ON a.node_id=n.node_id WHERE a.status='W' AND a.action_type IN (?, ?) AND a.date_dequeued=@now`

	var rows *sql.Rows

	rows, err = oDb.DB.QueryContext(ctx, query, ActionQTypePush, ActionQTypePull)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var line ActionQueueEntry
		if err = rows.Scan(&line.ID, &line.Command, &line.ActionType, &line.ConnectTo, &line.Fqdn, &line.ListenerPort, &line.FormId); err != nil {
			return
		}
		lines = append(lines, line)
	}
	err = rows.Err()
	return
}

func getPlaceholdersAndArgs(ids []int) (placeholders []string, args []any) {
	placeholders = make([]string, len(ids))
	args = make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id
	}
	return
}

func (oDb *DB) ActionQSetUnreachable(ctx context.Context, unreachableIds []int) error {
	placeholders, args := getPlaceholdersAndArgs(unreachableIds)
	request := fmt.Sprintf(`update action_queue set
                       status='T',
                       date_dequeued=NOW(),
                       ret=1,
                       stdout="",
                       stderr="unreachable"
                     where id in (%s)`, strings.Join(placeholders, ","))

	_, err := oDb.ExecContext(ctx, request, args...)
	return err
}

func (oDb *DB) ActionQSetInvalid(ctx context.Context, invalidIds []int) error {
	placeholders, args := getPlaceholdersAndArgs(invalidIds)
	request := fmt.Sprintf(`update action_queue set
                       status='T',
                       date_dequeued=NOW(),
                       ret=1,
                       stdout="",
                       stderr="invalid"
                     where id in (%s)`, strings.Join(placeholders, ","))

	_, err := oDb.ExecContext(ctx, request, args...)
	return err
}

func (oDb *DB) ActionQSetNotified(ctx context.Context, notifiedIds []int) error {
	placeholders, args := getPlaceholdersAndArgs(notifiedIds)
	request := fmt.Sprintf(`update action_queue set status='N' where id in (%s) and status='W'`, strings.Join(placeholders, ","))
	_, err := oDb.ExecContext(ctx, request, args...)
	return err
}

func (oDb *DB) ActionQSetQueued(ctx context.Context, queuedIds []int) error {
	placeholders, args := getPlaceholdersAndArgs(queuedIds)
	request := fmt.Sprintf(`update action_queue set status='Q' where id in (%s) and status='W'`, strings.Join(placeholders, ","))
	_, err := oDb.ExecContext(ctx, request, args...)
	return err
}

func (oDb *DB) ActionQPurge(ctx context.Context) error {
	request := `delete from action_queue where date_dequeued<date_sub(now(), interval 1 day) and status in ('T', 'C')`
	_, err := oDb.ExecContext(ctx, request)
	return err
}

func (oDb *DB) ActionQEventData(ctx context.Context) (map[string]any, error) {
	const query = `select
              (select count(id) from action_queue where status in ('Q', 'N', 'W', 'R', 'S')) as queued,
              (select count(id) from action_queue where ret!=0) as ko,
              (select count(id) from action_queue where ret=0 and status='T') as ok`
	row := oDb.DB.QueryRowContext(ctx, query)
	data := make(map[string]any)
	var queued, ko, ok int
	if err := row.Scan(&queued, &ko, &ok); err != nil {
		return data, err
	}
	data["queued"] = queued
	data["ko"] = ko
	data["ok"] = ok
	return data, nil
}

func (oDb *DB) ActionQSetRunning(ctx context.Context, runningIds []int) error {
	placeholders, args := getPlaceholdersAndArgs(runningIds)
	request := fmt.Sprintf(`update action_queue set status='R' where id in (%s)`, strings.Join(placeholders, ","))
	_, err := oDb.ExecContext(ctx, request, args...)
	return err
}

func (oDb *DB) ActionQSetDone(ctx context.Context, id int, ret int, stdout, stderr string) error {
	request := `update action_queue set
					   status='T',
					   date_dequeued=NOW(),
					   ret=?,
					   stdout=?,
					   stderr=?
					 where id=?`
	_, err := oDb.ExecContext(ctx, request, ret, stdout, stderr, id)
	return err
}

func (oDb *DB) ActionQGetActionQueueId(ctx context.Context, formId int) (int, error) {
	query := `select id from action_queue where form_id=?`
	row := oDb.DB.QueryRowContext(ctx, query, formId)
	var id int
	if err := row.Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
}

func (oDb *DB) ActionQCloseWorkflow(ctx context.Context, formId int) error {
	request := `update workflows set status="closed" where last_form_id=?`
	_, err := oDb.ExecContext(ctx, request, formId)
	return err
}

func (oDb *DB) ActionQSetFormNextId(ctx context.Context, id int) error {
	request := `update forms_store set form_next_id=0 where id=?`
	_, err := oDb.ExecContext(ctx, request, id)
	return err
}
