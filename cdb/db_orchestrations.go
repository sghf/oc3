package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

type (
	// Orchestration is a row of orchestrations: a plan of steps, StepsJSON as
	// the api reads and writes it.
	Orchestration struct {
		ID          int
		Name        string
		Description string
		StepsJSON   string
		Created     time.Time
		Updated     time.Time
		UpdatedBy   string
	}

	// OrchestrationRun is a row of orchestration_runs: a run and the plan it
	// runs, frozen when it started.
	OrchestrationRun struct {
		ID              int
		OrchestrationID *int
		Name            string
		PlanJSON        string
		Status          string
		CurrentStep     int
		Started         time.Time
		Ended           *time.Time
		UserID          *int64
		StartedBy       string
		Message         string
	}

	// OrchestrationRunStep is a row of orchestration_run_steps.
	OrchestrationRunStep struct {
		RunID   int
		Step    int
		Status  string
		Started *time.Time
		Ended   *time.Time
	}

	// OrchestrationRunTarget is a row of orchestration_run_targets: a target of
	// a step of a run, the action queued for it and its results.
	OrchestrationRunTarget struct {
		ID        int
		RunID     int
		Step      int
		Position  int
		NodeID    string
		SvcID     string
		ClusterID string
		Label     string
		Status    string
		ActionID  *int64
		Command   *string
		Ret       *int
		Stdout    *string
		Stderr    *string
		Message   string
		Queued    *time.Time
		Started   *time.Time
		Ended     *time.Time
	}

	// ActionQueueState is what a run reads of a queued action: its status, and
	// its results once done.
	ActionQueueState struct {
		ID      int64
		Status  string
		Ret     *int
		Stdout  *string
		Stderr  *string
		Command string
	}

	// InstanceRef names an instance: a service on a node.
	InstanceRef struct {
		SvcID  string
		NodeID string
	}
)

const orchestrationColumns = "id, name, description, steps_json, created, updated, updated_by"

func scanOrchestration(scan func(...any) error) (*Orchestration, error) {
	var o Orchestration
	if err := scan(&o.ID, &o.Name, &o.Description, &o.StepsJSON, &o.Created, &o.Updated, &o.UpdatedBy); err != nil {
		return nil, err
	}
	return &o, nil
}

// Orchestrations returns every orchestration, by name.
func (oDb *DB) Orchestrations(ctx context.Context) ([]Orchestration, error) {
	rows, err := oDb.DB.QueryContext(ctx, "SELECT "+orchestrationColumns+" FROM orchestrations ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	l := []Orchestration{}
	for rows.Next() {
		o, err := scanOrchestration(rows.Scan)
		if err != nil {
			return nil, err
		}
		l = append(l, *o)
	}
	return l, rows.Err()
}

// OrchestrationByID returns the orchestration, nil when there is none.
func (oDb *DB) OrchestrationByID(ctx context.Context, id int) (*Orchestration, error) {
	row := oDb.DB.QueryRowContext(ctx, "SELECT "+orchestrationColumns+" FROM orchestrations WHERE id = ?", id)
	o, err := scanOrchestration(row.Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return o, err
}

// OrchestrationNameTaken tells whether another orchestration than exceptID has
// the name.
func (oDb *DB) OrchestrationNameTaken(ctx context.Context, name string, exceptID int) (bool, error) {
	var n int
	err := oDb.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM orchestrations WHERE name = ? AND id != ?", name, exceptID).Scan(&n)
	return n > 0, err
}

// InsertOrchestration creates the orchestration and returns its id.
func (oDb *DB) InsertOrchestration(ctx context.Context, o Orchestration) (int, error) {
	result, err := oDb.DB.ExecContext(ctx,
		"INSERT INTO orchestrations (name, description, steps_json, created, updated, updated_by) VALUES (?, ?, ?, NOW(), NOW(), ?)",
		o.Name, o.Description, o.StepsJSON, o.UpdatedBy)
	if err != nil {
		return 0, err
	}
	oDb.SetChange("orchestrations")
	id, err := result.LastInsertId()
	return int(id), err
}

// UpdateOrchestration replaces the name, description and steps of the
// orchestration, false when there is none.
func (oDb *DB) UpdateOrchestration(ctx context.Context, o Orchestration) (bool, error) {
	result, err := oDb.DB.ExecContext(ctx,
		"UPDATE orchestrations SET name = ?, description = ?, steps_json = ?, updated = NOW(), updated_by = ? WHERE id = ?",
		o.Name, o.Description, o.StepsJSON, o.UpdatedBy, o.ID)
	if err != nil {
		return false, err
	}
	oDb.SetChange("orchestrations")
	// An unchanged row counts as no row: tell it from a missing one.
	if n, err := result.RowsAffected(); err == nil && n > 0 {
		return true, nil
	}
	existing, err := oDb.OrchestrationByID(ctx, o.ID)
	return existing != nil, err
}

// DeleteOrchestration deletes the orchestration, false when there is none. Its
// runs stay, unlinked.
func (oDb *DB) DeleteOrchestration(ctx context.Context, id int) (bool, error) {
	result, err := oDb.DB.ExecContext(ctx, "DELETE FROM orchestrations WHERE id = ?", id)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	if err != nil || n == 0 {
		return false, err
	}
	if _, err := oDb.DB.ExecContext(ctx, "UPDATE orchestration_runs SET orchestration_id = NULL WHERE orchestration_id = ?", id); err != nil {
		return false, err
	}
	oDb.SetChange("orchestrations", "orchestration_runs")
	return true, nil
}

// InsertOrchestrationRun creates a running run, its steps, the first one
// running, and their targets, all waiting. To be called in a transaction.
func (oDb *DB) InsertOrchestrationRun(ctx context.Context, run OrchestrationRun, steps int, targets []OrchestrationRunTarget) (int, error) {
	result, err := oDb.DB.ExecContext(ctx, `INSERT INTO orchestration_runs
		(orchestration_id, name, plan_json, status, current_step, started, user_id, started_by, message)
		VALUES (?, ?, ?, 'running', 0, NOW(), ?, ?, '')`,
		run.OrchestrationID, run.Name, run.PlanJSON, run.UserID, run.StartedBy)
	if err != nil {
		return 0, err
	}
	id64, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}
	id := int(id64)
	for step := 0; step < steps; step++ {
		status, started := "waiting", "NULL"
		if step == 0 {
			status, started = "running", "NOW()"
		}
		if _, err := oDb.DB.ExecContext(ctx,
			"INSERT INTO orchestration_run_steps (run_id, step, status, started) VALUES (?, ?, ?, "+started+")",
			id, step, status); err != nil {
			return 0, err
		}
	}
	for _, t := range targets {
		if _, err := oDb.DB.ExecContext(ctx, `INSERT INTO orchestration_run_targets
			(run_id, step, position, node_id, svc_id, cluster_id, label, status, message)
			VALUES (?, ?, ?, ?, ?, ?, ?, 'waiting', '')`,
			id, t.Step, t.Position, t.NodeID, t.SvcID, t.ClusterID, t.Label); err != nil {
			return 0, err
		}
	}
	oDb.SetChange("orchestration_runs")
	return id, nil
}

const orchestrationRunColumns = "id, orchestration_id, name, plan_json, status, current_step, started, ended, user_id, started_by, message"

func scanOrchestrationRun(scan func(...any) error) (*OrchestrationRun, error) {
	var (
		r      OrchestrationRun
		oid    sql.NullInt64
		ended  sql.NullTime
		userID sql.NullInt64
	)
	if err := scan(&r.ID, &oid, &r.Name, &r.PlanJSON, &r.Status, &r.CurrentStep, &r.Started, &ended, &userID, &r.StartedBy, &r.Message); err != nil {
		return nil, err
	}
	if oid.Valid {
		v := int(oid.Int64)
		r.OrchestrationID = &v
	}
	if ended.Valid {
		r.Ended = &ended.Time
	}
	if userID.Valid {
		r.UserID = &userID.Int64
	}
	return &r, nil
}

// OrchestrationRunByID returns the run, nil when there is none.
func (oDb *DB) OrchestrationRunByID(ctx context.Context, id int) (*OrchestrationRun, error) {
	row := oDb.DB.QueryRowContext(ctx, "SELECT "+orchestrationRunColumns+" FROM orchestration_runs WHERE id = ?", id)
	r, err := scanOrchestrationRun(row.Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return r, err
}

// OrchestrationRuns returns the runs, the latest first, of an orchestration
// when orchestrationID is set; limit 0 means no limit.
func (oDb *DB) OrchestrationRuns(ctx context.Context, orchestrationID *int, limit int) ([]OrchestrationRun, error) {
	query := "SELECT " + orchestrationRunColumns + " FROM orchestration_runs"
	var args []any
	if orchestrationID != nil {
		query += " WHERE orchestration_id = ?"
		args = append(args, *orchestrationID)
	}
	query += " ORDER BY id DESC"
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	l := []OrchestrationRun{}
	for rows.Next() {
		r, err := scanOrchestrationRun(rows.Scan)
		if err != nil {
			return nil, err
		}
		l = append(l, *r)
	}
	return l, rows.Err()
}

// OrchestrationRunIDsToAdvance returns the runs the engine has work on: those
// running, and those ended with actions still queued or running, whose results
// are still to be recorded.
func (oDb *DB) OrchestrationRunIDsToAdvance(ctx context.Context) ([]int, error) {
	rows, err := oDb.DB.QueryContext(ctx, `SELECT id FROM orchestration_runs WHERE status = 'running'
		UNION SELECT DISTINCT run_id FROM orchestration_run_targets WHERE status IN ('queued', 'running')`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var l []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		l = append(l, id)
	}
	return l, rows.Err()
}

// SetOrchestrationRun updates the status, current step, end and message of the
// run; ended is set when the status is not running, cleared otherwise.
func (oDb *DB) SetOrchestrationRun(ctx context.Context, id int, status string, currentStep int, message string) error {
	ended := "NOW()"
	if status == "running" {
		ended = "NULL"
	}
	_, err := oDb.DB.ExecContext(ctx,
		"UPDATE orchestration_runs SET status = ?, current_step = ?, message = ?, ended = "+ended+" WHERE id = ?",
		status, currentStep, message, id)
	if err == nil {
		oDb.SetChange("orchestration_runs")
	}
	return err
}

// OrchestrationRunSteps returns the steps of the runs, in order.
func (oDb *DB) OrchestrationRunSteps(ctx context.Context, runIDs []int) ([]OrchestrationRunStep, error) {
	if len(runIDs) == 0 {
		return nil, nil
	}
	placeholders, args := getPlaceholdersAndArgs(runIDs)
	rows, err := oDb.DB.QueryContext(ctx, fmt.Sprintf(
		"SELECT run_id, step, status, started, ended FROM orchestration_run_steps WHERE run_id IN (%s) ORDER BY run_id, step",
		strings.Join(placeholders, ",")), args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var l []OrchestrationRunStep
	for rows.Next() {
		var (
			s              OrchestrationRunStep
			started, ended sql.NullTime
		)
		if err := rows.Scan(&s.RunID, &s.Step, &s.Status, &started, &ended); err != nil {
			return nil, err
		}
		if started.Valid {
			s.Started = &started.Time
		}
		if ended.Valid {
			s.Ended = &ended.Time
		}
		l = append(l, s)
	}
	return l, rows.Err()
}

// SetOrchestrationRunStep updates the status of a step of a run, and its start
// or end as the status implies: running starts it, waiting clears both dates,
// another status ends it.
func (oDb *DB) SetOrchestrationRunStep(ctx context.Context, runID, step int, status string) error {
	var dates string
	switch status {
	case "running":
		dates = "started = COALESCE(started, NOW()), ended = NULL"
	case "waiting":
		dates = "started = NULL, ended = NULL"
	default:
		dates = "ended = NOW()"
	}
	_, err := oDb.DB.ExecContext(ctx,
		"UPDATE orchestration_run_steps SET status = ?, "+dates+" WHERE run_id = ? AND step = ?",
		status, runID, step)
	if err == nil {
		oDb.SetChange("orchestration_runs")
	}
	return err
}

// SetOrchestrationRunStepsFrom updates the status of the steps of a run from
// step on, as SetOrchestrationRunStep does.
func (oDb *DB) SetOrchestrationRunStepsFrom(ctx context.Context, runID, step int, status string) error {
	var dates string
	if status == "waiting" {
		dates = ", started = NULL, ended = NULL"
	}
	_, err := oDb.DB.ExecContext(ctx,
		"UPDATE orchestration_run_steps SET status = ?"+dates+" WHERE run_id = ? AND step >= ?",
		status, runID, step)
	if err == nil {
		oDb.SetChange("orchestration_runs")
	}
	return err
}

const orchestrationTargetColumns = "id, run_id, step, position, node_id, svc_id, cluster_id, label, status, action_id, command, ret, stdout, stderr, message, queued, started, ended"

// OrchestrationRunTargets returns the targets of a run, in step then position
// order; step < 0 means every step.
func (oDb *DB) OrchestrationRunTargets(ctx context.Context, runID, step int) ([]OrchestrationRunTarget, error) {
	query := "SELECT " + orchestrationTargetColumns + " FROM orchestration_run_targets WHERE run_id = ?"
	args := []any{runID}
	if step >= 0 {
		query += " AND step = ?"
		args = append(args, step)
	}
	query += " ORDER BY step, position"
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var l []OrchestrationRunTarget
	for rows.Next() {
		var (
			t                       OrchestrationRunTarget
			actionID                sql.NullInt64
			ret                     sql.NullInt64
			stdout, stderr, command sql.NullString
			queued, started, ended  sql.NullTime
		)
		if err := rows.Scan(&t.ID, &t.RunID, &t.Step, &t.Position, &t.NodeID, &t.SvcID, &t.ClusterID, &t.Label, &t.Status,
			&actionID, &command, &ret, &stdout, &stderr, &t.Message, &queued, &started, &ended); err != nil {
			return nil, err
		}
		if actionID.Valid {
			t.ActionID = &actionID.Int64
		}
		if command.Valid {
			t.Command = &command.String
		}
		if ret.Valid {
			v := int(ret.Int64)
			t.Ret = &v
		}
		if stdout.Valid {
			t.Stdout = &stdout.String
		}
		if stderr.Valid {
			t.Stderr = &stderr.String
		}
		if queued.Valid {
			t.Queued = &queued.Time
		}
		if started.Valid {
			t.Started = &started.Time
		}
		if ended.Valid {
			t.Ended = &ended.Time
		}
		l = append(l, t)
	}
	return l, rows.Err()
}

// OrchestrationRunTargetCounts returns, per run and step, the number of targets
// by status.
func (oDb *DB) OrchestrationRunTargetCounts(ctx context.Context, runIDs []int) (map[int]map[int]map[string]int, error) {
	m := map[int]map[int]map[string]int{}
	if len(runIDs) == 0 {
		return m, nil
	}
	placeholders, args := getPlaceholdersAndArgs(runIDs)
	rows, err := oDb.DB.QueryContext(ctx, fmt.Sprintf(
		"SELECT run_id, step, status, COUNT(*) FROM orchestration_run_targets WHERE run_id IN (%s) GROUP BY run_id, step, status",
		strings.Join(placeholders, ",")), args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var (
			runID, step, n int
			status         string
		)
		if err := rows.Scan(&runID, &step, &status, &n); err != nil {
			return nil, err
		}
		if m[runID] == nil {
			m[runID] = map[int]map[string]int{}
		}
		if m[runID][step] == nil {
			m[runID][step] = map[string]int{}
		}
		m[runID][step][status] = n
	}
	return m, rows.Err()
}

// SetOrchestrationTargetQueued records the action queued for a target, and its
// command.
func (oDb *DB) SetOrchestrationTargetQueued(ctx context.Context, id int, actionID int64, command string) error {
	_, err := oDb.DB.ExecContext(ctx,
		"UPDATE orchestration_run_targets SET status = 'queued', action_id = ?, command = ?, queued = NOW(), message = '' WHERE id = ?",
		actionID, command, id)
	if err == nil {
		oDb.SetChange("orchestration_runs")
	}
	return err
}

// SetOrchestrationTargetRunning records that an agent took the action of the
// target.
func (oDb *DB) SetOrchestrationTargetRunning(ctx context.Context, id int) error {
	_, err := oDb.DB.ExecContext(ctx,
		"UPDATE orchestration_run_targets SET status = 'running', started = COALESCE(started, NOW()) WHERE id = ?", id)
	if err == nil {
		oDb.SetChange("orchestration_runs")
	}
	return err
}

// SetOrchestrationTargetDone records the end of a target, its results when its
// action finished.
func (oDb *DB) SetOrchestrationTargetDone(ctx context.Context, id int, status string, ret *int, stdout, stderr *string, message string) error {
	_, err := oDb.DB.ExecContext(ctx, `UPDATE orchestration_run_targets SET status = ?, ret = ?, stdout = ?, stderr = ?,
		message = ?, started = COALESCE(started, queued), ended = NOW() WHERE id = ?`,
		status, ret, stdout, stderr, message, id)
	if err == nil {
		oDb.SetChange("orchestration_runs")
	}
	return err
}

// SetOrchestrationTargetsStatus sets the status, and the message, of the targets
// of a step of a run in one of the statuses from; step < 0 means every step from
// minStep on.
func (oDb *DB) SetOrchestrationTargetsStatus(ctx context.Context, runID, step, minStep int, from []string, status, message string) error {
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(from)), ",")
	query := "UPDATE orchestration_run_targets SET status = ?, message = ? WHERE run_id = ? AND status IN (" + placeholders + ")"
	args := []any{status, message, runID}
	for _, s := range from {
		args = append(args, s)
	}
	if step >= 0 {
		query += " AND step = ?"
		args = append(args, step)
	} else {
		query += " AND step >= ?"
		args = append(args, minStep)
	}
	_, err := oDb.DB.ExecContext(ctx, query, args...)
	if err == nil {
		oDb.SetChange("orchestration_runs")
	}
	return err
}

// ResetOrchestrationTargets makes the targets of a run from step on waiting
// again, their action and results forgotten, but those of step that succeeded.
func (oDb *DB) ResetOrchestrationTargets(ctx context.Context, runID, step int) error {
	_, err := oDb.DB.ExecContext(ctx, `UPDATE orchestration_run_targets SET status = 'waiting', action_id = NULL,
		command = NULL, ret = NULL, stdout = NULL, stderr = NULL, message = '', queued = NULL, started = NULL, ended = NULL
		WHERE run_id = ? AND (step > ? OR (step = ? AND status != 'ok'))`, runID, step, step)
	if err == nil {
		oDb.SetChange("orchestration_runs")
	}
	return err
}

// ActionQueueStates returns the status and results of the queued actions, by
// id; an action deleted from the queue is absent.
func (oDb *DB) ActionQueueStates(ctx context.Context, ids []int64) (map[int64]ActionQueueState, error) {
	m := map[int64]ActionQueueState{}
	if len(ids) == 0 {
		return m, nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	rows, err := oDb.DB.QueryContext(ctx,
		"SELECT id, status, ret, stdout, stderr, command FROM action_queue WHERE id IN ("+placeholders+")", args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var (
			s              ActionQueueState
			status         sql.NullString
			ret            sql.NullInt64
			stdout, stderr sql.NullString
		)
		if err := rows.Scan(&s.ID, &status, &ret, &stdout, &stderr, &s.Command); err != nil {
			return nil, err
		}
		s.Status = status.String
		if ret.Valid {
			v := int(ret.Int64)
			s.Ret = &v
		}
		if stdout.Valid {
			s.Stdout = &stdout.String
		}
		if stderr.Valid {
			s.Stderr = &stderr.String
		}
		m[s.ID] = s
	}
	return m, rows.Err()
}

// CancelWaitingAction cancels a queued action no agent took yet, and tells
// whether it did.
func (oDb *DB) CancelWaitingAction(ctx context.Context, id int64) (bool, error) {
	result, err := oDb.DB.ExecContext(ctx, "UPDATE action_queue SET status = 'C' WHERE id = ? AND status = 'W'", id)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	if n > 0 {
		oDb.SetChange("action_queue")
	}
	return n > 0, err
}

// InstancesOf returns the instances of the services on the nodes, by service
// then node name.
func (oDb *DB) InstancesOf(ctx context.Context, svcIDs, nodeIDs []string) ([]InstanceRef, error) {
	if len(svcIDs) == 0 || len(nodeIDs) == 0 {
		return nil, nil
	}
	args := make([]any, 0, len(svcIDs)+len(nodeIDs))
	for _, id := range svcIDs {
		args = append(args, id)
	}
	for _, id := range nodeIDs {
		args = append(args, id)
	}
	rows, err := oDb.DB.QueryContext(ctx, fmt.Sprintf(`SELECT DISTINCT m.svc_id, m.node_id FROM svcmon m
		JOIN services s ON s.svc_id = m.svc_id JOIN nodes n ON n.node_id = m.node_id
		WHERE m.svc_id IN (%s) AND m.node_id IN (%s) ORDER BY s.svcname, n.nodename`,
		strings.TrimSuffix(strings.Repeat("?,", len(svcIDs)), ","),
		strings.TrimSuffix(strings.Repeat("?,", len(nodeIDs)), ",")), args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var l []InstanceRef
	for rows.Next() {
		var r InstanceRef
		if err := rows.Scan(&r.SvcID, &r.NodeID); err != nil {
			return nil, err
		}
		l = append(l, r)
	}
	return l, rows.Err()
}

// ClustersOfNodes returns the clusters of the nodes, by name.
func (oDb *DB) ClustersOfNodes(ctx context.Context, nodeIDs []string) ([]string, error) {
	if len(nodeIDs) == 0 {
		return nil, nil
	}
	args := make([]any, len(nodeIDs))
	for i, id := range nodeIDs {
		args[i] = id
	}
	rows, err := oDb.DB.QueryContext(ctx, fmt.Sprintf(`SELECT DISTINCT c.cluster_id FROM nodes n
		JOIN clusters c ON c.cluster_id = n.cluster_id WHERE n.node_id IN (%s) ORDER BY c.cluster_name`,
		strings.TrimSuffix(strings.Repeat("?,", len(nodeIDs)), ",")), args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var l []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		l = append(l, id)
	}
	return l, rows.Err()
}

// ObjectNames returns the names of the objects of a table by id: services,
// nodes or clusters.
func (oDb *DB) ObjectNames(ctx context.Context, table string, ids []string) (map[string]string, error) {
	var idCol, nameCol string
	switch table {
	case "services":
		idCol, nameCol = "svc_id", "svcname"
	case "nodes":
		idCol, nameCol = "node_id", "nodename"
	case "clusters":
		idCol, nameCol = "cluster_id", "cluster_name"
	default:
		return nil, fmt.Errorf("no names for table %s", table)
	}
	m := map[string]string{}
	if len(ids) == 0 {
		return m, nil
	}
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	rows, err := oDb.DB.QueryContext(ctx, fmt.Sprintf("SELECT %s, %s FROM %s WHERE %s IN (%s)",
		idCol, nameCol, table, idCol, strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")), args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var id string
		var name sql.NullString
		if err := rows.Scan(&id, &name); err != nil {
			return nil, err
		}
		m[id] = name.String
	}
	return m, rows.Err()
}
