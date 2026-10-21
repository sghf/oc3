package cdb

import (
	"context"
	"fmt"
	"strings"
)

// orchestrationsListTable is the orchestrations as listed (GET /orchestrations):
// one row of the orchestrations table each, with the actions of its steps and
// its last run. A derived table under the name of the table, so that these
// computed columns are sorted and filtered as the others.
const orchestrationsListTable = `(SELECT o.id, o.name, o.description, o.steps_json AS steps,
	JSON_LENGTH(o.steps_json) AS step_count,
	REPLACE(REPLACE(REPLACE(JSON_EXTRACT(o.steps_json, '$[*].action'), '[', ''), ']', ''), '"', '') AS step_actions,
	o.created, o.updated, o.updated_by,
	(SELECT COUNT(*) FROM orchestration_runs r WHERE r.orchestration_id = o.id) AS run_count,
	(SELECT r.id FROM orchestration_runs r WHERE r.orchestration_id = o.id ORDER BY r.id DESC LIMIT 1) AS last_run_id,
	(SELECT r.status FROM orchestration_runs r WHERE r.orchestration_id = o.id ORDER BY r.id DESC LIMIT 1) AS last_run_status,
	(SELECT r.started FROM orchestration_runs r WHERE r.orchestration_id = o.id ORDER BY r.id DESC LIMIT 1) AS last_run_started
	FROM orchestrations o) AS orchestrations`

// orchestrationRunsListTable is the runs as listed (GET /orchestration_runs):
// one row of orchestration_runs each, with the count of its steps, of those
// done (succeeded, or failed and acknowledged), and its duration so far.
const orchestrationRunsListTable = `(SELECT r.id, r.orchestration_id, r.name, r.status, r.current_step,
	r.message, r.started, r.ended, r.started_by,
	(SELECT COUNT(*) FROM orchestration_run_steps s WHERE s.run_id = r.id) AS step_count,
	(SELECT COUNT(*) FROM orchestration_run_steps s WHERE s.run_id = r.id
		AND s.status IN ('succeeded', 'acknowledged')) AS steps_done,
	TIMESTAMPDIFF(SECOND, r.started, COALESCE(r.ended, NOW())) AS duration
	FROM orchestration_runs r) AS orchestration_runs`

func (oDb *DB) queryListTable(ctx context.Context, name, table, defaultOrder string, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("%s: no select expressions", name)
	}
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") + " FROM " + table + " WHERE 1=1"
	conds, args := p.FilterConditions()
	for _, cond := range conds {
		query += " AND " + cond
	}
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause(defaultOrder)
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// GetOrchestrationsList returns the orchestrations, by name unless sorted
// otherwise. The api lets Managers only read them: no visibility here.
func (oDb *DB) GetOrchestrationsList(ctx context.Context, p ListParams) ([]map[string]any, error) {
	return oDb.queryListTable(ctx, "getOrchestrationsList", orchestrationsListTable, "orchestrations.name", p)
}

// GetOrchestrationRunsList returns the runs, the latest first unless sorted
// otherwise. The api lets Managers only read them: no visibility here.
func (oDb *DB) GetOrchestrationRunsList(ctx context.Context, p ListParams) ([]map[string]any, error) {
	return oDb.queryListTable(ctx, "getOrchestrationRunsList", orchestrationRunsListTable, "orchestration_runs.id DESC", p)
}
