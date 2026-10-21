package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/opensvc/oc3/schema"
)

func buildServicesInstancesQuery(groups []string, isManager bool, selectExprs []string, filters []ColumnFilter) (string, []any, error) {
	// services is an inner join: the access check filters on services.svc_app.
	// nodes is a left join so that "nodes.*" props can be selected without dropping
	// an instance whose node row is missing.
	// b_action_errors holds the count of the instance's unacknowledged action
	// errors of the last day, one row per service and node: the "err" prop.
	q := From(schema.TSvcmon).
		Via(schema.TServices).
		LeftJoin(schema.TNodes).
		LeftJoinOn(schema.TBActionErrors,
			"b_action_errors.svc_id = svcmon.svc_id AND b_action_errors.node_id = svcmon.node_id").
		RawSelect(selectExprs...)

	if !isManager {
		cleanGroups := cleanGroups(groups)
		if len(cleanGroups) == 0 {
			q = q.WhereRaw("1=0")
		} else {
			args := make([]any, len(cleanGroups))
			for i, g := range cleanGroups {
				args[i] = g
			}
			q = q.WhereRaw(
				"services.svc_app IN ("+
					"SELECT a.app FROM apps a"+
					" JOIN apps_responsibles ar ON ar.app_id = a.id"+
					" JOIN auth_group ag ON ag.id = ar.group_id"+
					" WHERE ag.role IN ("+Placeholders(len(cleanGroups))+")"+
					")",
				args...,
			)
		}
	} else {
		q = q.Where(schema.SvcmonID, ">", 0)
	}

	// Column filters of the request, ANDed with the access control above.
	q = q.WhereFilters(filters)

	query, args, err := q.Build()
	if err != nil {
		return "", nil, fmt.Errorf("buildServicesInstancesQuery: %w", err)
	}
	return query, args, nil
}

func (oDb *DB) GetServicesInstances(ctx context.Context, p ListParams) ([]map[string]any, error) {
	query, args, err := buildServicesInstancesQuery(p.Groups, p.IsManager, p.SelectExprs, p.Filters)
	if err != nil {
		return nil, err
	}
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("svcmon.svc_id, svcmon.node_id")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getServicesInstances: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// GetServiceNodeInstance fetches the service instance on a specific node.
func (oDb *DB) GetServiceNodeInstance(ctx context.Context, svcID, nodeID string, p ListParams) ([]map[string]any, error) {
	query, args, err := buildServicesInstancesQuery(p.Groups, p.IsManager, p.SelectExprs, p.Filters)
	if err != nil {
		return nil, err
	}
	query += " AND svcmon.svc_id = ? AND svcmon.node_id = ?"
	args = append(args, svcID, nodeID)
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getServiceNodeInstance: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// GetNodeServices fetches all service instances on a specific node.
func (oDb *DB) GetNodeServices(ctx context.Context, nodeID string, p ListParams) ([]map[string]any, error) {
	query, args, err := buildServicesInstancesQuery(p.Groups, p.IsManager, p.SelectExprs, p.Filters)
	if err != nil {
		return nil, err
	}
	query += " AND svcmon.node_id = ?"
	args = append(args, nodeID)
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("svcmon.svc_id")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getNodeServices: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

func (oDb *DB) DeleteServiceInstanceCascade(ctx context.Context, svcID, nodeID string) (int64, error) {
	if svcID == "" || nodeID == "" {
		return 0, fmt.Errorf("DeleteServiceInstanceCascade: empty svc_id or node_id")
	}
	svcmonCount, err := oDb.execCountContext(ctx,
		"DELETE FROM svcmon WHERE svc_id = ? AND node_id = ?", svcID, nodeID)
	if err != nil {
		return 0, fmt.Errorf("DeleteServiceInstanceCascade svcmon: %w", err)
	}
	if svcmonCount == 0 {
		return 0, nil
	}
	oDb.SetChange("svcmon")
	for _, t := range instanceCascadeTables {
		query := "DELETE FROM " + t + " WHERE svc_id = ? AND node_id = ?"
		if _, err := oDb.ExecContext(ctx, query, svcID, nodeID); err != nil {
			return svcmonCount, fmt.Errorf("DeleteServiceInstanceCascade %s: %w", t, err)
		}
		oDb.SetChange(t)
	}
	return svcmonCount, nil
}

// instanceCascadeTables are the tables, besides svcmon, holding rows of an
// instance, which its deletion empties of them.
var instanceCascadeTables = []string{"dashboard", "resmon", "resinfo", "checks_live"}

// ExistingInstances returns, among the services of svcIDs, those with an
// instance on the node.
func (oDb *DB) ExistingInstances(ctx context.Context, nodeID string, svcIDs []string) (map[string]bool, error) {
	m := map[string]bool{}
	if len(svcIDs) == 0 {
		return m, nil
	}
	args := []any{nodeID}
	for _, id := range svcIDs {
		args = append(args, id)
	}
	rows, err := oDb.DB.QueryContext(ctx, "SELECT DISTINCT svc_id FROM svcmon WHERE node_id = ? AND svc_id IN ("+
		strings.TrimSuffix(strings.Repeat("?,", len(svcIDs)), ",")+")", args...)
	if err != nil {
		return nil, fmt.Errorf("existingInstances: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("existingInstances: %w", err)
		}
		m[id] = true
	}
	return m, rows.Err()
}

// DeleteServiceInstancesCascade deletes several instances as
// DeleteServiceInstanceCascade does one, by node: for each node, a statement per
// table for all the services whose instance on it goes, the svc_id index serving
// each. svcIDsByNode holds, by node id, the services.
func (oDb *DB) DeleteServiceInstancesCascade(ctx context.Context, svcIDsByNode map[string][]string) error {
	for nodeID, svcIDs := range svcIDsByNode {
		if nodeID == "" || len(svcIDs) == 0 {
			continue
		}
		args := []any{nodeID}
		for _, id := range svcIDs {
			if id == "" {
				return fmt.Errorf("DeleteServiceInstancesCascade: empty svc_id")
			}
			args = append(args, id)
		}
		placeholders := strings.TrimSuffix(strings.Repeat("?,", len(svcIDs)), ",")
		for _, t := range append([]string{"svcmon"}, instanceCascadeTables...) {
			query := "DELETE FROM " + t + " WHERE node_id = ? AND svc_id IN (" + placeholders + ")"
			if _, err := oDb.ExecContext(ctx, query, args...); err != nil {
				return fmt.Errorf("DeleteServiceInstancesCascade %s: %w", t, err)
			}
			oDb.SetChange(t)
		}
	}
	return nil
}

// GetServicesInstance fetches all instances of a single service by svc_id (UUID) or svcname.
func (oDb *DB) GetServicesInstance(ctx context.Context, svcID string, p ListParams) ([]map[string]any, error) {
	query, args, err := buildServicesInstancesQuery(p.Groups, p.IsManager, p.SelectExprs, p.Filters)
	if err != nil {
		return nil, err
	}
	query += " AND (svcmon.svc_id = ? OR services.svcname = ?)"
	args = append(args, svcID, svcID)
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("svcmon.node_id")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getServicesInstance: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// ServiceInstanceByID returns the svc_id and node_id of the service instance
func (oDb *DB) ServiceInstanceByID(ctx context.Context, id int) (string, string, error) {
	const query = "SELECT svc_id, node_id FROM svcmon WHERE id = ? LIMIT 1"
	var svcID, nodeID sql.NullString
	err := oDb.DB.QueryRowContext(ctx, query, id).Scan(&svcID, &nodeID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", nil
	}
	if err != nil {
		return "", "", fmt.Errorf("ServiceInstanceByID: %w", err)
	}
	return svcID.String, nodeID.String, nil
}
