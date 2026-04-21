package cdb

import (
	"context"
	"fmt"

	"github.com/opensvc/oc3/schema"
)

func buildServicesInstancesStatusLogQuery(groups []string, isManager bool, selectExprs []string) (string, []any, error) {
	q := From(schema.TSvcmonLog).
		Via(schema.TServices).
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
		q = q.Where(schema.SvcmonLogID, ">", 0)
	}

	query, args, err := q.Build()
	if err != nil {
		return "", nil, fmt.Errorf("buildServicesInstancesStatusLogQuery: %w", err)
	}
	return query, args, nil
}

func (oDb *DB) GetServicesInstancesStatusLog(ctx context.Context, p ListParams) ([]map[string]any, error) {
	query, args, err := buildServicesInstancesStatusLogQuery(p.Groups, p.IsManager, p.SelectExprs)
	if err != nil {
		return nil, err
	}
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("svcmon_log.svc_id, svcmon_log.node_id, svcmon_log.mon_begin")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getServicesInstancesStatusLog: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}
