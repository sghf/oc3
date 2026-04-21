package cdb

import (
	"context"
	"fmt"

	"github.com/opensvc/oc3/schema"
)

func buildServicesQuery(groups []string, isManager bool, selectExprs []string) (string, []any, error) {
	q := From(schema.TServices).
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
		q = q.Where(schema.ServicesID, ">", 0)
	}

	query, args, err := q.Build()
	if err != nil {
		return "", nil, fmt.Errorf("buildServicesQuery: %w", err)
	}
	return query, args, nil
}

func (oDb *DB) GetServices(ctx context.Context, p ListParams) ([]map[string]any, error) {
	query, args, err := buildServicesQuery(p.Groups, p.IsManager, p.SelectExprs)
	if err != nil {
		return nil, err
	}
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("services.svcname")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getServices: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// GetService fetches a single service by svc_id (UUID) or svcname.
func (oDb *DB) GetService(ctx context.Context, svcID string, p ListParams) ([]map[string]any, error) {
	query, args, err := buildServicesQuery(p.Groups, p.IsManager, p.SelectExprs)
	if err != nil {
		return nil, err
	}
	query += " AND (services.svc_id = ? OR services.svcname = ?)"
	args = append(args, svcID, svcID)
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getService: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}
