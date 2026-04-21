package cdb

import (
	"context"
	"fmt"

	"github.com/opensvc/oc3/schema"
)

func buildDisksQuery(groups []string, isManager bool, selectExprs []string) (string, []any, error) {
	q := From(schema.TDiskinfo).
		LeftJoin(schema.TSvcdisks, schema.TNodes, schema.TServices, schema.TApps).
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
				"diskinfo.disk_id IN ("+
					"SELECT sd.disk_id FROM svcdisks sd"+
					" JOIN nodes n ON sd.node_id = n.node_id"+
					" JOIN apps a ON n.app = a.app"+
					" JOIN apps_responsibles ar ON ar.app_id = a.id"+
					" JOIN auth_group ag ON ag.id = ar.group_id"+
					" WHERE ag.role IN ("+Placeholders(len(cleanGroups))+")"+
					")",
				args...,
			)
		}
	} else {
		q = q.Where(schema.DiskinfoID, ">", 0)
	}

	query, args, err := q.Build()
	if err != nil {
		return "", nil, fmt.Errorf("buildDisksQuery: %w", err)
	}
	return query, args, nil
}

func (oDb *DB) GetDisk(ctx context.Context, diskID string, p ListParams) ([]map[string]any, error) {
	query, args, err := buildDisksQuery(p.Groups, p.IsManager, p.SelectExprs)
	if err != nil {
		return nil, err
	}
	query += " AND diskinfo.disk_id = ?"
	args = append(args, diskID)
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("diskinfo.disk_id, diskinfo.disk_group")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getDisk: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

func (oDb *DB) GetNodeDisks(ctx context.Context, nodeID string, p ListParams) ([]map[string]any, error) {
	query, args, err := buildDisksQuery(p.Groups, p.IsManager, p.SelectExprs)
	if err != nil {
		return nil, err
	}
	query += " AND svcdisks.node_id = ?"
	args = append(args, nodeID)
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("diskinfo.disk_id, diskinfo.disk_group")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getNodeDisks: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

func (oDb *DB) GetDisks(ctx context.Context, p ListParams) ([]map[string]any, error) {
	query, args, err := buildDisksQuery(p.Groups, p.IsManager, p.SelectExprs)
	if err != nil {
		return nil, err
	}
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("diskinfo.disk_id, diskinfo.disk_group")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getDisks: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}
