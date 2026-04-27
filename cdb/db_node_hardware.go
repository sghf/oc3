package cdb

import (
	"context"
	"fmt"

	"github.com/opensvc/oc3/schema"
)

func buildNodeHardwareQuery(groups []string, isManager bool, selectExprs []string) (string, []any, error) {
	q := From(schema.TNodeHW).
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
				"node_hw.node_id IN ("+
					"SELECT n.node_id FROM nodes n"+
					" JOIN apps a ON n.app = a.app"+
					" JOIN apps_responsibles ar ON ar.app_id = a.id"+
					" JOIN auth_group ag ON ag.id = ar.group_id"+
					" WHERE ag.role IN ("+Placeholders(len(cleanGroups))+")"+
					")",
				args...,
			)
		}
	} else {
		q = q.Where(schema.NodeHWID, ">", 0)
	}

	query, args, err := q.Build()
	if err != nil {
		return "", nil, fmt.Errorf("buildNodeHardwareQuery: %w", err)
	}
	return query, args, nil
}

func (oDb *DB) GetNodeHardware(ctx context.Context, nodeID string, p ListParams) ([]map[string]any, error) {
	query, args, err := buildNodeHardwareQuery(p.Groups, p.IsManager, p.SelectExprs)
	if err != nil {
		return nil, err
	}
	query += " AND node_hw.node_id = ?"
	args = append(args, nodeID)
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("node_hw.hw_type, node_hw.hw_path")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getNodeHardware: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}
