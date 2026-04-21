package cdb

import (
	"context"
	"fmt"
	"strings"

	"github.com/opensvc/oc3/schema"
)

func buildNodeInterfacesQuery(nodeID string, p ListParams) (string, []any, error) {
	q := From(schema.TNodeIP).
		RawSelect(p.SelectExprs...).
		Where(schema.NodeIPNodeID, "=", nodeID)

	// LEFT JOIN nodes if needed.
	for _, prop := range p.Props {
		if strings.HasPrefix(prop, "nodes.") {
			q = q.LeftJoin(schema.TNodes)
			break
		}
	}

	if !p.IsManager {
		cleanGroups := cleanGroups(p.Groups)
		if len(cleanGroups) == 0 {
			q = q.WhereRaw("1=0")
		} else {
			args := make([]any, len(cleanGroups))
			for i, g := range cleanGroups {
				args[i] = g
			}
			q = q.WhereRaw(
				"node_ip.node_id IN ("+
					"SELECT n.node_id FROM nodes n"+
					" JOIN apps a ON n.app = a.app"+
					" JOIN apps_responsibles ar ON ar.app_id = a.id"+
					" JOIN auth_group ag ON ag.id = ar.group_id"+
					" WHERE ag.role IN ("+Placeholders(len(cleanGroups))+")"+
					")",
				args...,
			)
		}
	}

	query, args, err := q.Build()
	if err != nil {
		return "", nil, fmt.Errorf("buildNodeInterfacesQuery: %w", err)
	}
	return query, args, nil
}

func (oDb *DB) GetNodeInterfaces(ctx context.Context, nodeID string, p ListParams) ([]map[string]any, error) {
	query, args, err := buildNodeInterfacesQuery(nodeID, p)
	if err != nil {
		return nil, err
	}
	query += " " + p.GroupByClause("node_ip.intf") + " " + p.OrderByClause("node_ip.intf")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getNodeInterfaces: %w", err)
	}
	defer func() { _ = rows.Close() }()

	// Use nested output when cross-table props (containing a dot) are requested.
	for _, prop := range p.Props {
		if strings.Contains(prop, ".") {
			return scanRowsToNestedMaps(rows, p.Props, "node_ip")
		}
	}
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}
