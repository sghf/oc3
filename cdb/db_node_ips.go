package cdb

import (
	"context"
	"fmt"
	"strings"
)

func buildNodeIpsQuery(nodeID string, p ListParams) (string, []any, error) {
	if len(p.SelectExprs) == 0 {
		return "", nil, fmt.Errorf("buildNodeIpsQuery: no columns selected")
	}

	sb := &strings.Builder{}
	fmt.Fprintf(sb, "SELECT %s\nFROM v_nodenetworks", strings.Join(p.SelectExprs, ", "))

	args := []any{nodeID}
	sb.WriteString("\nWHERE node_id = ?")

	if !p.IsManager {
		cleanGroups := cleanGroups(p.Groups)
		if len(cleanGroups) == 0 {
			sb.WriteString("\n  AND 1=0")
		} else {
			args = append(args, func() []any {
				a := make([]any, len(cleanGroups))
				for i, g := range cleanGroups {
					a[i] = g
				}
				return a
			}()...)
			fmt.Fprintf(sb,
				"\n  AND node_id IN ("+
					"SELECT n.node_id FROM nodes n"+
					" JOIN apps a ON n.app = a.app"+
					" JOIN apps_responsibles ar ON ar.app_id = a.id"+
					" JOIN auth_group ag ON ag.id = ar.group_id"+
					" WHERE ag.role IN (%s)"+
					")",
				Placeholders(len(cleanGroups)),
			)
		}
	}

	return sb.String(), args, nil
}

func (oDb *DB) GetNodeIps(ctx context.Context, nodeID string, p ListParams) ([]map[string]any, error) {
	query, args, err := buildNodeIpsQuery(nodeID, p)
	if err != nil {
		return nil, err
	}
	query += " " + p.OrderByClause("addr")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getNodeIps: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}
