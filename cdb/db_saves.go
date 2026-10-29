package cdb

import (
	"context"
	"fmt"

	"github.com/opensvc/oc3/schema"
)

// SaveShare is the size and the number of the backups sharing a key, an
// application, a backup server, a group or a node or service.
type SaveShare struct {
	Key   string
	Label string
	Size  int64
	Count int64
}

// buildSavesQuery is the query of the backups visible to the caller, with their
// node and service: a Manager sees them all, another user those of the
// applications one of their groups is responsible for, as the historical view
// filters them by save_app (q_filter(app_field=db.saves.save_app)).
func buildSavesQuery(groups []string, isManager bool, selectExprs []string, filters []ColumnFilter) *Query {
	q := From(schema.TSaves).
		LeftJoin(schema.TNodes, schema.TServices).
		RawSelect(selectExprs...)
	if !isManager {
		clean := cleanGroups(groups)
		if len(clean) == 0 {
			q = q.WhereRaw("1=0")
		} else {
			q = q.WhereRaw("saves.save_app IN (SELECT apps.app FROM apps"+
				" JOIN apps_responsibles ON apps_responsibles.app_id = apps.id"+
				" JOIN auth_group ON auth_group.id = apps_responsibles.group_id"+
				" WHERE auth_group.role IN ("+Placeholders(len(clean))+"))", stringsToAny(clean)...)
		}
	} else {
		q = q.Where(schema.SavesID, ">", 0)
	}
	return q.WhereFilters(filters)
}

// GetSaves lists the backups visible to the caller, the latest first.
func (oDb *DB) GetSaves(ctx context.Context, p ListParams) ([]map[string]any, error) {
	query, args, err := buildSavesQuery(p.Groups, p.IsManager, p.SelectExprs, p.Filters).Build()
	if err != nil {
		return nil, fmt.Errorf("getSaves: %w", err)
	}
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("saves.save_date DESC, saves.id")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getSaves: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// GetSave returns a backup visible to the caller, none when there is no such
// backup or it is out of reach.
func (oDb *DB) GetSave(ctx context.Context, id int64, p ListParams) ([]map[string]any, error) {
	filters := append([]ColumnFilter{}, p.Filters...)
	filters = append(filters, ColumnFilter{Col: schema.SavesID, Expr: "saves.id = ?", Args: []any{id}})
	p.Filters = filters
	p.Limit = 1
	p.Offset = 0
	return oDb.GetSaves(ctx, p)
}

// saveShareKeys are the keys the backups are summed by, and how each reads.
var saveShareKeys = map[string]struct{ key, label string }{
	"app":    {"COALESCE(saves.save_app, '')", "COALESCE(saves.save_app, '')"},
	"server": {"saves.save_server", "saves.save_server"},
	"group":  {"saves.save_group", "saves.save_group"},
	// A backup of a service counts for it, another for its node, as the
	// historical chart by object does: the key names the column, "svc_id:<id>"
	// or "node_id:<id>", for a client to filter on it.
	"object": {
		"IF(saves.svc_id != '', CONCAT('svc_id:', saves.svc_id), CONCAT('node_id:', saves.node_id))",
		"IF(saves.svc_id != '', COALESCE(services.svcname, saves.svc_id), COALESCE(nodes.nodename, saves.node_id))",
	},
}

// IsSaveShareKey tells the keys SavesSummary sums the backups by.
func IsSaveShareKey(by string) bool {
	_, ok := saveShareKeys[by]
	return ok
}

// SavesSummary sums the size of the backups visible to the caller, and counts
// them, by app, server, group or object (node or service), the filters given
// applied: the historical Saves view draws them as its charts. The largest
// first.
func (oDb *DB) SavesSummary(ctx context.Context, groups []string, isManager bool, filters []ColumnFilter, by string) ([]SaveShare, error) {
	k, ok := saveShareKeys[by]
	if !ok {
		return nil, fmt.Errorf("savesSummary: unknown key %q", by)
	}
	query, args, err := buildSavesQuery(groups, isManager, []string{
		k.key + " AS share_key", "MAX(" + k.label + ") AS share_label",
		"COALESCE(SUM(saves.save_size), 0)", "COUNT(*)",
	}, filters).Build()
	if err != nil {
		return nil, fmt.Errorf("savesSummary: %w", err)
	}
	query += " GROUP BY share_key ORDER BY 3 DESC, share_key"
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("savesSummary: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []SaveShare
	for rows.Next() {
		var s SaveShare
		var key, label *string
		if err := rows.Scan(&key, &label, &s.Size, &s.Count); err != nil {
			return nil, fmt.Errorf("savesSummary: %w", err)
		}
		if key != nil {
			s.Key = *key
		}
		if label != nil {
			s.Label = *label
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
