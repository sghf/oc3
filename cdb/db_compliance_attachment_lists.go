package cdb

import (
	"context"
	"fmt"

	"github.com/opensvc/oc3/schema"
)

// whereAppResponsible keeps, for a caller other than a Manager, the rows whose app
// (appExpr) one of their groups is responsible for, as the node and service lists;
// for a Manager, every row of idCol.
func whereAppResponsible(q *Query, appExpr string, idCol *schema.Col, groups []string, isManager bool) *Query {
	if isManager {
		return q.Where(idCol, ">", 0)
	}
	clean := cleanGroups(groups)
	if len(clean) == 0 {
		return q.WhereRaw("1=0")
	}
	args := make([]any, len(clean))
	for i, g := range clean {
		args[i] = g
	}
	return q.WhereRaw(appExpr+" IN ("+
		"SELECT a.app FROM apps a"+
		" JOIN apps_responsibles ar ON ar.app_id = a.id"+
		" JOIN auth_group ag ON ag.id = ar.group_id"+
		" WHERE ag.role IN ("+Placeholders(len(clean))+")"+
		")", args...)
}

// compAttachmentTables are the tables of the attachments of a kind of compliance
// object, to nodes and to services, and the object table: the tables of the
// schema, by which the query builder knows a table is joined already, a column
// filter on it included.
func compAttachmentTables(k CompKind) (nodes, services, object *schema.Table) {
	if k.Table == CompModulesetKind.Table {
		return schema.TCompNodeModuleset, schema.TCompModulesetsServices, schema.TCompModuleset
	}
	return schema.TCompRulesetsNodes, schema.TCompRulesetsServices, schema.TCompRulesets
}

// GetCompNodeAttachments lists the nodes the caller may see with the modulesets,
// or the rulesets, attached to them: one row per node and object, and one with no
// object for a node that has none, as the historical collector's "node modulesets"
// and "node rulesets" tables. The session filterset and the column filters apply.
func (oDb *DB) GetCompNodeAttachments(ctx context.Context, k CompKind, p ListParams) ([]map[string]any, error) {
	attach, _, object := compAttachmentTables(k)
	q := From(schema.TNodes).
		LeftJoin(schema.TClusters).
		LeftJoinOn(attach, attach.Name+".node_id = nodes.node_id").
		LeftJoinOn(object, object.Name+".id = "+attach.Name+"."+k.FK).
		RawSelect(p.SelectExprs...)
	q = whereAppResponsible(q, "nodes.app", schema.NodesID, p.Groups, p.IsManager)
	q = q.WhereFilters(p.Filters)
	query, args, err := q.Build()
	if err != nil {
		return nil, fmt.Errorf("GetCompNodeAttachments: %w", err)
	}
	return oDb.compAttachmentRows(ctx, query, args, "nodes.nodename, "+object.Name+"."+k.NameCol, p)
}

// GetCompServiceAttachments lists the services the caller may see with the
// modulesets, or the rulesets, attached to them or to their encapsulated part
// (slave): one row per service, object and part, and one with no object for a
// service that has none, as the historical "service modulesets" and "service
// rulesets" tables.
func (oDb *DB) GetCompServiceAttachments(ctx context.Context, k CompKind, p ListParams) ([]map[string]any, error) {
	_, attach, object := compAttachmentTables(k)
	q := From(schema.TServices).
		LeftJoinOn(attach, attach.Name+".svc_id = services.svc_id").
		LeftJoinOn(object, object.Name+".id = "+attach.Name+"."+k.FK).
		RawSelect(p.SelectExprs...)
	q = whereAppResponsible(q, "services.svc_app", schema.ServicesID, p.Groups, p.IsManager)
	q = q.WhereFilters(p.Filters)
	query, args, err := q.Build()
	if err != nil {
		return nil, fmt.Errorf("GetCompServiceAttachments: %w", err)
	}
	return oDb.compAttachmentRows(ctx, query, args,
		"services.svcname, "+attach.Name+".slave, "+object.Name+"."+k.NameCol, p)
}

// compAttachmentRows runs a list query of attachments with the grouping, sort and
// page of p, by defaultOrder when no sort is asked.
func (oDb *DB) compAttachmentRows(ctx context.Context, query string, args []any, defaultOrder string, p ListParams) ([]map[string]any, error) {
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause(defaultOrder)
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("compliance attachments: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}
