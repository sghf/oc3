package cdb

import (
	"context"
	"fmt"
	"strings"
)

// GetUserNodes lists the nodes a user visible to the caller is responsible
// for, through the groups they are member of, as rest_get_user_nodes
// (init/models/rest/api_users.py:220) does: nodes.team_responsible names one
// of their groups. The user is designated and visible as for GetUserGroups.
func (oDb *DB) GetUserNodes(ctx context.Context, idOrEmail string, p ListParams) ([]map[string]any, error) {
	return oDb.userList(ctx, "getUserNodes", idOrEmail, p,
		" FROM nodes"+
			" JOIN auth_group ON auth_group.role = nodes.team_responsible"+
			" JOIN auth_membership ON auth_membership.group_id = auth_group.id"+
			" JOIN auth_user ON auth_user.id = auth_membership.user_id",
		"", "nodes.nodename, nodes.node_id")
}

// GetUserServices lists the services a user visible to the caller is
// responsible for, as rest_get_user_services (init/models/rest/api_users.py:252)
// does: the services of the applications one of their groups is responsible
// for, once each though several of their groups are.
func (oDb *DB) GetUserServices(ctx context.Context, idOrEmail string, p ListParams) ([]map[string]any, error) {
	return oDb.userList(ctx, "getUserServices", idOrEmail, p,
		" FROM services"+
			" JOIN apps ON apps.app = services.svc_app"+
			" JOIN apps_responsibles ON apps_responsibles.app_id = apps.id"+
			" JOIN auth_membership ON auth_membership.group_id = apps_responsibles.group_id"+
			" JOIN auth_user ON auth_user.id = auth_membership.user_id",
		"GROUP BY services.id", "services.svcname")
}

// GetUserPrimaryGroup lists the primary group of a user visible to the
// caller, none or one, as rest_get_user_primary_group
// (init/models/rest/api_users.py:753) does.
func (oDb *DB) GetUserPrimaryGroup(ctx context.Context, idOrEmail string, p ListParams) ([]map[string]any, error) {
	return oDb.userList(ctx, "getUserPrimaryGroup", idOrEmail, p,
		" FROM auth_group"+
			" JOIN auth_membership ON auth_membership.group_id = auth_group.id AND auth_membership.primary_group = 'T'"+
			" JOIN auth_user ON auth_user.id = auth_membership.user_id",
		"", "auth_group.role")
}

// userList runs a list about one user, the rows of from joined to auth_user:
// the user designated by idOrEmail, among those the caller may see
// (usersAuthClause), with the filters, grouping, order and page of p.
// groupBy applies when p asks for none.
func (oDb *DB) userList(ctx context.Context, name, idOrEmail string, p ListParams, from, groupBy, orderBy string) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("%s: no select expressions", name)
	}
	identClause, identArgs, ok := userIdentClause(idOrEmail, p.UserID)
	if !ok {
		return nil, nil
	}
	authClause, args := usersAuthClause(p)
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") + from +
		" WHERE " + authClause + " AND " + identClause
	args = append(args, identArgs...)
	filterConds, filterArgs := p.FilterConditions()
	for _, cond := range filterConds {
		query += " AND " + cond
	}
	args = append(args, filterArgs...)
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	} else if groupBy != "" {
		query += " " + groupBy
	}
	query += " " + p.OrderByClause(orderBy)
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}
