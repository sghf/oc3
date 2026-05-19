package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/opensvc/oc3/schema"
)

type (
	App struct {
		ID          int64  `json:"id"`
		App         string `json:"app"`
		Updated     string `json:"updated"`
		AppDomain   string `json:"app_domain"`
		AppTeamOps  string `json:"app_team_ops"`
		Description string `json:"description"`
	}

	AuthGroup struct {
		ID          int64  `json:"id"`
		Role        string `json:"role"`
		Privilege   bool   `json:"privilege"`
		Description string `json:"description"`
	}
)

func scanApps(rows *sql.Rows) ([]App, error) {
	apps := make([]App, 0)
	for rows.Next() {
		var (
			item        App
			updated     sql.NullString
			appDomain   sql.NullString
			appTeamOps  sql.NullString
			description sql.NullString
		)
		if err := rows.Scan(&item.ID, &item.App, &updated, &appDomain, &appTeamOps, &description); err != nil {
			return nil, fmt.Errorf("scanApps: %w", err)
		}
		if updated.Valid {
			item.Updated = updated.String
		}
		if appDomain.Valid {
			item.AppDomain = appDomain.String
		}
		if appTeamOps.Valid {
			item.AppTeamOps = appTeamOps.String
		}
		if description.Valid {
			item.Description = description.String
		}
		apps = append(apps, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scanApps rows: %w", err)
	}
	return apps, nil
}

func buildAppsQuery(groups []string, isManager bool, selectExprs []string) (string, []any, error) {
	q := From(schema.TApps).
		Distinct().
		RawSelect(selectExprs...)

	if !isManager {
		cleanGroups := cleanGroups(groups)
		q = q.Via(schema.TAppsResponsibles).
			WhereIn(schema.AuthGroupRole, cleanGroups)
	} else {
		q = q.Where(schema.AppsID, ">", 0)
	}

	query, args, err := q.Build()
	if err != nil {
		return "", nil, fmt.Errorf("buildAppsQuery: %w", err)
	}
	return query, args, nil
}

func buildAppsQueryAll(groups []string, isManager bool) (string, []any, error) {
	return buildAppsQuery(groups, isManager, []string{
		"apps.id", "apps.app",
		"COALESCE(apps.updated, '')", "COALESCE(apps.app_domain, '')",
		"COALESCE(apps.app_team_ops, '')", "COALESCE(apps.description, '')",
	})
}

func (oDb *DB) GetApps(ctx context.Context, p ListParams) ([]map[string]any, error) {
	query, args, err := buildAppsQuery(p.Groups, p.IsManager, p.SelectExprs)
	if err != nil {
		return nil, err
	}
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("apps.app, apps.id")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getApps: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

func (oDb *DB) GetApp(ctx context.Context, appIDOrName string, groups []string, isManager bool) (*App, error) {
	query, args, err := buildAppsQueryAll(groups, isManager)
	if err != nil {
		return nil, err
	}

	if id, err := strconv.ParseInt(appIDOrName, 10, 64); err == nil {
		query += " AND apps.id = ?"
		args = append(args, id)
	} else {
		query += " AND apps.app = ?"
		args = append(args, appIDOrName)
	}
	query += " ORDER BY apps.app, apps.id LIMIT 2"

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getApp: %w", err)
	}
	defer func() { _ = rows.Close() }()

	apps, err := scanApps(rows)
	if err != nil {
		return nil, fmt.Errorf("getApp: %w", err)
	}

	switch len(apps) {
	case 0:
		return nil, nil
	case 1:
		return &apps[0], nil
	default:
		return nil, fmt.Errorf("getApp: multiple apps found for %q", appIDOrName)
	}
}

// AuthGroupIdsForNode is the oc3 implementation of oc2 node_responsibles(node_id):
//
//	q = db.nodes.node_id == node_id
//	q &= db.apps.app == db.nodes.app
//	q &= db.apps_responsibles.app_id == db.apps.id
//	q &= db.auth_group.id == db.apps_responsibles.group_id
//	rows = db(q).select(db.auth_group.id)
//	return [r.id for r in rows]
func (oDb *DB) AuthGroupIdsForNode(ctx context.Context, nodeID string) (groupIds []int64, err error) {
	const query = `
		SELECT auth_group.id FROM auth_group
		JOIN apps_responsibles ON auth_group.id = apps_responsibles.group_id
		JOIN apps ON apps_responsibles.app_id = apps.id
		JOIN nodes ON apps.app = nodes.app
		WHERE nodes.node_id = ?`
	rows, err := oDb.DB.QueryContext(ctx, query, nodeID)
	if err != nil {
		err = fmt.Errorf("authGroupIdsForNode: %w", err)
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var groupID sql.NullInt64
		err = rows.Scan(&groupID)
		if err != nil {
			err = fmt.Errorf("authGroupIdsForNode: %w", err)
			return
		}
		if groupID.Valid {
			groupIds = append(groupIds, groupID.Int64)
		}
	}
	if err = rows.Err(); err != nil {
		return
	}
	return
}

// ResponsibleAppsForNode is the oc3 implementation of oc2 node_responsibles_apps(node_id):
//
//	groups = node_responsibles(node_id)
//	q = db.apps_responsibles.group_id.belongs(groups)
//	q &= db.apps_responsibles.app_id == db.apps.id
//	rows = db(q).select(db.apps.app)
//	return [r.app for r in rows]
func (oDb *DB) ResponsibleAppsForNode(ctx context.Context, nodeID string) (apps []string, err error) {
	var query = `
		SELECT apps.app FROM apps
		JOIN apps_responsibles ON apps.id = apps_responsibles.app_id
		JOIN auth_group ON apps_responsibles.group_id = auth_group.id
		WHERE auth_group.id in (
			SELECT auth_group.id FROM auth_group
			JOIN apps_responsibles ON auth_group.id = apps_responsibles.group_id
			JOIN apps ON apps_responsibles.app_id = apps.id
			JOIN nodes ON apps.app = nodes.app
			WHERE nodes.node_id = ?)`
	rows, err := oDb.DB.QueryContext(ctx, query, nodeID)
	if err != nil {
		err = fmt.Errorf("responsibleAppsForNode: %w", err)
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var app sql.NullString
		err = rows.Scan(&app)
		if err != nil {
			err = fmt.Errorf("responsibleAppsForNode: %w", err)
			return
		}
		if app.Valid {
			apps = append(apps, app.String)
		}
	}
	if err = rows.Err(); err != nil {
		err = fmt.Errorf("responsibleAppsForNode: %w", err)
		return
	}
	return
}

func (oDb *DB) appFromAppName(ctx context.Context, app string) (bool, *App, error) {
	const query = "SELECT id, app FROM apps WHERE app = ?"
	var (
		foundID  int64
		foundApp string
	)
	if app == "" {
		return false, nil, fmt.Errorf("can't find app from empty value")
	}
	err := oDb.DB.QueryRowContext(ctx, query, app).Scan(&foundID, &foundApp)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil, nil
	case err != nil:
		return false, nil, err
	default:
		return true, &App{ID: foundID, App: foundApp}, nil
	}
}

func (oDb *DB) AppExists(ctx context.Context, app string) (bool, error) {
	ok, _, err := oDb.appFromAppName(ctx, app)
	if err != nil {
		return false, fmt.Errorf("appExists: %w", err)
	}
	return ok, nil
}

func (oDb *DB) AppResponsible(ctx context.Context, appIDOrName string, groups []string, isManager bool, nodeID string) (bool, error) {
	if isManager {
		return true, nil
	}

	targetApp, err := oDb.GetApp(ctx, appIDOrName, nil, true)
	if err != nil {
		return false, fmt.Errorf("appResponsible: %w", err)
	}
	if targetApp == nil {
		return false, nil
	}

	if nodeID != "" {
		nodeAppID, ok, err := oDb.AppIDFromNodeID(ctx, nodeID)
		if err != nil {
			return false, fmt.Errorf("appResponsible: %w", err)
		}
		return ok && nodeAppID == targetApp.ID, nil
	}

	visibleApp, err := oDb.GetApp(ctx, appIDOrName, groups, false)
	if err != nil {
		return false, fmt.Errorf("appResponsible: %w", err)
	}
	return visibleApp != nil, nil
}

func (oDb *DB) GetAppResponsibles(ctx context.Context, appIDOrName string, groups []string, isManager bool, limit, offset int) ([]AuthGroup, error) {
	targetApp, err := oDb.GetApp(ctx, appIDOrName, nil, true)
	if err != nil {
		return nil, fmt.Errorf("getAppResponsibles: %w", err)
	}
	if targetApp == nil {
		return nil, nil
	}

	if !isManager {
		visibleApp, err := oDb.GetApp(ctx, appIDOrName, groups, false)
		if err != nil {
			return nil, fmt.Errorf("getAppResponsibles: %w", err)
		}
		if visibleApp == nil {
			return []AuthGroup{}, nil
		}
	}

	query := `
		SELECT auth_group.id, auth_group.role, auth_group.privilege, auth_group.description
		FROM auth_group
		JOIN apps_responsibles ON auth_group.id = apps_responsibles.group_id
		WHERE apps_responsibles.app_id = ?
		ORDER BY auth_group.role, auth_group.id
	`
	args := []any{targetApp.ID}
	query, args = appendLimitOffset(query, args, limit, offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getAppResponsibles: %w", err)
	}
	defer func() { _ = rows.Close() }()

	items := make([]AuthGroup, 0)
	for rows.Next() {
		var (
			item        AuthGroup
			role        sql.NullString
			privilege   sql.NullString
			description sql.NullString
		)
		if err := rows.Scan(&item.ID, &role, &privilege, &description); err != nil {
			return nil, fmt.Errorf("getAppResponsibles scan: %w", err)
		}
		if role.Valid {
			item.Role = role.String
		}
		if privilege.Valid {
			item.Privilege = privilege.String == "T"
		}
		if description.Valid {
			item.Description = description.String
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("getAppResponsibles rows: %w", err)
	}

	return items, nil
}

// GetAppNodes returns nodes belonging to the app name or id
func (oDb *DB) GetAppNodes(ctx context.Context, appIDOrName string, p ListParams) ([]map[string]any, error) {
	targetApp, err := oDb.GetApp(ctx, appIDOrName, nil, true)
	if err != nil {
		return nil, fmt.Errorf("GetAppNodes: %w", err)
	}
	if targetApp == nil {
		return nil, nil
	}

	if !p.IsManager {
		visibleApp, err := oDb.GetApp(ctx, appIDOrName, p.Groups, false)
		if err != nil {
			return nil, fmt.Errorf("GetAppNodes: %w", err)
		}
		if visibleApp == nil {
			return []map[string]any{}, nil
		}
	}

	query, args, err := buildNodesQuery(p.Groups, p.IsManager, p.SelectExprs)
	if err != nil {
		return nil, err
	}
	query += " AND nodes.app = ?"
	args = append(args, targetApp.App)
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("nodes.nodename")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("GetAppNodes: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

func (oDb *DB) GetAppPublications(ctx context.Context, appIDOrName string, groups []string, isManager bool, limit, offset int) ([]AuthGroup, error) {
	targetApp, err := oDb.GetApp(ctx, appIDOrName, nil, true)
	if err != nil {
		return nil, fmt.Errorf("getAppPublications: %w", err)
	}
	if targetApp == nil {
		return nil, nil
	}

	if !isManager {
		visibleApp, err := oDb.GetApp(ctx, appIDOrName, groups, false)
		if err != nil {
			return nil, fmt.Errorf("getAppPublications: %w", err)
		}
		if visibleApp == nil {
			return []AuthGroup{}, nil
		}
	}

	query := `
		SELECT auth_group.id, auth_group.role, auth_group.privilege, auth_group.description
		FROM auth_group
		JOIN apps_publications ON auth_group.id = apps_publications.group_id
		WHERE apps_publications.app_id = ?
		ORDER BY auth_group.role, auth_group.id
	`
	args := []any{targetApp.ID}
	query, args = appendLimitOffset(query, args, limit, offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getAppPublications: %w", err)
	}
	defer func() { _ = rows.Close() }()

	items := make([]AuthGroup, 0)
	for rows.Next() {
		var (
			item        AuthGroup
			role        sql.NullString
			privilege   sql.NullString
			description sql.NullString
		)
		if err := rows.Scan(&item.ID, &role, &privilege, &description); err != nil {
			return nil, fmt.Errorf("getAppPublications scan: %w", err)
		}
		if role.Valid {
			item.Role = role.String
		}
		if privilege.Valid {
			item.Privilege = privilege.String == "T"
		}
		if description.Valid {
			item.Description = description.String
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("getAppPublications rows: %w", err)
	}

	return items, nil
}

func (oDb *DB) isAppAllowedForNodeID(ctx context.Context, nodeID, app string) (bool, error) {
	// TODO: apps_responsibles PRIMARY KEY (app_id, group_id)
	const query = "" +
		"SELECT count(*) FROM (" +
		"  SELECT COUNT(`t`.`group_id`) AS `c`" +
		"  FROM (" +
		"      SELECT `ar`.`group_id` FROM `nodes` `n`, `apps` `a`, `apps_responsibles` `ar`" +
		"      WHERE `n`.`app` = `a`.`app` AND `ar`.`app_id` = `a`.`id` AND `n`.`node_id` = ?" +
		"    UNION ALL" +
		"      SELECT `ar`.`group_id` FROM `apps` `a`, `apps_responsibles` `ar`" +
		"      WHERE `ar`.`app_id` = `a`.`id` AND `a`.`app` = ?" +
		"   ) AS `t` GROUP BY `t`.`group_id`) `u`" +
		"WHERE `u`.`c` = 2"
	var found int64
	err := oDb.DB.QueryRowContext(ctx, query, nodeID, app).Scan(&found)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, err
	default:
		return found > 0, nil
	}
}

func (oDb *DB) AppFromNodeAndCandidateApp(ctx context.Context, candidateApp string, node *DBNode) (string, error) {
	app := candidateApp
	if candidateApp == "" {
		app = node.App
	} else if ok, err := oDb.isAppAllowedForNodeID(ctx, node.NodeID, candidateApp); err != nil {
		return "", fmt.Errorf("can't detect if app %s is allowed: %w", candidateApp, err)
	} else if !ok {
		app = node.App
	}
	if ok, _, err := oDb.appFromAppName(ctx, app); err != nil {
		return "", fmt.Errorf("can't verify guessed app %s: %w", app, err)
	} else if !ok {
		return "", fmt.Errorf("can't verify guessed app %s: app not found", app)
	}
	return app, nil
}

// AppIDFromNodeID retrieves the application ID associated with a given node ID from the database.
// Returns the app ID, a boolean indicating if the app exists, and an error if applicable.
// If the node ID is empty, it returns an error.
func (oDb *DB) AppIDFromNodeID(ctx context.Context, nodeID string) (int64, bool, error) {
	const query = "SELECT `apps`.`id` FROM `apps` JOIN `nodes` ON `apps`.`app` = `nodes`.`app` WHERE `nodes`.`node_id`  = ? LIMIT 1"
	var (
		appID sql.NullInt64
	)
	if nodeID == "" {
		return appID.Int64, false, fmt.Errorf("can't find app from empty node id value")
	}
	err := oDb.DB.QueryRowContext(ctx, query, nodeID).Scan(&appID)
	return checkRow(err, appID.Valid, appID.Int64)
}

// appIDFromNodeID retrieves the application ID associated with a given node ID from the database.
// Returns the app ID, a boolean indicating if the app exists, and an error if applicable.
// If the node ID is empty, it returns an error.
func (oDb *DB) AppIDFromObjectID(ctx context.Context, objectID string) (int64, bool, error) {
	const query = "SELECT `apps`.`id` FROM `apps` JOIN `services` ON `apps`.`app` = `services`.`svc_app` WHERE `services`.`svc_id`  = ? LIMIT 1"
	var (
		appID sql.NullInt64
	)
	if objectID == "" {
		return appID.Int64, false, fmt.Errorf("can't find app from empty object id value")
	}
	err := oDb.DB.QueryRowContext(ctx, query, objectID).Scan(&appID)
	return checkRow(err, appID.Valid, appID.Int64)
}

// AppIDFromObjectOrNodeIDs retrieves the application ID associated with the given objectID or nodeID in the database.
// It first attempts to find the application ID based on the objectID. If unsuccessful, it falls back to using the nodeID.
// Returns the application ID, a boolean indicating success, and an error if any issue occurs during retrieval.
//
// It is a port from oc2 `get_preferred_app`
//
//	    def get_preferred_app(node_id, svc_id):
//		       if svc_id is None:
//		           q = db.nodes.node_id == node_id
//		           q &= db.apps.app == db.nodes.app
//		           return db(q).select(db.apps.ALL).first()
//		       else:
//		           q = db.services.svc_id == svc_id
//		           q &= db.services.svc_app == db.apps.app
//		           row = db(q).select(db.apps.ALL).first()
//		           if row is None:
//		               q = db.nodes.node_id == node_id
//		               q &= db.apps.app == db.nodes.app
//		               return db(q).select(db.apps.ALL).first()
//		           return row
func (oDb *DB) AppIDFromObjectOrNodeIDs(ctx context.Context, nodeID, objectID string) (int64, bool, error) {
	if objectID != "" {
		if found, ok, err := oDb.AppIDFromObjectID(ctx, objectID); err == nil {
			// abort on error
			return found, ok, err
		} else if ok {
			return found, ok, err
		}
	}
	return oDb.AppIDFromNodeID(ctx, nodeID)
}

// AppQuotaExceeded returns true if the user has reached their app creation quota.
// A quota of 0 or NULL means unlimited.
func (oDb *DB) AppQuotaExceeded(ctx context.Context, userID int64) (bool, error) {
	var quota sql.NullInt64
	err := oDb.DB.QueryRowContext(ctx,
		"SELECT quota_app FROM auth_user WHERE id = ?", userID,
	).Scan(&quota)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("appQuotaExceeded: %w", err)
	case !quota.Valid || quota.Int64 == 0:
		return false, nil
	}

	var count int64
	err = oDb.DB.QueryRowContext(ctx,
		`SELECT COUNT(DISTINCT apps_responsibles.app_id)
		 FROM apps_responsibles
		 JOIN auth_membership ON apps_responsibles.group_id = auth_membership.group_id
		 WHERE auth_membership.user_id = ?`, userID,
	).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("appQuotaExceeded count: %w", err)
	}
	return count >= quota.Int64, nil
}

func (oDb *DB) InsertApp(ctx context.Context, app, description, appDomain, appTeamOps string) (*App, error) {
	const query = `INSERT INTO apps (app, description, app_domain, app_team_ops) VALUES (?, ?, ?, ?)`
	result, err := oDb.DB.ExecContext(ctx, query, app,
		sql.NullString{String: description, Valid: description != ""},
		sql.NullString{String: appDomain, Valid: appDomain != ""},
		sql.NullString{String: appTeamOps, Valid: appTeamOps != ""},
	)
	if err != nil {
		return nil, fmt.Errorf("insertApp: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return nil, fmt.Errorf("insertApp lastInsertId: %w", err)
	}
	oDb.SetChange("apps")
	return &App{ID: id, App: app, Description: description, AppDomain: appDomain, AppTeamOps: appTeamOps}, nil
}

type UpdateAppFields struct {
	App         *string
	Description *string
	AppDomain   *string
	AppTeamOps  *string
}

func (oDb *DB) UpdateApp(ctx context.Context, appID int64, fields UpdateAppFields) error {
	setClauses := []string{}
	args := []any{}
	if fields.App != nil {
		setClauses = append(setClauses, "app = ?")
		args = append(args, *fields.App)
	}
	if fields.Description != nil {
		setClauses = append(setClauses, "description = ?")
		args = append(args, sql.NullString{String: *fields.Description, Valid: true})
	}
	if fields.AppDomain != nil {
		setClauses = append(setClauses, "app_domain = ?")
		args = append(args, sql.NullString{String: *fields.AppDomain, Valid: true})
	}
	if fields.AppTeamOps != nil {
		setClauses = append(setClauses, "app_team_ops = ?")
		args = append(args, sql.NullString{String: *fields.AppTeamOps, Valid: true})
	}
	if len(setClauses) == 0 {
		return nil
	}
	query := "UPDATE apps SET " + strings.Join(setClauses, ", ") + " WHERE id = ?"
	args = append(args, appID)
	if _, err := oDb.DB.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("updateApp: %w", err)
	}
	oDb.SetChange("apps")
	return nil
}

func (oDb *DB) UpdateNodesApp(ctx context.Context, oldApp, newApp string) error {
	const query = `UPDATE nodes SET app = ? WHERE app = ?`
	if _, err := oDb.DB.ExecContext(ctx, query, newApp, oldApp); err != nil {
		return fmt.Errorf("updateNodesApp: %w", err)
	}
	return nil
}

func (oDb *DB) UpdateServicesApp(ctx context.Context, oldApp, newApp string) error {
	const query = `UPDATE services SET svc_app = ? WHERE svc_app = ?`
	if _, err := oDb.DB.ExecContext(ctx, query, newApp, oldApp); err != nil {
		return fmt.Errorf("updateServicesApp: %w", err)
	}
	return nil
}

func (oDb *DB) InsertAppResponsible(ctx context.Context, appID, groupID int64) error {
	const query = `INSERT INTO apps_responsibles (app_id, group_id) VALUES (?, ?)`
	if _, err := oDb.DB.ExecContext(ctx, query, appID, groupID); err != nil {
		return fmt.Errorf("insertAppResponsible: %w", err)
	}
	oDb.SetChange("apps_responsibles")
	return nil
}

func (oDb *DB) InsertAppPublication(ctx context.Context, appID, groupID int64) error {
	const query = `INSERT INTO apps_publications (app_id, group_id) VALUES (?, ?)`
	if _, err := oDb.DB.ExecContext(ctx, query, appID, groupID); err != nil {
		return fmt.Errorf("insertAppPublication: %w", err)
	}
	oDb.SetChange("apps_publications")
	return nil
}

func (oDb *DB) AppUsageCounts(ctx context.Context, app string) (nodesCount, servicesCount int64, err error) {
	const nodeQuery = `SELECT COUNT(*) FROM nodes WHERE app = ?`
	if err = oDb.DB.QueryRowContext(ctx, nodeQuery, app).Scan(&nodesCount); err != nil {
		err = fmt.Errorf("appUsageCounts nodes: %w", err)
		return
	}

	const serviceQuery = `SELECT COUNT(*) FROM services WHERE svc_app = ?`
	if err = oDb.DB.QueryRowContext(ctx, serviceQuery, app).Scan(&servicesCount); err != nil {
		err = fmt.Errorf("appUsageCounts services: %w", err)
		return
	}

	return
}

func (oDb *DB) DeleteApp(ctx context.Context, appID int64) error {
	const query = `DELETE FROM apps WHERE id = ?`
	if _, err := oDb.DB.ExecContext(ctx, query, appID); err != nil {
		return fmt.Errorf("deleteApp: %w", err)
	}
	oDb.SetChange("apps")
	return nil
}

func (oDb *DB) DeleteAppResponsibles(ctx context.Context, appID int64) error {
	const query = `DELETE FROM apps_responsibles WHERE app_id = ?`
	if _, err := oDb.DB.ExecContext(ctx, query, appID); err != nil {
		return fmt.Errorf("deleteAppResponsibles: %w", err)
	}
	oDb.SetChange("apps_responsibles")
	return nil
}

func (oDb *DB) DeleteAppPublications(ctx context.Context, appID int64) error {
	const query = `DELETE FROM apps_publications WHERE app_id = ?`
	if _, err := oDb.DB.ExecContext(ctx, query, appID); err != nil {
		return fmt.Errorf("deleteAppPublications: %w", err)
	}
	oDb.SetChange("apps_publications")
	return nil
}

func (oDb *DB) AppsWithoutResponsible(ctx context.Context) (apps []string, err error) {
	const query = `SELECT apps.app
	  FROM apps
	  LEFT JOIN apps_responsibles
	  ON apps.id=apps_responsibles.app_id
	  WHERE apps_responsibles.group_id IS NULL`
	rows, err := oDb.DB.QueryContext(ctx, query)
	if err != nil {
		err = fmt.Errorf("AppsWithoutResponsible: %w", err)
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var app sql.NullString
		err = rows.Scan(&app)
		if err != nil {
			err = fmt.Errorf("AppsWithoutResponsible: %w", err)
			return
		}
		if app.Valid {
			apps = append(apps, app.String)
		}
	}
	if err = rows.Err(); err != nil {
		return
	}
	return
}
