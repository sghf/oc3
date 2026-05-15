package cdb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

type Moduleset struct {
	ID      int    `json:"id"`
	Name    string `json:"modset_name"`
	Author  string `json:"modset_author"`
	Updated string `json:"modset_updated"`
}

type Ruleset struct {
	ID     int    `json:"id"`
	Name   string `json:"ruleset_name"`
	Public bool   `json:"ruleset_public"`
	Type   string `json:"ruleset_type"`
}

func (oDb *DB) PurgeCompModulesetsNodes(ctx context.Context) error {
	var query = `DELETE
		FROM comp_node_moduleset
		WHERE
		  node_id NOT IN (
                    SELECT DISTINCT node_id FROM nodes
                   )`
	if count, err := oDb.execCountContext(ctx, query); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("comp_node_moduleset")
	}
	return nil
}

func (oDb *DB) PurgeCompRulesetsNodes(ctx context.Context) error {
	var query = `DELETE
		FROM comp_rulesets_nodes
		WHERE
		  node_id NOT IN (
                    SELECT DISTINCT node_id FROM nodes
                   )`
	if count, err := oDb.execCountContext(ctx, query); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("comp_rulesets_nodes")
	}
	return nil
}

func (oDb *DB) PurgeCompRulesetsServices(ctx context.Context) error {
	var query = `DELETE
		FROM comp_rulesets_services
		WHERE
		  svc_id NOT IN (
                    SELECT DISTINCT svc_id FROM svcmon
                   )`
	if count, err := oDb.execCountContext(ctx, query); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("comp_rulesets_services")
	}
	return nil
}

func (oDb *DB) PurgeCompModulesetsServices(ctx context.Context) error {
	var query = `DELETE
		FROM comp_modulesets_services
		WHERE
		  svc_id NOT IN (
                    SELECT DISTINCT svc_id FROM svcmon
                   )`
	if count, err := oDb.execCountContext(ctx, query); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("comp_modulesets_services")
	}
	return nil
}

// purge entries older than 30 days
func (oDb *DB) PurgeCompStatusOutdated(ctx context.Context) error {
	var query = `DELETE
		FROM comp_status
		WHERE
		  run_date < DATE_SUB(NOW(), INTERVAL 31 DAY)`
	if count, err := oDb.execCountContext(ctx, query); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("comp_status")
	}
	return nil
}

// purge svc compliance status for deleted services
func (oDb *DB) PurgeCompStatusSvcOrphans(ctx context.Context) error {
	var query = `DELETE FROM comp_status
             WHERE
               svc_id != "" and
               svc_id NOT IN (
                 SELECT DISTINCT svc_id FROM svcmon
	       )`
	if count, err := oDb.execCountContext(ctx, query); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("comp_status")
	}
	return nil
}

// purge node compliance status for deleted nodes
func (oDb *DB) PurgeCompStatusNodeOrphans(ctx context.Context) error {
	var query = `DELETE FROM comp_status
             WHERE
               node_id != "" and
               node_id NOT IN (
                 SELECT DISTINCT node_id FROM nodes
	       )`
	if count, err := oDb.execCountContext(ctx, query); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("comp_status")
	}
	return nil
}

// purge compliance status older than 7 days for modules in no moduleset, ie not schedulable
func (oDb *DB) PurgeCompStatusModulesetOrphans(ctx context.Context) error {
	var query = `DELETE FROM comp_status
             WHERE
	       run_date < DATE_SUB(NOW(), INTERVAL 7 DAY) AND
               run_module NOT IN (
                 SELECT modset_mod_name FROM comp_moduleset_modules
	       )`
	if count, err := oDb.execCountContext(ctx, query); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("comp_status")
	}
	return nil
}

// purge node compliance status older than 7 days for unattached modules
func (oDb *DB) PurgeCompStatusNodeUnattached(ctx context.Context) error {
	var query = `DELETE FROM comp_status
             WHERE
	       run_date < DATE_SUB(NOW(), INTERVAL 7 DAY) AND
	       svc_id = "" AND
               run_module NOT IN (
                 SELECT modset_mod_name
                 FROM comp_moduleset_modules
                 WHERE modset_id IN (
                   SELECT modset_id
                   FROM comp_node_moduleset
                 )
	       )`
	if count, err := oDb.execCountContext(ctx, query); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("comp_status")
	}
	return nil
}

// purge svc compliance status older than 7 days for unattached modules
func (oDb *DB) PurgeCompStatusSvcUnattached(ctx context.Context) error {
	var query = `DELETE FROM comp_status
             WHERE
	       run_date < DATE_SUB(NOW(), INTERVAL 7 DAY) AND
	       svc_id = "" AND
               run_module NOT IN (
                 SELECT modset_mod_name
                 FROM comp_moduleset_modules
                 WHERE modset_id IN (
                   SELECT modset_id
                   FROM comp_modulesets_services
                 )
	       )`
	if count, err := oDb.execCountContext(ctx, query); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("comp_status")
	}
	return nil
}

// returns the moduleset name for a given moduleset ID.
func (oDb *DB) CompModulesetName(ctx context.Context, modulesetID string) (string, error) {
	const query = "SELECT modset_name FROM comp_moduleset WHERE id = ?"
	var modulesetName string

	err := oDb.DB.QueryRowContext(ctx, query, modulesetID).Scan(&modulesetName)
	if err != nil {
		return "", fmt.Errorf("compModulesetName: %w", err)
	}
	return modulesetName, nil
}

// check if a moduleset is already attached to a node
func (oDb *DB) CompModulesetAttached(ctx context.Context, nodeID, modulesetID string) (bool, error) {
	const query = "SELECT EXISTS(SELECT 1 FROM comp_node_moduleset WHERE node_id = ? AND modset_id = ?)"
	var exists bool

	err := oDb.DB.QueryRowContext(ctx, query, nodeID, modulesetID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("compModulesetAttached: %w", err)
	}
	return exists, nil
}

// detach moduleset(s) from a node
func (oDb *DB) CompModulesetDetachNode(ctx context.Context, nodeID string, modulesetIDs []string) (int64, error) {
	if len(modulesetIDs) == 0 {
		return 0, nil
	}

	query := "DELETE FROM comp_node_moduleset WHERE node_id = ? AND modset_id IN ("
	args := []any{nodeID}

	for i, id := range modulesetIDs {
		if i > 0 {
			query += ","
		}
		query += "?"
		args = append(args, id)
	}
	query += ")"

	result, err := oDb.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("compModulesetDetachNode: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("compModulesetDetachNode rowsAffected: %w", err)
	}

	if rows > 0 {
		oDb.SetChange("comp_node_moduleset")
		oDb.Session.NotifyChanges(ctx)
	}

	return rows, nil
}

// returns the ruleset name for a given ruleset ID.
func (oDb *DB) CompRulesetName(ctx context.Context, rulesetID string) (string, error) {
	const query = "SELECT ruleset_name FROM comp_rulesets WHERE id = ?"
	var rulesetName string

	err := oDb.DB.QueryRowContext(ctx, query, rulesetID).Scan(&rulesetName)
	if err != nil {
		return "", fmt.Errorf("compRulesetName: %w", err)
	}
	return rulesetName, nil
}

// check if a ruleset is already attached to a node
func (oDb *DB) CompRulesetAttached(ctx context.Context, nodeID, rulesetID string) (bool, error) {
	const query = "SELECT EXISTS(SELECT 1 FROM comp_rulesets_nodes WHERE node_id = ? AND ruleset_id = ?)"
	var exists bool

	err := oDb.DB.QueryRowContext(ctx, query, nodeID, rulesetID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("compRulesetAttached: %w", err)
	}
	return exists, nil
}

// detach ruleset(s) from a node
func (oDb *DB) CompRulesetDetachNode(ctx context.Context, nodeID string, rulesetIDs []string) (int64, error) {
	if len(rulesetIDs) == 0 {
		return 0, nil
	}

	query := "DELETE FROM comp_rulesets_nodes WHERE node_id = ? AND ruleset_id IN ("
	args := []any{nodeID}

	for i, id := range rulesetIDs {
		if i > 0 {
			query += ","
		}
		query += "?"
		args = append(args, id)
	}
	query += ")"

	result, err := oDb.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("compRulesetDetachNode: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("compRulesetDetachNode rowsAffected: %w", err)
	}

	if rows > 0 {
		oDb.SetChange("comp_rulesets_nodes")
		oDb.Session.NotifyChanges(ctx)
	}

	return rows, nil
}

// find attached modulesets for a node
func (oDb *DB) CompNodeModulesets(ctx context.Context, nodeID string) (modulesets []int, err error) {
	const query = `SELECT modset_id FROM comp_node_moduleset WHERE node_id = ?`
	var rows *sql.Rows
	rows, err = oDb.DB.QueryContext(ctx, query, nodeID)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var modsetID sql.NullInt64
		err = rows.Scan(&modsetID)
		if err != nil {
			return
		}
		if modsetID.Valid {
			modulesets = append(modulesets, int(modsetID.Int64))
		}
	}
	err = rows.Err()
	return
}

// find attached rulesets for a node
func (oDb *DB) CompNodeRulesets(ctx context.Context, nodeID string) (rulesets []int, err error) {
	const query = `SELECT ruleset_id FROM comp_rulesets_nodes WHERE node_id = ?`
	var rows *sql.Rows
	rows, err = oDb.DB.QueryContext(ctx, query, nodeID)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var rulesetID sql.NullInt64
		err = rows.Scan(&rulesetID)
		if err != nil {
			return
		}
		if rulesetID.Valid {
			rulesets = append(rulesets, int(rulesetID.Int64))
		}
	}
	err = rows.Err()
	return
}

// get candidate modulesets for a node (modulesets that can be attached but are not yet attached)
func (oDb *DB) CompNodeCandidateModulesets(ctx context.Context, nodeID string, attachedModulesets []int, groups []string, isManager bool, limit, offset int) ([]Moduleset, error) {
	var query = `
		SELECT DISTINCT comp_moduleset.id, comp_moduleset.modset_name, comp_moduleset.modset_author, comp_moduleset.modset_updated
		FROM comp_moduleset
		JOIN comp_moduleset_team_publication ON comp_moduleset.id = comp_moduleset_team_publication.modset_id
		JOIN auth_group ON auth_group.id = comp_moduleset_team_publication.group_id
		JOIN nodes ON nodes.node_id = ?
		WHERE (nodes.team_responsible = auth_group.role OR auth_group.role = 'Everybody')
	`

	args := []any{nodeID}

	filter, filterArgs, err := QFilter(ctx, QFilterInput{
		NodeField:  "nodes.node_id",
		IsManager:  isManager,
		UserGroups: groups,
		ResolvePublishedNodes: func(ctx context.Context) ([]string, error) {
			return oDb.PublishedNodeIDsForGroups(ctx, groups)
		},
	})

	if err != nil {
		return nil, err
	}
	if filter != "" {
		query += " AND (" + filter + ")"
		args = append(args, filterArgs...)
	}

	if len(attachedModulesets) > 0 {
		query += " AND comp_moduleset.id NOT IN (?"
		args = append(args, attachedModulesets[0])
		for i := 1; i < len(attachedModulesets); i++ {
			query += ", ?"
			args = append(args, attachedModulesets[i])
		}
		query += ")"
	}

	query += " ORDER BY comp_moduleset.modset_name, comp_moduleset.id"
	query, args = appendLimitOffset(query, args, limit, offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("compNodeCandidateModulesets: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var candidates []Moduleset
	for rows.Next() {
		var candidate Moduleset
		if err := rows.Scan(&candidate.ID, &candidate.Name, &candidate.Author, &candidate.Updated); err != nil {
			return nil, fmt.Errorf("compNodeCandidateModulesets scan: %w", err)
		}
		candidates = append(candidates, candidate)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("compNodeCandidateModulesets rows: %w", err)
	}

	return candidates, nil
}

// get candidate rulesets for a node (rulesets that can be attached but are not yet attached)
func (oDb *DB) CompNodeCandidateRulesets(ctx context.Context, nodeID string, attachedRulesets []int, groups []string, isManager bool, limit, offset int) ([]Ruleset, error) {
	var query = `
		SELECT DISTINCT comp_rulesets.id, comp_rulesets.ruleset_name, comp_rulesets.ruleset_public, comp_rulesets.ruleset_type
		FROM comp_rulesets
		JOIN comp_ruleset_team_publication ON comp_rulesets.id = comp_ruleset_team_publication.ruleset_id
		JOIN auth_group ON auth_group.id = comp_ruleset_team_publication.group_id
		JOIN nodes ON nodes.node_id = ?
		WHERE comp_rulesets.ruleset_type = 'explicit'
		AND comp_rulesets.ruleset_public = 'T'
		AND (nodes.team_responsible = auth_group.role OR auth_group.role = 'Everybody')
	`

	args := []any{nodeID}
	filter, filterArgs, err := QFilter(ctx, QFilterInput{
		NodeField:  "nodes.node_id",
		IsManager:  isManager,
		UserGroups: groups,
		ResolvePublishedNodes: func(ctx context.Context) ([]string, error) {
			return oDb.PublishedNodeIDsForGroups(ctx, groups)
		},
	})
	if err != nil {
		return nil, err
	}
	if filter != "" {
		query += " AND (" + filter + ")"
		args = append(args, filterArgs...)
	}

	if len(attachedRulesets) > 0 {
		query += " AND comp_rulesets.id NOT IN (?"
		args = append(args, attachedRulesets[0])
		for i := 1; i < len(attachedRulesets); i++ {
			query += ", ?"
			args = append(args, attachedRulesets[i])
		}
		query += ")"
	}

	query += " ORDER BY comp_rulesets.ruleset_name, comp_rulesets.id"
	query, args = appendLimitOffset(query, args, limit, offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("compNodeCandidateRulesets: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var candidates []Ruleset
	for rows.Next() {
		var candidate Ruleset
		var publicStr string
		if err := rows.Scan(&candidate.ID, &candidate.Name, &publicStr, &candidate.Type); err != nil {
			return nil, fmt.Errorf("compNodeCandidateRulesets scan: %w", err)
		}
		candidate.Public = (publicStr == "T")
		candidates = append(candidates, candidate)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("compNodeCandidateRulesets rows: %w", err)
	}

	return candidates, nil
}

// get attached modulesets for a node with details
func (oDb *DB) CompNodeAttachedModulesets(ctx context.Context, nodeID string, groups []string, isManager bool, limit, offset int) ([]Moduleset, error) {
	query := `
		SELECT comp_moduleset.id, comp_moduleset.modset_name, comp_moduleset.modset_author, comp_moduleset.modset_updated
		FROM comp_moduleset
		JOIN comp_node_moduleset ON comp_moduleset.id = comp_node_moduleset.modset_id
		WHERE comp_node_moduleset.node_id = ?
	`

	args := []any{nodeID}
	filter, filterArgs, err := QFilter(ctx, QFilterInput{
		NodeField:  "comp_node_moduleset.node_id",
		IsManager:  isManager,
		UserGroups: groups,
		ResolvePublishedNodes: func(ctx context.Context) ([]string, error) {
			return oDb.PublishedNodeIDsForGroups(ctx, groups)
		},
	})
	if err != nil {
		return nil, err
	}
	if filter != "" {
		query += " AND (" + filter + ")"
		args = append(args, filterArgs...)
	}

	query += " ORDER BY comp_moduleset.modset_name, comp_moduleset.id"
	query, args = appendLimitOffset(query, args, limit, offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("compNodeAttachedModulesets: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var modulesets []Moduleset
	for rows.Next() {
		var moduleset Moduleset
		if err := rows.Scan(&moduleset.ID, &moduleset.Name, &moduleset.Author, &moduleset.Updated); err != nil {
			return nil, fmt.Errorf("compNodeAttachedModulesets scan: %w", err)
		}
		modulesets = append(modulesets, moduleset)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("compNodeAttachedModulesets rows: %w", err)
	}

	return modulesets, nil
}

// get attached rulesets for a node with details
func (oDb *DB) CompNodeAttachedRulesets(ctx context.Context, nodeID string, groups []string, isManager bool, limit, offset int) ([]Ruleset, error) {
	query := `
		SELECT comp_rulesets.id, comp_rulesets.ruleset_name, comp_rulesets.ruleset_public, comp_rulesets.ruleset_type
		FROM comp_rulesets
		JOIN comp_rulesets_nodes ON comp_rulesets.id = comp_rulesets_nodes.ruleset_id
		WHERE comp_rulesets_nodes.node_id = ?
	`

	args := []any{nodeID}
	filter, filterArgs, err := QFilter(ctx, QFilterInput{
		NodeField:  "comp_rulesets_nodes.node_id",
		IsManager:  isManager,
		UserGroups: groups,
		ResolvePublishedNodes: func(ctx context.Context) ([]string, error) {
			return oDb.PublishedNodeIDsForGroups(ctx, groups)
		},
	})
	if err != nil {
		return nil, err
	}
	if filter != "" {
		query += " AND (" + filter + ")"
		args = append(args, filterArgs...)
	}

	query += " ORDER BY comp_rulesets.ruleset_name, comp_rulesets.id"
	query, args = appendLimitOffset(query, args, limit, offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("compNodeAttachedRulesets: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var rulesets []Ruleset
	for rows.Next() {
		var ruleset Ruleset
		var publicStr string
		if err := rows.Scan(&ruleset.ID, &ruleset.Name, &publicStr, &ruleset.Type); err != nil {
			return nil, fmt.Errorf("compNodeAttachedRulesets scan: %w", err)
		}
		ruleset.Public = (publicStr == "T")
		rulesets = append(rulesets, ruleset)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("compNodeAttachedRulesets rows: %w", err)
	}

	return rulesets, nil
}

// checks if a moduleset can be attached to a node.
func (oDb *DB) CompModulesetAttachable(ctx context.Context, nodeID, modulesetID string) (bool, error) {
	hasEveryBody, err := oDb.modulesetHasEverybodyPublication(ctx, modulesetID)
	if err != nil {
		return false, fmt.Errorf("compModulesetAttachable: %w", err)
	}
	if hasEveryBody {
		return true, nil
	}

	const query = `
        SELECT EXISTS(
            SELECT 1 FROM nodes
            JOIN auth_group ON nodes.team_responsible = auth_group.role
            JOIN comp_moduleset_team_publication ON auth_group.id = comp_moduleset_team_publication.group_id
            JOIN comp_moduleset ON comp_moduleset_team_publication.modset_id = comp_moduleset.id
            WHERE comp_moduleset.id = ?
            AND nodes.node_id = ?
        )
    `

	var attachable bool
	err = oDb.DB.QueryRowContext(ctx, query, modulesetID, nodeID).Scan(&attachable)
	if err != nil {
		return false, fmt.Errorf("compModulesetAttachable: %w", err)
	}
	return attachable, nil
}

// checks if a moduleset has "Everybody" publication rights.
func (oDb *DB) modulesetHasEverybodyPublication(ctx context.Context, modulesetID string) (bool, error) {
	const query = `
        SELECT EXISTS(
            SELECT 1 FROM auth_group
            JOIN comp_moduleset_team_publication ON auth_group.id = comp_moduleset_team_publication.group_id
            WHERE auth_group.role = 'Everybody'
            AND comp_moduleset_team_publication.modset_id = ?
        )
    `
	var exists bool
	err := oDb.DB.QueryRowContext(ctx, query, modulesetID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("modulesetHasEverybodyPublication: %w", err)
	}
	return exists, nil
}

// checks if a ruleset can be attached to a node.
func (oDb *DB) CompRulesetAttachable(ctx context.Context, nodeID, rulesetID string) (bool, error) {
	hasEveryBody, err := oDb.RulesetHasEverybodyPublication(ctx, rulesetID)
	if err != nil {
		return false, fmt.Errorf("compRulesetAttachable: %w", err)
	}
	if hasEveryBody {
		return true, nil
	}

	const query = `
        SELECT EXISTS(
            SELECT 1 FROM nodes
            JOIN auth_group ON nodes.team_responsible = auth_group.role
            JOIN comp_ruleset_team_publication ON auth_group.id = comp_ruleset_team_publication.group_id
            JOIN comp_rulesets ON comp_ruleset_team_publication.ruleset_id = comp_rulesets.id
            WHERE comp_rulesets.id = ?
            AND comp_rulesets.ruleset_public = "T"
            AND comp_rulesets.ruleset_type = 'explicit'
            AND nodes.node_id = ?
        )
    `

	var attachable bool
	err = oDb.DB.QueryRowContext(ctx, query, rulesetID, nodeID).Scan(&attachable)
	if err != nil {
		return false, fmt.Errorf("compRulesetAttachable: %w", err)
	}
	return attachable, nil
}

// attach a moduleset to a node
func (oDb *DB) CompModulesetAttachNode(ctx context.Context, nodeID, modulesetID string) (int64, error) {
	const query = "INSERT INTO comp_node_moduleset (node_id, modset_id) VALUES (?, ?)"

	result, err := oDb.ExecContext(ctx, query, nodeID, modulesetID)
	if err != nil {
		return 0, fmt.Errorf("compModulesetAttachNode: %w", err)
	}

	if rows, err := result.RowsAffected(); err == nil && rows > 0 {
		oDb.SetChange("comp_node_moduleset")
		oDb.Session.NotifyChanges(ctx)
	}

	id, _ := result.LastInsertId()
	return id, nil
}

// checks if a ruleset has "Everybody" publication rights.
func (oDb *DB) RulesetHasEverybodyPublication(ctx context.Context, rulesetID string) (bool, error) {
	const query = `
        SELECT EXISTS(
            SELECT 1 FROM auth_group
            JOIN comp_ruleset_team_publication ON auth_group.id = comp_ruleset_team_publication.group_id
            WHERE auth_group.role = 'Everybody'
            AND comp_ruleset_team_publication.ruleset_id = ?
        )
    `
	var exists bool
	err := oDb.DB.QueryRowContext(ctx, query, rulesetID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("rulesetHasEverybodyPublication: %w", err)
	}
	return exists, nil
}

// attach a ruleset to a node
func (oDb *DB) CompRulesetAttachNode(ctx context.Context, nodeID, rulesetID string) (int64, error) {
	const query = "INSERT INTO comp_rulesets_nodes (node_id, ruleset_id) VALUES (?, ?)"

	result, err := oDb.ExecContext(ctx, query, nodeID, rulesetID)
	if err != nil {
		return 0, fmt.Errorf("compRulesetAttachNode: %w", err)
	}

	if rows, err := result.RowsAffected(); err == nil && rows > 0 {
		oDb.SetChange("comp_rulesets_nodes")
		oDb.Session.NotifyChanges(ctx)
	}

	id, _ := result.LastInsertId()
	return id, nil
}

func buildCompStatusQuery(groups []string, isManager bool, selectExprs []string) (string, []any, error) {
	if len(selectExprs) == 0 {
		return "", nil, fmt.Errorf("buildCompStatusQuery: no columns selected")
	}
	query := "SELECT " + strings.Join(selectExprs, ", ") + "\nFROM comp_status"
	var args []any

	if !isManager {
		clean := cleanGroups(groups)
		query += "\nJOIN nodes ON comp_status.node_id = nodes.node_id"
		if len(clean) == 0 {
			query += "\nWHERE 1=0"
		} else {
			query += "\nWHERE (nodes.team_responsible = 'Everybody' OR nodes.team_responsible IN (" + Placeholders(len(clean)) + "))"
			for _, g := range clean {
				args = append(args, g)
			}
		}
	} else {
		query += "\nWHERE 1=1"
	}
	return query, args, nil
}

// GetNodeComplianceStatus returns the last compliance check run status entries for a node.
func (oDb *DB) GetNodeComplianceStatus(ctx context.Context, nodeID string, p ListParams) ([]map[string]any, error) {
	query, args, err := buildCompStatusQuery(p.Groups, p.IsManager, p.SelectExprs)
	if err != nil {
		return nil, err
	}
	query += " AND comp_status.node_id = ?"
	args = append(args, nodeID)
	query += " " + p.OrderByClause("comp_status.run_date DESC")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getNodeComplianceStatus: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}
