package cdb

import (
	"context"
	"fmt"
)

func (oDb *DB) PurgeCompModulesetsNodes(ctx context.Context) error {
	var query = `DELETE
		FROM comp_node_moduleset
		WHERE
		  node_id NOT IN (
                    SELECT DISTINCT node_id FROM nodes
                   )`
	if result, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return err
	} else if affected, err := result.RowsAffected(); err != nil {
		return err
	} else if affected > 0 {
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
	if result, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return err
	} else if affected, err := result.RowsAffected(); err != nil {
		return err
	} else if affected > 0 {
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
	if result, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return err
	} else if affected, err := result.RowsAffected(); err != nil {
		return err
	} else if affected > 0 {
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
	if result, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return err
	} else if affected, err := result.RowsAffected(); err != nil {
		return err
	} else if affected > 0 {
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
	if result, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return err
	} else if affected, err := result.RowsAffected(); err != nil {
		return err
	} else if affected > 0 {
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
	if result, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return err
	} else if affected, err := result.RowsAffected(); err != nil {
		return err
	} else if affected > 0 {
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
	if result, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return err
	} else if affected, err := result.RowsAffected(); err != nil {
		return err
	} else if affected > 0 {
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
	if result, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return err
	} else if affected, err := result.RowsAffected(); err != nil {
		return err
	} else if affected > 0 {
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
	if result, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return err
	} else if affected, err := result.RowsAffected(); err != nil {
		return err
	} else if affected > 0 {
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
	if result, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return err
	} else if affected, err := result.RowsAffected(); err != nil {
		return err
	} else if affected > 0 {
		oDb.SetChange("comp_status")
	}
	return nil
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

// attach a moduleset to a node
func (oDb *DB) CompModulesetAttachNode(ctx context.Context, nodeID, modulesetID string) (int64, error) {
	const query = "INSERT INTO comp_node_moduleset (node_id, modset_id) VALUES (?, ?)"

	result, err := oDb.DB.ExecContext(ctx, query, nodeID, modulesetID)
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

// attach a ruleset to a node
func (oDb *DB) CompRulesetAttachNode(ctx context.Context, nodeID, rulesetID string) (int64, error) {
	const query = "INSERT INTO comp_rulesets_nodes (node_id, ruleset_id) VALUES (?, ?)"

	result, err := oDb.DB.ExecContext(ctx, query, nodeID, rulesetID)
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

	result, err := oDb.DB.ExecContext(ctx, query, args...)
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

	result, err := oDb.DB.ExecContext(ctx, query, args...)
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
