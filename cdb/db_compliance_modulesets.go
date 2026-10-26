package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
)

// GetComplianceModulesets lists the modulesets published to one of the caller's
// groups, every moduleset for a manager; one when id is set.
func (oDb *DB) GetComplianceModulesets(ctx context.Context, id *int64, p ListParams) ([]map[string]any, error) {
	cond, args := compObjectVisibleCond(CompModulesetKind, p.Groups, p.IsManager)
	conds := []string{cond}
	if id != nil {
		conds = append(conds, "comp_moduleset.id = ?")
		args = append(args, *id)
	}
	return oDb.listQuery(ctx, "getComplianceModulesets", "comp_moduleset", conds, args, "comp_moduleset.modset_name", p)
}

// CompModulesetID resolves a moduleset given by id or by name, as
// moduleset_id_q(); false when there is none.
func (oDb *DB) CompModulesetID(ctx context.Context, idOrName string) (int64, bool, error) {
	var id int64
	var err error
	if n, convErr := strconv.ParseInt(idOrName, 10, 64); convErr == nil {
		err = oDb.DB.QueryRowContext(ctx, "SELECT id FROM comp_moduleset WHERE id = ?", n).Scan(&id)
	} else {
		err = oDb.DB.QueryRowContext(ctx, "SELECT id FROM comp_moduleset WHERE modset_name = ? LIMIT 1", idOrName).Scan(&id)
	}
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, nil
	case err != nil:
		return 0, false, fmt.Errorf("compModulesetID: %w", err)
	}
	return id, true, nil
}

// CreateCompModuleset inserts a moduleset authored by author, published to and
// under the responsibility of groupID, as create_moduleset() and
// add_default_teams_to_modset().
func (oDb *DB) CreateCompModuleset(ctx context.Context, name, author string, groupID int64) (int64, error) {
	res, err := oDb.DB.ExecContext(ctx, "INSERT INTO comp_moduleset (modset_name, modset_author, modset_updated) VALUES (?, ?, NOW())", name, author)
	if err != nil {
		return 0, fmt.Errorf("createCompModuleset: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("createCompModuleset: %w", err)
	}
	if err := oDb.addModulesetTeams(ctx, id, groupID); err != nil {
		return 0, err
	}
	oDb.SetChange("comp_moduleset")
	return id, nil
}

func (oDb *DB) addModulesetTeams(ctx context.Context, modsetID, groupID int64) error {
	for _, table := range []string{"comp_moduleset_team_responsible", "comp_moduleset_team_publication"} {
		if _, err := oDb.DB.ExecContext(ctx, "INSERT INTO "+table+" (modset_id, group_id) VALUES (?, ?)", modsetID, groupID); err != nil {
			return fmt.Errorf("addModulesetTeams: %w", err)
		}
		oDb.SetChange(table)
	}
	return nil
}

// RenameCompModuleset renames a moduleset and records the update date.
func (oDb *DB) RenameCompModuleset(ctx context.Context, id int64, name string) error {
	if _, err := oDb.DB.ExecContext(ctx, "UPDATE comp_moduleset SET modset_name = ?, modset_updated = NOW() WHERE id = ?", name, id); err != nil {
		return fmt.Errorf("renameCompModuleset: %w", err)
	}
	oDb.SetChange("comp_moduleset")
	return nil
}

// DeleteCompModuleset deletes a moduleset with its node and service attachments,
// publication and responsible groups, modules, parent and child relations and
// ruleset links, as delete_moduleset().
func (oDb *DB) DeleteCompModuleset(ctx context.Context, id int64) error {
	for _, q := range []struct{ table, cond string }{
		{"comp_node_moduleset", "modset_id = ?"},
		{"comp_modulesets_services", "modset_id = ?"},
		{"comp_moduleset_team_publication", "modset_id = ?"},
		{"comp_moduleset_team_responsible", "modset_id = ?"},
		{"comp_moduleset_modules", "modset_id = ?"},
		{"comp_moduleset", "id = ?"},
		{"comp_moduleset_moduleset", "parent_modset_id = ? OR child_modset_id = ?"},
		{"comp_moduleset_ruleset", "modset_id = ?"},
	} {
		args := []any{id}
		if q.table == "comp_moduleset_moduleset" {
			args = append(args, id)
		}
		if _, err := oDb.DB.ExecContext(ctx, "DELETE FROM "+q.table+" WHERE "+q.cond, args...); err != nil {
			return fmt.Errorf("deleteCompModuleset: %s: %w", q.table, err)
		}
		oDb.SetChange(q.table)
	}
	return nil
}

// CloneCompModuleset copies a moduleset as cloneName, "<name>_clone" when
// empty, authored by author: its modules, its rulesets and its children, under
// the responsibility and publication of groupID, as clone_moduleset(). It
// returns the id and the name of the copy.
func (oDb *DB) CloneCompModuleset(ctx context.Context, id int64, author string, groupID int64, cloneName string) (int64, string, error) {
	var name string
	err := oDb.DB.QueryRowContext(ctx, "SELECT modset_name FROM comp_moduleset WHERE id = ?", id).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, "", ErrCompNotFound
	} else if err != nil {
		return 0, "", fmt.Errorf("cloneCompModuleset: %w", err)
	}
	if cloneName == "" {
		cloneName = name + "_clone"
	}
	if _, found, err := oDb.CompModulesetID(ctx, cloneName); err != nil {
		return 0, "", err
	} else if found {
		return 0, "", fmt.Errorf("%w: a moduleset named %s already exists", ErrCompConflict, cloneName)
	}
	res, err := oDb.DB.ExecContext(ctx, "INSERT INTO comp_moduleset (modset_name, modset_author, modset_updated) VALUES (?, ?, NOW())", cloneName, author)
	if err != nil {
		return 0, "", fmt.Errorf("cloneCompModuleset: %w", err)
	}
	newID, err := res.LastInsertId()
	if err != nil {
		return 0, "", fmt.Errorf("cloneCompModuleset: %w", err)
	}
	for _, st := range []struct {
		query string
		args  []any
	}{
		{"INSERT INTO comp_moduleset_modules (modset_id, modset_mod_name, modset_mod_author, modset_mod_updated, autofix)" +
			" SELECT ?, modset_mod_name, modset_mod_author, NOW(), autofix FROM comp_moduleset_modules WHERE modset_id = ?", []any{newID, id}},
		{"INSERT INTO comp_moduleset_ruleset (modset_id, ruleset_id) SELECT ?, ruleset_id FROM comp_moduleset_ruleset WHERE modset_id = ?", []any{newID, id}},
		{"INSERT INTO comp_moduleset_moduleset (parent_modset_id, child_modset_id) SELECT ?, child_modset_id FROM comp_moduleset_moduleset WHERE parent_modset_id = ?", []any{newID, id}},
	} {
		if _, err := oDb.DB.ExecContext(ctx, st.query, st.args...); err != nil {
			return 0, "", fmt.Errorf("cloneCompModuleset: %w", err)
		}
	}
	if err := oDb.addModulesetTeams(ctx, newID, groupID); err != nil {
		return 0, "", err
	}
	for _, t := range []string{"comp_moduleset", "comp_moduleset_modules", "comp_moduleset_ruleset", "comp_moduleset_moduleset"} {
		oDb.SetChange(t)
	}
	return newID, cloneName, nil
}

// CompModulesetUsage lists the modulesets holding a moduleset as a child.
func (oDb *DB) CompModulesetUsage(ctx context.Context, id int64) (map[string][]map[string]any, error) {
	parents, err := oDb.namedPairs(ctx, "SELECT m.id, m.modset_name FROM comp_moduleset_moduleset mm"+
		" JOIN comp_moduleset m ON m.id = mm.parent_modset_id WHERE mm.child_modset_id = ? ORDER BY m.modset_name", id, "id", "modset_name")
	if err != nil {
		return nil, fmt.Errorf("compModulesetUsage: %w", err)
	}
	return map[string][]map[string]any{"modulesets": parents}, nil
}
