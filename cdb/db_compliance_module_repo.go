package cdb

import (
	"context"
	"fmt"
)

// ModulesetRef is a moduleset by id and name.
type ModulesetRef struct {
	ID   int64
	Name string
}

// ModulesetsNamingModule returns the modulesets holding a module of that name,
// by name.
func (oDb *DB) ModulesetsNamingModule(ctx context.Context, module string) ([]ModulesetRef, error) {
	rows, err := oDb.DB.QueryContext(ctx, `SELECT DISTINCT m.id, m.modset_name
		FROM comp_moduleset_modules mm JOIN comp_moduleset m ON m.id = mm.modset_id
		WHERE mm.modset_mod_name = ? ORDER BY m.modset_name`, module)
	if err != nil {
		return nil, fmt.Errorf("modulesetsNamingModule: %w", err)
	}
	defer func() { _ = rows.Close() }()
	l := []ModulesetRef{}
	for rows.Next() {
		var r ModulesetRef
		if err := rows.Scan(&r.ID, &r.Name); err != nil {
			return nil, fmt.Errorf("modulesetsNamingModule: %w", err)
		}
		l = append(l, r)
	}
	return l, rows.Err()
}

// RenameModulesetModule makes the modulesets naming the module from name it
// to instead, as the module is renamed, and returns how many changed. A
// moduleset naming both keeps one entry, the one naming to.
func (oDb *DB) RenameModulesetModule(ctx context.Context, from, to string) (int64, error) {
	// Within the transaction of the caller, or one of its own: the two
	// statements apply together.
	exec := oDb.DB
	commit := func() error { return nil }
	if !oDb.HasTx {
		tx, err := oDb.dbPool.BeginTx(ctx, nil)
		if err != nil {
			return 0, fmt.Errorf("renameModulesetModule: %w", err)
		}
		defer func() { _ = tx.Rollback() }()
		exec, commit = tx, tx.Commit
	}
	// IGNORE: a moduleset naming to already keeps its row naming from, removed next.
	res, err := exec.ExecContext(ctx, `UPDATE IGNORE comp_moduleset_modules
		SET modset_mod_name = ?, modset_mod_updated = NOW() WHERE modset_mod_name = ?`, to, from)
	if err != nil {
		return 0, fmt.Errorf("renameModulesetModule: %w", err)
	}
	n, _ := res.RowsAffected()
	if _, err := exec.ExecContext(ctx, `DELETE FROM comp_moduleset_modules WHERE modset_mod_name = ?`, from); err != nil {
		return 0, fmt.Errorf("renameModulesetModule: %w", err)
	}
	if err := commit(); err != nil {
		return 0, fmt.Errorf("renameModulesetModule: %w", err)
	}
	if n > 0 {
		oDb.SetChange("comp_moduleset_modules")
	}
	return n, nil
}
