package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// rolesCond restricts an auth_group alias to the caller's groups, "1=0" without
// any: the join condition of the publication and responsibility checks.
func rolesCond(alias string, groups []string, orgOnly bool) (string, []any) {
	clean := cleanGroups(groups)
	if len(clean) == 0 {
		return "1=0", nil
	}
	args := make([]any, len(clean))
	for i, g := range clean {
		args[i] = g
	}
	cond := alias + ".role IN (" + Placeholders(len(clean)) + ")"
	if orgOnly {
		cond += " AND " + alias + ".privilege = 'F'"
	}
	return cond, args
}

// compRulesetVisibleCond restricts comp_rulesets to the rulesets published to one
// of the caller's groups, every ruleset for a manager, as the historical
// rest_get_compliance_rulesets.
func compRulesetVisibleCond(groups []string, isManager bool) (string, []any) {
	if isManager {
		return "1=1", nil
	}
	cond, args := rolesCond("ag", groups, false)
	return "comp_rulesets.id IN (SELECT p.ruleset_id FROM comp_ruleset_team_publication p" +
		" JOIN auth_group ag ON ag.id = p.group_id WHERE " + cond + ")", args
}

// GetComplianceRulesets lists the rulesets the caller may see; one when id is set.
func (oDb *DB) GetComplianceRulesets(ctx context.Context, id *int64, p ListParams) ([]map[string]any, error) {
	cond, args := compRulesetVisibleCond(p.Groups, p.IsManager)
	conds := []string{cond}
	if id != nil {
		conds = append(conds, "comp_rulesets.id = ?")
		args = append(args, *id)
	}
	return oDb.listQuery(ctx, "getComplianceRulesets", "comp_rulesets", conds, args, "comp_rulesets.ruleset_name", p)
}

// CompRulesetID resolves a ruleset given by id or by name, as ruleset_id_q();
// false when there is none.
func (oDb *DB) CompRulesetID(ctx context.Context, idOrName string) (int64, bool, error) {
	var id int64
	var err error
	if n, convErr := strconv.ParseInt(idOrName, 10, 64); convErr == nil {
		err = oDb.DB.QueryRowContext(ctx, "SELECT id FROM comp_rulesets WHERE id = ?", n).Scan(&id)
	} else {
		err = oDb.DB.QueryRowContext(ctx, "SELECT id FROM comp_rulesets WHERE ruleset_name = ? LIMIT 1", idOrName).Scan(&id)
	}
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, nil
	case err != nil:
		return 0, false, fmt.Errorf("compRulesetID: %w", err)
	}
	return id, true, nil
}

// CompRulesetVisible tells whether a ruleset is published to one of the caller's
// groups; always for a manager.
func (oDb *DB) CompRulesetVisible(ctx context.Context, id int64, groups []string, isManager bool) (bool, error) {
	cond, args := compRulesetVisibleCond(groups, isManager)
	return oDb.exists(ctx, "compRulesetVisible", "SELECT 1 FROM comp_rulesets WHERE comp_rulesets.id = ? AND "+cond, append([]any{id}, args...)...)
}

// CompRulesetResponsible tells whether one of the caller's organization groups is
// responsible for a ruleset, as ruleset_responsible(); always for a manager.
func (oDb *DB) CompRulesetResponsible(ctx context.Context, id int64, groups []string, isManager bool) (bool, error) {
	if isManager {
		return oDb.exists(ctx, "compRulesetResponsible", "SELECT 1 FROM comp_rulesets WHERE id = ?", id)
	}
	cond, args := rolesCond("ag", groups, true)
	return oDb.exists(ctx, "compRulesetResponsible", "SELECT 1 FROM comp_ruleset_team_responsible r"+
		" JOIN auth_group ag ON ag.id = r.group_id WHERE r.ruleset_id = ? AND "+cond, append([]any{id}, args...)...)
}

func (oDb *DB) exists(ctx context.Context, name, query string, args ...any) (bool, error) {
	var one int
	err := oDb.DB.QueryRowContext(ctx, query+" LIMIT 1", args...).Scan(&one)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("%s: %w", name, err)
	}
	return true, nil
}

// CompRulesetName returns the name of a ruleset by id.
func (oDb *DB) CompRulesetNameByID(ctx context.Context, id int64) (string, error) {
	var name sql.NullString
	if err := oDb.DB.QueryRowContext(ctx, "SELECT ruleset_name FROM comp_rulesets WHERE id = ?", id).Scan(&name); err != nil {
		return "", fmt.Errorf("compRulesetNameByID: %w", err)
	}
	return name.String, nil
}

// DefaultGroupOrManager returns the group a new compliance object is given to:
// the user's default group, else the Manager group, as add_default_teams().
func (oDb *DB) DefaultGroupOrManager(ctx context.Context, userID *int64) (int64, error) {
	if userID != nil {
		if gid, ok, err := oDb.UserDefaultGroupID(ctx, *userID); err != nil {
			return 0, err
		} else if ok {
			return gid, nil
		}
	}
	var gid int64
	if err := oDb.DB.QueryRowContext(ctx, "SELECT id FROM auth_group WHERE role = 'Manager' LIMIT 1").Scan(&gid); err != nil {
		return 0, fmt.Errorf("defaultGroupOrManager: %w", err)
	}
	return gid, nil
}

// CreateCompRuleset inserts a ruleset, published to and under the responsibility
// of groupID, as create_ruleset() and add_default_teams().
func (oDb *DB) CreateCompRuleset(ctx context.Context, name, rtype, public string, groupID int64) (int64, error) {
	res, err := oDb.DB.ExecContext(ctx, "INSERT INTO comp_rulesets (ruleset_name, ruleset_type, ruleset_public) VALUES (?, ?, ?)",
		name, rtype, public)
	if err != nil {
		return 0, fmt.Errorf("createCompRuleset: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("createCompRuleset: %w", err)
	}
	if err := oDb.addRulesetTeams(ctx, id, groupID); err != nil {
		return 0, err
	}
	oDb.SetChange("comp_rulesets")
	return id, nil
}

func (oDb *DB) addRulesetTeams(ctx context.Context, rulesetID, groupID int64) error {
	for _, table := range []string{"comp_ruleset_team_responsible", "comp_ruleset_team_publication"} {
		if _, err := oDb.DB.ExecContext(ctx, "INSERT INTO "+table+" (ruleset_id, group_id) VALUES (?, ?)", rulesetID, groupID); err != nil {
			return fmt.Errorf("addRulesetTeams: %w", err)
		}
		oDb.SetChange(table)
	}
	return nil
}

// UpdateCompRuleset sets the given columns of a ruleset; the keys are checked by
// the caller.
func (oDb *DB) UpdateCompRuleset(ctx context.Context, id int64, fields map[string]any) error {
	if len(fields) == 0 {
		return nil
	}
	sets := make([]string, 0, len(fields))
	args := make([]any, 0, len(fields)+1)
	for _, k := range sortedKeys(fields) {
		sets = append(sets, k+" = ?")
		args = append(args, fields[k])
	}
	args = append(args, id)
	if _, err := oDb.DB.ExecContext(ctx, "UPDATE comp_rulesets SET "+strings.Join(sets, ", ")+" WHERE id = ?", args...); err != nil {
		return fmt.Errorf("updateCompRuleset: %w", err)
	}
	oDb.SetChange("comp_rulesets")
	return nil
}

// DeleteCompRuleset deletes a ruleset with its filtersets, node and service
// attachments, publication and responsible groups, parent and child relations,
// variables and moduleset links, as delete_ruleset().
func (oDb *DB) DeleteCompRuleset(ctx context.Context, id int64) error {
	for _, q := range []struct{ table, cond string }{
		{"comp_rulesets_filtersets", "ruleset_id = ?"},
		{"comp_rulesets_nodes", "ruleset_id = ?"},
		{"comp_rulesets_services", "ruleset_id = ?"},
		{"comp_ruleset_team_publication", "ruleset_id = ?"},
		{"comp_ruleset_team_responsible", "ruleset_id = ?"},
		{"comp_rulesets_rulesets", "parent_rset_id = ? OR child_rset_id = ?"},
		{"comp_rulesets_variables", "ruleset_id = ?"},
		{"comp_rulesets", "id = ?"},
		{"comp_moduleset_ruleset", "ruleset_id = ?"},
	} {
		args := []any{id}
		if strings.Count(q.cond, "?") == 2 {
			args = append(args, id)
		}
		if _, err := oDb.DB.ExecContext(ctx, "DELETE FROM "+q.table+" WHERE "+q.cond, args...); err != nil {
			return fmt.Errorf("deleteCompRuleset: %s: %w", q.table, err)
		}
		oDb.SetChange(q.table)
	}
	return nil
}

// CloneCompRuleset copies a ruleset as cloneName, "<name>_clone" when empty: its
// type and publicity, the filterset of a contextual ruleset, its variables
// (authored by author), its children, under the responsibility and publication
// of groupID, as clone_ruleset(). It returns the id and the name of the copy.
func (oDb *DB) CloneCompRuleset(ctx context.Context, id int64, author string, groupID int64, cloneName string) (int64, string, error) {
	var name, rtype, public sql.NullString
	err := oDb.DB.QueryRowContext(ctx, "SELECT ruleset_name, ruleset_type, ruleset_public FROM comp_rulesets WHERE id = ?", id).
		Scan(&name, &rtype, &public)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, "", ErrCompNotFound
	} else if err != nil {
		return 0, "", fmt.Errorf("cloneCompRuleset: %w", err)
	}
	if cloneName == "" {
		cloneName = name.String + "_clone"
	}
	if _, found, err := oDb.CompRulesetID(ctx, cloneName); err != nil {
		return 0, "", err
	} else if found {
		return 0, "", fmt.Errorf("%w: a ruleset named %s already exists", ErrCompConflict, cloneName)
	}
	res, err := oDb.DB.ExecContext(ctx, "INSERT INTO comp_rulesets (ruleset_name, ruleset_type, ruleset_public) VALUES (?, ?, ?)",
		cloneName, rtype, public)
	if err != nil {
		return 0, "", fmt.Errorf("cloneCompRuleset: %w", err)
	}
	newID, err := res.LastInsertId()
	if err != nil {
		return 0, "", fmt.Errorf("cloneCompRuleset: %w", err)
	}
	stmts := []struct {
		query string
		args  []any
	}{
		{"INSERT INTO comp_rulesets_variables (ruleset_id, var_name, var_class, var_value, var_author, var_updated)" +
			" SELECT ?, var_name, var_class, var_value, ?, NOW() FROM comp_rulesets_variables WHERE ruleset_id = ?", []any{newID, author, id}},
		{"INSERT INTO comp_rulesets_rulesets (parent_rset_id, child_rset_id)" +
			" SELECT ?, child_rset_id FROM comp_rulesets_rulesets WHERE parent_rset_id = ?", []any{newID, id}},
	}
	if rtype.String == "contextual" {
		stmts = append(stmts, struct {
			query string
			args  []any
		}{"INSERT INTO comp_rulesets_filtersets (ruleset_id, fset_id)" +
			" SELECT ?, fset_id FROM comp_rulesets_filtersets WHERE ruleset_id = ? LIMIT 1", []any{newID, id}})
	}
	for _, st := range stmts {
		if _, err := oDb.DB.ExecContext(ctx, st.query, st.args...); err != nil {
			return 0, "", fmt.Errorf("cloneCompRuleset: %w", err)
		}
	}
	if err := oDb.addRulesetTeams(ctx, newID, groupID); err != nil {
		return 0, "", err
	}
	for _, t := range []string{"comp_rulesets", "comp_rulesets_variables", "comp_rulesets_rulesets", "comp_rulesets_filtersets"} {
		oDb.SetChange(t)
	}
	return newID, cloneName, nil
}

// CompRulesetsChains rebuilds comp_rulesets_chains, the flattened ruleset
// hierarchy: one row per ruleset (chain_len 1), and one per path from a ruleset to
// each of its descendants, named "a > b > c", as comp_rulesets_chains().
func (oDb *DB) CompRulesetsChains(ctx context.Context) error {
	names := map[int64]string{}
	var order []int64
	rows, err := oDb.DB.QueryContext(ctx, "SELECT id, COALESCE(ruleset_name, '') FROM comp_rulesets ORDER BY id")
	if err != nil {
		return fmt.Errorf("compRulesetsChains: %w", err)
	}
	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			_ = rows.Close()
			return fmt.Errorf("compRulesetsChains: %w", err)
		}
		names[id] = name
		order = append(order, id)
	}
	_ = rows.Close()
	children := map[int64][]int64{}
	rows, err = oDb.DB.QueryContext(ctx, "SELECT rr.parent_rset_id, rr.child_rset_id FROM comp_rulesets_rulesets rr"+
		" JOIN comp_rulesets r ON r.id = rr.child_rset_id ORDER BY rr.id")
	if err != nil {
		return fmt.Errorf("compRulesetsChains: %w", err)
	}
	for rows.Next() {
		var parent, child int64
		if err := rows.Scan(&parent, &child); err != nil {
			_ = rows.Close()
			return fmt.Errorf("compRulesetsChains: %w", err)
		}
		children[parent] = append(children[parent], child)
	}
	_ = rows.Close()

	type chain struct {
		head, tail int64
		length     int
		text       string
	}
	var chains []chain
	var walk func(path []int64)
	walk = func(path []int64) {
		last := path[len(path)-1]
		for _, child := range children[last] {
			// A ruleset already in the path would loop: the relation is refused on
			// attachment, a stored cycle is not followed.
			loop := false
			for _, p := range path {
				if p == child {
					loop = true
				}
			}
			if loop {
				continue
			}
			next := append(append([]int64{}, path...), child)
			parts := make([]string, len(next))
			for i, id := range next {
				parts[i] = names[id]
			}
			chains = append(chains, chain{next[0], child, len(next), strings.Join(parts, " > ")})
			walk(next)
		}
	}
	for _, id := range order {
		chains = append(chains, chain{id, id, 1, ""})
		walk([]int64{id})
	}

	if _, err := oDb.DB.ExecContext(ctx, "DELETE FROM comp_rulesets_chains"); err != nil {
		return fmt.Errorf("compRulesetsChains: %w", err)
	}
	for start := 0; start < len(chains); start += 500 {
		end := min(start+500, len(chains))
		values := make([]string, 0, end-start)
		args := make([]any, 0, 4*(end-start))
		for _, c := range chains[start:end] {
			values = append(values, "(?, ?, ?, ?)")
			args = append(args, c.head, c.tail, c.length, c.text)
		}
		if _, err := oDb.DB.ExecContext(ctx, "INSERT INTO comp_rulesets_chains (head_rset_id, tail_rset_id, chain_len, chain) VALUES "+
			strings.Join(values, ", ")+" ON DUPLICATE KEY UPDATE chain_len = VALUES(chain_len), chain = VALUES(chain)", args...); err != nil {
			return fmt.Errorf("compRulesetsChains: %w", err)
		}
	}
	oDb.SetChange("comp_rulesets_chains")
	return nil
}

// CompRulesetUsage lists what uses a ruleset: the modulesets and the parent
// rulesets holding it, the nodes and the services it is attached to.
func (oDb *DB) CompRulesetUsage(ctx context.Context, id int64) (map[string][]map[string]any, error) {
	out := map[string][]map[string]any{}
	for _, q := range []struct{ key, idKey, nameKey, query string }{
		{"modulesets", "id", "modset_name", "SELECT m.id, m.modset_name FROM comp_moduleset_ruleset mr JOIN comp_moduleset m ON m.id = mr.modset_id WHERE mr.ruleset_id = ? ORDER BY m.modset_name"},
		{"rulesets", "id", "ruleset_name", "SELECT r.id, COALESCE(r.ruleset_name, '') FROM comp_rulesets_rulesets rr JOIN comp_rulesets r ON r.id = rr.parent_rset_id WHERE rr.child_rset_id = ? ORDER BY r.ruleset_name"},
		{"nodes", "node_id", "nodename", "SELECT rn.node_id, COALESCE(n.nodename, '') FROM comp_rulesets_nodes rn JOIN nodes n ON n.node_id = rn.node_id WHERE rn.ruleset_id = ? ORDER BY n.nodename"},
		{"services", "svc_id", "svcname", "SELECT rs.svc_id, COALESCE(s.svcname, '') FROM comp_rulesets_services rs JOIN services s ON s.svc_id = rs.svc_id WHERE rs.ruleset_id = ? ORDER BY s.svcname"},
	} {
		entries, err := oDb.namedPairs(ctx, q.query, id, q.idKey, q.nameKey)
		if err != nil {
			return nil, fmt.Errorf("compRulesetUsage: %s: %w", q.key, err)
		}
		out[q.key] = entries
	}
	return out, nil
}

// namedPairs runs a query returning (id, name) rows, as a list of objects with
// the given keys; an integer id stays a number.
func (oDb *DB) namedPairs(ctx context.Context, query string, arg any, idKey, nameKey string) ([]map[string]any, error) {
	rows, err := oDb.DB.QueryContext(ctx, query, arg)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := []map[string]any{}
	for rows.Next() {
		var id, name sql.NullString
		if err := rows.Scan(&id, &name); err != nil {
			return nil, err
		}
		var idv any = id.String
		if n, err := strconv.ParseInt(id.String, 10, 64); err == nil && idKey == "id" {
			idv = n
		}
		out = append(out, map[string]any{idKey: idv, nameKey: name.String})
	}
	return out, rows.Err()
}

// Errors of the compliance designer, mapped to HTTP statuses by the handlers.
var (
	ErrCompNotFound = errors.New("not found")
	ErrCompConflict = errors.New("conflict")
)

// RulesetIsContextual tells whether a ruleset is contextual, applied through its
// filterset rather than by attachment.
func (oDb *DB) RulesetIsContextual(ctx context.Context, id int64) (bool, error) {
	return oDb.exists(ctx, "rulesetIsContextual", "SELECT 1 FROM comp_rulesets WHERE id = ? AND ruleset_type = 'contextual'", id)
}
