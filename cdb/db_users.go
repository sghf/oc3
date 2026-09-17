package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// usersAuthClause returns the WHERE fragment restricting a query on auth_user to
// the users the caller may see, mirroring allowed_user_ids_q() of the python
// collector (init/models/rest/lib_users.py:11).
//
// Manager and UserManager see every user. Anybody else sees the users sharing
// one of its organisational groups — the non-privilege groups it belongs to,
// excluding "UnaffectedProjects" (user_org_group_ids(), init/models/auth.py:380)
// — plus itself. The target membership ignores the "Everybody" group, which
// holds all users and would defeat the restriction.
func usersAuthClause(p ListParams) (string, []any) {
	if p.IsManager || p.HasGroup("UserManager") {
		return "auth_user.id > 0", nil
	}
	if p.UserID == nil {
		// Not authenticated as a user: no organisational group, and no self.
		return "1=0", nil
	}
	const clause = `(auth_user.id IN (
			SELECT peer.user_id
			FROM auth_membership peer
			JOIN auth_group peer_group ON peer_group.id = peer.group_id
			WHERE peer_group.role != 'Everybody'
			  AND peer.group_id IN (
				SELECT own_group.id
				FROM auth_group own_group
				JOIN auth_membership own ON own.group_id = own_group.id
				WHERE own.user_id = ?
				  AND own_group.privilege = 'F'
				  AND own_group.role != 'UnaffectedProjects'
			  )
		) OR auth_user.id = ?)`
	return clause, []any{*p.UserID, *p.UserID}
}

// GetUsers lists the users visible to the caller.
func (oDb *DB) GetUsers(ctx context.Context, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getUsers: no select expressions")
	}
	authClause, args := usersAuthClause(p)
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") +
		" FROM auth_user WHERE " + authClause
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("auth_user.email, auth_user.id")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getUsers: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// GetUser fetches a single user visible to the caller, identified the way
// user_id_q() does — see userIdentClause.
func (oDb *DB) GetUser(ctx context.Context, idOrEmail string, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getUser: no select expressions")
	}

	identClause, identArgs, ok := userIdentClause(idOrEmail, p.UserID)
	if !ok {
		return nil, nil
	}

	authClause, args := usersAuthClause(p)
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") +
		" FROM auth_user WHERE " + authClause + " AND " + identClause
	args = append(args, identArgs...)
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("auth_user.email, auth_user.id")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getUser: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// userIdentClause returns the WHERE fragment selecting the user designated by
// idOrEmail, mirroring user_id_q() (init/models/rest/lib_users.py:25): an email
// when the identifier contains "@", the caller itself for "self", an
// auth_user.id otherwise.
//
// ok is false when the identifier cannot designate anybody: "self" without a
// caller identity, or a value that is neither an email nor a number. The python
// code compares such a value to the numeric id column, which never matches.
func userIdentClause(idOrEmail string, callerID *int64) (clause string, args []any, ok bool) {
	switch {
	case strings.Contains(idOrEmail, "@"):
		return "auth_user.email = ?", []any{idOrEmail}, true
	case idOrEmail == "self":
		if callerID == nil {
			return "", nil, false
		}
		return "auth_user.id = ?", []any{*callerID}, true
	default:
		id, err := strconv.ParseInt(idOrEmail, 10, 64)
		if err != nil {
			return "", nil, false
		}
		return "auth_user.id = ?", []any{id}, true
	}
}

// UserIDForPrefs resolves idOrEmail to an auth_user.id the caller is allowed to
// see. found is false when no such user exists or it is not visible, which
// rest_post_user_prefs answers with a 403 (init/models/rest/api_users.py:141).
func (oDb *DB) UserIDForPrefs(ctx context.Context, idOrEmail string, p ListParams) (int64, bool, error) {
	identClause, identArgs, ok := userIdentClause(idOrEmail, p.UserID)
	if !ok {
		return 0, false, nil
	}
	authClause, args := usersAuthClause(p)
	query := "SELECT auth_user.id FROM auth_user WHERE " + authClause + " AND " + identClause + " LIMIT 1"
	args = append(args, identArgs...)

	var id sql.NullInt64
	err := oDb.DB.QueryRowContext(ctx, query, args...).Scan(&id)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, nil
	case err != nil:
		return 0, false, fmt.Errorf("userIDForPrefs: %w", err)
	}
	return id.Int64, id.Valid, nil
}

// UserPrefs returns the raw preferences document of the user designated by
// idOrEmail, as stored in user_prefs.prefs.
//
// found is false when the user is unknown, not visible to the caller, has no
// user_prefs row, or a NULL document — rest_get_user_prefs returns an empty
// object for all of these alike (init/models/rest/api_users.py:112).
func (oDb *DB) UserPrefs(ctx context.Context, idOrEmail string, p ListParams) (string, bool, error) {
	identClause, identArgs, ok := userIdentClause(idOrEmail, p.UserID)
	if !ok {
		return "", false, nil
	}
	authClause, args := usersAuthClause(p)
	query := "SELECT user_prefs.prefs FROM auth_user" +
		" JOIN user_prefs ON user_prefs.user_id = auth_user.id" +
		" WHERE " + authClause + " AND " + identClause + " LIMIT 1"
	args = append(args, identArgs...)

	var prefs sql.NullString
	err := oDb.DB.QueryRowContext(ctx, query, args...).Scan(&prefs)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", false, nil
	case err != nil:
		return "", false, fmt.Errorf("userPrefs: %w", err)
	}
	return prefs.String, prefs.Valid, nil
}

// SaveUserPrefs stores the preferences document of a user, updating the existing
// user_prefs row or inserting one.
//
// The select-then-write mirrors web2py's update_or_insert: user_prefs.user_id
// carries no unique index, so ON DUPLICATE KEY cannot be relied upon. Call it on
// a transaction-scoped *DB so the read and the write cannot interleave.
func (oDb *DB) SaveUserPrefs(ctx context.Context, userID int64, prefs string) error {
	const selectQuery = "SELECT id FROM user_prefs WHERE user_id = ? LIMIT 1"
	var rowID sql.NullInt64
	err := oDb.DB.QueryRowContext(ctx, selectQuery, userID).Scan(&rowID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		const insertQuery = "INSERT INTO user_prefs (user_id, prefs) VALUES (?, ?)"
		if _, err := oDb.DB.ExecContext(ctx, insertQuery, userID, prefs); err != nil {
			return fmt.Errorf("saveUserPrefs insert: %w", err)
		}
	case err != nil:
		return fmt.Errorf("saveUserPrefs select: %w", err)
	default:
		const updateQuery = "UPDATE user_prefs SET prefs = ? WHERE id = ?"
		if _, err := oDb.DB.ExecContext(ctx, updateQuery, prefs, rowID.Int64); err != nil {
			return fmt.Errorf("saveUserPrefs update: %w", err)
		}
	}
	oDb.SetChange("user_prefs")
	return nil
}

// return the default app for a user
// if the user belongs to several apps, return the first one in alphabetical order
func (oDb *DB) UserDefaultApp(ctx context.Context, userID *int64) (string, error) {
	if userID == nil {
		return "", fmt.Errorf("userDefaultApp: missing user id")
	}

	groupIDs, err := oDb.userGroupIDs(ctx, userID)
	if err != nil {
		return "", fmt.Errorf("userDefaultApp: %w", err)
	}
	if len(groupIDs) == 0 {
		return "", nil
	}

	groupClause, args := inClause("apps_responsibles.group_id", toAnyInt64Slice(groupIDs))
	query := "SELECT apps.app FROM apps " +
		"JOIN apps_responsibles ON apps_responsibles.app_id = apps.id " +
		"WHERE " + groupClause + " AND apps.app <> '' AND apps.app IS NOT NULL " +
		"ORDER BY apps.app LIMIT 1"

	var app sql.NullString

	err = oDb.DB.QueryRowContext(ctx, query, args...).Scan(&app)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", nil
	case err != nil:
		return "", fmt.Errorf("userDefaultApp: %w", err)
	case !app.Valid || app.String == "":
		return "", nil
	default:
		return app.String, nil
	}
}

// return the list of group IDs a user belongs to.
func (oDb *DB) userGroupIDs(ctx context.Context, userID *int64) ([]int64, error) {
	if userID == nil {
		return nil, fmt.Errorf("userGroupIDs: missing user id")
	}

	const query = "SELECT auth_group.id FROM auth_group " +
		"JOIN auth_membership ON auth_membership.group_id = auth_group.id " +
		"WHERE auth_membership.user_id = ?"
	rows, err := oDb.DB.QueryContext(ctx, query, *userID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	groupIDs := []int64{}
	for rows.Next() {
		var groupID int64
		if err := rows.Scan(&groupID); err != nil {
			return nil, err
		}
		groupIDs = append(groupIDs, groupID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return groupIDs, nil
}

// return the primary group ID for a user
func (oDb *DB) UserPrimaryGroupID(ctx context.Context, userID int64) (int64, bool, error) {
	const query = "SELECT auth_group.id FROM auth_group " +
		"JOIN auth_membership ON auth_membership.group_id = auth_group.id " +
		"WHERE auth_membership.user_id = ? AND auth_membership.primary_group = 'T' " +
		"LIMIT 1"

	var groupID sql.NullInt64
	err := oDb.DB.QueryRowContext(ctx, query, userID).Scan(&groupID)
	return checkRow(err, groupID.Valid, groupID.Int64)
}

// return the private group ID for a user (group with role 'user_<userID>' and privilege 'F')
func (oDb *DB) UserPrivateGroupID(ctx context.Context, userID int64) (int64, bool, error) {
	const query = "SELECT auth_group.id FROM auth_group " +
		"JOIN auth_membership ON auth_membership.group_id = auth_group.id " +
		"WHERE auth_membership.user_id = ? AND auth_group.role LIKE 'user_%' AND auth_group.privilege = 'F' " +
		"LIMIT 1"
	var groupID sql.NullInt64
	err := oDb.DB.QueryRowContext(ctx, query, userID).Scan(&groupID)
	return checkRow(err, groupID.Valid, groupID.Int64)
}

// / return the default group ID for a user
// - first try to find a primary group
// - if not found, try to find a private group
// - if not found, try to find a group with privilege 'F' and role != 'Everybody'
func (oDb *DB) UserDefaultGroupID(ctx context.Context, userID int64) (int64, bool, error) {
	if gid, ok, err := oDb.UserPrimaryGroupID(ctx, userID); err != nil {
		return 0, false, err
	} else if ok {
		return gid, true, nil
	}

	if gid, ok, err := oDb.UserPrivateGroupID(ctx, userID); err != nil {
		return 0, false, err
	} else if ok {
		return gid, true, nil
	}

	const query = "SELECT auth_group.id FROM auth_group " +
		"JOIN auth_membership ON auth_membership.group_id = auth_group.id " +
		"WHERE auth_membership.user_id = ? AND auth_group.privilege = 'F' AND auth_group.role != 'Everybody' " +
		"ORDER BY auth_group.role LIMIT 1"

	var groupID sql.NullInt64
	err := oDb.DB.QueryRowContext(ctx, query, userID).Scan(&groupID)
	return checkRow(err, groupID.Valid, groupID.Int64)
}

// return the default group for a user
func (oDb *DB) UserDefaultGroup(ctx context.Context, userID int64) (string, bool, error) {
	gid, ok, err := oDb.UserDefaultGroupID(ctx, userID)
	if err != nil {
		return "", false, err
	}
	if !ok {
		return "", false, nil
	}

	const query = "SELECT role FROM auth_group WHERE id = ?"
	var role sql.NullString
	err = oDb.DB.QueryRowContext(ctx, query, gid).Scan(&role)
	return checkRow(err, role.Valid, role.String)
}
