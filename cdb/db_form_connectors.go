package cdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// FormConnector is a row of form_connectors: an external API the forms call
// through oc3, with what a Manager allows them to do there. Secrets holds the
// sealed JSON object of its secrets, nil when it has none.
type FormConnector struct {
	ID          int64
	Name        string
	Description string
	BaseURL     string
	Methods     []string
	Paths       []string
	Networks    []string
	Headers     map[string]string
	TimeoutS    int
	MaxBytes    int64
	AuthType    string
	Auth        map[string]string
	TLS         map[string]string
	Secrets     *SealedSecret
	UpdatedBy   string
	Created     time.Time
	Updated     time.Time
	// Groups are the ids of the groups the connector is published to.
	Groups []int64
}

// SealedSecret is a secret sealed with the safe master key: the ciphertext, the
// id of the master key and the wrapped per-record key.
type SealedSecret struct {
	Content    []byte
	KeyID      string
	WrappedKey []byte
}

const formConnectorColumns = "id, name, description, base_url, methods, paths_json, networks_json, headers_json," +
	" timeout_s, max_bytes, auth_type, auth_json, tls_json, secrets, secrets_key_id, secrets_wrapped_key," +
	" updated_by, created, updated"

func scanFormConnector(scan func(...any) error) (*FormConnector, error) {
	var (
		c                                 FormConnector
		methods, paths, networks, headers string
		auth, tlsJSON                     string
		secrets, wrapped                  []byte
		keyID                             sql.NullString
	)
	if err := scan(&c.ID, &c.Name, &c.Description, &c.BaseURL, &methods, &paths, &networks, &headers,
		&c.TimeoutS, &c.MaxBytes, &c.AuthType, &auth, &tlsJSON, &secrets, &keyID, &wrapped,
		&c.UpdatedBy, &c.Created, &c.Updated); err != nil {
		return nil, err
	}
	if methods != "" {
		c.Methods = strings.Split(methods, ",")
	}
	for _, f := range []struct {
		raw string
		dst any
	}{{paths, &c.Paths}, {networks, &c.Networks}, {headers, &c.Headers}, {auth, &c.Auth}, {tlsJSON, &c.TLS}} {
		if f.raw == "" {
			continue
		}
		if err := json.Unmarshal([]byte(f.raw), f.dst); err != nil {
			return nil, fmt.Errorf("form connector %d: %w", c.ID, err)
		}
	}
	if len(secrets) > 0 && keyID.Valid {
		c.Secrets = &SealedSecret{Content: secrets, KeyID: keyID.String, WrappedKey: wrapped}
	}
	return &c, nil
}

func jsonText(v any) (string, error) {
	b, err := json.Marshal(v)
	return string(b), err
}

// FormConnectors lists the connectors, by name, with their groups.
func (oDb *DB) FormConnectors(ctx context.Context) ([]FormConnector, error) {
	rows, err := oDb.DB.QueryContext(ctx, "SELECT "+formConnectorColumns+" FROM form_connectors ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []FormConnector
	for rows.Next() {
		c, err := scanFormConnector(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, *c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	groups, err := oDb.formConnectorGroups(ctx)
	if err != nil {
		return nil, err
	}
	for i := range out {
		out[i].Groups = groups[out[i].ID]
	}
	return out, nil
}

// FormConnectorByID returns the connector, nil when there is none.
func (oDb *DB) FormConnectorByID(ctx context.Context, id int64) (*FormConnector, error) {
	return oDb.formConnectorWhere(ctx, "id = ?", id)
}

// FormConnectorByName returns the connector, nil when there is none.
func (oDb *DB) FormConnectorByName(ctx context.Context, name string) (*FormConnector, error) {
	return oDb.formConnectorWhere(ctx, "name = ?", name)
}

func (oDb *DB) formConnectorWhere(ctx context.Context, where string, arg any) (*FormConnector, error) {
	row := oDb.DB.QueryRowContext(ctx, "SELECT "+formConnectorColumns+" FROM form_connectors WHERE "+where, arg)
	c, err := scanFormConnector(row.Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	groups, err := oDb.formConnectorGroups(ctx, c.ID)
	if err != nil {
		return nil, err
	}
	c.Groups = groups[c.ID]
	return c, nil
}

// formConnectorGroups returns the group ids of the connectors given, all when
// none is, by connector.
func (oDb *DB) formConnectorGroups(ctx context.Context, ids ...int64) (map[int64][]int64, error) {
	query := "SELECT connector_id, group_id FROM form_connector_groups"
	var args []any
	if len(ids) > 0 {
		query += " WHERE connector_id IN (?" + strings.Repeat(", ?", len(ids)-1) + ")"
		for _, id := range ids {
			args = append(args, id)
		}
	}
	rows, err := oDb.DB.QueryContext(ctx, query+" ORDER BY connector_id, group_id", args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[int64][]int64{}
	for rows.Next() {
		var cid, gid int64
		if err := rows.Scan(&cid, &gid); err != nil {
			return nil, err
		}
		out[cid] = append(out[cid], gid)
	}
	return out, rows.Err()
}

// FormConnectorNameTaken tells whether another connector than exceptID has the name.
func (oDb *DB) FormConnectorNameTaken(ctx context.Context, name string, exceptID int64) (bool, error) {
	var n int
	err := oDb.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM form_connectors WHERE name = ? AND id != ?", name, exceptID).Scan(&n)
	return n > 0, err
}

// formConnectorValues are the columns InsertFormConnector and
// UpdateFormConnector write, in formConnectorColumns order from name to
// updated_by.
func formConnectorValues(c FormConnector) ([]any, error) {
	paths, err := jsonText(nonNil(c.Paths))
	if err != nil {
		return nil, err
	}
	networks, err := jsonText(nonNil(c.Networks))
	if err != nil {
		return nil, err
	}
	headers, err := jsonText(nonNilMap(c.Headers))
	if err != nil {
		return nil, err
	}
	auth, err := jsonText(nonNilMap(c.Auth))
	if err != nil {
		return nil, err
	}
	tlsJSON, err := jsonText(nonNilMap(c.TLS))
	if err != nil {
		return nil, err
	}
	var secrets, keyID, wrapped any
	if c.Secrets != nil {
		secrets, keyID, wrapped = c.Secrets.Content, c.Secrets.KeyID, c.Secrets.WrappedKey
	}
	return []any{c.Name, c.Description, c.BaseURL, strings.Join(c.Methods, ","), paths, networks, headers,
		c.TimeoutS, c.MaxBytes, c.AuthType, auth, tlsJSON, secrets, keyID, wrapped, c.UpdatedBy}, nil
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func nonNilMap(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}

// InsertFormConnector creates the connector without secrets, which are sealed
// under the label of its id (SetFormConnectorSecrets), and its groups; it returns
// its id.
func (oDb *DB) InsertFormConnector(ctx context.Context, c FormConnector) (int64, error) {
	c.Secrets = nil
	values, err := formConnectorValues(c)
	if err != nil {
		return 0, err
	}
	result, err := oDb.DB.ExecContext(ctx, `INSERT INTO form_connectors
		(name, description, base_url, methods, paths_json, networks_json, headers_json, timeout_s, max_bytes,
		 auth_type, auth_json, tls_json, secrets, secrets_key_id, secrets_wrapped_key, updated_by, created, updated)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())`, values...)
	if err != nil {
		return 0, err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}
	if err := oDb.setFormConnectorGroups(ctx, id, c.Groups); err != nil {
		return 0, err
	}
	oDb.SetChange("form_connectors")
	return id, nil
}

// UpdateFormConnector replaces the connector, its secrets and its groups
// included, false when there is none.
func (oDb *DB) UpdateFormConnector(ctx context.Context, c FormConnector) (bool, error) {
	values, err := formConnectorValues(c)
	if err != nil {
		return false, err
	}
	result, err := oDb.DB.ExecContext(ctx, `UPDATE form_connectors SET
		name = ?, description = ?, base_url = ?, methods = ?, paths_json = ?, networks_json = ?, headers_json = ?,
		timeout_s = ?, max_bytes = ?, auth_type = ?, auth_json = ?, tls_json = ?,
		secrets = ?, secrets_key_id = ?, secrets_wrapped_key = ?, updated_by = ?, updated = NOW()
		WHERE id = ?`, append(values, c.ID)...)
	if err != nil {
		return false, err
	}
	if n, err := result.RowsAffected(); err != nil {
		return false, err
	} else if n == 0 {
		// An unchanged row counts as no row: tell it from a missing one.
		existing, err := oDb.FormConnectorByID(ctx, c.ID)
		if err != nil || existing == nil {
			return false, err
		}
	}
	if err := oDb.setFormConnectorGroups(ctx, c.ID, c.Groups); err != nil {
		return false, err
	}
	oDb.SetChange("form_connectors")
	return true, nil
}

func (oDb *DB) setFormConnectorGroups(ctx context.Context, id int64, groups []int64) error {
	if _, err := oDb.DB.ExecContext(ctx, "DELETE FROM form_connector_groups WHERE connector_id = ?", id); err != nil {
		return err
	}
	for _, gid := range groups {
		if _, err := oDb.DB.ExecContext(ctx, "INSERT INTO form_connector_groups (connector_id, group_id) VALUES (?, ?)", id, gid); err != nil {
			return err
		}
	}
	return nil
}

// DeleteFormConnector deletes the connector and its groups, false when there is
// none.
func (oDb *DB) DeleteFormConnector(ctx context.Context, id int64) (bool, error) {
	result, err := oDb.DB.ExecContext(ctx, "DELETE FROM form_connectors WHERE id = ?", id)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	if err != nil || n == 0 {
		return false, err
	}
	if _, err := oDb.DB.ExecContext(ctx, "DELETE FROM form_connector_groups WHERE connector_id = ?", id); err != nil {
		return false, err
	}
	oDb.SetChange("form_connectors")
	return true, nil
}

// FormConnectorUsable tells whether the form may call the connector: one of the
// groups responsible for the form is among those the connector is published to.
func (oDb *DB) FormConnectorUsable(ctx context.Context, connectorID, formID int64) (bool, error) {
	var n int
	err := oDb.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM form_connector_groups g
		JOIN forms_team_responsible r ON r.group_id = g.group_id
		WHERE g.connector_id = ? AND r.form_id = ?`, connectorID, formID).Scan(&n)
	return n > 0, err
}
