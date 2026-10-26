package serverhandlers

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/safe"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/egress"
)

// formConnectorSecretKeys are the secrets a connector may hold, sealed together
// as one JSON object.
var formConnectorSecretKeys = []string{"password", "token", "api_key", "client_secret", "client_key"}

var (
	formConnectorName  = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
	httpToken          = regexp.MustCompile(`^[A-Za-z0-9!#$%&'*+.^_|~-]+$`)
	formConnectorHeads = []string{"authorization", "proxy-authorization", "cookie", "host", "content-length",
		"transfer-encoding", "connection", "te", "upgrade", "keep-alive"}
)

const (
	formConnectorDefaultTimeout = 10
	formConnectorMaxTimeout     = 300
	formConnectorDefaultBytes   = 5 * 1024 * 1024
	formConnectorMinBytes       = 1024
	formConnectorMaxBytes       = 50 * 1024 * 1024
)

// checkConnectorsManager refuses anyone but a Manager: a connector decides what
// oc3 calls on the network, and with which credentials.
func checkConnectorsManager(c echo.Context) error {
	if !IsAuthByUser(c) {
		return httpErrorf(http.StatusUnauthorized, "user authentication required")
	}
	if !IsManager(c) {
		return httpErrorf(http.StatusForbidden, "Manager privilege required")
	}
	return nil
}

// formConnectorLabel is the label the secrets of a connector are sealed under:
// copied to another connector, they do not open.
func formConnectorLabel(id int64) string {
	return "form-connector:" + strconv.FormatInt(id, 10)
}

// openFormConnectorSecrets returns the secrets of a connector, none when it has
// none.
func (a *Api) openFormConnectorSecrets(conn *cdb.FormConnector) (map[string]string, error) {
	out := map[string]string{}
	if conn.Secrets == nil {
		return out, nil
	}
	if a.Safe == nil || a.Safe.Ring == nil {
		return nil, fmt.Errorf("the secrets of connector %s are sealed, but no safe master key is configured (safe.keys_file)", conn.Name)
	}
	plain, err := safe.OpenSecret(a.Safe.Ring, formConnectorLabel(conn.ID), conn.Secrets.Content, conn.Secrets.KeyID, conn.Secrets.WrappedKey)
	if err != nil {
		return nil, fmt.Errorf("the secrets of connector %s do not open: %w", conn.Name, err)
	}
	if err := json.Unmarshal(plain, &out); err != nil {
		return nil, fmt.Errorf("the secrets of connector %s are unreadable", conn.Name)
	}
	return out, nil
}

// sealFormConnectorSecrets seals the secrets of a connector, nil when it has none.
func (a *Api) sealFormConnectorSecrets(id int64, secrets map[string]string) (*cdb.SealedSecret, error) {
	if len(secrets) == 0 {
		return nil, nil
	}
	if a.Safe == nil || a.Safe.Ring.Active() == nil {
		return nil, httpErrorf(http.StatusBadRequest, "a connector with secrets needs the safe master key: configure safe.keys_file")
	}
	plain, err := json.Marshal(secrets)
	if err != nil {
		return nil, err
	}
	sealed, err := safe.SealSecret(a.Safe.Ring, formConnectorLabel(id), plain)
	if err != nil {
		return nil, err
	}
	return &cdb.SealedSecret{Content: sealed.Content, KeyID: sealed.KeyID, WrappedKey: sealed.WrappedKey}, nil
}

// readFormConnectorInput reads the body of a create or a replace, and the change
// of each secret it asks: a value to set, nil to remove; absent, kept.
func readFormConnectorInput(c echo.Context) (server.FormConnectorInput, map[string]*string, error) {
	var in server.FormConnectorInput
	raw, err := io.ReadAll(io.LimitReader(c.Request().Body, 1<<20))
	if err != nil {
		return in, nil, httpErrorf(http.StatusBadRequest, "cannot read the body: %s", err)
	}
	if err := json.Unmarshal(raw, &in); err != nil {
		return in, nil, httpErrorf(http.StatusBadRequest, "invalid body: %s", err)
	}
	// The generated type tells a null secret from an absent one no more: read
	// the object itself.
	var secretsOnly struct {
		Secrets map[string]*string `json:"secrets"`
	}
	if err := json.Unmarshal(raw, &secretsOnly); err != nil {
		return in, nil, httpErrorf(http.StatusBadRequest, "invalid secrets: %s", err)
	}
	for k := range secretsOnly.Secrets {
		if !slices.Contains(formConnectorSecretKeys, k) {
			return in, nil, httpErrorf(http.StatusBadRequest, "unknown secret %q: expected one of %s", k, strings.Join(formConnectorSecretKeys, ", "))
		}
	}
	return in, secretsOnly.Secrets, nil
}

// buildFormConnector checks a connector as given, its secrets being the current
// ones with the changes asked, and returns the row to write. Only the secrets
// its authentication and TLS settings use are kept.
func buildFormConnector(in server.FormConnectorInput, current map[string]string, changes map[string]*string) (cdb.FormConnector, map[string]string, error) {
	bad := func(format string, args ...any) (cdb.FormConnector, map[string]string, error) {
		return cdb.FormConnector{}, nil, httpErrorf(http.StatusBadRequest, format, args...)
	}
	conn := cdb.FormConnector{Name: strings.TrimSpace(in.Name), TimeoutS: formConnectorDefaultTimeout, MaxBytes: formConnectorDefaultBytes}
	if !formConnectorName.MatchString(conn.Name) {
		return bad("name: 1 to 64 letters, digits, \".\", \"_\" or \"-\" expected")
	}
	if in.Description != nil {
		conn.Description = strings.TrimSpace(*in.Description)
	}

	u, err := neturl.Parse(strings.TrimSpace(in.BaseUrl))
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		return bad("base_url: an absolute http or https url expected")
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return bad("base_url: no credentials, query or fragment: they go in the authentication settings")
	}
	u.Path = strings.TrimRight(u.Path, "/")
	u.RawPath = ""
	conn.BaseURL = u.String()

	conn.Methods = []string{http.MethodGet}
	if in.Methods != nil && len(*in.Methods) > 0 {
		conn.Methods = nil
		for _, m := range *in.Methods {
			switch string(m) {
			case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
				if !slices.Contains(conn.Methods, string(m)) {
					conn.Methods = append(conn.Methods, string(m))
				}
			default:
				return bad("methods: %q is none of GET, POST, PUT, PATCH, DELETE", m)
			}
		}
	}

	if in.Paths != nil {
		for _, p := range *in.Paths {
			p = strings.TrimSpace(p)
			if p == "/" {
				continue
			}
			p = strings.TrimRight(p, "/")
			if !isConnectorPath(p) {
				return bad("paths: %q is no path prefix: it starts with /, without query, fragment, backslash, empty or dot segment", p)
			}
			conn.Paths = append(conn.Paths, p)
		}
	}
	if in.Networks != nil {
		if _, err := egress.ParsePrefixes(*in.Networks); err != nil {
			return bad("networks: %s", err)
		}
		conn.Networks = *in.Networks
	}
	if in.Headers != nil {
		conn.Headers = map[string]string{}
		for k, v := range *in.Headers {
			if !httpToken.MatchString(k) || slices.Contains(formConnectorHeads, strings.ToLower(k)) {
				return bad("headers: %q is not a header the connector may set", k)
			}
			if strings.ContainsAny(v, "\r\n") {
				return bad("headers: the value of %s holds a line break", k)
			}
			conn.Headers[k] = v
		}
	}
	if in.Timeout != nil {
		if *in.Timeout < 1 || *in.Timeout > formConnectorMaxTimeout {
			return bad("timeout: 1 to %d seconds", formConnectorMaxTimeout)
		}
		conn.TimeoutS = *in.Timeout
	}
	if in.MaxBytes != nil {
		if *in.MaxBytes < formConnectorMinBytes || *in.MaxBytes > formConnectorMaxBytes {
			return bad("max_bytes: %d to %d", formConnectorMinBytes, formConnectorMaxBytes)
		}
		conn.MaxBytes = *in.MaxBytes
	}

	secrets := map[string]string{}
	for k, v := range current {
		secrets[k] = v
	}
	for k, v := range changes {
		if v == nil || *v == "" {
			delete(secrets, k)
		} else {
			secrets[k] = *v
		}
	}
	keep := map[string]bool{}
	need := func(key string) error {
		if secrets[key] == "" {
			return httpErrorf(http.StatusBadRequest, "secrets.%s is required by the authentication %s", key, in.Auth.Type)
		}
		keep[key] = true
		return nil
	}
	str := func(p *string) string {
		if p == nil {
			return ""
		}
		return strings.TrimSpace(*p)
	}
	conn.AuthType = string(in.Auth.Type)
	conn.Auth = map[string]string{}
	switch in.Auth.Type {
	case server.FormConnectorAuthNone:
	case server.FormConnectorAuthBasic:
		if conn.Auth["username"] = str(in.Auth.Username); conn.Auth["username"] == "" {
			return bad("auth.username is required by the authentication basic")
		}
		if err := need("password"); err != nil {
			return cdb.FormConnector{}, nil, err
		}
	case server.FormConnectorAuthBearer:
		if err := need("token"); err != nil {
			return cdb.FormConnector{}, nil, err
		}
	case server.FormConnectorAuthApiKey:
		where := in.Auth.In
		if where == nil || (*where != server.FormConnectorApiKeyInHeader && *where != server.FormConnectorApiKeyInQuery) {
			return bad("auth.in: header or query")
		}
		conn.Auth["in"] = string(*where)
		conn.Auth["name"] = str(in.Auth.Name)
		if !httpToken.MatchString(conn.Auth["name"]) || slices.Contains(formConnectorHeads, strings.ToLower(conn.Auth["name"])) {
			return bad("auth.name: the header or query parameter carrying the key")
		}
		if err := need("api_key"); err != nil {
			return cdb.FormConnector{}, nil, err
		}
	case server.FormConnectorAuthOAuth2ClientCredentials:
		tokenURL, err := neturl.Parse(str(in.Auth.TokenUrl))
		if err != nil || tokenURL.Scheme != "https" || tokenURL.Host == "" {
			return bad("auth.token_url: an absolute https url expected")
		}
		conn.Auth["token_url"] = tokenURL.String()
		if conn.Auth["client_id"] = str(in.Auth.ClientId); conn.Auth["client_id"] == "" {
			return bad("auth.client_id is required by the authentication oauth2_client_credentials")
		}
		conn.Auth["scopes"] = str(in.Auth.Scopes)
		conn.Auth["audience"] = str(in.Auth.Audience)
		conn.Auth["auth_style"] = string(server.FormConnectorAuthStyleHeader)
		if in.Auth.AuthStyle != nil {
			if *in.Auth.AuthStyle != server.FormConnectorAuthStyleHeader && *in.Auth.AuthStyle != server.FormConnectorAuthStyleParams {
				return bad("auth.auth_style: header or params")
			}
			conn.Auth["auth_style"] = string(*in.Auth.AuthStyle)
		}
		if err := need("client_secret"); err != nil {
			return cdb.FormConnector{}, nil, err
		}
	default:
		return bad("auth.type: %q is none of none, basic, bearer, api_key, oauth2_client_credentials", in.Auth.Type)
	}

	conn.TLS = map[string]string{}
	if in.Tls != nil {
		if ca := str(in.Tls.CaBundle); ca != "" {
			if !x509.NewCertPool().AppendCertsFromPEM([]byte(ca)) {
				return bad("tls.ca_bundle: no PEM certificate found")
			}
			conn.TLS["ca_bundle"] = ca
		}
		if name := str(in.Tls.ServerName); name != "" {
			conn.TLS["server_name"] = name
		}
		if cert := str(in.Tls.ClientCert); cert != "" {
			if err := need("client_key"); err != nil {
				return bad("tls.client_cert needs its key, secrets.client_key")
			}
			if _, err := tls.X509KeyPair([]byte(cert), []byte(secrets["client_key"])); err != nil {
				return bad("tls.client_cert and secrets.client_key do not make a key pair: %s", err)
			}
			conn.TLS["client_cert"] = cert
		}
	}
	if u.Scheme == "http" && (conn.AuthType != string(server.FormConnectorAuthNone) || conn.TLS["client_cert"] != "") {
		return bad("base_url: https is required for a connector sending credentials")
	}

	kept := map[string]string{}
	for k := range keep {
		kept[k] = secrets[k]
	}
	return conn, kept, nil
}

// isConnectorPath tells a path a connector may call: absolute, without query,
// fragment, backslash, empty or dot segment ("." or "..", escaped or not).
func isConnectorPath(p string) bool {
	if !strings.HasPrefix(p, "/") || strings.ContainsAny(p, "?#\\") {
		return false
	}
	for _, seg := range strings.Split(p[1:], "/") {
		if seg == "" {
			return false
		}
		if unescaped, err := neturl.PathUnescape(seg); err != nil || unescaped == "." || unescaped == ".." {
			return false
		}
	}
	return true
}

// formConnectorAPI is a connector as the API shows it: its secrets told set or
// not, never given.
func (a *Api) formConnectorAPI(ctx context.Context, conn *cdb.FormConnector) (server.FormConnector, error) {
	out := server.FormConnector{
		Id:          conn.ID,
		Name:        conn.Name,
		Description: conn.Description,
		BaseUrl:     conn.BaseURL,
		Paths:       nonNilStrings(conn.Paths),
		Networks:    nonNilStrings(conn.Networks),
		Headers:     conn.Headers,
		Timeout:     conn.TimeoutS,
		MaxBytes:    conn.MaxBytes,
		UpdatedBy:   conn.UpdatedBy,
		Created:     conn.Created.Format(time.RFC3339),
		Updated:     conn.Updated.Format(time.RFC3339),
	}
	if out.Headers == nil {
		out.Headers = map[string]string{}
	}
	for _, m := range conn.Methods {
		out.Methods = append(out.Methods, server.FormConnectorMethod(m))
	}
	out.Auth.Type = server.FormConnectorAuthType(conn.AuthType)
	opt := func(k string) *string {
		if v := conn.Auth[k]; v != "" {
			return &v
		}
		return nil
	}
	out.Auth.Username, out.Auth.Name, out.Auth.TokenUrl = opt("username"), opt("name"), opt("token_url")
	out.Auth.ClientId, out.Auth.Scopes, out.Auth.Audience = opt("client_id"), opt("scopes"), opt("audience")
	if v := conn.Auth["in"]; v != "" {
		in := server.FormConnectorAuthIn(v)
		out.Auth.In = &in
	}
	if v := conn.Auth["auth_style"]; v != "" {
		style := server.FormConnectorAuthAuthStyle(v)
		out.Auth.AuthStyle = &style
	}
	tlsOpt := func(k string) *string {
		if v := conn.TLS[k]; v != "" {
			return &v
		}
		return nil
	}
	out.Tls = server.FormConnectorTLS{CaBundle: tlsOpt("ca_bundle"), ClientCert: tlsOpt("client_cert"), ServerName: tlsOpt("server_name")}
	secrets, err := a.openFormConnectorSecrets(conn)
	if err != nil {
		return out, err
	}
	out.Secrets.Password = secrets["password"] != ""
	out.Secrets.Token = secrets["token"] != ""
	out.Secrets.ApiKey = secrets["api_key"] != ""
	out.Secrets.ClientSecret = secrets["client_secret"] != ""
	out.Secrets.ClientKey = secrets["client_key"] != ""
	out.Groups = []struct {
		Id   int64  `json:"id"`
		Role string `json:"role"`
	}{}
	for _, gid := range conn.Groups {
		role := ""
		if g, found, err := a.ODB.AuthGroupByIDOrRole(ctx, strconv.FormatInt(gid, 10)); err != nil {
			return out, err
		} else if found {
			role = g.Role
		}
		out.Groups = append(out.Groups, struct {
			Id   int64  `json:"id"`
			Role string `json:"role"`
		}{Id: gid, Role: role})
	}
	return out, nil
}

func nonNilStrings(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// formConnectorAudit writes an audit line of a change of connector, never with
// a secret.
func (a *Api) formConnectorAudit(c echo.Context, action, format string, dict map[string]any) {
	userEmail, _ := c.Get(XUserEmail).(string)
	entry := cdb.LogEntry{Action: action, User: userEmail, Fmt: format, Dict: dict, Level: "info"}
	if err := a.ODB.Log(c.Request().Context(), entry); err != nil {
		echolog.GetLogHandler(c, action).Error("cannot write audit log", "error", err)
	}
}

// GetFormConnectors handles GET /form_connectors.
func (a *Api) GetFormConnectors(c echo.Context) error {
	if err := requireFormsManager(c); err != nil {
		return httpProblem(c, err)
	}
	ctx := c.Request().Context()
	conns, err := a.ODB.FormConnectors(ctx)
	if err != nil {
		return httpProblem(c, httpInternal(echolog.GetLogHandler(c, "GetFormConnectors"), "cannot read the connectors", err))
	}
	data := make([]server.FormConnector, 0, len(conns))
	for i := range conns {
		item, err := a.formConnectorAPI(ctx, &conns[i])
		if err != nil {
			return httpProblem(c, httpInternal(echolog.GetLogHandler(c, "GetFormConnectors"), "cannot read the connector "+conns[i].Name, err))
		}
		data = append(data, item)
	}
	return c.JSON(http.StatusOK, map[string]any{"data": data})
}

// GetFormConnector handles GET /form_connectors/{connector_id}.
func (a *Api) GetFormConnector(c echo.Context, connectorId int64) error {
	if err := requireFormsManager(c); err != nil {
		return httpProblem(c, err)
	}
	return a.formConnectorResponse(c, connectorId)
}

func (a *Api) formConnectorResponse(c echo.Context, id int64) error {
	ctx := c.Request().Context()
	log := echolog.GetLogHandler(c, "GetFormConnector")
	conn, err := a.ODB.FormConnectorByID(ctx, id)
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot read the connector", err))
	}
	if conn == nil {
		return httpProblem(c, httpErrorf(http.StatusNotFound, "connector %d not found", id))
	}
	out, err := a.formConnectorAPI(ctx, conn)
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot read the connector", err))
	}
	return c.JSON(http.StatusOK, out)
}

// checkFormConnectorGroups refuses a group id that names no group.
func (a *Api) checkFormConnectorGroups(ctx context.Context, ids []int64) error {
	for _, id := range ids {
		if _, found, err := a.ODB.AuthGroupByIDOrRole(ctx, strconv.FormatInt(id, 10)); err != nil {
			return err
		} else if !found {
			return httpErrorf(http.StatusBadRequest, "groups: group %d not found", id)
		}
	}
	return nil
}

// PostFormConnectors handles POST /form_connectors.
func (a *Api) PostFormConnectors(c echo.Context) error {
	if err := checkConnectorsManager(c); err != nil {
		return httpProblem(c, err)
	}
	return a.saveFormConnector(c, 0)
}

// PutFormConnector handles PUT /form_connectors/{connector_id}.
func (a *Api) PutFormConnector(c echo.Context, connectorId int64) error {
	if err := checkConnectorsManager(c); err != nil {
		return httpProblem(c, err)
	}
	return a.saveFormConnector(c, connectorId)
}

// saveFormConnector creates the connector given (id 0) or replaces one, its
// secrets sealed under the label of its id, in one transaction.
func (a *Api) saveFormConnector(c echo.Context, id int64) error {
	const name = "PutFormConnector"
	ctx := c.Request().Context()
	log := echolog.GetLogHandler(c, name)
	in, changes, err := readFormConnectorInput(c)
	if err != nil {
		return httpProblem(c, err)
	}
	current := map[string]string{}
	if id != 0 {
		existing, err := a.ODB.FormConnectorByID(ctx, id)
		if err != nil {
			return httpProblem(c, httpInternal(log, "cannot read the connector", err))
		}
		if existing == nil {
			return httpProblem(c, httpErrorf(http.StatusNotFound, "connector %d not found", id))
		}
		if current, err = a.openFormConnectorSecrets(existing); err != nil {
			return httpProblem(c, httpInternal(log, "cannot open the connector secrets", err))
		}
	}
	conn, secrets, err := buildFormConnector(in, current, changes)
	if err != nil {
		return httpProblem(c, err)
	}
	if in.Groups != nil {
		conn.Groups = *in.Groups
	}
	if err := a.checkFormConnectorGroups(ctx, conn.Groups); err != nil {
		return httpProblem(c, err)
	}
	if taken, err := a.ODB.FormConnectorNameTaken(ctx, conn.Name, id); err != nil {
		return httpProblem(c, httpInternal(log, "cannot check the connector name", err))
	} else if taken {
		return httpProblem(c, httpErrorf(http.StatusConflict, "a connector named %s already exists", conn.Name))
	}
	if caller, err := a.formCaller(ctx, c); err == nil {
		conn.UpdatedBy = caller.name
	}

	tx, _, _, err := a.ODB.BeginTxWithControl(ctx, log, &sql.TxOptions{})
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot start transaction", err))
	}
	defer func() { _ = tx.Rollback() }()
	if id == 0 {
		if id, err = tx.InsertFormConnector(ctx, conn); err != nil {
			return httpProblem(c, httpInternal(log, "cannot create the connector", err))
		}
	}
	conn.ID = id
	if conn.Secrets, err = a.sealFormConnectorSecrets(id, secrets); err != nil {
		return httpProblem(c, err)
	}
	if _, err := tx.UpdateFormConnector(ctx, conn); err != nil {
		return httpProblem(c, httpInternal(log, "cannot save the connector", err))
	}
	if err := tx.Commit(); err != nil {
		return httpProblem(c, httpInternal(log, "cannot save the connector", err))
	}
	changed := make([]string, 0, len(changes))
	for k := range changes {
		changed = append(changed, k)
	}
	slices.Sort(changed)
	a.formConnectorAudit(c, "form.connector.change", "saved connector %(name)s (%(base_url)s, auth %(auth)s, secrets changed: %(secrets)s)",
		map[string]any{"name": conn.Name, "base_url": conn.BaseURL, "auth": conn.AuthType, "secrets": strings.Join(changed, ", ")})
	return a.formConnectorResponse(c, id)
}

// DeleteFormConnector handles DELETE /form_connectors/{connector_id}.
func (a *Api) DeleteFormConnector(c echo.Context, connectorId int64) error {
	if err := checkConnectorsManager(c); err != nil {
		return httpProblem(c, err)
	}
	ctx := c.Request().Context()
	log := echolog.GetLogHandler(c, "DeleteFormConnector")
	conn, err := a.ODB.FormConnectorByID(ctx, connectorId)
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot read the connector", err))
	}
	if conn == nil {
		return httpProblem(c, httpErrorf(http.StatusNotFound, "connector %d not found", connectorId))
	}
	if _, err := a.ODB.DeleteFormConnector(ctx, connectorId); err != nil {
		return httpProblem(c, httpInternal(log, "cannot delete the connector", err))
	}
	a.formConnectorAudit(c, "form.connector.delete", "deleted connector %(name)s", map[string]any{"name": conn.Name})
	return c.NoContent(http.StatusNoContent)
}

// PostFormConnectorTest handles POST /form_connectors/{connector_id}/test.
func (a *Api) PostFormConnectorTest(c echo.Context, connectorId int64) error {
	if err := checkConnectorsManager(c); err != nil {
		return httpProblem(c, err)
	}
	ctx := c.Request().Context()
	log := echolog.GetLogHandler(c, "PostFormConnectorTest")
	var in server.FormConnectorTestInput
	if err := c.Bind(&in); err != nil {
		return httpProblem(c, httpErrorf(http.StatusBadRequest, "invalid body: %s", err))
	}
	conn, err := a.ODB.FormConnectorByID(ctx, connectorId)
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot read the connector", err))
	}
	if conn == nil {
		return httpProblem(c, httpErrorf(http.StatusNotFound, "connector %d not found", connectorId))
	}
	method := http.MethodGet
	if in.Method != nil {
		method = string(*in.Method)
	}
	query := neturl.Values{}
	if in.Query != nil {
		for k, v := range *in.Query {
			query.Set(k, v)
		}
	}
	started := time.Now()
	resp, err := a.connectorDo(ctx, conn, connectorRequest{Method: method, Path: strings.TrimSpace(in.Path), Query: query})
	out := server.FormConnectorTestResult{DurationMs: int(time.Since(started).Milliseconds())}
	var refusal *connectorRefusal
	switch {
	case asConnectorRefusal(err, &refusal):
		return httpProblem(c, httpErrorf(http.StatusBadRequest, "%s", refusal.msg))
	case err != nil:
		text := err.Error()
		out.Error = &text
		if resp != nil {
			out.Url = resp.URL
		}
	default:
		out.Url = resp.URL
		out.Status = &resp.Status
		out.ContentType = &resp.ContentType
		body := string(resp.Raw)
		if len(body) > 4096 {
			body = body[:4096]
		}
		out.Body = &body
	}
	a.formConnectorAudit(c, "form.connector.test", "tested connector %(name)s: %(method)s %(path)s", map[string]any{
		"name": conn.Name, "method": method, "path": in.Path,
	})
	return c.JSON(http.StatusOK, out)
}
