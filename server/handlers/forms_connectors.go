package serverhandlers

import (
	"context"
	"errors"
	"net/http"
	neturl "net/url"
	"strconv"
	"strings"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
)

// The inputs and the rest outputs of a form calling an external API name its
// connector (Connector) and a path relative to its base url (Function), as in:
//
//	Connector: cmdb
//	Function: /servers/#node_id/interfaces
//	Args: ["status = active"]
//	ResultPath: items
//
// oc3 builds the call from the form as stored: the user gives the values of its
// references, never the url.

// formConnector returns the connector a form names, once checked the form may
// call it: published to one of the groups responsible for the form.
func (s *formSubmission) formConnector(ctx context.Context, name string) (*cdb.FormConnector, error) {
	conn, err := s.a.ODB.FormConnectorByName(ctx, name)
	if err != nil {
		return nil, httpInternal(s.log, "cannot read the connector", err)
	}
	if conn == nil {
		return nil, httpErrorf(http.StatusBadRequest, "the form names the connector %s, which does not exist", name)
	}
	if s.formID < 0 {
		return nil, httpErrorf(http.StatusBadRequest, "an internal form calls no connector")
	}
	usable, err := s.a.ODB.FormConnectorUsable(ctx, conn.ID, s.formID)
	if err != nil {
		return nil, httpInternal(s.log, "cannot check the connector publication", err)
	}
	if !usable {
		return nil, httpErrorf(http.StatusForbidden, "the connector %s is not published to a group responsible for the form %s", name, s.formName)
	}
	return conn, nil
}

// connectorPath builds the path of a call from a Function relative to the base
// url of a connector, each value escaped as a path segment (formRestArgs).
func (s *formSubmission) connectorPath(fn string, data any) (string, error) {
	fn = strings.ReplaceAll(strings.TrimSpace(fn), "#user_id", strconv.FormatInt(s.caller.id, 10))
	if !strings.HasPrefix(fn, "/") {
		return "", httpErrorf(http.StatusBadRequest, "the Function of a connector call is a path relative to its base url, starting with /: %s", fn)
	}
	args, err := formRestArgs(fn, data)
	if err != nil {
		return "", httpErrorf(http.StatusBadRequest, "cannot build the path %s: missing %s", fn, err)
	}
	return "/" + strings.Join(args, "/"), nil
}

// connectorQuery builds the query of a call from the "key = value" Args of an
// input, their references replaced.
func (s *formSubmission) connectorQuery(input map[string]any, data map[string]any) (neturl.Values, error) {
	query := neturl.Values{}
	userID := strconv.FormatInt(s.caller.id, 10)
	for _, entry := range defList(input, "Args") {
		es, ok := entry.(string)
		if !ok {
			continue
		}
		k, v, found := strings.Cut(strings.ReplaceAll(es, "#user_id", userID), "=")
		if !found {
			continue
		}
		v = formDereference(strings.TrimSpace(v), data, "")
		if formRef.MatchString(v) {
			return nil, httpErrorf(http.StatusBadRequest, "cannot build the argument %s: missing %s", strings.TrimSpace(k), v)
		}
		query.Add(strings.Join(strings.Fields(k), ""), v)
	}
	return query, nil
}

// connectorCandidates fetches the candidates of an input calling a connector: a
// GET of its Function, its Args as query, the list read at its ResultPath. A
// failure of the external API is a 502.
func (s *formSubmission) connectorCandidates(ctx context.Context, input map[string]any, data map[string]any) ([]any, error) {
	conn, err := s.formConnector(ctx, defString(input, "Connector"))
	if err != nil {
		return nil, err
	}
	path, err := s.connectorPath(defString(input, "Function"), data)
	if err != nil {
		return nil, err
	}
	query, err := s.connectorQuery(input, data)
	if err != nil {
		return nil, err
	}
	resp, err := s.a.connectorDo(ctx, conn, connectorRequest{Method: http.MethodGet, Path: path, Query: query})
	var refusal *connectorRefusal
	switch {
	case asConnectorRefusal(err, &refusal):
		return nil, httpErrorf(http.StatusBadRequest, "%s", refusal.msg)
	case err != nil:
		return nil, httpErrorf(http.StatusBadGateway, "connector %s: %s", conn.Name, err)
	case resp.Status >= 300:
		return nil, httpErrorf(http.StatusBadGateway, "%s", connectorErrorText(conn, resp))
	case resp.Body == nil:
		return nil, httpErrorf(http.StatusBadGateway, "connector %s answered no JSON (%s)", conn.Name, resp.ContentType)
	}
	rows, err := connectorRows(resp.Body, defString(input, "ResultPath"))
	if err != nil {
		return nil, httpErrorf(http.StatusBadGateway, "connector %s: %s", conn.Name, err)
	}
	return rows, nil
}

// PostFormInputCandidates handles POST /forms/{form_id}/inputs/{input_id}/candidates.
func (a *Api) PostFormInputCandidates(c echo.Context, formId int, inputId string) error {
	ctx := c.Request().Context()
	log := echolog.GetLogHandler(c, "PostFormInputCandidates")
	var body server.FormCandidatesInput
	if err := c.Bind(&body); err != nil {
		return httpProblem(c, httpErrorf(http.StatusBadRequest, "invalid body: %s", err))
	}
	s := &formSubmission{a: a, log: log, formID: int64(formId)}
	var err error
	if s.caller, err = a.formCaller(ctx, c); err != nil {
		return httpProblem(c, httpInternal(log, "cannot read the caller", err))
	}
	if err := s.loadForm(ctx, c); err != nil {
		return httpProblem(c, err)
	}
	var input map[string]any
	for _, candidate := range defMaps(s.definition, "Inputs") {
		if defString(candidate, "Id") == inputId {
			input = candidate
			break
		}
	}
	if input == nil {
		return httpProblem(c, httpErrorf(http.StatusNotFound, "the form %s has no input %s", s.formName, inputId))
	}
	if defString(input, "Connector") == "" {
		return httpProblem(c, httpErrorf(http.StatusBadRequest, "the input %s calls no connector", inputId))
	}
	data := map[string]any{}
	if body.Data != nil {
		data = *body.Data
	}
	rows, err := s.connectorCandidates(ctx, input, data)
	if err != nil {
		return httpProblem(c, err)
	}
	return c.JSON(http.StatusOK, map[string]any{"data": rows})
}

// validateConnectorCandidates checks the values of a strict input calling a
// connector are among its candidates, compared on key ("id", "#id").
func (s *formSubmission) validateConnectorCandidates(ctx context.Context, input map[string]any, data map[string]any, inputID, key string, vals []any, mandatory bool) error {
	rows, err := s.connectorCandidates(ctx, input, data)
	if err != nil {
		var he *httpError
		if errors.As(err, &he) {
			return httpErrorf(http.StatusBadRequest, "cannot verify the submitted value is a valid candidate: %s", he.msg)
		}
		return err
	}
	key = strings.TrimLeft(key, "#")
	var candidates []string
	for _, row := range rows {
		cv, err := formGetVal(legacyRowShape(row), key)
		if err != nil {
			continue
		}
		candidates = append(candidates, formValueText(cv))
	}
	for _, v := range vals {
		keyVal := v
		if m, ok := v.(map[string]any); ok {
			keyVal = m[key]
		}
		if isFalsy(keyVal) && !mandatory {
			continue
		}
		if !containsString(candidates, formValueText(keyVal)) {
			return httpErrorf(http.StatusBadRequest, "Input '%s' value '%s' not in the candidates of connector %s",
				inputID, formValueText(keyVal), defString(input, "Connector"))
		}
	}
	return nil
}

// outputConnector makes a rest output calling a connector: its Handler method
// on its Function, the output data as JSON body. The answer, at ResultPath if
// given, becomes the output's data; a failed answer, its error.
func (s *formSubmission) outputConnector(ctx context.Context, output map[string]any, method string, vars any) error {
	outputID := defString(output, "Id")
	conn, err := s.formConnector(ctx, defString(output, "Connector"))
	if err != nil {
		return err
	}
	path, err := s.connectorPath(defString(output, "Function"), s.data)
	if err != nil {
		return err
	}
	resp, err := s.a.connectorDo(ctx, conn, connectorRequest{Method: method, Path: path, Body: vars})
	if err != nil {
		return err
	}
	s.formLog(ctx, outputID, 0, "form.submit", "connector %(connector)s answered %(status)s",
		map[string]any{"connector": conn.Name, "status": resp.Status})
	if resp.Status >= 300 {
		return errors.New(connectorErrorText(conn, resp))
	}
	var result any = resp.Body
	if path := defString(output, "ResultPath"); path != "" && resp.Body != nil {
		cur := resp.Body
		for _, k := range strings.Split(path, ".") {
			if m, ok := cur.(map[string]any); ok {
				cur = m[k]
			} else {
				cur = nil
			}
		}
		result = cur
	}
	if result != nil {
		s.mu.Lock()
		ensureMap(s.results, "outputs")[outputID] = result
		s.mu.Unlock()
	}
	return nil
}
