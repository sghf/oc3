package serverhandlers

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/spf13/viper"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// internalFormsJSON holds the forms the historical collector defines in code
// (lib_forms_internal.py), designated by a negative id.
//
//go:embed forms_internal.json
var internalFormsJSON []byte

type internalForm struct {
	ID         int64          `json:"id"`
	Name       string         `json:"form_name"`
	Folder     string         `json:"form_folder"`
	Type       string         `json:"form_type"`
	Definition map[string]any `json:"form_definition"`
}

var (
	internalFormsOnce sync.Once
	internalForms     map[int64]internalForm
)

// internalFormByID returns an internal form, as get_internal_form() does. The
// definition is decoded on every call: the submission assigns output ids in it.
func internalFormByID(id int64) (internalForm, bool) {
	internalFormsOnce.Do(func() {
		var list []internalForm
		if err := json.Unmarshal(internalFormsJSON, &list); err != nil {
			panic(fmt.Sprintf("forms_internal.json: %s", err))
		}
		internalForms = make(map[int64]internalForm, len(list))
		for _, f := range list {
			internalForms[f.ID] = f
		}
	})
	f, ok := internalForms[id]
	if !ok {
		return f, false
	}
	b, _ := json.Marshal(f.Definition)
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.UseNumber()
	var definition map[string]any
	_ = decoder.Decode(&definition)
	f.Definition = definition
	return f, true
}

// formSubmission is one PUT /forms/{form_id}: the form, the submitted data, the
// caller and the results structure, stored in form_output_results and updated
// as the outputs run. Outputs may run after the request has been answered, for
// an Async form: the submission carries all it needs.
type formSubmission struct {
	a          *Api
	log        *slog.Logger
	formID     int64
	formName   string
	form       *cdb.Form // nil for an internal form
	definition map[string]any
	data       any
	prevWfid   *int64
	caller     formCaller
	nodeID     string
	userEmail  string
	auth       string // Authorization header, for the calls of rest outputs
	// cookie is the OIDC session cookie of the submitter, replayed like auth on
	// the local calls of rest outputs: a session-authenticated submitter has no
	// Authorization header to replay.
	cookie string

	mu        sync.Mutex
	results   map[string]any
	resultsID int64
}

// PutForm handles PUT /forms/{form_id}: submit a form.
func (a *Api) PutForm(c echo.Context, formId int) error {
	log := echolog.GetLogHandler(c, "PutForm")
	ctx := c.Request().Context()

	entries, isList, err := decodeEntries(c)
	if err != nil {
		return httpProblem(c, err)
	}
	if isList {
		return JSONProblem(c, http.StatusBadRequest, "expecting an object with the data and prev_wfid keys")
	}
	body := entries[0]

	s := &formSubmission{a: a, log: log, formID: int64(formId), auth: c.Request().Header.Get("Authorization")}
	if a.OIDC != nil {
		if cookie, err := c.Cookie(a.OIDC.SessionCookieName()); err == nil && cookie.Value != "" {
			s.cookie = cookie.Name + "=" + cookie.Value
		}
	}
	s.userEmail, _ = c.Get(XUserEmail).(string)
	s.nodeID, _ = c.Get(XNodeID).(string)
	if s.caller, err = a.formCaller(ctx, c); err != nil {
		return httpProblem(c, httpInternal(log, "cannot read the caller", err))
	}

	// The data may come as its JSON string form, as a posted form variable.
	s.data = body["data"]
	if text, ok := s.data.(string); ok {
		decoder := json.NewDecoder(strings.NewReader(text))
		decoder.UseNumber()
		if err := decoder.Decode(&s.data); err != nil {
			return JSONProblemf(c, http.StatusBadRequest, "unparsable form data: %s", text)
		}
	}
	if prev, ok := entryString(body, "prev_wfid"); ok && prev != "" && prev != "None" {
		id, err := strconv.ParseInt(prev, 10, 64)
		if err != nil {
			return JSONProblemf(c, http.StatusBadRequest, "invalid prev_wfid %q", prev)
		}
		s.prevWfid = &id
	}

	if err := s.loadForm(ctx, c); err != nil {
		return httpProblem(c, err)
	}

	s.results = map[string]any{
		"form_id":        s.formID,
		"submitted_data": s.data,
		"outputs_order":  []any{},
		"request_data":   map[string]any{},
		"outputs":        map[string]any{},
		"log":            map[string]any{},
		"returncode":     0,
		"status":         "QUEUED",
	}

	if err := s.authorizeContinuation(ctx, c); err != nil {
		return httpProblem(c, err)
	}

	if done, err := s.workflowStepDone(ctx); err != nil {
		return httpProblem(c, httpInternal(log, "cannot read the workflow step", err))
	} else if done {
		s.appendLog("", 1, "This step is already completed (id=%(id)d)", map[string]any{"id": *s.prevWfid})
		return c.JSON(http.StatusOK, s.results)
	}

	if err := s.validate(ctx); err != nil {
		return httpProblem(c, err)
	}

	var userID *int64
	if s.caller.id > 0 {
		userID = &s.caller.id
	}
	b, err := json.Marshal(s.results)
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot encode the results", err))
	}
	if s.resultsID, err = a.ODB.InsertFormOutputResults(ctx, userID, s.nodeID, "", string(b)); err != nil {
		return httpProblem(c, httpInternal(log, "cannot store the results", err))
	}
	s.results["results_id"] = s.resultsID
	log.Info("form submitted", "form_id", s.formID, "results_id", s.resultsID)

	if defBool(s.definition, "Async") {
		// Answered at once; the outputs run on the server, the results record
		// telling their progress, as the historical collector queued them.
		response := s.snapshot()
		go s.run(context.WithoutCancel(ctx))
		return c.JSON(http.StatusOK, response)
	}
	s.run(ctx)
	return c.JSON(http.StatusOK, s.snapshot())
}

// loadForm finds the form: an internal one for a negative id, else a form of
// the database the caller may use: published to one of their groups, unless a
// manager.
func (s *formSubmission) loadForm(ctx context.Context, c echo.Context) error {
	notFound := httpErrorf(http.StatusNotFound, "the requested form does not exist or you don't have permission to use it")
	if s.formID < 0 {
		f, ok := internalFormByID(s.formID)
		if !ok {
			return notFound
		}
		s.formName = f.Name
		s.definition = f.Definition
		return nil
	}
	visible, err := s.a.ODB.FormVisible(ctx, s.formID, UserGroupsFromContext(c), IsManager(c))
	if err != nil {
		return httpInternal(s.log, "cannot check form publication", err)
	}
	if !visible {
		return notFound
	}
	if s.form, err = s.a.ODB.FormByID(ctx, s.formID); err != nil {
		return httpInternal(s.log, "cannot read the form", err)
	}
	if s.form == nil {
		return notFound
	}
	s.formName = s.form.Name
	definition, err := parseFormYaml(s.form.Yaml)
	if err != nil {
		return httpErrorf(http.StatusBadRequest, "invalid form definition: %s", err)
	}
	s.definition, _ = definition.(map[string]any)
	if s.definition == nil {
		s.definition = map[string]any{}
	}
	return nil
}

// authorizeContinuation checks that a submission continuing a workflow step
// (prev_wfid) may decide on it: the form must be one of the next forms the step
// names, and the caller its assignee, by full name or through one of their
// groups. The historical collector only hid the choice in the browser
// (workflow_continuation() checks neither); a Manager gets no exemption, as
// there. A form without workflow output does not continue the step: nothing to
// check.
func (s *formSubmission) authorizeContinuation(ctx context.Context, c echo.Context) error {
	if s.formID < 0 || s.prevWfid == nil || !hasWorkflowOutput(s.definition) {
		return nil
	}
	step, err := s.a.ODB.StoredFormStepByID(ctx, *s.prevWfid)
	if err != nil {
		return httpInternal(s.log, "cannot read the workflow step", err)
	}
	if step == nil {
		return httpErrorf(http.StatusNotFound, "workflow step %d not found", *s.prevWfid)
	}
	var stepDefinition map[string]any
	if parsed, err := parseFormYaml(step.Yaml); err == nil {
		stepDefinition, _ = parsed.(map[string]any)
	}
	if !slices.Contains(workflowNextForms(stepDefinition), s.formName) {
		return httpErrorf(http.StatusForbidden, "the %s form is not a next step of workflow step %d", s.formName, *s.prevWfid)
	}
	if !stepAssignedTo(step.Assignee, s.caller.name, UserGroupsFromContext(c)) {
		return httpErrorf(http.StatusForbidden, "workflow step %d awaits the decision of %s", *s.prevWfid, strings.TrimSpace(step.Assignee))
	}
	return nil
}

// hasWorkflowOutput tells whether a form definition stores its submissions as
// workflow steps.
func hasWorkflowOutput(definition map[string]any) bool {
	for _, output := range defMaps(definition, "Outputs") {
		if defString(output, "Dest") == "workflow" {
			return true
		}
	}
	return false
}

// workflowNextForms lists the forms a workflow step may be followed by: the
// NextForms of the workflow outputs of the form it was submitted with, those of
// their Success and Error scripts included, outputWorkflow choosing between the
// two by the outcome of the step's run.
func workflowNextForms(definition map[string]any) []string {
	var names []string
	add := func(defs map[string]any) {
		for _, name := range defList(defs, "NextForms") {
			if text, ok := name.(string); ok {
				names = append(names, text)
			}
		}
	}
	for _, output := range defMaps(definition, "Outputs") {
		if defString(output, "Dest") != "workflow" {
			continue
		}
		add(output)
		if scripts, ok := output["Scripts"].(map[string]any); ok {
			for _, key := range []string{"Success", "Error"} {
				if defs, ok := scripts[key].(map[string]any); ok {
					add(defs)
				}
			}
		}
	}
	return names
}

// stepAssignedTo tells whether a workflow step assigned to `assignee` awaits the
// caller: their full name, or the role of one of their groups. Names compare
// trimmed: a user without first name is stored with a leading space.
func stepAssignedTo(assignee, callerName string, groups []string) bool {
	who := strings.TrimSpace(assignee)
	if who == "" {
		return false
	}
	return who == strings.TrimSpace(callerName) || slices.Contains(groups, who)
}

// workflowStepDone tells whether the submission continues a workflow step that
// already has a next step, as workflow_continuation() does.
func (s *formSubmission) workflowStepDone(ctx context.Context) (bool, error) {
	if s.formID < 0 || s.prevWfid == nil {
		return false, nil
	}
	for _, output := range defMaps(s.definition, "Outputs") {
		if defString(output, "Dest") != "workflow" {
			continue
		}
		prev, err := s.a.ODB.StoredFormLinkByID(ctx, *s.prevWfid)
		if err != nil {
			return false, err
		}
		if prev != nil && prev.NextID != nil {
			return true, nil
		}
	}
	return false, nil
}

// snapshot returns a copy of the results structure, safe to encode while the
// outputs keep running.
func (s *formSubmission) snapshot() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, _ := json.Marshal(s.results)
	var out map[string]any
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.UseNumber()
	_ = decoder.Decode(&out)
	return out
}

// appendLog adds a line to the log of an output, without writing the collector
// log: for the lines of rest answers, as the historical collector appends them
// directly.
func (s *formSubmission) appendLog(outputID string, ret int, format string, d map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	logs := ensureMap(s.results, "log")
	list, _ := logs[outputID].([]any)
	if d == nil {
		d = map[string]any{}
	}
	logs[outputID] = append(list, []any{ret, format, d})
}

// formLog adds a line to the log of an output and to the collector log, as
// form_log() does.
func (s *formSubmission) formLog(ctx context.Context, outputID string, ret int, action, format string, d map[string]any) {
	s.appendLog(outputID, ret, format, d)
	level := "info"
	if ret != 0 {
		level = "error"
	}
	if d == nil {
		d = map[string]any{}
	}
	if err := s.a.ODB.Log(ctx, cdb.LogEntry{Action: action, User: s.userEmail, Fmt: format, Dict: d, Level: level}); err != nil {
		s.log.Error("cannot write audit log", logkey.Error, err)
	}
}

// addReturnCode adds to the return code of the submission.
func (s *formSubmission) addReturnCode(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.results["returncode"] = s.returnCodeLocked() + n
}

func (s *formSubmission) returnCode() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.returnCodeLocked()
}

func (s *formSubmission) returnCodeLocked() int {
	switch t := s.results["returncode"].(type) {
	case int:
		return t
	case json.Number:
		n, _ := t.Int64()
		return int(n)
	case float64:
		return int(t)
	}
	return 0
}

func (s *formSubmission) setStatus(status string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.results["status"] = status
}

// save stores the results structure, as update_results() does. mergeOutputs
// first takes in the outputs other clients stored meanwhile, through
// PUT /form_output_results, as reload_outputs=True does.
func (s *formSubmission) save(ctx context.Context, mergeOutputs bool) {
	if mergeOutputs {
		if current, err := s.stored(ctx); err == nil {
			s.mu.Lock()
			outputs := ensureMap(s.results, "outputs")
			if stored, ok := current["outputs"].(map[string]any); ok {
				for k, v := range stored {
					outputs[k] = v
				}
			}
			s.mu.Unlock()
		}
	}
	s.mu.Lock()
	b, err := json.Marshal(s.results)
	s.mu.Unlock()
	if err != nil {
		s.log.Error("cannot encode the results", logkey.Error, err)
		return
	}
	if err := s.a.ODB.UpdateFormOutputResults(ctx, s.resultsID, string(b)); err != nil {
		s.log.Error("cannot store the results", "results_id", s.resultsID, logkey.Error, err)
		return
	}
	if err := s.a.ODB.Session.NotifyChanges(ctx); err != nil {
		s.log.Error("cannot notify changes", logkey.Error, err)
	}
}

func (s *formSubmission) stored(ctx context.Context) (map[string]any, error) {
	text, err := s.a.ODB.ReadFormOutputResults(ctx, s.resultsID)
	if err != nil {
		return nil, err
	}
	var current map[string]any
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.UseNumber()
	if err := decoder.Decode(&current); err != nil {
		return nil, err
	}
	return current, nil
}

// reload replaces the results structure by the stored one, as reload_results()
// does after a rest call, which may have changed it.
func (s *formSubmission) reload(ctx context.Context) {
	current, err := s.stored(ctx)
	if err != nil {
		s.log.Error("cannot reload the results", "results_id", s.resultsID, logkey.Error, err)
		return
	}
	s.mu.Lock()
	s.results = current
	s.mu.Unlock()
}

// run runs the outputs of the form in order, as __form_submit() does: an output
// may be skipped after an error, or by its condition; an output failing stops
// the run.
func (s *formSubmission) run(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			s.formLog(ctx, "", 1, "form.submit", fmt.Sprint(r), nil)
			s.addReturnCode(1)
		}
		s.setStatus("COMPLETED")
		s.save(ctx, false)
	}()

	s.setStatus("RUNNING")
	s.save(ctx, false)

	outputs := defMaps(s.definition, "Outputs")
	for i, output := range outputs {
		if _, ok := output["Id"]; !ok {
			output["Id"] = fmt.Sprintf("output-%d", i)
		}
	}
	for _, output := range outputs {
		outputID := defString(output, "Id")
		s.mu.Lock()
		ensureMap(s.results, "log")[outputID] = []any{}
		order, _ := s.results["outputs_order"].([]any)
		s.results["outputs_order"] = append(order, outputID)
		s.mu.Unlock()

		if defBool(output, "SkipOnErrors") && s.returnCode() != 0 {
			s.formLog(ctx, outputID, 1, "form.submit", "%(output_id)s: skip (previous output error)",
				map[string]any{"output_id": outputID})
			s.save(ctx, false)
			continue
		}
		run, err := checkOutputCondition(output, s.data)
		if err != nil {
			s.formLog(ctx, outputID, 1, "form.submit", err.Error(), nil)
			s.save(ctx, false)
			continue
		}
		if !run {
			continue
		}

		switch defString(output, "Dest") {
		case "db":
			err = s.outputDB(ctx, output)
		case "script":
			err = s.outputScript(ctx, output)
		case "rest":
			err = s.outputRest(ctx, output)
		case "mail":
			err = s.outputMail(ctx, output, nil, 0)
		case "workflow":
			err = s.outputWorkflow(ctx, output)
		}
		if err != nil {
			s.formLog(ctx, outputID, 1, "form.submit", err.Error(), nil)
			s.addReturnCode(1)
			break
		}
		s.save(ctx, false)
	}
}

// formsAPIBase is the base url of the collector API, for the rest outputs and
// the dynamic candidates checks, which call it as the submitter: the configured
// server.forms.api_url, else the local listener of this server.
func formsAPIBase() string {
	if base := viper.GetString("server.forms.api_url"); base != "" {
		return strings.TrimRight(base, "/")
	}
	addr := viper.GetString("server.addr")
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "http://" + addr + "/api"
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port) + "/api"
}

var formsHTTPClient = &http.Client{Timeout: 10 * time.Minute}

// apiResponse is the answer of an API call: its status and its decoded body.
type apiResponse struct {
	status int
	body   any
	raw    []byte
}

// callAPI calls the collector API as the submitter: a GET carries vars in its
// query string, the other methods in a JSON body. The external urls of the rest
// outputs go through callExternal, without the credentials.
func (s *formSubmission) callAPI(ctx context.Context, method, path string, vars any) (*apiResponse, error) {
	target := formsAPIBase() + path
	var body io.Reader
	if method == http.MethodGet {
		if m, ok := vars.(map[string]any); ok && len(m) > 0 {
			q := url.Values{}
			for k, v := range m {
				q.Set(k, formValueText(v))
			}
			target += "?" + q.Encode()
		}
	} else if vars != nil {
		b, err := json.Marshal(vars)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if s.auth != "" {
		req.Header.Set("Authorization", s.auth)
	}
	if s.cookie != "" {
		// A local call, without Origin: the CSRF header is what a session
		// request that changes something needs.
		req.Header.Set("Cookie", s.cookie)
		req.Header.Set(CSRFHeader, "1")
	}
	resp, err := formsHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	out := &apiResponse{status: resp.StatusCode, raw: raw}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	_ = decoder.Decode(&out.body)
	return out, nil
}

// problemTextOf returns the message of an API error answer.
func problemTextOf(r *apiResponse) string {
	if m, ok := r.body.(map[string]any); ok {
		if text, ok := m["text"].(string); ok {
			return text
		}
	}
	return strings.TrimSpace(string(r.raw))
}
