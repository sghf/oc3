package serverhandlers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// The checks, as the historical collector's api_checks.py serves them: the
// checks the agents report (/checks/live), the thresholds set on one of them
// (/checks/settings), the default thresholds of a check type
// (/checks/defaults) and the contextual thresholds of the checks of the nodes
// a filterset selects (/checks/contextual_settings). A check takes the
// thresholds of the first source having some: its settings, then the last
// matching contextual thresholds, then the best matching defaults
// (cdb.ChecksLiveUpdateThresholds).
//
// A change of the settings of a check applies at once to the checks of its
// node, with their alerts. A change of defaults or of contextual thresholds
// applies, in the background, to the nodes having checks of the type: the
// historical collector queued it, or left it to the next check run of the
// nodes, and did not refresh the alerts.

// chkTypeMaxLen is the length of checks_live.chk_type.
const chkTypeMaxLen = 10

// checksRefreshTimeout bounds a background refresh of the thresholds.
const checksRefreshTimeout = 30 * time.Minute

// checksRefreshMu runs the background refreshes one after the other: a series
// of changes must not set the thresholds of the same nodes concurrently.
var checksRefreshMu sync.Mutex

func requirePrivilege(c echo.Context, group string) error {
	if !IsAuthByUser(c) {
		return denyRequest(c, http.StatusUnauthorized, "user authentication required")
	}
	if !IsManager(c) && !HasGroup(c, group) {
		return denyRequest(c, http.StatusForbidden, "%s privilege required", group)
	}
	return nil
}

// GetChecksLive handles GET /checks/live
func (a *Api) GetChecksLive(c echo.Context, params server.GetChecksLiveParams) error {
	return a.handleList(c, "GetChecksLive", "check",
		listParams(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, params.Filter),
		func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return a.ODB.GetChecks(ctx, p)
		})
}

// GetChecksSettings handles GET /checks/settings
func (a *Api) GetChecksSettings(c echo.Context, params server.GetChecksSettingsParams) error {
	return a.handleList(c, "GetChecksSettings", "check_setting",
		listParams(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, params.Filter),
		func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return a.ODB.GetChecksSettings(ctx, p)
		})
}

// GetChecksDefaults handles GET /checks/defaults
func (a *Api) GetChecksDefaults(c echo.Context, params server.GetChecksDefaultsParams) error {
	return a.handleList(c, "GetChecksDefaults", "check_default",
		listParams(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, params.Filter),
		func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return a.ODB.GetChecksDefaults(ctx, p)
		})
}

// GetChecksContextual handles GET /checks/contextual_settings
func (a *Api) GetChecksContextual(c echo.Context, params server.GetChecksContextualParams) error {
	return a.handleList(c, "GetChecksContextual", "check_contextual",
		listParams(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, params.Filter),
		func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return a.ODB.GetChecksContextual(ctx, p)
		})
}

// checksLog writes an audit entry about a check of a node, or about thresholds
// not tied to one when nodeID is empty.
func (a *Api) checksLog(c echo.Context, ctx context.Context, log *slog.Logger, action, format string, dict map[string]any, nodeID, svcID string) {
	userEmail, _ := c.Get(XUserEmail).(string)
	entry := cdb.LogEntry{Action: action, User: userEmail, Fmt: format, Dict: dict, Level: "info"}
	if parsed, err := uuid.Parse(nodeID); err == nil {
		entry.NodeID = &parsed
	}
	if parsed, err := uuid.Parse(svcID); err == nil {
		entry.SvcID = &parsed
	}
	if err := a.ODB.Log(ctx, entry); err != nil {
		log.Error("cannot write audit log", logkey.Error, err)
	}
}

// refreshNodeChecks sets the thresholds and the alerts of the checks of a node
// again, after a change of the settings of one of them; it returns whether the
// request may go on, having answered 500 otherwise.
func (a *Api) refreshNodeChecks(c echo.Context, ctx context.Context, log *slog.Logger, nodeID string) (bool, error) {
	if err := a.ODB.ChecksRefreshNode(ctx, nodeID); err != nil {
		log.Error("cannot set the thresholds of the checks again", logkey.NodeID, nodeID, logkey.Error, err)
		return false, JSONProblemf(c, http.StatusInternalServerError, "thresholds saved, but the checks could not take them yet: they will at the next check run of the node")
	}
	return true, nil
}

// refreshChecksInBackground sets the thresholds and the alerts of the checks
// of the types again, on every node having some, after the request.
func (a *Api) refreshChecksInBackground(log *slog.Logger, types ...string) {
	go func() {
		checksRefreshMu.Lock()
		defer checksRefreshMu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), checksRefreshTimeout)
		defer cancel()
		seen := map[string]bool{}
		for _, chkType := range types {
			if seen[chkType] {
				continue
			}
			seen[chkType] = true
			nodes, err := a.ODB.NodesWithChecksOfType(ctx, chkType)
			if err != nil {
				log.Error("cannot list the nodes to refresh the checks of", "chk_type", chkType, logkey.Error, err)
				continue
			}
			for _, nodeID := range nodes {
				if err := a.ODB.ChecksRefreshNode(ctx, nodeID); err != nil {
					log.Error("cannot refresh the checks of a node", logkey.NodeID, nodeID, logkey.Error, err)
				}
			}
			log.Info("checks thresholds refreshed", "chk_type", chkType, "nodes", len(nodes))
		}
		if err := a.ODB.Session.NotifyChanges(ctx); err != nil {
			log.Error("cannot notify changes", logkey.Error, err)
		}
	}()
}

// infoAnswer is the answer of a write that returns no record: its outcome message.
func infoAnswer(format string, args ...any) server.InfoResponse {
	info := fmt.Sprintf(format, args...)
	return server.InfoResponse{Info: &info}
}

// DeleteCheckLive handles DELETE /checks/live/{id}
func (a *Api) DeleteCheckLive(c echo.Context, id int64) error {
	log := echolog.GetLogHandler(c, "DeleteCheckLive")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	if err := requirePrivilege(c, "CheckExec"); err != nil {
		return err
	}
	check, o := a.checkLiveByID(log, ctx, id)
	if check != nil {
		o = a.deleteCheckOne(c, log, ctx, check)
	}
	if o.ok() {
		a.notifyChanges(log, ctx)
	}
	return writeOutcome(c, o)
}

// checkLiveByID reads a check, nil with the outcome to answer when it cannot.
func (a *Api) checkLiveByID(log *slog.Logger, ctx context.Context, id int64) (*cdb.CheckLive, batchOutcome) {
	check, err := a.ODB.CheckLiveByID(ctx, id)
	if err != nil {
		log.Error("cannot read the check", "id", id, logkey.Error, err)
		return nil, batchFail(http.StatusInternalServerError, "cannot read the check")
	}
	if check == nil {
		return nil, batchFail(http.StatusNotFound, "check %d not found", id)
	}
	return check, batchOutcome{}
}

// nodeResponsibleOutcome tells whether the user may change the checks of the
// node, a failure outcome when not: a Manager, or a user responsible for its
// application.
func (a *Api) nodeResponsibleOutcome(c echo.Context, ctx context.Context, log *slog.Logger, nodeID string) (batchOutcome, bool) {
	if IsManager(c) {
		return batchOutcome{}, true
	}
	responsible, err := a.ODB.NodeResponsible(ctx, nodeID, UserGroupsFromContext(c), false)
	if err != nil {
		log.Error("cannot check node responsibility", logkey.NodeID, nodeID, logkey.Error, err)
		return batchFail(http.StatusInternalServerError, "cannot check node responsibility"), false
	}
	if !responsible {
		return batchFail(http.StatusForbidden, "user is not responsible for node %s", nodeID), false
	}
	return batchOutcome{}, true
}

// deleteCheckOne deletes a check instance; its node is responsible for it.
func (a *Api) deleteCheckOne(c echo.Context, log *slog.Logger, ctx context.Context, check *cdb.CheckLive) batchOutcome {
	if o, ok := a.nodeResponsibleOutcome(c, ctx, log, check.NodeID); !ok {
		return o
	}
	if err := a.ODB.DeleteCheckLive(ctx, check); err != nil {
		log.Error("cannot delete the check", "id", check.ID, logkey.Error, err)
		return batchFail(http.StatusInternalServerError, "cannot delete the check")
	}
	a.checksLog(c, ctx, log, "check.delete", "delete check instance %(data)s",
		map[string]any{"data": check.Type + "-" + check.Instance}, check.NodeID, check.SvcID)
	return batchOK("check %s:%s deleted", check.Type, check.Instance)
}

func checkKey(nodeID string, svcID *string, chkType, chkInstance string) cdb.CheckKey {
	k := cdb.CheckKey{NodeID: nodeID, Type: chkType, Instance: chkInstance}
	if svcID != nil {
		k.SvcID = *svcID
	}
	return k
}

func keyOfCheck(check *cdb.CheckLive) cdb.CheckKey {
	return cdb.CheckKey{NodeID: check.NodeID, SvcID: check.SvcID, Type: check.Type, Instance: check.Instance}
}

// PostChecksSettings handles POST /checks/settings
func (a *Api) PostChecksSettings(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostChecksSettings")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	if err := requirePrivilege(c, "CheckExec"); err != nil {
		return err
	}
	var body server.PostChecksSettingsJSONRequestBody
	if err := c.Bind(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	k := checkKey(body.NodeId, body.SvcId, body.ChkType, body.ChkInstance)
	check, err := a.ODB.CheckLiveByKey(ctx, k)
	if err != nil {
		log.Error("cannot read the check", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the check")
	}
	if check == nil {
		return JSONProblemf(c, http.StatusNotFound, "check instance not found")
	}
	o := a.setCheckThresholdsOne(c, log, ctx, check, body.ChkLow, body.ChkHigh)
	if !o.ok() {
		return writeOutcome(c, o)
	}
	if ok, err := a.refreshNodeChecks(c, ctx, log, k.NodeID); !ok {
		return err
	}
	a.notifyChanges(log, ctx)
	return writeOutcome(c, o)
}

// setCheckThresholdsOne sets the thresholds of a check, a threshold not given
// keeping the one the check has now. The checks of its node are to be refreshed
// after (ChecksRefreshNode).
func (a *Api) setCheckThresholdsOne(c echo.Context, log *slog.Logger, ctx context.Context, check *cdb.CheckLive, newLow, newHigh *int64) batchOutcome {
	if o, ok := a.nodeResponsibleOutcome(c, ctx, log, check.NodeID); !ok {
		return o
	}
	low, high := check.Low, check.High
	if newLow != nil {
		low = sql.NullInt64{Int64: *newLow, Valid: true}
	}
	if newHigh != nil {
		high = sql.NullInt64{Int64: *newHigh, Valid: true}
	}
	if !low.Valid || !high.Valid {
		return batchFail(http.StatusBadRequest, "the check has no threshold to keep: give both chk_low and chk_high")
	}
	if low.Int64 > high.Int64 {
		return batchFail(http.StatusBadRequest, "chk_low %d is above chk_high %d", low.Int64, high.Int64)
	}
	k := keyOfCheck(check)
	userEmail, _ := c.Get(XUserEmail).(string)
	if err := a.ODB.SetCheckSettings(ctx, k, low.Int64, high.Int64, userEmail); err != nil {
		log.Error("cannot set the thresholds of the check", logkey.Error, err)
		return batchFail(http.StatusInternalServerError, "cannot set the thresholds of the check")
	}
	a.checksLog(c, ctx, log, "check.settings.change",
		"set thresholds of check %(chk_type)s:%(chk_instance)s to %(chk_low)d - %(chk_high)d",
		map[string]any{"chk_type": k.Type, "chk_instance": k.Instance, "chk_low": low.Int64, "chk_high": high.Int64},
		k.NodeID, k.SvcID)
	return batchOK("thresholds of check %s:%s set to %d - %d", k.Type, k.Instance, low.Int64, high.Int64)
}

// DeleteChecksSettings handles DELETE /checks/settings
func (a *Api) DeleteChecksSettings(c echo.Context) error {
	log := echolog.GetLogHandler(c, "DeleteChecksSettings")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	if err := requirePrivilege(c, "CheckExec"); err != nil {
		return err
	}
	var body server.DeleteChecksSettingsJSONRequestBody
	if err := c.Bind(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	if body.NodeId == "" || body.ChkType == "" {
		return JSONProblemf(c, http.StatusBadRequest, "node_id, chk_type and chk_instance are required")
	}
	k := checkKey(body.NodeId, body.SvcId, body.ChkType, body.ChkInstance)
	o := a.resetCheckThresholdsOne(c, log, ctx, k)
	if !o.ok() {
		return writeOutcome(c, o)
	}
	if ok, err := a.refreshNodeChecks(c, ctx, log, k.NodeID); !ok {
		return err
	}
	a.notifyChanges(log, ctx)
	return writeOutcome(c, o)
}

// resetCheckThresholdsOne removes the thresholds set on a check, which takes
// those of its defaults again once its node is refreshed (ChecksRefreshNode).
func (a *Api) resetCheckThresholdsOne(c echo.Context, log *slog.Logger, ctx context.Context, k cdb.CheckKey) batchOutcome {
	if o, ok := a.nodeResponsibleOutcome(c, ctx, log, k.NodeID); !ok {
		return o
	}
	deleted, err := a.ODB.DeleteCheckSettings(ctx, k)
	if err != nil {
		log.Error("cannot remove the thresholds of the check", logkey.Error, err)
		return batchFail(http.StatusInternalServerError, "cannot remove the thresholds of the check")
	}
	if !deleted {
		return batchFail(http.StatusNotFound, "no thresholds set on check %s:%s", k.Type, k.Instance)
	}
	a.checksLog(c, ctx, log, "check.settings.delete", "reset thresholds of check %(chk_type)s:%(chk_instance)s",
		map[string]any{"chk_type": k.Type, "chk_instance": k.Instance}, k.NodeID, k.SvcID)
	return batchOK("thresholds of check %s:%s reset", k.Type, k.Instance)
}

// validChkType checks a check type: given, and as long as the column allows.
func validChkType(t string) error {
	if strings.TrimSpace(t) == "" {
		return errors.New("chk_type is required")
	}
	if len(t) > chkTypeMaxLen {
		return fmt.Errorf("chk_type is at most %d characters", chkTypeMaxLen)
	}
	return nil
}

// validPattern checks an instance pattern: a regular expression.
func validPattern(prop, pattern string) error {
	if _, err := regexp.Compile(pattern); err != nil {
		return fmt.Errorf("%s is not a valid regular expression: %s", prop, err)
	}
	return nil
}

func nullInt(p *int64) sql.NullInt64 {
	if p == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: *p, Valid: true}
}

func validThresholds(low, high sql.NullInt64) error {
	if low.Valid && high.Valid && low.Int64 > high.Int64 {
		return fmt.Errorf("chk_low %d is above chk_high %d", low.Int64, high.Int64)
	}
	return nil
}

// PostChecksDefaults handles POST /checks/defaults
func (a *Api) PostChecksDefaults(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostChecksDefaults")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	if err := requirePrivilege(c, "CheckManager"); err != nil {
		return err
	}
	var body server.PostChecksDefaultsJSONRequestBody
	if err := c.Bind(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	d := cdb.CheckDefault{Low: nullInt(body.ChkLow), High: nullInt(body.ChkHigh)}
	if body.ChkType != nil {
		d.Type = strings.TrimSpace(*body.ChkType)
	}
	if body.ChkInst != nil {
		d.Inst = *body.ChkInst
	}
	if body.ChkPrio != nil {
		d.Prio = *body.ChkPrio
	}
	if err := errors.Join(validChkType(d.Type), validPattern("chk_inst", d.Inst), validThresholds(d.Low, d.High)); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	id, err := a.ODB.InsertCheckDefault(ctx, d)
	if errors.Is(err, cdb.ErrDuplicate) {
		return JSONProblemf(c, http.StatusConflict, "defaults of %s for instances %q exist already", d.Type, d.Inst)
	}
	if err != nil {
		log.Error("cannot add the defaults", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot add the defaults")
	}
	a.checksLog(c, ctx, log, "check.defaults.add", "added check default %(chk_type)s:%(chk_inst)s",
		map[string]any{"chk_type": d.Type, "chk_inst": d.Inst}, "", "")
	a.notifyChanges(log, ctx)
	a.refreshChecksInBackground(log, d.Type)
	return c.JSON(http.StatusOK, server.CheckIdResponse{Id: id, Info: infoAnswer("check default %d added", id).Info})
}

// PostCheckDefault handles POST /checks/defaults/{id}
func (a *Api) PostCheckDefault(c echo.Context, id int64) error {
	log := echolog.GetLogHandler(c, "PostCheckDefault")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	if err := requirePrivilege(c, "CheckManager"); err != nil {
		return err
	}
	var body server.PostCheckDefaultJSONRequestBody
	if err := c.Bind(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	d, err := a.ODB.CheckDefaultByID(ctx, id)
	if err != nil {
		log.Error("cannot read the defaults", "id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the defaults")
	}
	if d == nil {
		return JSONProblemf(c, http.StatusNotFound, "check default %d not found", id)
	}
	before := *d
	if body.ChkType != nil {
		d.Type = strings.TrimSpace(*body.ChkType)
	}
	if body.ChkInst != nil {
		d.Inst = *body.ChkInst
	}
	if body.ChkLow != nil {
		d.Low = nullInt(body.ChkLow)
	}
	if body.ChkHigh != nil {
		d.High = nullInt(body.ChkHigh)
	}
	if body.ChkPrio != nil {
		d.Prio = *body.ChkPrio
	}
	if err := errors.Join(validChkType(d.Type), validPattern("chk_inst", d.Inst), validThresholds(d.Low, d.High)); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	if err := a.ODB.UpdateCheckDefault(ctx, *d); errors.Is(err, cdb.ErrDuplicate) {
		return JSONProblemf(c, http.StatusConflict, "defaults of %s for instances %q exist already", d.Type, d.Inst)
	} else if err != nil {
		log.Error("cannot change the defaults", "id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot change the defaults")
	}
	a.checksLog(c, ctx, log, "check.defaults.change", "changed check default %(id)d: %(changes)s",
		map[string]any{"id": id, "changes": checkDefaultChanges(before, *d)}, "", "")
	a.notifyChanges(log, ctx)
	a.refreshChecksInBackground(log, before.Type, d.Type)
	return c.JSON(http.StatusOK, infoAnswer("check default %d changed", id))
}

func formatNull(v sql.NullInt64) string {
	if !v.Valid {
		return "none"
	}
	return fmt.Sprint(v.Int64)
}

func checkDefaultChanges(before, after cdb.CheckDefault) string {
	var l []string
	add := func(prop, b, a string) {
		if b != a {
			l = append(l, fmt.Sprintf("%s: %s => %s", prop, b, a))
		}
	}
	add("chk_type", before.Type, after.Type)
	add("chk_inst", before.Inst, after.Inst)
	add("chk_low", formatNull(before.Low), formatNull(after.Low))
	add("chk_high", formatNull(before.High), formatNull(after.High))
	add("chk_prio", fmt.Sprint(before.Prio), fmt.Sprint(after.Prio))
	return strings.Join(l, ", ")
}

// DeleteCheckDefault handles DELETE /checks/defaults/{id}
func (a *Api) DeleteCheckDefault(c echo.Context, id int64) error {
	log := echolog.GetLogHandler(c, "DeleteCheckDefault")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	if err := requirePrivilege(c, "CheckManager"); err != nil {
		return err
	}
	d, err := a.ODB.CheckDefaultByID(ctx, id)
	if err != nil {
		log.Error("cannot read the defaults", "id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the defaults")
	}
	if d == nil {
		return JSONProblemf(c, http.StatusNotFound, "check default %d not found", id)
	}
	if err := a.ODB.DeleteCheckDefault(ctx, id); err != nil {
		log.Error("cannot delete the defaults", "id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot delete the defaults")
	}
	a.checksLog(c, ctx, log, "check.defaults.delete", "deleted check default %(chk_type)s:%(chk_inst)s",
		map[string]any{"chk_type": d.Type, "chk_inst": d.Inst}, "", "")
	a.notifyChanges(log, ctx)
	a.refreshChecksInBackground(log, d.Type)
	return c.JSON(http.StatusOK, infoAnswer("check default %d deleted", id))
}

// contextualFilterset returns the name of the filterset of contextual
// thresholds, and whether the request may go on: it answers 400 when there is
// no filterset of that id.
func (a *Api) contextualFilterset(c echo.Context, ctx context.Context, log *slog.Logger, fsetID int64) (string, bool, error) {
	id, name, err := a.ODB.FiltersetByIDOrName(ctx, fmt.Sprint(fsetID))
	if err != nil {
		log.Error("cannot read the filterset", "fset_id", fsetID, logkey.Error, err)
		return "", false, JSONProblemf(c, http.StatusInternalServerError, "cannot read the filterset")
	}
	if id == 0 {
		return "", false, JSONProblemf(c, http.StatusBadRequest, "filterset %d not found", fsetID)
	}
	return name, true, nil
}

func validContextual(t cdb.CheckContextual) error {
	return errors.Join(
		validChkType(t.Type),
		func() error {
			if t.Instance == "" {
				return errors.New("chk_instance is required")
			}
			return validPattern("chk_instance", t.Instance)
		}(),
		validThresholds(sql.NullInt64{Int64: t.Low, Valid: true}, sql.NullInt64{Int64: t.High, Valid: true}),
	)
}

// PostChecksContextual handles POST /checks/contextual_settings
func (a *Api) PostChecksContextual(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostChecksContextual")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	if err := requirePrivilege(c, "ContextCheckManager"); err != nil {
		return err
	}
	var body server.PostChecksContextualJSONRequestBody
	if err := c.Bind(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	if body.FsetId == nil || body.ChkType == nil || body.ChkInstance == nil || body.ChkLow == nil || body.ChkHigh == nil {
		return JSONProblemf(c, http.StatusBadRequest, "fset_id, chk_type, chk_instance, chk_low and chk_high are required")
	}
	t := cdb.CheckContextual{FsetID: *body.FsetId, Type: strings.TrimSpace(*body.ChkType), Instance: *body.ChkInstance, Low: *body.ChkLow, High: *body.ChkHigh}
	if err := validContextual(t); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	name, ok, err := a.contextualFilterset(c, ctx, log, t.FsetID)
	if !ok {
		return err
	}
	id, err := a.ODB.InsertCheckContextual(ctx, t)
	if errors.Is(err, cdb.ErrDuplicate) {
		return JSONProblemf(c, http.StatusConflict, "thresholds of %s:%s exist already for filterset %s", t.Type, t.Instance, name)
	}
	if err != nil {
		log.Error("cannot add the contextual thresholds", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot add the contextual thresholds")
	}
	a.checksLog(c, ctx, log, "check.contextual_settings.add", "added contextual thresholds %(data)s",
		map[string]any{"data": fmt.Sprintf("%s %s:%s %d:%d", name, t.Type, t.Instance, t.Low, t.High)}, "", "")
	a.notifyChanges(log, ctx)
	a.refreshChecksInBackground(log, t.Type)
	return c.JSON(http.StatusOK, server.CheckIdResponse{Id: id, Info: infoAnswer("checks contextual instance %d added", id).Info})
}

// PostCheckContextual handles POST /checks/contextual_settings/{id}
func (a *Api) PostCheckContextual(c echo.Context, id int64) error {
	log := echolog.GetLogHandler(c, "PostCheckContextual")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	if err := requirePrivilege(c, "ContextCheckManager"); err != nil {
		return err
	}
	var body server.PostCheckContextualJSONRequestBody
	if err := c.Bind(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	t, err := a.ODB.CheckContextualByID(ctx, id)
	if err != nil {
		log.Error("cannot read the contextual thresholds", "id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the contextual thresholds")
	}
	if t == nil {
		return JSONProblemf(c, http.StatusNotFound, "contextual thresholds %d not found", id)
	}
	before := *t
	if body.FsetId != nil {
		t.FsetID = *body.FsetId
	}
	if body.ChkType != nil {
		t.Type = strings.TrimSpace(*body.ChkType)
	}
	if body.ChkInstance != nil {
		t.Instance = *body.ChkInstance
	}
	if body.ChkLow != nil {
		t.Low = *body.ChkLow
	}
	if body.ChkHigh != nil {
		t.High = *body.ChkHigh
	}
	if err := validContextual(*t); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	name, ok, err := a.contextualFilterset(c, ctx, log, t.FsetID)
	if !ok {
		return err
	}
	if err := a.ODB.UpdateCheckContextual(ctx, *t); errors.Is(err, cdb.ErrDuplicate) {
		return JSONProblemf(c, http.StatusConflict, "thresholds of %s:%s exist already for filterset %s", t.Type, t.Instance, name)
	} else if err != nil {
		log.Error("cannot change the contextual thresholds", "id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot change the contextual thresholds")
	}
	a.checksLog(c, ctx, log, "check.contextual_settings.change", "changed contextual thresholds %(id)d: %(data)s",
		map[string]any{"id": id, "data": fmt.Sprintf("%s %s:%s %d:%d", name, t.Type, t.Instance, t.Low, t.High)}, "", "")
	a.notifyChanges(log, ctx)
	a.refreshChecksInBackground(log, before.Type, t.Type)
	return c.JSON(http.StatusOK, infoAnswer("contextual thresholds %d changed", id))
}

// DeleteCheckContextual handles DELETE /checks/contextual_settings/{id}
func (a *Api) DeleteCheckContextual(c echo.Context, id int64) error {
	log := echolog.GetLogHandler(c, "DeleteCheckContextual")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	if err := requirePrivilege(c, "ContextCheckManager"); err != nil {
		return err
	}
	t, err := a.ODB.CheckContextualByID(ctx, id)
	if err != nil {
		log.Error("cannot read the contextual thresholds", "id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the contextual thresholds")
	}
	if t == nil {
		return JSONProblemf(c, http.StatusNotFound, "contextual thresholds %d not found", id)
	}
	if err := a.ODB.DeleteCheckContextual(ctx, id); err != nil {
		log.Error("cannot delete the contextual thresholds", "id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot delete the contextual thresholds")
	}
	a.checksLog(c, ctx, log, "check.contextual_settings.delete", "deleted contextual thresholds %(data)s",
		map[string]any{"data": fmt.Sprintf("%s %s:%s %d:%d", t.FsetName, t.Type, t.Instance, t.Low, t.High)}, "", "")
	a.notifyChanges(log, ctx)
	a.refreshChecksInBackground(log, t.Type)
	return c.JSON(http.StatusOK, infoAnswer("contextual thresholds %d deleted", id))
}
