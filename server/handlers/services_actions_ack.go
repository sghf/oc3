package serverhandlers

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// PostServicesActions handles POST /services_actions: the bulk form of the
// acknowledgement, the action named by the 'id' key of the body.
func (a *Api) PostServicesActions(c echo.Context) error {
	var body server.PostServicesActionsJSONBody
	if err := c.Bind(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	if body.Id == 0 {
		return JSONProblemf(c, http.StatusBadRequest, "the 'id' key must be specified")
	}
	return a.ackServiceAction(c, body.Id, int(body.Ack), body.AckedComment)
}

// PostServicesAction handles POST /services_actions/{action_id}
func (a *Api) PostServicesAction(c echo.Context, actionId int64) error {
	var body server.ServiceActionAckRequest
	if err := c.Bind(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	return a.ackServiceAction(c, actionId, int(body.Ack), body.AckedComment)
}

// ackServiceAction acknowledges a failed action, as the historical
// rest_post_services_action, and answers: the count of the unacknowledged
// errors of its instance and its "action errors" alert follow at once.
func (a *Api) ackServiceAction(c echo.Context, id int64, ack int, comment *string) error {
	log := echolog.GetLogHandler(c, "PostServicesAction")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}
	if ack != 1 {
		return JSONProblemf(c, http.StatusBadRequest, "ack must be 1: an acknowledgement is not withdrawn")
	}
	text := ""
	if comment != nil {
		text = *comment
	}
	o, instance := a.ackServiceActionOne(c, log, ctx, id, text)
	if instance != nil {
		a.recountActionErrors(log, ctx, *instance)
		a.notifyChanges(log, ctx)
	}
	return writeOutcome(c, o)
}

// actionInstance is the instance of a service whose action errors are counted.
type actionInstance struct{ svcID, nodeID string }

// ackServiceActionOne acknowledges a failed action: the responsibility of the
// service is required, an action not failed or acknowledged already is skipped.
// Unlike the historical collector, the failed lines the action logged are
// acknowledged with it: the count includes them, and an acknowledged action left
// it above zero. It returns the instance whose count is then to follow, nil
// when nothing changed.
func (a *Api) ackServiceActionOne(c echo.Context, log *slog.Logger, ctx context.Context, id int64, text string) (batchOutcome, *actionInstance) {
	odb := a.ODB
	action, err := odb.ServiceActionForAck(ctx, id)
	if err != nil {
		log.Error("cannot read the action", "id", id, logkey.Error, err)
		return batchFail(http.StatusInternalServerError, "cannot read the action"), nil
	}
	if action == nil {
		return batchFail(http.StatusNotFound, "log entry %d does not exist", id), nil
	}
	responsible, err := odb.ServiceResponsible(ctx, action.SvcID, UserGroupsFromContext(c), IsManager(c))
	if err != nil {
		log.Error("cannot check service responsibility", "svc_id", action.SvcID, logkey.Error, err)
		return batchFail(http.StatusInternalServerError, "cannot check service responsibility"), nil
	}
	if !responsible {
		return batchFail(http.StatusForbidden, "user is not responsible for the service of action %d", id), nil
	}
	if action.Status != "err" {
		return batchOK("skip acknowledge of action %d in status '%s'", id, action.Status), nil
	}
	if action.Acked {
		return batchOK("skip acknowledge of action %d: already acknowledged", id), nil
	}
	userEmail, _ := c.Get(XUserEmail).(string)
	if err := odb.AckServiceAction(ctx, action, userEmail, text); err != nil {
		log.Error("cannot acknowledge the action", "id", id, logkey.Error, err)
		return batchFail(http.StatusInternalServerError, "cannot acknowledge the action"), nil
	}
	entry := cdb.LogEntry{
		Action: "actions.change",
		User:   userEmail,
		Fmt:    "changed action %(id)s: %(data)s",
		Dict:   map[string]any{"id": fmt.Sprint(id), "data": fmt.Sprintf("ack: 1, acked_comment: %s", text)},
		Level:  "info",
	}
	if parsed, err := uuid.Parse(action.NodeID); err == nil {
		entry.NodeID = &parsed
	}
	if parsed, err := uuid.Parse(action.SvcID); err == nil {
		entry.SvcID = &parsed
	}
	if err := odb.Log(ctx, entry); err != nil {
		log.Error("cannot write audit log", logkey.Error, err)
	}
	return batchOK("action %d acknowledged", id), &actionInstance{svcID: action.SvcID, nodeID: action.NodeID}
}

// recountActionErrors counts the unacknowledged action errors of an instance
// again, then sets its alert, as the feed of the actions does.
func (a *Api) recountActionErrors(log *slog.Logger, ctx context.Context, i actionInstance) {
	odb := a.ODB
	if err := odb.UpdateActionErrors(ctx, i.svcID, i.nodeID); err != nil {
		log.Error("cannot count the action errors", "svc_id", i.svcID, logkey.Error, err)
	} else if err := odb.UpdateDashActionErrors(ctx, i.svcID, i.nodeID); err != nil {
		log.Error("cannot update the action errors alert", "svc_id", i.svcID, logkey.Error, err)
	}
	odb.SetChange("b_action_errors", "dashboard")
}
