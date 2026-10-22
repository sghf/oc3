package serverhandlers

import (
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/availability"
	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

const (
	statusLogDefaultDays = 7
	statusLogMaxDays     = 365
)

func statusLogDays(days *int) (int, bool) {
	if days == nil {
		return statusLogDefaultDays, true
	}
	if *days < 1 {
		return 0, false
	}
	return min(*days, statusLogMaxDays), true
}

// GetServiceStatusLog handles GET /services/{svc_id}/status_log: the
// availability periods of a visible service over the last days.
func (a *Api) GetServiceStatusLog(c echo.Context, svcId string, params server.GetServiceStatusLogParams) error {
	log := echolog.GetLogHandler(c, "GetServiceStatusLog")
	days, ok := statusLogDays(params.Days)
	if !ok {
		return JSONProblemf(c, http.StatusBadRequest, "days must be positive")
	}
	if err := a.resolveService(c, log, svcId); err != nil {
		return err
	}
	ctx := c.Request().Context()
	svc, err := a.ODB.ServiceBySvcIDOrName(ctx, svcId, UserGroupsFromContext(c), IsManager(c))
	if errors.Is(err, cdb.ErrServiceAmbiguous) {
		return JSONProblemf(c, http.StatusConflict, "%s", err)
	}
	if err != nil {
		log.Error("cannot resolve service", "svc_id", svcId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve service %s", svcId)
	}
	if svc == nil {
		return JSONProblemf(c, http.StatusNotFound, "service %s not found", svcId)
	}
	periods, err := a.ODB.ServiceStatusPeriods(ctx, svc.SvcID, days)
	if err != nil {
		log.Error("cannot read the status log", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the status log")
	}
	acks, err := a.ODB.ServiceStatusAcks(ctx, svc.SvcID, days)
	if err != nil {
		log.Error("cannot read the justifications", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the justifications")
	}
	// A justification is the one of the period of the same bounds.
	byBounds := make(map[[2]string]cdb.StatusAck, len(acks))
	for _, ack := range acks {
		byBounds[[2]string{ack.Begin, ack.End}] = ack
	}
	data := make([]server.ServiceStatusPeriod, 0, len(periods))
	for _, p := range periods {
		period := server.ServiceStatusPeriod{Status: p.Avail, Begin: p.Begin, End: p.End}
		if ack, ok := byBounds[[2]string{p.Begin, p.End}]; ok {
			period.Ack = &server.StatusAck{Comment: ack.Comment, Account: ack.Account, AckedBy: ack.By, AckedOn: ack.On}
		}
		data = append(data, period)
	}
	avail := availability.Compute(periods, acks, days, time.Now())
	return c.JSON(http.StatusOK, map[string]any{
		"data": data,
		"availability": server.ServiceAvailability{
			From:       avail.From.Format(time.DateTime),
			To:         avail.To.Format(time.DateTime),
			Rate:       avail.Rate(),
			AvailableS: int(avail.Available.Seconds()),
			ExcludedS:  int(avail.Excluded.Seconds()),
			CountedS:   int(avail.Counted.Seconds()),
		},
	})
}

// serviceForAck resolves the service of a justification change and checks that
// the caller may make it: user authentication, visibility and responsibility of
// the service, a Manager being responsible for all.
func (a *Api) serviceForAck(c echo.Context, log *slog.Logger, svcId string) (*cdb.DBService, error) {
	if !IsAuthByUser(c) {
		return nil, denyRequest(c, http.StatusUnauthorized, "user authentication required")
	}
	if err := a.resolveService(c, log, svcId); err != nil {
		return nil, err
	}
	ctx := c.Request().Context()
	svc, err := a.ODB.ServiceBySvcIDOrName(ctx, svcId, UserGroupsFromContext(c), IsManager(c))
	if errors.Is(err, cdb.ErrServiceAmbiguous) {
		return nil, denyRequest(c, http.StatusConflict, "%s", err)
	}
	if err != nil {
		log.Error("cannot resolve service", "svc_id", svcId, logkey.Error, err)
		return nil, denyRequest(c, http.StatusInternalServerError, "cannot resolve service %s", svcId)
	}
	if svc == nil {
		return nil, denyRequest(c, http.StatusNotFound, "service %s not found", svcId)
	}
	responsible, err := a.ODB.ServiceResponsible(ctx, svc.SvcID, UserGroupsFromContext(c), IsManager(c))
	if err != nil {
		log.Error("cannot check the responsibility", logkey.Error, err)
		return nil, denyRequest(c, http.StatusInternalServerError, "cannot check the responsibility")
	}
	if !responsible {
		return nil, denyRequest(c, http.StatusForbidden, "you are not responsible for service %s", svc.Svcname)
	}
	return svc, nil
}

func validBounds(begin, end string) bool {
	b, ok1 := availability.ParseCollectorTime(begin)
	e, ok2 := availability.ParseCollectorTime(end)
	return ok1 && ok2 && e.After(b)
}

// PutServiceStatusLogAck handles PUT /services/{svc_id}/status_log/ack: justify a
// period of the availability of a service, as ack() of the historical
// svcmon_log controller.
func (a *Api) PutServiceStatusLogAck(c echo.Context, svcId string) error {
	log := echolog.GetLogHandler(c, "PutServiceStatusLogAck")
	var body server.PutServiceStatusLogAckJSONRequestBody
	if err := c.Bind(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	if !validBounds(body.Begin, body.End) {
		return JSONProblemf(c, http.StatusBadRequest, "begin and end must be \"YYYY-MM-DD HH:MM:SS\" dates, begin first")
	}
	comment := strings.TrimSpace(body.Comment)
	if comment == "" {
		return JSONProblemf(c, http.StatusBadRequest, "a justification needs a comment")
	}
	svc, err := a.serviceForAck(c, log, svcId)
	if err != nil {
		return err
	}
	ctx := c.Request().Context()
	userEmail, _ := c.Get(XUserEmail).(string)
	if err := a.ODB.SetServiceStatusAck(ctx, svc.SvcID, body.Begin, body.End, comment, body.Account, userEmail); err != nil {
		log.Error("cannot store the justification", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot store the justification")
	}
	a.refreshAvailability(c, log, svc.SvcID)
	a.logAck(c, log, svc, "acknowledged unavailability range: %(svcname)s (%(begin)s>%(end)s), accounted: %(account)s",
		map[string]any{"svcname": svc.Svcname, "begin": body.Begin, "end": body.End, "account": body.Account, "comment": comment})
	return c.JSON(http.StatusOK, map[string]string{"info": "period justified"})
}

// DeleteServiceStatusLogAck handles DELETE /services/{svc_id}/status_log/ack.
func (a *Api) DeleteServiceStatusLogAck(c echo.Context, svcId string, params server.DeleteServiceStatusLogAckParams) error {
	log := echolog.GetLogHandler(c, "DeleteServiceStatusLogAck")
	if !validBounds(params.Begin, params.End) {
		return JSONProblemf(c, http.StatusBadRequest, "begin and end must be \"YYYY-MM-DD HH:MM:SS\" dates, begin first")
	}
	svc, err := a.serviceForAck(c, log, svcId)
	if err != nil {
		return err
	}
	found, err := a.ODB.DeleteServiceStatusAck(c.Request().Context(), svc.SvcID, params.Begin, params.End)
	if err != nil {
		log.Error("cannot remove the justification", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot remove the justification")
	}
	if !found {
		return JSONProblemf(c, http.StatusNotFound, "this period is not justified")
	}
	a.refreshAvailability(c, log, svc.SvcID)
	a.logAck(c, log, svc, "removed the justification of %(svcname)s (%(begin)s>%(end)s)",
		map[string]any{"svcname": svc.Svcname, "begin": params.Begin, "end": params.End})
	return c.JSON(http.StatusOK, map[string]string{"info": "justification removed"})
}

// refreshAvailability stores at once the 30-day availability of a service whose
// justifications changed, rather than at the next scheduler run.
func (a *Api) refreshAvailability(c echo.Context, log *slog.Logger, svcID string) {
	if err := availability.Refresh(c.Request().Context(), a.ODB, []string{svcID}, time.Now()); err != nil {
		log.Error("cannot refresh the availability", logkey.Error, err)
	}
}

func (a *Api) logAck(c echo.Context, log *slog.Logger, svc *cdb.DBService, format string, dict map[string]any) {
	ctx := c.Request().Context()
	userEmail, _ := c.Get(XUserEmail).(string)
	entry := cdb.LogEntry{Action: "availability.ack", User: userEmail, Fmt: format, Dict: dict, Level: "info"}
	if parsed, err := uuid.Parse(svc.SvcID); err == nil {
		entry.SvcID = &parsed
	}
	if err := a.ODB.Log(ctx, entry); err != nil {
		log.Error("cannot write audit log", logkey.Error, err)
	}
	if err := a.ODB.Session.NotifyChanges(ctx); err != nil {
		log.Debug("cannot notify changes", logkey.Error, err)
	}
}

// GetServiceInstanceStatusLog handles GET
// /services/{svc_id}/instances/{node_id}/status_log: the availability and
// overall status periods of an instance of a visible service over the last days.
func (a *Api) GetServiceInstanceStatusLog(c echo.Context, svcId string, nodeId string, params server.GetServiceInstanceStatusLogParams) error {
	log := echolog.GetLogHandler(c, "GetServiceInstanceStatusLog")
	days, ok := statusLogDays(params.Days)
	if !ok {
		return JSONProblemf(c, http.StatusBadRequest, "days must be positive")
	}
	if err := a.resolveService(c, log, svcId); err != nil {
		return err
	}
	ctx := c.Request().Context()
	svc, err := a.ODB.ServiceBySvcIDOrName(ctx, svcId, UserGroupsFromContext(c), IsManager(c))
	if errors.Is(err, cdb.ErrServiceAmbiguous) {
		return JSONProblemf(c, http.StatusConflict, "%s", err)
	}
	if err != nil {
		log.Error("cannot resolve service", "svc_id", svcId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve service %s", svcId)
	}
	if svc == nil {
		return JSONProblemf(c, http.StatusNotFound, "service %s not found", svcId)
	}
	node, err := a.ODB.NodeByNodeIDOrNodename(ctx, nodeId)
	if err != nil {
		log.Error("cannot resolve node", logkey.NodeID, nodeId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve node %s", nodeId)
	}
	if node == nil {
		return JSONProblemf(c, http.StatusNotFound, "node %s not found", nodeId)
	}
	periods, err := a.ODB.InstanceStatusPeriods(ctx, svc.SvcID, node.NodeID, days)
	if err != nil {
		log.Error("cannot read the status log", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the status log")
	}
	data := make([]server.InstanceStatusPeriod, 0, len(periods))
	for _, p := range periods {
		data = append(data, server.InstanceStatusPeriod{Avail: p.Avail, Overall: p.Overall, Begin: p.Begin, End: p.End})
	}
	return c.JSON(http.StatusOK, map[string]any{"data": data})
}
