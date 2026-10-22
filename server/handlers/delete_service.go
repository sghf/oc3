package serverhandlers

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// DeleteService handles DELETE /services/{svc_id}
func (a *Api) DeleteService(c echo.Context, svcId string) error {
	log := echolog.GetLogHandler(c, "DeleteService")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}

	log.Info("called", "svc_id", svcId)

	return a.deleteServiceByKey(c, log, ctx, svcId)
}

// deleteServiceByKey deletes the service designated by its svc_id or name,
// cascading on the service instances and the dashboard entries, and answers.
func (a *Api) deleteServiceByKey(c echo.Context, log *slog.Logger, ctx context.Context, svcId string) error {
	o := a.deleteServiceOne(c, log, ctx, svcId)
	if o.ok() {
		if err := a.ODB.Session.NotifyChanges(ctx); err != nil {
			log.Error("cannot notify changes", logkey.Error, err)
		}
	}
	return writeOutcome(c, o)
}

// deleteServiceOne deletes a service and its instances, in a transaction of its
// own, after checking the caller is responsible for it.
func (a *Api) deleteServiceOne(c echo.Context, log *slog.Logger, ctx context.Context, svcId string) batchOutcome {
	target, o := a.checkServiceDeletion(c, log, ctx, svcId)
	if target == nil {
		return o
	}
	return a.deleteServicesChecked(c, log, ctx, []serviceToDelete{*target})[0]
}

// serviceToDelete is a service the caller may delete, and how the audit names it.
type serviceToDelete struct {
	svcID string
	label string
}

// checkServiceDeletion checks the caller may delete a service: it exists, and
// the caller is responsible for it. It returns the service, nil with the
// outcome when not.
func (a *Api) checkServiceDeletion(c echo.Context, log *slog.Logger, ctx context.Context, svcId string) (*serviceToDelete, batchOutcome) {
	odb := a.ODB
	if svcId == "" {
		return nil, batchFail(http.StatusBadRequest, "invalid svc_id: ''")
	}
	svc, err := odb.ServiceBySvcIDOrName(ctx, svcId, UserGroupsFromContext(c), IsManager(c))
	if errors.Is(err, cdb.ErrServiceAmbiguous) {
		return nil, batchFail(http.StatusConflict, "%s", err)
	}
	if err != nil {
		log.Error("cannot resolve service", "svc_id", svcId, logkey.Error, err)
		return nil, batchFail(http.StatusInternalServerError, "cannot resolve service %s", svcId)
	}
	if svc == nil {
		return nil, batchFail(http.StatusNotFound, "service %s not found", svcId)
	}
	responsible, err := odb.ServiceResponsible(ctx, svc.SvcID, UserGroupsFromContext(c), IsManager(c))
	if err != nil {
		log.Error("cannot check service responsibility", "svc_id", svc.SvcID, logkey.Error, err)
		return nil, batchFail(http.StatusInternalServerError, "cannot check service responsibility")
	}
	if !responsible {
		return nil, batchFail(http.StatusForbidden, "you are not responsible for service %s", svc.Svcname)
	}
	label := svc.Svcname + " in no app"
	if svc.SvcApp != "" {
		label = svc.Svcname + " in app " + svc.SvcApp
	}
	return &serviceToDelete{svcID: svc.SvcID, label: label}, batchOutcome{}
}

// deleteServicesChecked deletes services already checked, in one transaction:
// an audit entry each, then their rows in every table, a statement per table for
// all of them (DeleteServicesCascade). It returns their outcomes, in order; a
// failure fails them all, the transaction rolled back.
func (a *Api) deleteServicesChecked(c echo.Context, log *slog.Logger, ctx context.Context, services []serviceToDelete) []batchOutcome {
	outcomes := make([]batchOutcome, len(services))
	failAll := func(o batchOutcome) []batchOutcome {
		for i := range outcomes {
			outcomes[i] = o
		}
		return outcomes
	}
	tx, markSuccess, endTx, err := a.ODB.BeginTxWithControl(ctx, log, &sql.TxOptions{})
	if err != nil {
		log.Error("cannot start transaction", logkey.Error, err)
		return failAll(batchFail(http.StatusInternalServerError, "cannot delete service"))
	}
	defer endTx()
	userEmail, _ := c.Get(XUserEmail).(string)
	ids := make([]string, len(services))
	for i, svc := range services {
		ids[i] = svc.svcID
		if logErr := tx.Log(ctx, cdb.LogEntry{
			Action: "service.delete",
			User:   userEmail,
			Fmt:    "delete service %(data)s",
			Dict:   map[string]any{"data": svc.label},
			Level:  "info",
		}); logErr != nil {
			log.Error("cannot write audit log", logkey.Error, logErr)
			return failAll(batchFail(http.StatusInternalServerError, "cannot write audit log"))
		}
	}
	if err := tx.DeleteServicesCascade(ctx, ids); err != nil {
		log.Error("cannot delete services", "count", len(ids), logkey.Error, err)
		return failAll(batchFail(http.StatusInternalServerError, "cannot delete service"))
	}
	markSuccess()
	for i, svc := range services {
		outcomes[i] = batchOK("service %s deleted", svc.label)
	}
	return outcomes
}
