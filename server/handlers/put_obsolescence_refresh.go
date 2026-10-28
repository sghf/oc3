package serverhandlers

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// PutObsolescenceRefresh handles PUT /obsolescence/refresh
func (a *Api) PutObsolescenceRefresh(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PutObsolescenceRefresh")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if err := requireObsManager(c); err != nil {
		return err
	}

	odb := a.ODB

	if err := odb.StatObsolescenceOS(ctx); err != nil {
		log.Error("cannot refresh os obsolescence settings", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot refresh obsolescence settings")
	}
	if err := odb.StatObsolescenceHW(ctx); err != nil {
		log.Error("cannot refresh hw obsolescence settings", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot refresh obsolescence settings")
	}
	if err := odb.PurgeAlertsObsWithout(ctx); err != nil {
		log.Error("cannot purge obsolescence dashboard alerts", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot refresh obsolescence settings")
	}
	if err := odb.UpdateNodesObsolescence(ctx); err != nil {
		log.Error("cannot update node obsolescence dates", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot refresh obsolescence settings")
	}

	a.auditLog(c, ctx, log, cdb.LogEntry{
		Action: "obsolescence.refresh",
		Fmt:    "obsolescence settings refreshed: os names and hardware models gathered, alerts and node dates updated",
	})
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	return c.JSON(http.StatusOK, map[string]string{"info": "Obsolescence settings refreshed"})
}
