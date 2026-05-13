package serverhandlers

import (
	"context"
	"database/sql"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// DeleteApps handles DELETE /apps/{app_id}
func (a *Api) DeleteApps(c echo.Context, appId string) error {
	log := echolog.GetLogHandler(c, "DeleteApps")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}

	isManager := IsManager(c)
	if !isManager {
		return JSONProblemf(c, http.StatusForbidden, "AppManager privilege required")
	}

	odb := cdb.New(a.DB)
	odb.CreateSession(a.Ev)

	log.Info("called", "app_id", appId)

	app, err := odb.GetApp(ctx, appId, nil, true)
	if err != nil {
		log.Error("cannot get app", "app_id", appId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get app")
	}
	if app == nil {
		return JSONProblemf(c, http.StatusNotFound, "app %s not found", appId)
	}

	responsible, err := odb.AppResponsible(ctx, appId, UserGroupsFromContext(c), isManager, "")
	if err != nil {
		log.Error("cannot check app responsibility", "app_id", appId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check app responsibility")
	}
	if !responsible {
		return JSONProblemf(c, http.StatusForbidden, "you are not responsible for this app")
	}

	nodesCount, servicesCount, err := odb.AppUsageCounts(ctx, app.App)
	if err != nil {
		log.Error("cannot count app usage", "app_id", appId, "app", app.App, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot count app usage")
	}
	if nodesCount+servicesCount > 0 {
		return JSONProblemf(c, http.StatusConflict, "this app code cannot be deleted. used by %d nodes and %d services", nodesCount, servicesCount)
	}

	markSuccess, endTx, err := odb.BeginTxWithControl(ctx, log, &sql.TxOptions{})
	if err != nil {
		log.Error("cannot start transaction", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot delete app")
	}
	defer endTx()

	if err := odb.DeleteApp(ctx, app.ID); err != nil {
		log.Error("cannot delete app", "app_id", appId, "app", app.App, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot delete app")
	}
	if err := odb.DeleteAppResponsibles(ctx, app.ID); err != nil {
		log.Error("cannot delete app responsibles", "app_id", appId, "app", app.App, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot delete app responsibles")
	}
	if err := odb.DeleteAppPublications(ctx, app.ID); err != nil {
		log.Error("cannot delete app publications", "app_id", appId, "app", app.App, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot delete app publications")
	}

	userEmail, _ := c.Get(XUserEmail).(string)
	if err := odb.Log(ctx, cdb.LogEntry{
		Action: "apps.delete",
		User:   userEmail,
		Fmt:    "app %(app)s deleted",
		Dict: map[string]any{
			"app": app.App,
		},
		Level: "info",
	}); err != nil {
		log.Error("cannot write audit log", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot write audit log")
	}

	markSuccess()

	if err := odb.Session.NotifyTableChangeWithData(ctx, "apps", map[string]any{"id": app.ID}); err != nil {
		log.Error("cannot notify apps change", logkey.Error, err)
	}
	if err := odb.Session.NotifyTableChangeWithData(ctx, "apps_responsibles", nil); err != nil {
		log.Error("cannot notify apps_responsibles change", logkey.Error, err)
	}
	if err := odb.Session.NotifyTableChangeWithData(ctx, "apps_publications", nil); err != nil {
		log.Error("cannot notify apps_publications change", logkey.Error, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		"info": "app " + app.App + " deleted",
	})
}
