package serverhandlers

import (
	"context"
	"database/sql"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// PostApp handles POST /apps/{app_id}
func (a *Api) PostApp(c echo.Context, appId string) error {
	log := echolog.GetLogHandler(c, "PostApp")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}

	isManager := IsManager(c)
	if !isManager {
		return JSONProblemf(c, http.StatusForbidden, "AppManager privilege required")
	}

	var body server.PostAppJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	log.Info("called", "app_id", appId)

	odb := cdb.New(a.DB)
	odb.CreateSession(a.Ev)

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

	fields := cdb.UpdateAppFields{
		App:        body.App,
		Description: body.Description,
		AppDomain:  body.AppDomain,
		AppTeamOps: body.AppTeamOps,
	}

	markSuccess, endTx, err := odb.BeginTxWithControl(ctx, log, &sql.TxOptions{})
	if err != nil {
		log.Error("cannot start transaction", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot update app")
	}
	defer endTx()

	if err := odb.UpdateApp(ctx, app.ID, fields); err != nil {
		log.Error("cannot update app", "app_id", appId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot update app")
	}

	// If the app code is renamed, update nodes and services references
	if body.App != nil && *body.App != app.App {
		if err := odb.UpdateNodesApp(ctx, app.App, *body.App); err != nil {
			log.Error("cannot update nodes app", logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot update nodes app reference")
		}
		if err := odb.UpdateServicesApp(ctx, app.App, *body.App); err != nil {
			log.Error("cannot update services app", logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot update services app reference")
		}
	}

	userEmail, _ := c.Get(XUserEmail).(string)
	if err := odb.Log(ctx, cdb.LogEntry{
		Action: "apps.change",
		User:   userEmail,
		Fmt:    "app %(app)s changed",
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

	newAppId := appId
	if body.App != nil {
		newAppId = *body.App
	}
	updated, err := odb.GetApp(ctx, newAppId, nil, true)
	if err != nil || updated == nil {
		return JSONProblemf(c, http.StatusInternalServerError, "cannot fetch updated app")
	}
	return c.JSON(http.StatusOK, updated)
}
