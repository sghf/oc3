package serverhandlers

import (
	"context"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
	"github.com/opensvc/oc3/xauth"
)

// PostApps handles POST /apps
func (a *Api) PostApps(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostApps")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}

	if !IsManager(c) {
		return JSONProblemf(c, http.StatusForbidden, "AppManager privilege required")
	}

	var body server.PostAppsJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	if body.App == "" {
		return JSONProblemf(c, http.StatusBadRequest, "missing required field: app")
	}

	log.Info("called", "app", body.App)

	exists, err := odb.AppExists(ctx, body.App)
	if err != nil {
		log.Error("cannot check app existence", "app", body.App, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check app existence")
	}
	if exists {
		return JSONProblemf(c, http.StatusConflict, "app %s already exists", body.App)
	}

	user := UserInfoFromContext(c)
	if user == nil {
		return JSONProblemf(c, http.StatusUnauthorized, "missing user context")
	}
	userIDStr := user.GetExtensions().Get(xauth.XUserID)
	userID, err := strconv.ParseInt(userIDStr, 10, 64)
	if err != nil {
		return JSONProblemf(c, http.StatusBadRequest, "invalid user id")
	}

	exceeded, err := odb.AppQuotaExceeded(ctx, userID)
	if err != nil {
		log.Error("cannot check app quota", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check app quota")
	}
	if exceeded {
		return JSONProblemf(c, http.StatusForbidden, "app quota exceeded")
	}

	groupID, ok, err := odb.UserDefaultGroupID(ctx, userID)
	if err != nil {
		log.Error("cannot find default group", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot find default group")
	}
	if !ok {
		return JSONProblemf(c, http.StatusInternalServerError, "user has no default group")
	}

	var description, appDomain, appTeamOps string
	if body.Description != nil {
		description = *body.Description
	}
	if body.AppDomain != nil {
		appDomain = *body.AppDomain
	}
	if body.AppTeamOps != nil {
		appTeamOps = *body.AppTeamOps
	}

	app, err := odb.InsertApp(ctx, body.App, description, appDomain, appTeamOps)
	if err != nil {
		log.Error("cannot insert app", "app", body.App, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot create app")
	}

	if err := odb.InsertAppResponsible(ctx, app.ID, groupID); err != nil {
		log.Error("cannot insert app responsible", "app", body.App, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot set app responsible")
	}

	if err := odb.InsertAppPublication(ctx, app.ID, groupID); err != nil {
		log.Error("cannot insert app publication", "app", body.App, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot set app publication")
	}

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	userEmail, _ := c.Get(XUserEmail).(string)
	logErr := odb.Log(ctx, cdb.LogEntry{
		Action: "apps.create",
		User:   userEmail,
		Fmt:    "app %(app)s created. data %(data)s",
		Dict: map[string]any{
			"app":  app.App,
			"data": body,
		},
		Level: "info",
	})
	if logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}

	return c.JSON(http.StatusOK, app)
}
