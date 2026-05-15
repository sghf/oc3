package serverhandlers

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetAppAmIResponsible handles GET /apps/{app_id}/am_i_responsible
func (a *Api) GetAppAmIResponsible(c echo.Context, appId string) error {
	log := echolog.GetLogHandler(c, "GetAppAmIResponsible")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	groups := UserGroupsFromContext(c)
	isManager := IsManager(c)
	nodeID, _ := c.Get(XNodeID).(string)

	log.Info("called", "app_id", appId, "is_manager", isManager)

	app, err := odb.GetApp(ctx, appId, nil, true)
	if err != nil {
		log.Error("cannot resolve app", "app_id", appId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve app %s", appId)
	}
	if app == nil {
		return JSONProblemf(c, http.StatusNotFound, "app %s not found", appId)
	}

	responsible, err := odb.AppResponsible(ctx, appId, groups, isManager, nodeID)
	if err != nil {
		log.Error("cannot check app responsibility", "app_id", appId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check responsibility for app %s", appId)
	}
	if !responsible {
		return JSONProblemf(c, http.StatusForbidden, "you are not responsible for app %s", appId)
	}

	return c.JSON(http.StatusOK, true)
}
