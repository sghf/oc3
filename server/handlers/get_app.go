package serverhandlers

import (
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetApp handles GET /apps/{app_id}
func (a *Api) GetApp(c echo.Context, appId string, params server.GetAppParams) error {
	props, err := buildProps(params.Props, propsMapping["app"])
	if err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	log := echolog.GetLogHandler(c, "GetApp")
	odb := a.ODB
	ctx := c.Request().Context()
	groups := UserGroupsFromContext(c)
	isManager := IsManager(c)

	log.Info("called", "app_id", appId, "props", props, "is_manager", isManager)

	app, err := odb.GetApp(ctx, appId, groups, isManager)
	if err != nil {
		log.Error("cannot get app", "app_id", appId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get app")
	}
	if app == nil {
		return JSONProblemf(c, http.StatusNotFound, "app %s not found", appId)
	}

	filteredItem, err := filterItemFields(app, props)
	if err != nil {
		log.Error("cannot project app props", "app_id", appId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot project app props")
	}

	return c.JSON(http.StatusOK, filteredItem)
}
