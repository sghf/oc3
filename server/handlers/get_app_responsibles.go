package serverhandlers

import (
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetAppResponsibles handles GET /apps/{app_id}/responsibles
func (a *Api) GetAppResponsibles(c echo.Context, appId string, params server.GetAppResponsiblesParams) error {
	query, err := buildListQueryParameters(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, propsMapping["auth_group"])
	if err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	log := echolog.GetLogHandler(c, "GetAppResponsibles")
	odb := a.ODB
	ctx := c.Request().Context()
	groups := UserGroupsFromContext(c)
	isManager := IsManager(c)

	log.Info("called", "app_id", appId, "limit", query.Page.Limit, "offset", query.Page.Offset, "props", query.Props, "is_manager", isManager)

	app, err := odb.GetApp(ctx, appId, nil, true)
	if err != nil {
		log.Error("cannot resolve app", "app_id", appId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve app %s", appId)
	}
	if app == nil {
		return JSONProblemf(c, http.StatusNotFound, "app %s not found", appId)
	}

	items, err := odb.GetAppResponsibles(ctx, appId, groups, isManager, query.Page.Limit, query.Page.Offset)
	if err != nil {
		log.Error("cannot get app responsibles", "app_id", appId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get responsibles for app %s", appId)
	}

	filteredItems, err := filterItemsFields(items, query.Props)
	if err != nil {
		log.Error("cannot project group props", "app_id", appId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot project group props")
	}

	return c.JSON(http.StatusOK, newListResponse(filteredItems, propsMapping["auth_group"], query))
}
