package serverhandlers

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetAppNodes handles GET /apps/{app_id}/nodes
func (a *Api) GetAppNodes(c echo.Context, appId string, params server.GetAppNodesParams) error {
	log := echolog.GetLogHandler(c, "GetAppNodes")
	odb := a.ODB
	ctx := c.Request().Context()

	app, err := odb.GetApp(ctx, appId, nil, true)
	if err != nil {
		log.Error("cannot resolve app", "app_id", appId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve app %s", appId)
	}
	if app == nil {
		return JSONProblemf(c, http.StatusNotFound, "app %s not found", appId)
	}

	log.Info("called", "app_id", appId)

	return a.handleList(c, "GetAppNodes", "node", listEndpointParams{
		props:   params.Props,
		limit:   params.Limit,
		offset:  params.Offset,
		meta:    params.Meta,
		stats:   params.Stats,
		orderby: params.Orderby,
		groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return odb.GetAppNodes(ctx, appId, p)
	})
}
