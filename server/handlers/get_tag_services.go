package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetTagServices handles GET /tags/{tag_id}/services
func (a *Api) GetTagServices(c echo.Context, tagId int, params server.GetTagServicesParams) error {
	log := echolog.GetLogHandler(c, "GetTagServices")
	log.Info("called", logkey.TagID, tagId)

	odb := a.ODB
	return a.handleList(c, "GetTagServices", "service", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return odb.GetTagServices(ctx, tagId, p)
	})
}
