package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
)

// GetTagsNodes handles GET /tags/nodes
func (a *Api) GetTagsNodes(c echo.Context, params server.GetTagsNodesParams) error {
	log := echolog.GetLogHandler(c, "GetTagsNodes")
	log.Info("called")

	odb := a.ODB
	return a.handleList(c, "GetTagsNodes", "node_tag", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return odb.GetTagsNodes(ctx, p)
	})
}
