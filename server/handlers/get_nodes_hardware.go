package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// GetNodesHardware handles GET /nodes/hardware
func (a *Api) GetNodesHardware(c echo.Context, params server.GetNodesHardwareParams) error {
	odb := a.getODB()
	return a.handleList(c, "GetNodesHardware", "node_hw", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return odb.GetNodesHardware(ctx, p)
	})
}
