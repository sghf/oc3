package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// GetNode handles GET /nodes/{node_id}
func (a *Api) GetNode(c echo.Context, nodeId string, params server.GetNodeParams) error {
	return a.handleItem(c, "GetNode", "node", "node_id", nodeId, listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.getODB().GetNode(ctx, nodeId, p)
	})
}
