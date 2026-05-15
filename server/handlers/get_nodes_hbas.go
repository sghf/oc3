package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// GetNodesHbas handles GET /nodes/hbas
func (a *Api) GetNodesHbas(c echo.Context, params server.GetNodesHbasParams) error {
	odb := a.ODB
	return a.handleList(c, "GetNodesHbas", "hba", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return odb.GetHbas(ctx, p)
	})
}
