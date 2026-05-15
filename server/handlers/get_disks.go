package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// GetDisks handles GET /disks
func (a *Api) GetDisks(c echo.Context, params server.GetDisksParams) error {
	odb := a.ODB
	return a.handleList(c, "GetDisks", "disk", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return odb.GetDisks(ctx, p)
	})
}
