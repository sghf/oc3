package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// GetService handles GET /services/{svc_id}
func (a *Api) GetService(c echo.Context, svcId string, params server.GetServiceParams) error {
	return a.handleItem(c, "GetService", "service", "svc_id", svcId, listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetService(ctx, svcId, p)
	})
}
