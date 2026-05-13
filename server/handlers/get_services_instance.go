package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// GetServicesInstance handles GET /services_instances/{svc_id}
func (a *Api) GetServicesInstance(c echo.Context, svcId string, params server.GetServicesInstanceParams) error {
	return a.handleItem(c, "GetServicesInstance", "instance", "svc_id", svcId, listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.getODB().GetServicesInstance(ctx, svcId, p)
	})
}
