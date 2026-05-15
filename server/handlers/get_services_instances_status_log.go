package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// GetServicesInstancesStatusLog handles GET /services_instances_status_log
func (a *Api) GetServicesInstancesStatusLog(c echo.Context, params server.GetServicesInstancesStatusLogParams) error {
	odb := a.ODB
	return a.handleList(c, "GetServicesInstancesStatusLog", "instance_status_log", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return odb.GetServicesInstancesStatusLog(ctx, p)
	})
}
