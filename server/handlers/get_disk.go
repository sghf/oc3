package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// GetDisk handles GET /disks/{disk_id}
func (a *Api) GetDisk(c echo.Context, diskId string, params server.GetDiskParams) error {
	return a.handleItem(c, "GetDisk", "disk", "disk_id", diskId, listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.getODB().GetDisk(ctx, diskId, p)
	})
}
