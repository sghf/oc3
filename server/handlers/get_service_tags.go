package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
)

// GetServiceTags handles GET /services/{svc_id}/tags
func (a *Api) GetServiceTags(c echo.Context, svcId string, params server.GetServiceTagsParams) error {
	log := echolog.GetLogHandler(c, "GetServiceTags")
	log.Info("called", "svc_id", svcId)
	if err := a.resolveService(c, log, svcId); err != nil {
		return err
	}
	return a.handleList(c, "GetServiceTags", "tag", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.getODB().GetServiceTags(ctx, svcId, p)
	})
}
