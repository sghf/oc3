package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetTagNodes handles GET /tags/{tag_id}/nodes
func (a *Api) GetTagNodes(c echo.Context, tagIdParam int, params server.GetTagNodesParams) error {
	log := echolog.GetLogHandler(c, "GetTagNodes")
	log.Info("called", logkey.TagID, tagIdParam)

	odb := a.ODB
	return a.handleList(c, "GetTagNodes", "node", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return odb.GetTagNodes(ctx, tagIdParam, p)
	})
}
